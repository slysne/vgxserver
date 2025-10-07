/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_vxtable.c
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

#include "_vgx.h"
#include "_vxarcvector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static const int8_t vertex_index_order = 9;
// TODO: ARE WE CURRENTLY DOUBLE-LOCKING?  INDEX ACCESS METHODS ARE _CS, AND THE FRAMEHASH IS ALSO SYNCED INTERNALLY. FIX.
static const vgx_mapping_spec_t vertex_index_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_128BIT | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;

static int64_t __cxmalloc_rebuild_add_vertex_CS( cxmalloc_object_processing_context_t *rebuild, vgx_Vertex_t *vertex );
static int64_t __rebuild_index_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype );
static framehash_t * __create_new_index_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype );
static int __add_vertex_to_index_CS_WL( framehash_t *index, vgx_Vertex_t *vertex_WL );
static int __remove_vertex_from_index_CS_WL( vgx_Graph_t *self, framehash_t *index, vgx_Vertex_t *vertex_WL, vgx_vertex_type_t vertex_type );
static framehash_t * __select_index( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype );
static framehash_t * __select_index_by_name_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type, CString_t **CSTR__error );
static int64_t __cxmalloc_collect_vertex_ROG_or_CSNOWL( cxmalloc_object_processing_context_t *scan_context, vgx_Vertex_t *vertex );
static int64_t __cxmalloc_collect_outarcs_ROG_or_CSNOWL( cxmalloc_object_processing_context_t *scan_context, vgx_Vertex_t *vertex );
static int64_t __FH_collect_vertex_ROG_or_CSNOWL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __FH_collect_outarcs_ROG_or_CSNOWL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __cxmalloc_lock_vertex_CS_NT( cxmalloc_object_processing_context_t *incref_context, vgx_Vertex_t *vertex );
static int64_t __cxmalloc_erase_vertex_CS_WL_NT( cxmalloc_object_processing_context_t *erase_context, vgx_Vertex_t *vertex_WL );
static int64_t __cxmalloc_unlock_vertex_CS_NT( cxmalloc_object_processing_context_t *decref_context, vgx_Vertex_t *vertex );
static int64_t __cxmalloc_count_vertex_properties_ROG( cxmalloc_object_processing_context_t *counter, vgx_Vertex_t *vertex );
static int64_t __cxmalloc_count_vertex_tmx_ROG( cxmalloc_object_processing_context_t *counter, vgx_Vertex_t *vertex );
static int64_t __initialize_vertices_CS_NT( vgx_Graph_t *self, vgx_vertex_type_t vxtype, bool *virtual_remain );
static int64_t __FH_unindex_main_CS_NT( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __discard_index_CS_NT( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype );


typedef struct __s_processor_control_t {
  int64_t t0;
  int64_t tt;
  int64_t n;
  void *filter;
  vgx_ExecutionTimingBudget_t *tb;
  vgx_ExecutionTimingBudget_t *zb;
  vgx_AccessReason_t *reason;
} __processor_control_t;



#ifdef VGX_CONSISTENCY_CHECK

/**************************************************************************//**
 * __assert_active_vertex_indexed
 *
 ******************************************************************************
 */
static void __assert_active_vertex_indexed( vgx_Vertex_t *vertex, const objectid_t *obid ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex );
  int act = linehead->data.flags._act;
  int64_t refc = linehead->data.refc;
  QWORD desc = vertex->descriptor.bits;
  objectid_t *internalid = __vertex_internalid( vertex );
  if( act == 0 || refc == 0 || desc == 0xccccccccccccccccULL || !idmatch( obid, internalid ) ) {
    FATAL( 0x001, "DEFUNCT VERTEX INDEXED: act=%d refc=%lld desc=0x%016X obid=%016X%016X internalid=%016X%016X", act, refc, desc, obid->H, obid->L, internalid->H, internalid->L );
  }
}
#define ASSERT_VERTEX( VertexPtr, ObidPtr ) __assert_active_vertex_indexed( VertexPtr, ObidPtr )
#else
#define ASSERT_VERTEX( VeretxPtr, ObidPtr ) ((void)0)
#endif




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_rebuild_add_vertex_CS( cxmalloc_object_processing_context_t *rebuild, vgx_Vertex_t *vertex ) {
  if( vertex && __vertex_is_manifestation_null( vertex ) == false ) {
    vgx_vertex_type_t *vxtype = (vgx_vertex_type_t*)rebuild->filter;
    if( vxtype == NULL || vertex->descriptor.type.enumeration == *vxtype ) {
      framehash_t *index = (framehash_t*)rebuild->input;
      objectid_t *pobid = COMLIB_OBJECT_GETID( vertex );
      if( CALLABLE( index )->SetObj128Nolock( index, pobid, COMLIB_OBJECT( vertex ) ) == CELL_VALUE_TYPE_OBJECT128 ) {
        if( vxtype == NULL ) {
          __vertex_set_indexed_main( vertex );
        }
        else {
          __vertex_set_indexed_type( vertex );
        }
        Vertex_INCREF_WL( vertex );
        return 1;
      }
      else {
        return -1;
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __rebuild_index_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype ) {
  int64_t n_rebuild = 0;
  // Trigger re-creation of index from vertex allocator if empty (allocator will be empty for a brand new graph so nothing will happen)
  framehash_t *index;
  if( vxtype == VERTEX_TYPE_ENUMERATION_WILDCARD ) {
    index = self->vxtable;
  }
  else {
    index = self->vxtypeidx[ vxtype ];
  }
  if( CALLABLE( index )->Items( index ) == 0 ) {
    cxmalloc_object_processing_context_t index_rebuild_context = {0};
    index_rebuild_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
    index_rebuild_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_rebuild_add_vertex_CS;
    if( vxtype != VERTEX_TYPE_ENUMERATION_WILDCARD ) {
      index_rebuild_context.filter = &vxtype;
    }
    else {
      index_rebuild_context.filter = NULL;
    }
    index_rebuild_context.input = index;
    CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &index_rebuild_context );
    int64_t n_indexed = CALLABLE( index )->Items( index );
    int64_t n_active = index_rebuild_context.n_objects_active;
    if( n_active == n_indexed ) {
      if( n_indexed > 0 ) {
#ifdef VXTABLE_PERSIST
        // Persist the restored index to disk
        if( CALLABLE( index )->BulkSerialize( index, true ) < 0 ) {
          n_rebuild = -1;
        }
        else {
          n_rebuild = n_indexed;
        }
#else
        n_rebuild = n_indexed;
#endif
      }
    }
    else {
      WARN( 0xD01, "Index rebuild object count mismatch: found %lld active, indexed %lld", n_active, n_indexed );
    }
  }
  return n_rebuild;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_t * __create_new_index_CS( vgx_Graph_t *self, vgx_vertex_type_t vxtype ) {
  framehash_t *index = NULL;
  // SCARY
  CString_t *CSTR__table_prefix = NULL;
  CString_t *CSTR__table_name = NULL;
  CString_t *CSTR__table_fullpath = NULL;

  XTRY {
    // Set the prefix
    if( vxtype == VERTEX_TYPE_ENUMERATION_WILDCARD ) {
      CSTR__table_prefix = CStringNewFormat( "vxtable" );
    }
    else {
      CSTR__table_prefix = CStringNewFormat( "vxtable_type_%02X", vxtype );
    }
    if( CSTR__table_prefix == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD11 );
    }

    // Set the table name
    if( (CSTR__table_name = iString.Utility.NewGraphMapName( self, CSTR__table_prefix ) ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD12 );
    }

#ifdef VXTABLE_PERSIST
    // Set the table full path
    if( (CSTR__table_fullpath = iString.NewFormat( NULL, "%s/graph/index", CALLABLE(self)->FullPath(self) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD13 );
    }
#endif

    // Create or restore the index
#ifndef FRAMEHASH_NO_CHANGELOG
    if( (index = iMapping.NewMap( CSTR__table_fullpath, CSTR__table_name, MAPPING_SIZE_UNLIMITED, vertex_index_order, vertex_index_spec, CLASS_vgx_Vertex_t )) == NULL ) {
#else
    if( (index = iMapping.NewMap( CSTR__table_fullpath, CSTR__table_name, MAPPING_SIZE_UNLIMITED, vertex_index_order, vertex_index_spec, CLASS_NONE )) == NULL ) {
#endif
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD14 );
    }

  }
  XCATCH( errcode ) {
    iMapping.DeleteMap( &index );
  }
  XFINALLY {
    iString.Discard( &CSTR__table_prefix );
    iString.Discard( &CSTR__table_name );
    iString.Discard( &CSTR__table_fullpath );
  }

  return index;
}



/*******************************************************************//**
 * Add vertex to the index. Index will become owner or one additional
 * reference after successful indexing. If vertex already exists in index
 * no action is performed.
 *
 * Returns: 1 : Vertex added to index, one additional reference owned
 *          0 : No action
 *         -1 : Error
 ***********************************************************************
 */
static int __add_vertex_to_index_CS_WL( framehash_t *index, vgx_Vertex_t *vertex_WL ) {
  objectid_t *pobid = COMLIB_OBJECT_GETID( vertex_WL );
  framehash_vtable_t *iFH = CALLABLE( index );
  // Perform indexing if not already indexed
  if( iFH->HasObj128Nolock( index, pobid ) != CELL_VALUE_TYPE_OBJECT128 ) {
    // Add vertex to index
    if( iFH->SetObj128Nolock( index, pobid, COMLIB_OBJECT( vertex_WL ) ) == CELL_VALUE_TYPE_OBJECT128 ) {
      // Index owns a new reference to vertex
      Vertex_INCREF_WL( vertex_WL );

      // Capture
      iOperation.Graph_CS.SetModified( vertex_WL->graph );

      return 1;
    }
    // Indexing error
    else {
      return -1;
    }
  }
  // Already indexed, no action
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 * Return: 1 : Unindexing completed
 *         0 : No action (vertex was not indexed)
 *        -1 : Error
 ***********************************************************************
 */
static int __remove_vertex_from_index_CS_WL( vgx_Graph_t *self, framehash_t *index, vgx_Vertex_t *vertex_WL, vgx_vertex_type_t vertex_type ) {
  if( index ) {
    // Remove vertex from index: this will discard one vertex reference when successful (--refcount)
    objectid_t obid = CALLABLE( vertex_WL )->InternalID( vertex_WL );
    framehash_valuetype_t vtype = CALLABLE( index )->DelObj128Nolock( index, &obid );
    if( vtype == CELL_VALUE_TYPE_OBJECT128 ) {
      // Type index empty - remove type
      if( vertex_type != VERTEX_TYPE_ENUMERATION_NONE && CALLABLE( index )->Items( index ) == 0 ) {
        // No vertices of this type exist in the system anymore. We remove the type mapping
        // from the type enumerator.
        iEnumerator_CS.VertexType.Remove( self, vertex_type );
      }
      
      // Capture
      iOperation.Graph_CS.SetModified( self );

      return 1;  // ok, removed from index
    }
    // Vertex was not indexed, no action
    else if( vtype != CELL_VALUE_TYPE_ERROR ) {
      return 0; // ok, no action
    }
    // Error removing vertex from generic index
    else {
      return -1; // error
    }
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
__inline static framehash_t * __select_index( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype ) {
  // Select specific type index
  switch( vxtype ) {
  case VERTEX_TYPE_ENUMERATION_WILDCARD:
    return self->vxtable;

  case VERTEX_TYPE_ENUMERATION_NO_MAPPING:
    return NULL;

  default:
    return self->vxtypeidx[ vxtype ];
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_t * __select_index_by_name_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_type, CString_t **CSTR__error ) {
  framehash_t *index = NULL;
  XTRY {
    vgx_vertex_type_t vxtype;
    // NULL means the untyped vertex
    if( CSTR__vertex_type == NULL ) {
      vxtype = VERTEX_TYPE_ENUMERATION_VERTEX;
    }
    else if( CALLABLE( CSTR__vertex_type )->EqualsChars( CSTR__vertex_type, "*" ) ) {
      vxtype = VERTEX_TYPE_ENUMERATION_WILDCARD;
    }
    // Encode type string
    else {
      vxtype = (vgx_vertex_type_t)iEnumerator_CS.VertexType.GetEnum( self, CSTR__vertex_type );
    }

    if( vxtype == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
      __format_error_string( CSTR__error, "no index for vertex type '%s'", CStringValue( CSTR__vertex_type ) );
      THROW_SILENT( CXLIB_ERR_LOOKUP, 0x001 );
    }
    else if( !__vertex_type_enumeration_valid( vxtype ) ) {
      __format_error_string( CSTR__error, "invalid vertex type '%s'", CStringValue( CSTR__vertex_type ) );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    if( (index = __select_index( self, vxtype )) == NULL ) {
      __set_error_string( CSTR__error, "internal error" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

  }
  XCATCH( errcode ) {
    index = NULL;
  }
  XFINALLY {
  }
  return index;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void COLLECT_VERTEX( vgx_VertexCollector_context_t *collector, vgx_LockableArc_t *larc ) {
  collector->n_collectable++;
  collector->n_vertices++;
  collector->collect_vertex( collector, larc );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __execution_timeout( __processor_control_t *control ) {
  // Execution timeout!
  if( _vgx_is_execution_limited( control->tb ) 
      && 
      ((++control->n) & 0xFFF) == 0
      &&
      __GET_CURRENT_MILLISECOND_TICK() > control->tt )
  {
    _vgx_set_execution_halted( control->tb, VGX_ACCESS_REASON_EXECUTION_TIMEOUT );
    *control->reason = control->tb->reason;
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
static int64_t __cxmalloc_collect_vertex_ROG_or_CSNOWL( cxmalloc_object_processing_context_t *scan_context, vgx_Vertex_t *vertex ) {
  //
  //
  // IMPORTANT FOR THIS FUNCTION TO NEVER LEAVE CS!
  //
  //

  if( vertex && __vertex_is_manifestation_null( vertex ) == false ) {
    __processor_control_t *control = scan_context->filter;
    
    // Execution timeout!
    if( __execution_timeout( control ) ) {
      scan_context->completed = true;
      scan_context->error = true;
      return -1;
    }

    vgx_VertexFilter_context_t *filter = control->filter;
    vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
    if( filter->filter( filter, vertex, &match ) ) {
      vgx_VertexCollector_context_t *collector = scan_context->output;
      if( collector->n_remain-- > 0 ) {
        // Obtain a lock and pass into collector if collector requires lock
        if( collector->locked_head_access ) {
          // Non-blocking readonly lock attempt (No writelocks by other threads should be possible) - will NOT LEAVE CS
          vgx_Vertex_t *vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( collector->graph, vertex, control->zb, VGX_VERTEX_RECORD_NONE );
          __set_access_reason( control->reason, control->zb->reason );
          if( vertex_RO ) {
            vgx_LockableArc_t LARC = VGX_LOCKABLE_ARC_INIT( vertex_RO, 1, VGX_PREDICATOR_NONE, vertex_RO, 1 );
            COLLECT_VERTEX( collector, &LARC );
            // Release vertex only if still locked (collector may have stolen the lock)
            if( LARC.acquired.head_lock > 0 ) {
              // SHOULD NOT LEAVE CS
              // TODO: Verify never give up CS
              _vxgraph_state__unlock_vertex_CS_LCK( collector->graph, &vertex_RO, VGX_VERTEX_RECORD_NONE );
            }
            return 1;
          }
          // Can't acquire readonly vertex (too many readers?)
          else {
            scan_context->completed = true;
            scan_context->error = true;
            return -1;
          }
        }
        // Collector does not require lock, either graph is readonly or vertex won't be dereferenced when rendered
        else {
          vgx_LockableArc_t LARC = VGX_LOCKABLE_ARC_INIT( vertex, 0, VGX_PREDICATOR_NONE, vertex, 0 );
          COLLECT_VERTEX( collector, &LARC );
        }
      }
      else {
        scan_context->completed = true;
      }
    }
    else if( __is_arcfilter_error( match ) ) {
      scan_context->completed = true;
      scan_context->error = true;
      if( filter->timing_budget->reason != VGX_ACCESS_REASON_NONE ) {
        *control->reason = filter->timing_budget->reason;
      }
      else {
        *control->reason = VGX_ACCESS_REASON_ERROR;
      }
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_collect_outarcs_ROG_or_CSNOWL( cxmalloc_object_processing_context_t *scan_context, vgx_Vertex_t *vertex ) {
  //
  //
  // IMPORTANT FOR THIS FUNCTION TO NEVER LEAVE CS!
  //
  //

  int64_t hit = 0;
  if( vertex && __vertex_is_manifestation_null( vertex ) == false ) {
    __processor_control_t *control = scan_context->filter;
    
    // Execution timeout!
    if( __execution_timeout( control ) ) {
      scan_context->completed = true;
      scan_context->error = true;
      return -1;
    }

    vgx_VertexFilter_context_t *filter = control->filter;
    vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)scan_context->output;
    vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
    // Obtain a lock and pass into collector if collector requires lock
    if( collector->locked_tail_access ) {
      // Non-blocking readonly lock attempt (No writelocks by other threads should be possible) - will NOT LEAVE CS
      vgx_Vertex_t *tail_RO = _vxgraph_state__lock_vertex_readonly_CS( collector->graph, vertex, control->zb, VGX_VERTEX_RECORD_NONE );
      __set_access_reason( control->reason, filter->timing_budget->reason );
      if( tail_RO ) {
        if( filter->filter( filter, tail_RO, &match ) ) { // <= this will also collect
          ++hit;
          if( collector->n_remain <= 0 ) {
            scan_context->completed = true;
          }
        }
        // SHOULD NOT LEAVE CS
        // TODO: Verify never give up CS
        _vxgraph_state__unlock_vertex_CS_LCK( collector->graph, &tail_RO, VGX_VERTEX_RECORD_NONE );
      }
      else {
        scan_context->completed = true;
        scan_context->error = true;
        hit = -1;
      }
    }
    // Collector does not require lock, either graph is readonly or vertex won't be dereferenced when rendered
    else {
      if( filter->filter( filter, vertex, &match ) ) { // <= this will also collect
        ++hit;
        if( collector->n_remain <= 0 ) {
          scan_context->completed = true;
        }
      }
    }
    if( __is_arcfilter_error( match ) ) {
      scan_context->completed = true;
      scan_context->error = true;
      if( filter->timing_budget->reason != VGX_ACCESS_REASON_NONE ) {
        *control->reason = filter->timing_budget->reason;
      }
      else {
        *control->reason = VGX_ACCESS_REASON_ERROR;
      }
      hit = -1;
    }
  }
  return hit;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __FH_collect_vertex_ROG_or_CSNOWL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  //
  //
  // IMPORTANT FOR THIS FUNCTION TO NEVER LEAVE CS!
  //
  //

  int64_t hit = 0;

  __processor_control_t *control = processor->processor.input;
  vgx_VertexFilter_context_t *filter = control->filter;
    
  // Execution timeout!
  if( __execution_timeout( control ) ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return -1;
  }

  vgx_Vertex_t *vertex = (vgx_Vertex_t*)APTR_GET_POINTER( fh_cell );
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
  if( filter->filter( filter, vertex, &match ) ) {
    ++hit;
    vgx_VertexCollector_context_t *collector = (vgx_VertexCollector_context_t*)processor->processor.output;
    if( collector->n_remain-- > 0 ) {
      // Obtain a lock and pass into collector if collector requires lock
      if( collector->locked_head_access ) {
        // Non-blocking readonly lock attempt (No writelocks by other threads should be possible) - will NOT LEAVE CS
        vgx_Vertex_t *vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( collector->graph, vertex, control->zb, VGX_VERTEX_RECORD_NONE );
        __set_access_reason( control->reason, filter->timing_budget->reason );
        if( vertex_RO ) {
          vgx_LockableArc_t LARC = VGX_LOCKABLE_ARC_INIT( vertex_RO, 1, VGX_PREDICATOR_NONE, vertex_RO, 1 );
          COLLECT_VERTEX( collector, &LARC );
          // Release vertex only if still locked (collector may have stolen the lock)
          if( LARC.acquired.head_lock > 0 ) {
            // SHOULD NOT LEAVE CS
            // TODO: Verify never give up CS
            _vxgraph_state__unlock_vertex_CS_LCK( collector->graph, &vertex_RO, VGX_VERTEX_RECORD_NONE );
          }
        }
        // Can't acquire readonly vertex (too many readers?)
        else {
          // TODO: Find a way to propagate error reason
          FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
          hit = -1;
        }
      }
      // Collector does not require lock, either graph is readonly or vertex won't be dereferenced when rendered
      else {
        vgx_LockableArc_t LARC = VGX_LOCKABLE_ARC_INIT( vertex, 0, VGX_PREDICATOR_NONE, vertex, 0 );
        COLLECT_VERTEX( collector, &LARC );
      }
    }
    else {
      FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    }
  }
  else if( __is_arcfilter_error( match ) ) {
    // TODO: Find a way to propagate error reason
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    hit = -1;
  }
  return hit;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __FH_collect_outarcs_ROG_or_CSNOWL( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  //
  //
  // IMPORTANT FOR THIS FUNCTION TO NEVER LEAVE CS!
  //
  //

  int64_t hit = 0;

  __processor_control_t *control = processor->processor.input;
  vgx_VertexFilter_context_t *filter = control->filter;
    
  // Execution timeout!
  if( __execution_timeout( control ) ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return -1;
  }

  vgx_Vertex_t *tail = (vgx_Vertex_t*)APTR_GET_POINTER( fh_cell );
  vgx_ArcCollector_context_t *collector = (vgx_ArcCollector_context_t*)processor->processor.output;
  vgx_ArcFilter_match match = VGX_ARC_FILTER_MATCH_MISS;
  // Obtain a lock and pass into collector if collector requires lock
  if( collector->locked_tail_access ) {
    // Non-blocking readonly lock attempt (No writelocks by other threads should be possible) - will NOT LEAVE CS
    vgx_Vertex_t *tail_RO = _vxgraph_state__lock_vertex_readonly_CS( collector->graph, tail, control->zb, VGX_VERTEX_RECORD_NONE );
    __set_access_reason( control->reason, filter->timing_budget->reason );
    if( tail_RO ) {
      if( filter->filter( filter, tail_RO, &match ) ) {
        ++hit;
        if( collector->n_remain <= 0 ) {
          FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
        }
      }
      // SHOULD NOT LEAVE CS
      // TODO: Verify never give up CS
      _vxgraph_state__unlock_vertex_CS_LCK( collector->graph, &tail_RO, VGX_VERTEX_RECORD_OPERATION );
    }
    // Can't acquire readonly vertex (too many readers?)
    else {
      // TODO: Find a way to propagate error
      hit = -1;
    }
  }
  // Collector does not require lock, either graph is readonly or vertex won't be dereferenced when rendered
  else {
    if( filter->filter( filter, tail, &match ) ) {
      ++hit;
      if( collector->n_remain <= 0 ) {
        FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
      }
    }
  }
  if( __is_arcfilter_error( match ) ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    hit = -1;
  }
  return hit;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __cxmalloc_lock_vertex_CS_NT( cxmalloc_object_processing_context_t *incref_context, vgx_Vertex_t *vertex ) {
  if( vertex ) {
    if( __vertex_is_unlocked( vertex ) ) {
      __vertex_lock_writable_CS( vertex );
      _cxmalloc_object_incref_nolock( vertex );
      // Count vertices that are terminals for forward-only arcs
      if( __arcvector_cell_is_indegree_counter_only( &vertex->inarcs ) ) {
        vgx_vertex_type_t vxtype = *(vgx_vertex_type_t*)incref_context->filter;
        if( vxtype == VERTEX_TYPE_ENUMERATION_WILDCARD || vertex->descriptor.type.enumeration == vxtype ) {
          ++(*(int64_t*)incref_context->output); // n_fwdonly_terminals
        }
      }
      return 1;
    }
    else {
      return -1;
    }
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
static int64_t __cxmalloc_erase_vertex_CS_WL_NT( cxmalloc_object_processing_context_t *erase_context, vgx_Vertex_t *vertex_WL ) {
  if( vertex_WL && !__vertex_is_defunct( vertex_WL ) ) {
    vgx_vertex_type_t vxtype = *(vgx_vertex_type_t*)erase_context->filter;
    
    // Vertex type filter match
    if( vxtype == VERTEX_TYPE_ENUMERATION_WILDCARD || vertex_WL->descriptor.type.enumeration == vxtype ) {
      // Vertex now becomes defunct
      CALLABLE( vertex_WL )->Initialize_CS( vertex_WL );

      // Remove from event schedule if needed - we are deleting vertex forcefully anyway
      if( __vertex_has_event_scheduled( vertex_WL ) ) {
        iGraphEvent.CancelVertexEvent.ImmediateDrop_CS_WL_NT( vertex_WL->graph, vertex_WL );
      }

      // NOTE: This processor visits each vertex that will be affected by truncation and therefore this is our
      // only chance to modify vertex flags. Since this processor is part of the larger truncation process we reset
      // the index flags here. The index will later be dropped in a quick way where we don't have the opportunity
      // to reset flags on individual vertices.
      // TODO: What if truncation fails at a later stage before dropping the index? Then these flags will have been
      // cleared on vertices that are not actually unindexed.
      __vertex_clear_indexed( vertex_WL );

      return 1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_virtualize_vertex_CS_WL_NT( cxmalloc_object_processing_context_t *erase_context, vgx_Vertex_t *vertex_WL ) {
  if( vertex_WL && !__vertex_is_defunct( vertex_WL ) ) {
    vgx_vertex_type_t vxtype = *(vgx_vertex_type_t*)erase_context->filter;
    
    // Vertex type filter match
    if( vxtype == VERTEX_TYPE_ENUMERATION_WILDCARD || vertex_WL->descriptor.type.enumeration == vxtype ) {
      // Vertex now becomes virtual
      CALLABLE( vertex_WL )->Virtualize_CS( vertex_WL );

      // Remove from event schedule if needed - we are deleting vertex forcefully anyway
      if( __vertex_has_event_scheduled( vertex_WL ) ) {
        iGraphEvent.CancelVertexEvent.ImmediateDrop_CS_WL_NT( vertex_WL->graph, vertex_WL );
      }

      return 1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __cxmalloc_unlock_vertex_CS_NT( cxmalloc_object_processing_context_t *decref_context, vgx_Vertex_t *vertex ) {
  if( vertex ) {
    int64_t refcnt = _cxmalloc_object_refcnt_nolock( vertex );
    // Vertex is isolated or null, at risk of deallocation which is NOT allowed inside cxmalloc processor!
    // Yep, it would be deallocated were we to unlock it here. Instead add to pending queue and let the
    // outside caller finalize the unlocking of this vertex.
    vgx_VertexStateContext_man_t man = __vertex_get_manifestation( vertex );
    if( (__vertex_is_manifestation_virtual( vertex ) || __vertex_is_manifestation_null( vertex )) && refcnt == VXTABLE_VERTEX_REFCOUNT + 1 ) {
      // Force vertex REAL temporarily to avoid nasty logic to trigger when unlocking later
      __vertex_set_manifestation_real( vertex );
      vgx_Vertex_t *V = vertex;
      _vxgraph_state__unlock_vertex_CS_LCK( vertex->graph, &V, VGX_VERTEX_RECORD_OPERATION ); 
      // Restore original manifestation
      __vertex_set_manifestation( vertex, man );
    }
    // Ok to unlock vertex
    else {
      _vxgraph_state__unlock_vertex_CS_LCK( vertex->graph, &vertex, VGX_VERTEX_RECORD_OPERATION ); 
    }
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
static int64_t __cxmalloc_count_vertex_properties_ROG( cxmalloc_object_processing_context_t *counter, vgx_Vertex_t *vertex ) {
  if( vertex ) {
    int64_t np = CALLABLE( vertex )->NumProperties( vertex );
    *(int64_t*)counter->output += np;
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
static int64_t __cxmalloc_count_vertex_tmx_ROG( cxmalloc_object_processing_context_t *counter, vgx_Vertex_t *vertex ) {
  if( vertex ) {
    if( __vertex_has_expiration( vertex ) ) {
      *(int64_t*)counter->output += 1;
    }
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
static int64_t __initialize_vertices_CS_NT( vgx_Graph_t *self, vgx_vertex_type_t vxtype, bool *virtual_remain ) {

  int64_t n_vertices = 0;

  // First lock ALL vertices to ensure nothing gets destroyed until we're done
  DEBUG( 0xD61, "Obtaining write locks for all vertices" );
  cxmalloc_object_processing_context_t lock_context = {0};
  lock_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_lock_vertex_CS_NT;
  lock_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
  lock_context.filter = &vxtype;
  int64_t n_fwdonly_terminals = 0;
  lock_context.output = &n_fwdonly_terminals;
  int64_t n_locked = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &lock_context );
  DEBUG( 0xD62, "Processed %lld objects, locked %lld vertices", lock_context.n_objects_processed, lock_context.n_objects_active );
  if( n_locked < 0 ) {
    CRITICAL( 0xD63, "Failed to obtain write locks for all vertices. Process aborted." );
  }
  else {
    // Virtualize (don't erase) if forward-only terminals among target type 
    *virtual_remain = vxtype != VERTEX_TYPE_ENUMERATION_WILDCARD && n_fwdonly_terminals > 0;
#ifdef HASVERBOSE
    const char *action = *virtual_remain ? "Virtualizing" : "Erasing";
    // Initialize vertex objects of selected type
    if( vxtype != VERTEX_TYPE_ENUMERATION_WILDCARD ) {
      const CString_t *CSTR__vertex_type = iEnumerator_CS.VertexType.Decode( self, vxtype );
      VERBOSE( 0xD64, "%s all vertices of type '%s'", action, (CSTR__vertex_type ? CStringValue( CSTR__vertex_type ) : "???") );
    }
    else {
      VERBOSE( 0xD65, "%s all vertices", action );
    }
#endif
    cxmalloc_object_processing_context_t erase_context = {0};
    if( n_fwdonly_terminals == 0 ) {
      erase_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_erase_vertex_CS_WL_NT;
    }
    else {
      erase_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_virtualize_vertex_CS_WL_NT;
    }
    erase_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
    erase_context.filter = &vxtype;
    int64_t n_erased = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &erase_context );
    n_vertices = erase_context.n_objects_active;
    DEBUG( 0xD66, "Processed %lld objects, set %lld vertices defunct", erase_context.n_objects_processed, n_vertices );
    if( n_erased < 0 ) {
      CRITICAL( 0xD67, "Failed to erase all vertex objects. System now in a corrupted state." );
    }
  }
  // Finally unlock ALL vertices
  VERBOSE( 0xD68, "Releasing write locks for all vertices" );
  cxmalloc_object_processing_context_t unlock_context = {0};
  unlock_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_unlock_vertex_CS_NT;
  unlock_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
  int64_t n_unlocked = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &unlock_context );
  DEBUG( 0xD69, "Processed %lld objects, released %lld vertices", unlock_context.n_objects_processed, unlock_context.n_objects_active );
  if( n_unlocked < 0 ) {
    CRITICAL( 0xD6A, "Failed to release write locks for all vertices. Some vertices remain locked and may no longer be accessible." );
  }

  return n_vertices;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __FH_unindex_main_CS_NT( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  framehash_t *main_index = (framehash_t*)processor->processor.input;
  vgx_Vertex_t *vertex = (vgx_Vertex_t*)APTR_GET_POINTER( fh_cell );
  objectid_t obid; 
  idcpy( &obid, COMLIB_OBJECT_GETID( vertex ) );
  if( CALLABLE( main_index )->DelObj128Nolock( main_index, &obid ) == CELL_VALUE_TYPE_OBJECT128 ) {
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
static int64_t __discard_index_CS_NT( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype ) {

  int64_t n_discarded = 0;
  framehash_t *selected_index = __select_index( self, vxtype );

  if( selected_index ) {

    n_discarded = CALLABLE( selected_index )->Items( selected_index );
    
    // Full truncation - simple discard of all tables
    if( selected_index == self->vxtable ) {
      // Clear the entire vxtable (all vertices decrefed once, refcnt=1 after this)
      if( CALLABLE( self->vxtable )->Discard( self->vxtable ) != 0 ) {
        WARN( 0xD71, "Main vertex index was not cleared, remaining items: %lld", CALLABLE( self->vxtable )->Items( self->vxtable ) );
      }
      
      // Clean all type specific indices (all vertices decrefed once, refcnt -> 0 and deallocate)
      // Remove all type enumerations
      for( vgx_vertex_type_t vxtype_x = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype_x <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype_x++ ) {
        // refcnt = 1 after this
        framehash_t *type_index = self->vxtypeidx[ vxtype_x ];
        if( type_index ) {
          CALLABLE( type_index )->Discard( type_index );
          iEnumerator_CS.VertexType.Remove( self, vxtype_x );
        }
      }

      SubGraphOrder( self, n_discarded );

    }
    // Type-specific truncation
    else {
      int64_t n_main_original = CALLABLE( self->vxtable )->Items( self->vxtable );

      // All vertices indexed in type-specific index will be removed from main index (refcnt=1 after this)
      framehash_processing_context_t main_unindex_context = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &selected_index->_topframe, &selected_index->_dynamic, __FH_unindex_main_CS_NT );
      FRAMEHASH_PROCESSOR_SET_IO( &main_unindex_context, self->vxtable, NULL );
      int64_t n_main_removed = iFramehash.processing.ProcessNolockNocache( &main_unindex_context );
      SubGraphOrder( self, n_main_removed );

      // Verify the expected drop in main index
      int64_t n_main_reduced = CALLABLE( self->vxtable )->Items( self->vxtable );

      // Verify removed items matches size of type-specific index
      if( n_main_removed != n_discarded ) {
        WARN( 0xD72, "Incorrect number of removed items from main index: %lld, expected %lld", n_main_removed, n_discarded );
      }

      // Verify removed items corresponds with original vs current size of main index
      if( n_main_reduced != n_main_original - n_main_removed ) {
        WARN( 0xD73, "Unexpected drop in main index:%lld, expected %lld", n_main_removed, n_main_original - n_main_reduced );
      }

      // Clear the type specific index (selected vertices decrefed once, refcnt -> 0 and deallocate)
      // NOTE: The vertex index flags (main/type) have been cleared already in the main unindexing above.
      CALLABLE( selected_index )->Discard( selected_index );

      // Remove type enumeration
      iEnumerator_CS.VertexType.Remove( self, vxtype ); 

    }
  }

  return n_discarded;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef VXTABLE_PERSIST

/**************************************************************************//**
 * _vxgraph_vxtable__create_index_OPEN
 *
 ******************************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__create_index_OPEN( vgx_Graph_t *self ) {

#define PUSH_VERTEX_ALLOCATOR_CURRENT_THREAD( VertexAllocator )   \
  do {                                                            \
    cxmalloc_family_t *__previous__ = ivertexalloc.GetCurrent();  \
    ivertexalloc.SetCurrent( VertexAllocator );

#define POP_VERTEX_ALLOCATOR                  \
    ivertexalloc.SetCurrent( __previous__ );  \
  } WHILE_ZERO

  int ret = 0;

  // Set the vertex allocator to be used during deserialization (if we're restoring index from disk)
  PUSH_VERTEX_ALLOCATOR_CURRENT_THREAD( self->vertex_allocator ) {
    GRAPH_LOCK( self ) {
      
      CtptrList_t *CSTR__vxtype_list = NULL;

      XTRY {
        // Make sure we don't accidentaly make a new table directory if it already exists
        if( self->vxtable != NULL || self->vxtypeidx != NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD21 );
        }

        // ------------------------------
        // Create the global vertex index
        // ------------------------------

        // Create or restore the index
        if( (self->vxtable = __create_new_index_CS( self, VERTEX_TYPE_ENUMERATION_WILDCARD )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD22 );
        }
        if( CALLABLE( self->vxtable )->Items( self->vxtable ) == 0 ) {
          if( __rebuild_index_CS( self, VERTEX_TYPE_ENUMERATION_WILDCARD ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0xD23 );
          }
        }

        // ---------------------------------
        // Create all the type index entries
        // ---------------------------------

        // Create the type index directory entries
        if( (self->vxtypeidx = (framehash_t**)calloc( VERTEX_TYPE_ENUMERATION_MAX_ENTRIES, sizeof( framehash_t* ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD24 );
        }

        // Create all type indexes
        if( (CSTR__vxtype_list = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD25 );
        }
        if( iEnumerator_CS.VertexType.GetAll( self, CSTR__vxtype_list ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD26 );
        }
        int64_t n_vtx = CALLABLE( CSTR__vxtype_list )->Length( CSTR__vxtype_list );
        for( int64_t i=0; i<n_vtx; i++ ) {
          // Get the next type string
          tptr_t tptr;
          if( CALLABLE( CSTR__vxtype_list )->Get( CSTR__vxtype_list, i, &tptr ) == 1 ) {
            const CString_t *CSTR__vxtype = (const CString_t*)TPTR_GET_PTR56( &tptr );
            // vxtype assumed to be valid since we are restoring previous data
            vgx_vertex_type_t vxtype = (vgx_vertex_type_t)iEnumerator_CS.VertexType.GetEnum( self, CSTR__vxtype );
            if( vxtype == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0xD27 );
            }
            if( !__vertex_type_enumeration_valid( vxtype ) ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0xD28 );
            }
            if( (self->vxtypeidx[ vxtype ] = __create_new_index_CS( self, vxtype )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0xD29 );
            }
            // Trigger rebuild if file was missing
            framehash_t *index = self->vxtypeidx[ vxtype ];
            if( CALLABLE( index )->Items( index ) == 0 && self->order > 0 ) {
              if( __rebuild_index_CS( self, vxtype ) < 0 ) {
                THROW_ERROR( CXLIB_ERR_GENERAL, 0xD2A );
              }
            }
          }
        }
      }
      XCATCH( errcode ) {
        _vxgraph_vxtable__destroy_index_CS( self );
        ret = -1;
      }
      XFINALLY {
        // Clean up lists
        if( CSTR__vxtype_list ) {
          COMLIB_OBJECT_DESTROY( CSTR__vxtype_list );
        }
      }
    } GRAPH_RELEASE;
  } POP_VERTEX_ALLOCATOR;
  return ret;
}
#endif


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxgraph_vxtable__destroy_index_CS( vgx_Graph_t *self ) {
  // Destroy the generic index
  iMapping.DeleteMap( &self->vxtable );

  // Destroy the type index
  if( self->vxtypeidx ) {
    // Destroy any type index that exists
    for( int vxtype = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype++ ) {
      iMapping.DeleteMap( &self->vxtypeidx[ vxtype ] );
    }
    // Destroy the vertex index directory
    free( self->vxtypeidx );
    self->vxtypeidx = NULL;
  }
}



/*******************************************************************//**
 *
 * Index will own additional vertex references after insertion.
 * Return: > 0: Number of additional references owned
 *         0  : No action
 *        -1  : Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__index_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {

  int indexed = 0;
  vgx_vertex_type_t vxtype = vertex_WL->descriptor.type.enumeration;
  if( self->vxtypeidx[ vxtype ] == NULL ) {
    if( (self->vxtypeidx[ vxtype ] = __create_new_index_CS( self, vxtype )) == NULL ) {
      return -1;
    }
  }
  framehash_t *typeindex = self->vxtypeidx[ vxtype ];

  // Add vertex to generic index
  if( __add_vertex_to_index_CS_WL( self->vxtable, vertex_WL ) == 1 ) {
    __vertex_set_indexed_main( vertex_WL );
    indexed++;
    // Add vertex to type index
    if( __add_vertex_to_index_CS_WL( typeindex, vertex_WL ) == 1 ) {
      __vertex_set_indexed_type( vertex_WL );
      indexed++;
      // Success, graph order +1
      IncGraphOrder( self );
    }
    // Error, roll back
    else{
      _vxgraph_vxtable__unindex_vertex_CS_WL( self, vertex_WL );
      indexed = -1;
    }
  }

  return indexed;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__index_vertex_OPEN_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  int indexed;
  GRAPH_LOCK( self ) {
    indexed = _vxgraph_vxtable__index_vertex_CS_WL( self, vertex_WL );
  } GRAPH_RELEASE;
  return indexed;
}



/*******************************************************************//**
 *
 * Index will own one additional vertex reference after insertion into type index.
 * Return: 1 : Vertex added to type index
 *         0 : Not added (already exists)
 *        -1 : Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__index_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  // Add vertex to type index
  vgx_vertex_type_t vxtype = vertex_WL->descriptor.type.enumeration;
  if( self->vxtypeidx[ vxtype ] == NULL ) {
    if( (self->vxtypeidx[ vxtype ] = __create_new_index_CS( self, vxtype )) == NULL ) {
      return -1;
    }
  }
  framehash_t *typeindex = self->vxtypeidx[ vxtype ];
  int ret = __add_vertex_to_index_CS_WL( typeindex, vertex_WL );
  if( ret == 1 ) {
    __vertex_set_indexed_type( vertex_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 * Index will discard any owned references.
 * Return: >0 : Number of vertex references discarded (Unindexing completed)
 *         0  : No action (vertex was not indexed)
 *        -1  : Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__unindex_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  int unindexed = 0;

  // Make sure we don't disappear until all is done
  __vertex_inc_writer_recursion_CS( vertex_WL );
  Vertex_INCREF_WL( vertex_WL );

  // Remove vertex from generic index
  if( __remove_vertex_from_index_CS_WL( self, self->vxtable, vertex_WL, VERTEX_TYPE_ENUMERATION_NONE ) == 1 ) {
    __vertex_clear_indexed_main( vertex_WL );
    unindexed++;
    // Remove vertex from type index
    vgx_vertex_type_t vertex_type = vertex_WL->descriptor.type.enumeration;
    framehash_t *typeindex = self->vxtypeidx[ vertex_type ];
    // ------------------------------------------------------
    // [AMBD-710]
    // Sometimes this typeindex is NULL, resulting in partial indexing. 
    // TODO: Find a way to reproduce this problem so we can fix it.
    // ------------------------------------------------------
    if( typeindex ) {
      int type_unindexed = __remove_vertex_from_index_CS_WL( self, typeindex, vertex_WL, vertex_type );
      // OK
      if( type_unindexed == 1 ) {
        __vertex_clear_indexed_type( vertex_WL );
        unindexed++;
      }
      // Error
      else if( type_unindexed < 0 ) {
        unindexed = -1;
      }
      // No action (index was partal )
      else {
        const char *prefix = CALLABLE( vertex_WL )->IDPrefix( vertex_WL );
        int tp = vertex_WL->descriptor.type.enumeration;
        CRITICAL( 0xD21, "Partial unindexing of '%s', failed to remove from typeindex", prefix, tp );
      }
    }
    else {
      const char *prefix = CALLABLE( vertex_WL )->IDPrefix( vertex_WL );
      int tp = vertex_WL->descriptor.type.enumeration;
      CRITICAL( 0xD32, "Partial unindexing of '%s', type index missing: %d", prefix, tp );
    }
    // Success, graph order -1
    DecGraphOrder( self );

  }

  __vertex_dec_writer_recursion_CS( vertex_WL );
  Vertex_DECREF_WL( vertex_WL );

  return unindexed;
}



/*******************************************************************//**
 *
 * Index will discard any owned references.
 * Return: 1 : Unindexing completed
 *         0 : No action (vertex was not indexed)
 *        -1 : Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__unindex_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  vgx_vertex_type_t vertex_type = vertex_WL->descriptor.type.enumeration;
  framehash_t *typeindex = self->vxtypeidx[ vertex_type ];
  int ret = __remove_vertex_from_index_CS_WL( self, typeindex, vertex_WL, vertex_type );
  if( ret == 1 ) {
    __vertex_clear_indexed_type( vertex_WL );
  }
  return ret;
}



/*******************************************************************//**
 *
 * Update the type index for this vertex. Vertex is first removed from
 * its existing index and then added to the new index according to its
 * new type.
 * Return: 1 : Re-indexing completed
 *         0 : No action (vertex was not re-indexed)
 *        -1 : Error
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_vxtable__reindex_vertex_type_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_vertex_type_t new_type ) {
  // Remove vertex from old type index
  vgx_vertex_type_t old_type = __vertex_get_type( vertex_WL );
  framehash_t *typeindex = self->vxtypeidx[ old_type ];
  int ret = __remove_vertex_from_index_CS_WL( self, typeindex, vertex_WL, old_type );
  if( ret < 0 ) {
    return -1;
  }

  // Set new type
  __vertex_set_type( vertex_WL, new_type );

  // Capture
  iOperation.Vertex_WL.ChangeType( vertex_WL, new_type );

  // Add vertex to new type index
  ret = _vxgraph_vxtable__index_vertex_type_CS_WL( self, vertex_WL ); // graph -> dirty
  if( ret < 0 ) {
    return -1;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_vxtable__query_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  if( obid == NULL && CSTR__idstr ) { // <= ideally avoid this since we're holding the state lock
    obid = CStringObid( CSTR__idstr );
  }
  // Perform lookup in selected index
  framehash_t *index = __select_index( self, vxtype );
  if( index && obid ) {
    vgx_Vertex_t *vertex = NULL;
    if( CALLABLE( index )->GetObj128Nolock( index, obid, (comlib_object_t**)&vertex ) == CELL_VALUE_TYPE_OBJECT128 ) {
      ASSERT_VERTEX( vertex, obid );
      // Hit
      return vertex;
    }
  }

  // Miss
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_vxtable__exists_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  if( obid == NULL && CSTR__idstr ) { // <= ideally avoid this since we're holding the state lock
    obid = CStringObid( CSTR__idstr );
  }

  // Perform lookup in selected index
  framehash_t *index = __select_index( self, vxtype );
  if( index && obid ) {
    if( CALLABLE( index )->HasObj128Nolock( index, obid ) == CELL_VALUE_TYPE_OBJECT128 ) {
      return true;
    }
  }

  return false;
}



/*******************************************************************//**
 * WARNING: Caller owns returned CString !!
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_vxtable__get_idstring_CS( vgx_Graph_t *self, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  // Perform lookup in selected index
  framehash_t *index = __select_index( self, vxtype );
  if( index ) {
    vgx_Vertex_t *vertex = NULL;
    if( CALLABLE( index )->GetObj128Nolock( index, obid, (comlib_object_t**)&vertex ) == CELL_VALUE_TYPE_OBJECT128 ) {
      ASSERT_VERTEX( vertex, obid );
      return CALLABLE( vertex )->IDCString( vertex );
    }
  }
  // Miss
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__len_CS( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype ) {
  framehash_t *index = __select_index( self, vxtype );
  if( index ) {
    return CALLABLE( index )->Items( index );
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
DLL_HIDDEN int _vxgraph_vxtable__set_readonly_CS( vgx_Graph_t *self ) {
  int ret = 0;
  XTRY {
    // Set main table readonly
    if( CALLABLE( self->vxtable )->SetReadonly( self->vxtable ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xD41 );
    }

    // Set type-specific tables readonly
    for( vgx_vertex_type_t vxtype = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype++ ) {
      framehash_t *index = self->vxtypeidx[ vxtype ];
      if( index ) {
        if( CALLABLE( index )->SetReadonly( index ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xD42 );
        }
      }
    }
  }
  XCATCH( errcode ) {
    _vxgraph_vxtable__clear_readonly_CS( self );
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
DLL_HIDDEN int _vxgraph_vxtable__clear_readonly_CS( vgx_Graph_t *self ) {
  // Set main table writable
  CALLABLE( self->vxtable )->ClearReadonly( self->vxtable );

  // Set type-specific tables writable
  for( vgx_vertex_type_t vxtype = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype++ ) {
    framehash_t *index = self->vxtypeidx[ vxtype ];
    if( index ) {
      CALLABLE( index )->ClearReadonly( index );
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__collect_items_ROG_or_CSNOWL( vgx_Graph_t *self, vgx_global_search_context_t *search, vgx_VertexFilter_context_t *filter ) {
  if( search->collector.mode == VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST ) {
    return 0;
  }

  int64_t n_collected = 0;
  framehash_t *index = NULL;
  const objectid_t *obid = NULL;

  // If graph is WRITABLE at this point no writable vertices exist (other than any writable
  // vertices held by current thread) It is safe to proceed with vertex scan since we are in
  // CS and no vertices can become writable for the duration of the scan.
  // If graph is READONLY (for duration of scan) we don't have CS and we don't need to
  // lock vertices.

  XDO {
    if( filter ) {
      if( filter->type != VGX_VERTEX_FILTER_TYPE_PASS ) {
        vgx_vertex_probe_t *probe = ((vgx_GenericVertexFilter_context_t*)filter)->vertex_probe;
        if( probe ) {
          // Try to use a specific index if vertex type is part of the filter
          index = __select_index( self, probe->vertex_type );
          if( index == NULL ) {
            XBREAK;
          }
          // Does probe request a specific vertex?
          if( (probe->spec & _VERTEX_PROBE_ID_MASK) == (_VERTEX_PROBE_ID_ENA | _VERTEX_PROBE_ID_EQU) ) {
            if( probe->CSTR__idlist && iString.List.Size( probe->CSTR__idlist ) == 1 ) {
              obid = CStringObid( iString.List.GetItem( probe->CSTR__idlist, 0 ) );
            }
          }
        }
      }
    }

    // Default to the generic index
    if( index == NULL ) {
      index = self->vxtable;
    }

    // Trivial case: we are requesting collection of a specific vertex only
    if( obid ) {
      vgx_Vertex_t *vertex = NULL;
      if( CALLABLE( index )->GetObj128Nolock( index, obid, (comlib_object_t**)&vertex ) == CELL_VALUE_TYPE_OBJECT128 ) {
        vgx_ArcFilter_match match;
        // Vertices
        if( search->collector.mode == VGX_COLLECTOR_MODE_COLLECT_VERTICES ) {
          if( search->collector.base->n_remain-- > 0 ) {
            if( filter->filter( filter, vertex, &match ) ) {
              // TODO: Don't waste time locking if GRAPH is readonly?
              vgx_Vertex_t *vertex_RO = _vxgraph_state__lock_vertex_readonly_OPEN( self, vertex, search->timing_budget, VGX_VERTEX_RECORD_NONE );
              if( vertex_RO ) {
                vgx_LockableArc_t LARC = VGX_LOCKABLE_ARC_INIT( vertex_RO, 1, VGX_PREDICATOR_NONE, vertex_RO, 1 );
                COLLECT_VERTEX( search->collector.vertex, &LARC );
                n_collected = 1;
                // Release vertex only if still locked (collector may have stolen the lock)
                if( LARC.acquired.head_lock > 0 ) {
                  _vxgraph_state__unlock_vertex_OPEN_LCK( self, &vertex_RO, VGX_VERTEX_RECORD_OPERATION );
                }
              }
              // Can't acquire readonly for some reason (too many readers?)
              else {
                n_collected = -1;
              }
            }
            else if( __is_arcfilter_error( match ) ) {
              // Error
              __set_access_reason( &search->timing_budget->reason, VGX_ACCESS_REASON_ERROR );
              n_collected = -1;
            }
          }
        }
        // Arcs
        else if( search->collector.mode == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
          if( !filter->filter( filter, vertex, &match ) && __is_arcfilter_error( match ) ) {
            // Error
            __set_access_reason( &search->timing_budget->reason, VGX_ACCESS_REASON_ERROR );
            n_collected = -1;
          }
        }
        // ???
        else {
          // Error
          __set_access_reason( &search->timing_budget->reason, VGX_ACCESS_REASON_INVALID );
          n_collected = -1;
        }
      }
    }
    // Normal case: scan
    else if( filter ) {

      // Thread ID
      filter->current_thread = GET_CURRENT_THREAD_ID();

      // Scan allocator instead of index when it is faster to do so
      bool allocator_scan = false;
      if( index == self->vxtable ) {
        allocator_scan = true; // always faster to scan allocator instead of full index
      }
      else {
        int64_t n_partial = CALLABLE( index )->Items( index );
        int64_t n_full = CALLABLE( self->vxtable )->Items( self->vxtable );
        if( n_full > 0 && (double)n_partial / n_full > 0.25 ) {
          allocator_scan = true; // allocator scan when selected index holds more than 1/4 of all vertices
        }
      }

      bool random = false;
      if( search->ranking_context && _vgx_sortby( search->ranking_context->sortspec ) == VGX_SORTBY_RANDOM ) {
        random = true;
        allocator_scan = true;
      }


      // Set up processor control
      int64_t tick = __GET_CURRENT_MILLISECOND_TICK();
      vgx_ExecutionTimingBudget_t zero_budget = _vgx_get_graph_zero_execution_timing_budget( self );
      __processor_control_t control = {
        .t0     = tick,
        .tt     = tick + search->timing_budget->t_remain_ms,
        .n      = 0,
        .filter = filter,
        .tb     = search->timing_budget,
        .zb     = &zero_budget,
        .reason = &search->timing_budget->reason
      };

      // Collect vertices
      if( search->collector.mode == VGX_COLLECTOR_MODE_COLLECT_VERTICES ) {
        // Scan Vertex Allocator
        if( allocator_scan ) {
          cxmalloc_object_processing_context_t scan_context = {0};
          scan_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
          if( random ) {
            scan_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_collect_vertex_ROG_or_CSNOWL;
          }
          else {
            scan_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_collect_vertex_ROG_or_CSNOWL;
          }
          scan_context.filter = &control;
          scan_context.output = search->collector.vertex;
          CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &scan_context );
          if( scan_context.error ) {
            return -1;
          }
        }
        // Scan Vertex Index (faster when index is small)
        else {
          framehash_processing_context_t collect_vertex = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &index->_topframe, &index->_dynamic, __FH_collect_vertex_ROG_or_CSNOWL );
          FRAMEHASH_PROCESSOR_SET_IO( &collect_vertex, &control, search->collector.vertex );
          if( iFramehash.processing.ProcessNolockNocache( &collect_vertex ) < 0 ) {
            return -1;
          }
        }
        
        n_collected = search->collector.vertex->n_vertices;
      }
      // Collect arcs
      else if( search->collector.mode == VGX_COLLECTOR_MODE_COLLECT_ARCS ) {
        vgx_vertex_probe_t *probe = ((vgx_GenericVertexFilter_context_t*)filter)->vertex_probe;
        if( probe->advanced.next.neighborhood_probe ) {
          // Scan Vertex Allocator
          if( allocator_scan ) {
            cxmalloc_object_processing_context_t scan_context = {0};
            scan_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
            scan_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_collect_outarcs_ROG_or_CSNOWL;
            scan_context.filter = &control;
            scan_context.output = search->collector.arc;
            CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &scan_context );
            if( scan_context.error ) {
              return -1;
            }
          }
          // Scan Vertex Index (faster when index is small)
          else {
            framehash_processing_context_t collect_arcs = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &index->_topframe, &index->_dynamic, __FH_collect_outarcs_ROG_or_CSNOWL );
            FRAMEHASH_PROCESSOR_SET_IO( &collect_arcs, &control, search->collector.arc );
            if( iFramehash.processing.ProcessNolockNocache( &collect_arcs ) < 0 ) {
              return -1;
            }
          }
        }

        n_collected = search->collector.arc->n_arcs;
      }
    }
  }
  XFINALLY {
  }

  return n_collected;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__count_vertex_properties_ROG( vgx_Graph_t *self ) {
  int64_t nprop = 0;
  GRAPH_LOCK( self ) {
    if( CALLABLE( self )->advanced->IsGraphReadonly( self ) ) {
      cxmalloc_object_processing_context_t counter = {0};
      counter.output = &nprop;
      counter.process_object = (f_cxmalloc_object_processor)__cxmalloc_count_vertex_properties_ROG;
      counter.object_class = COMLIB_CLASS( vgx_Vertex_t );
      if( CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &counter ) < 0 ) {
        nprop = -1;
      }
    }
    else {
      nprop = -1;
    }
  } GRAPH_RELEASE;
  return nprop;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__count_vertex_tmx_ROG( vgx_Graph_t *self ) {
  int64_t n_tmx = 0;
  GRAPH_LOCK( self ) {
    if( CALLABLE( self )->advanced->IsGraphReadonly( self ) ) {
      cxmalloc_object_processing_context_t counter = {0};
      counter.output = &n_tmx;
      counter.process_object = (f_cxmalloc_object_processor)__cxmalloc_count_vertex_tmx_ROG;
      counter.object_class = COMLIB_CLASS( vgx_Vertex_t );
      if( CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &counter ) < 0 ) {
        n_tmx = -1;
      }
    }
    else {
      n_tmx = -1;
    }
  } GRAPH_RELEASE;
  return n_tmx;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __prepare_vertex_CS_NT( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t refcnt = 0;

#ifdef VGX_CONSISTENCY_CHECK
  vgx_Vertex_t *vertex = (vgx_Vertex_t*)APTR_GET_POINTER( fh_cell );
  if( __vertex_is_unlocked( vertex ) ) {
    vgx_Vertex_t *vertex_WL = __vertex_lock_writable_CS( vertex );
    Vertex_INCREF_WL( vertex_WL );
    refcnt = __check_vertex_consistency_WL( vertex_WL );
    __vertex_unlock_writable_CS( vertex_WL );
    Vertex_DECREF_WL( vertex_WL );
  }
#endif

  return refcnt;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__prepare_vertices_CS_NT( vgx_Graph_t *self ) {
  int64_t ret = 0;
  framehash_t *index = self->vxtable;
#ifdef HASVERBOSE
  int64_t n = CALLABLE( index )->Items( index );
  VERBOSE( 0xD91, "Preparing %lld vertices in graph '%s'", n, CStringValue( CALLABLE(self)->Name(self) ) );
#endif
  framehash_processing_context_t prepare_vertex = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &index->_topframe, &index->_dynamic, __prepare_vertex_CS_NT );
  FRAMEHASH_PROCESSOR_SET_IO( &prepare_vertex, NULL, NULL );
  if( iFramehash.processing.ProcessNolockNocache( &prepare_vertex ) < 0 ) {
    CRITICAL( 0xD92, "Vertex preparation failed" );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __clear_relationships_CS( vgx_Graph_t *self ) {
  int ret = 0;
  CtptrList_t *enums = NULL;

  XTRY {
    if( (enums = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( iEnumerator_CS.Relationship.GetEnums( self, enums ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    int64_t sz = CALLABLE( enums )->Length( enums );

    for( int64_t i=0; i<sz; i++ ) {
      tptr_t data;
      if( CALLABLE( enums )->Get( enums, i, &data ) == 1 ) {
        vgx_predicator_rel_enum rel = (vgx_predicator_rel_enum)TPTR_AS_INTEGER( &data );
        if( iEnumerator_CS.Relationship.Remove( self, rel ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
      }
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( enums ) {
      COMLIB_OBJECT_DESTROY( enums );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __clear_vertextypes_CS( vgx_Graph_t *self ) {
  int ret = 0;
  CtptrList_t *enums = NULL;

  XTRY {
    if( (enums = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( iEnumerator_CS.VertexType.GetEnums( self, enums ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    int64_t sz = CALLABLE( enums )->Length( enums );

    for( int64_t i=0; i<sz; i++ ) {
      tptr_t data;
      if( CALLABLE( enums )->Get( enums, i, &data ) == 1 ) {
        vgx_vertex_type_t vertex_type = (vgx_vertex_type_t)TPTR_AS_INTEGER( &data );
        if( iEnumerator_CS.VertexType.Remove( self, vertex_type ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }
      }
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( enums ) {
      COMLIB_OBJECT_DESTROY( enums );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__truncate_noeventproc_CS( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype, CString_t **CSTR__error ) {
  int64_t n_removed = 0;

  framehash_t *index = __select_index( self, vxtype );
  if( index == NULL ) {
    return 0;
  }

  XTRY {

    // ---------------------------------
    // 1. Make sure no vertices are busy
    // ---------------------------------
    int64_t n_WL = _vgx_graph_get_vertex_WL_count_CS( self );
    int64_t n_RO = _vgx_graph_get_vertex_RO_count_CS( self );
    int64_t n_acquired = n_WL + n_RO;
    if( n_acquired != 0 ) {
      if( CSTR__error ) {
        __format_error_string( CSTR__error, "Truncation not allowed, %lld open vertices. (writable:%lld readonly:%lld)", n_acquired, n_WL, n_RO );
      }
      THROW_SILENT( CXLIB_ERR_SEMAPHORE, 0xD81 );
    }

    // -------------------------------
    // 2. Gather pre-truncation counts
    // -------------------------------

    // Get the graph order and size before we begin
    int64_t original_order = GraphOrder( self );
    int64_t current_order = original_order;

    // Get the total vertex count from selected index
    int64_t n_indexed = CALLABLE( index )->Items( index );


    // -------------------------------------------
    // 3. Initialize all vertices of selected type
    // -------------------------------------------

    // Initialize vertices
    bool virtual_remain = false;
    int64_t n_initialized = __initialize_vertices_CS_NT( self, vxtype, &virtual_remain );

    // Make sure we initialized the correct number of vertices
    if( n_initialized != n_indexed ) {
      WARN( 0xD82, "Initialized vertices (%lld) does not match index item count (%lld)", n_initialized, n_indexed );
    }


    // Proceed with un-indexing if no virtual vertices remain
    if( !virtual_remain ) {
      // -------------------
      // 4. Unindex vertices
      // -------------------
      n_removed = __discard_index_CS_NT( self, vxtype );
      current_order = GraphOrder( self );

      // ----------------
      // 5. Verify counts
      // ----------------
      if( original_order - n_removed != current_order ) {
        WARN( 0xD83, "Incorrect graph order after truncation: %lld, expected %lld", current_order, original_order - n_removed );
      }
    }

    // No more cleanup possible if graph still populated
    if( current_order > 0  ) {
      XBREAK;
    }
    // --------------------------------------------------------------------------------------

    // -----------------
    // 6. Clean up enums
    // -----------------
    if( __clear_relationships_CS( self ) < 0 ) {
      WARN( 0xD84, "Failed to remove relationship enumerations" );
    }

    if( __clear_vertextypes_CS( self ) < 0 ) {
      WARN( 0xD85, "Failed to remove vertex type enumerations" );
    }

    // -----------------
    // 7. Truncate vprop
    // -----------------
    // Destroy and re-initialize to truncate vprop file back to commit point
    _vxvertex_property__virtual_properties_destroy( self );
    if( _vxvertex_property__virtual_properties_init( self) < 0 ) {
      THROW_SILENT( CXLIB_ERR_SEMAPHORE, 0xD86 );
    }

    // ---------------------------------------
    // 8. Clean up arcvector framehash dynamic
    // ---------------------------------------

    if( iFramehash.dynamic.ResetDynamicSimple( &self->arcvector_fhdyn ) < 0 ) {
      WARN( 0xD87, "Failed to reset arcvector framehash dynamic" );
    }



  }
  XCATCH( errcode ) {
    n_removed = -1;
  }
  XFINALLY {

  }

  return n_removed;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_vxtable__query_OPEN( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  vgx_Vertex_t *vertex;

  if( obid == NULL ) {
    obid = CStringObid( CSTR__idstr );
  }
  
  GRAPH_LOCK( self ) {
    vertex = _vxgraph_vxtable__query_CS( self, CSTR__idstr, obid, vxtype );
  } GRAPH_RELEASE;
  
  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_vxtable__exists_OPEN( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  bool exists;

  if( obid == NULL ) {
    obid = CStringObid( CSTR__idstr );
  }

  GRAPH_LOCK( self ) {
    exists = _vxgraph_vxtable__exists_CS( self, CSTR__idstr, obid, vxtype );
  } GRAPH_RELEASE;
  
  return exists;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxgraph_vxtable__get_idstring_OPEN( vgx_Graph_t *self, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype ) {
  CString_t *CSTR__id;
  GRAPH_LOCK( self ) {
    CSTR__id = _vxgraph_vxtable__get_idstring_CS( self, obid, vxtype );
  } GRAPH_RELEASE;
  return CSTR__id;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__len_OPEN( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype ) {
  int64_t len;
  GRAPH_LOCK( self ) {
    len = _vxgraph_vxtable__len_CS( self, vxtype );
  } GRAPH_RELEASE;
  return len;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__collect_items_OPEN( vgx_Graph_t *self, vgx_global_search_context_t *search, bool readonly_graph, vgx_VertexFilter_context_t *filter ) {
  int64_t n_collected = -1;

  // If the graph is READONLY (and will remain so for the duration) we don't need
  // the CS and we also don't need to acquire vertex locks.
  if( readonly_graph ) {
    n_collected = _vxgraph_vxtable__collect_items_ROG_or_CSNOWL( self, search, filter );
  }
  // If the graph is WRITABLE we have to stay in CS and ensure no other threads have
  // writable vertices for the whole duration of collection. We can't allow any other
  // threads to modify the vxtable or the underlying vertex allocator.
  else {
    GRAPH_LOCK( self ) {
      BEGIN_STATIC_GRAPH_CS( self, search->timing_budget ) {
        n_collected = _vxgraph_vxtable__collect_items_ROG_or_CSNOWL( self, search, filter );
      } END_STATIC_GRAPH_CS;
    } GRAPH_RELEASE;
  }

  return n_collected;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__truncate_OPEN( vgx_Graph_t *self, vgx_VertexTypeEnumeration_t vxtype, CString_t **CSTR__error ) {
  int64_t n_removed = 0;

  int suspended_eventproc_here = 0;
  int suspended_emitter_here = 0;
  

  GRAPH_LOCK( self ) {
    if( _vgx_is_writable_CS( &self->readonly ) ) {
      BEGIN_DISALLOW_READONLY_CS( &self->readonly ) {
        if( _vxgraph_tracker__has_writable_locks_CS( self ) || _vxgraph_tracker__has_readonly_locks_CS( self ) ) {
          __set_error_string( CSTR__error, "Current thread holds one or more vertex locks, cannot truncate." );
          n_removed = -1;
        }
        else {
          // -----------------------------------------------
          // Stop Event Processor activity before truncation
          // -----------------------------------------------
          vgx_ExecutionTimingBudget_t disable_budget = _vgx_get_graph_execution_timing_budget( self, 5000 );
          GRAPH_SUSPEND_LOCK( self ) {
            if( (suspended_eventproc_here = iGraphEvent.NOCS.Disable( self, &disable_budget )) < 0 ) {
              __format_error_string( CSTR__error, "Unable to stop event processor (timeout after %d ms), cannot truncate at this time.", disable_budget.timeout_ms );
              n_removed = -1;
            }
          } GRAPH_RESUME_LOCK;

          // TODO:
          //
          //
          // Ensure NO OTHER BACKGROUND ACTIVITY.
          //
          // Case: VGX Server was running, executing a call that caused deadlock.
          //       1. Truncator thread locked allocator family, then tried to get graph lock.
          //       2. Server thread had graph lock, then tried to get allocator family lock. 
          // 
          // TRUNCATOR process assumes NOTHING ELSE IS RUNNING. This assumption was broken.
          //
          //

          // ------------------
          // Perform truncation
          // ------------------
          if( n_removed == 0 ) {
            // Close graph operation
            CLOSE_GRAPH_OPERATION_CS( self );

            // Suspend operation output
            if( iOperation.Emitter_CS.IsReady( self ) ) {
              if( iOperation.Emitter_CS.Suspend( self, 60000 ) < 1 ) {
                __set_error_string( CSTR__error, "Unable to suspend operation processor, cannot truncate." );
                n_removed = -1;
              }
              else {
                suspended_emitter_here = 1;
              }
            }

            // Proceed
            if( n_removed == 0 ) {
              vgx_EventProcessor_t *EVP_WL = NULL;
              bool can_truncate = true;
              if( iGraphEvent.IsReady( self ) ) {
                // Acquire event processor
                GRAPH_SUSPEND_LOCK( self ) {
                  if( (EVP_WL = iGraphEvent.Acquire( self, 1000 )) == NULL ) {
                    can_truncate = false;
                  }
                } GRAPH_RESUME_LOCK;
              }

              // Truncate
              if( can_truncate ) {
                n_removed = _vxgraph_vxtable__truncate_noeventproc_CS( self, vxtype, CSTR__error );
              }
              // Failed to acquire processor
              else {
                __set_error_string( CSTR__error, "Unable to acquire event processor lock, cannot truncate." );
                n_removed = -1;
              }

              if( EVP_WL ) {
                iGraphEvent.Release( &EVP_WL );
              }

              // Resume operation output if we suspended it above
              if( suspended_emitter_here ) {
                if( iOperation.Emitter_CS.Resume( self ) < 0 ) {
                  // This is a big problem.
                  CRITICAL( 0x001, "Unable to resume operation output after trucation" );
                }
              }
            }

            // Re-open graph operation
            if( OPEN_GRAPH_OPERATION_CS( self ) < 0 ) {
              CRITICAL( 0x002, "Unable to re-open graph operation after trucation" );
            }

            if( n_removed > 0 ) {
              // Capture and commit
              iOperation.Graph_CS.Truncate( self, vxtype, n_removed );
              COMMIT_GRAPH_OPERATION_CS( self );
            }

          }

        }
      } END_DISALLOW_READONLY_CS;
    }
    else {
      _vgx_request_write_CS( &self->readonly );
      if( CSTR__error && *CSTR__error == NULL ) {
        *CSTR__error = CStringNew( "Graph is readonly, cannot truncate." );
      }
      n_removed = -1;
    }
  } GRAPH_RELEASE;
  
  // -------------------------------
  // Resume Event Processor activity
  // -------------------------------
  if( suspended_eventproc_here > 0 ) {
    vgx_ExecutionTimingBudget_t long_timeout = _vgx_get_execution_timing_budget( 0, 10000 );
    if( iGraphEvent.NOCS.Enable( self, &long_timeout ) < 0 ) {
      CRITICAL( 0x003, "Failed to resume Event Processor after truncating graph" );
    }
  }

  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int _vxgraph_vxtable__get_index_counters_OPEN( vgx_Graph_t *self, framehash_perfcounters_t *counters, const CString_t *CSTR__vertex_type, CString_t **CSTR__error ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    framehash_t *index = __select_index_by_name_CS( self, CSTR__vertex_type, CSTR__error );
    if( index ) {
      CALLABLE( index )->GetPerfCounters( index, counters );
    }
    else {
      ret = -1;
    }
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int _vxgraph_vxtable__reset_index_counters_OPEN( vgx_Graph_t *self ) {
  int ret = 0;
  GRAPH_LOCK( self ) {

    // Reset counters for all type indexes
    for( vgx_vertex_type_t vxtype_x = __VERTEX_TYPE_ENUMERATION_START_SYS_RANGE; vxtype_x <= __VERTEX_TYPE_ENUMERATION_END_USER_RANGE; vxtype_x++ ) {
      framehash_t *type_index = self->vxtypeidx[ vxtype_x ];
      if( type_index ) {
        CALLABLE( type_index )->ResetPerfCounters( type_index );
      }
    }

    // Reset counters for main index
    if( self->vxtable ) {
      CALLABLE( self->vxtable )->ResetPerfCounters( self->vxtable );
    }
  } GRAPH_RELEASE;
  return ret;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __cxmalloc_sync_vertex_CS_NT( cxmalloc_object_processing_context_t *sync_context, vgx_Vertex_t *vertex ) {
  int64_t ret = 0;
  if( vertex && !__vertex_is_defunct( vertex ) ) {
    vgx_Graph_t *graph = vertex->graph;
    vgx_Vertex_t *vertex_WL = __vertex_lock_writable_CS( vertex );
    Vertex_INCREF_WL( vertex_WL );
    if( iOperation.Open_CS( graph, &vertex_WL->operation, COMLIB_OBJECT( vertex_WL ), true ) < 0 ) {
      // ERROR: can't open operation
      ret = -1;
    }
    else {
      vgx_Vertex_constructor_args_t args;
      args.vxtype = vertex_WL->descriptor.type.enumeration;
      args.ts = vertex_WL->TMC;
      args.rank = vertex_WL->rank;
      args.CSTR__idstring = vertex_WL->identifier.CSTR__idstr;
      if( args.CSTR__idstring ) {
        icstringobject.IncrefNolock( (CString_t*)args.CSTR__idstring );
      }
      else {
        args.CSTR__idstring = icstringobject.New( graph->ephemeral_string_allocator_context, vertex_WL->identifier.idprefix.data );
      }
      // New vertex instance
      iOperation.Vertex_CS.New( vertex_WL, &args );
      iOperation.Commit_CS( graph, &graph->operation, true );
      icstringobject.DecrefNolock( (CString_t*)args.CSTR__idstring );

      // TODO: Manifestation
      // TODO: TMM ?

      // Expiration
      if( vertex_WL->TMX.vertex_ts < TIME_EXPIRES_NEVER ) {
        iOperation.Vertex_WL.SetTMX( vertex_WL, vertex_WL->TMX.vertex_ts );
      }

      // Properties
      _vxvertex_property__sync_properties_WL_CS_NT( vertex_WL );

      // Vector
      if( vertex_WL->vector ) {
        iOperation.Vertex_WL.SetVector( vertex_WL, vertex_WL->vector );
      }

      iOperation.Close_CS( graph, &vertex_WL->operation, true );

      ret = 1;
    }
    __vertex_unlock_writable_CS( vertex_WL );
    Vertex_DECREF_WL( vertex_WL );
    if( ret > 0 ) {
      if( _vxdurable_operation__emitter_checkpoint_CS( graph, sync_context, 1 ) < 0 ) {
        ret = -1;
      }
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __operation_sync_vertices_CS_NT( vgx_Graph_t *self ) {
  int64_t n_vertices = 0;
  cxmalloc_object_processing_context_t sync_context = {0};
  sync_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_sync_vertex_CS_NT;
  sync_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
  int64_t cnt = 0;
  sync_context.output = &cnt; // ++ each iteration
  int64_t delta_counter = 0;
  sync_context.input = &delta_counter; // +obj_delta once in a while
  int64_t n_synced = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &sync_context );
  n_vertices = sync_context.n_objects_active;
  if( n_synced < 0 ) {
    WARN( 0x001, "Vertex sync incomplete" );
    return -1;
  }
  return n_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __cxmalloc_sync_vertex_outarcs_CS_NT( cxmalloc_object_processing_context_t *sync_context, vgx_Vertex_t *vertex ) {
  int64_t ret = 0;
  if( vertex && !__vertex_is_defunct( vertex ) ) {
    ret = _vxarcvector_cellproc__operation_sync_outarcs_CS_NT( vertex );
    if( _vxdurable_operation__emitter_checkpoint_CS( vertex->graph, sync_context, ret ) < 0 ) {
      ret = -1;
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __operation_sync_arcs_CS_NT( vgx_Graph_t *self ) {

  int64_t n_arcs = 0;
  cxmalloc_object_processing_context_t sync_context = {0};
  sync_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_sync_vertex_outarcs_CS_NT;
  sync_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
  int64_t cnt = 0;
  sync_context.output = &cnt; // ++ each iteration
  int64_t delta_counter = 0;
  sync_context.input = &delta_counter; // +obj_delta once in a while
  int64_t n_synced = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &sync_context );
  n_arcs = sync_context.n_objects_active;
  if( n_synced < 0 ) {
    WARN( 0x001, "Arc sync incomplete" );
    return -1;
  }

  return n_arcs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __cxmalloc_sync_virtual_vertex_CS_NT( cxmalloc_object_processing_context_t *sync_context, vgx_Vertex_t *vertex ) {
  int64_t ret = 0;
  if( vertex && !__vertex_is_defunct( vertex ) && __vertex_is_manifestation_virtual( vertex ) ) {
    vgx_Graph_t *graph = vertex->graph;
    vgx_Vertex_t *vertex_WL = __vertex_lock_writable_CS( vertex );
    Vertex_INCREF_WL( vertex_WL );
    if( iOperation.Open_CS( graph, &vertex_WL->operation, COMLIB_OBJECT( vertex_WL ), true ) < 0 ) {
      // ERROR: can't open operation
      ret = -1;
    }
    else {
      // Send delete for virtual vertex
      iOperation.Vertex_CS.Delete( vertex_WL );
      iOperation.Commit_CS( graph, &graph->operation, true );
      iOperation.Close_CS( graph, &vertex_WL->operation, true );
      ret = 1;
    }
    __vertex_unlock_writable_CS( vertex_WL );
    Vertex_DECREF_WL( vertex_WL );
    if( ret > 0 ) {
      // We don't count virtual deletes against progress, hence obj_cnt=0
      if( _vxdurable_operation__emitter_checkpoint_CS( vertex->graph, sync_context, 0 ) < 0 ) {
        ret = -1;
      }
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __operation_sync_virtual_vertices_CS_NT( vgx_Graph_t *self ) {
  int64_t n_vertices = 0;
  cxmalloc_object_processing_context_t sync_context = {0};
  sync_context.process_object = (f_cxmalloc_object_processor)__cxmalloc_sync_virtual_vertex_CS_NT;
  sync_context.object_class = COMLIB_CLASS( vgx_Vertex_t );
  int64_t cnt = 0;
  sync_context.output = &cnt; // ++ each iteration
  int64_t delta_counter = 0;
  sync_context.input = &delta_counter; // not used
  int64_t n_synced = CALLABLE( self->vertex_allocator )->ProcessObjects( self->vertex_allocator, &sync_context );
  n_vertices = sync_context.n_objects_active;
  if( n_synced < 0 ) {
    WARN( 0x001, "Virtual vertex deletes sync incomplete" );
    return -1;
  }
  return n_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__operation_sync_vertices_CS_NT_NOROG( vgx_Graph_t *self ) {
  int64_t n_vertices = 0;

  n_vertices = __operation_sync_vertices_CS_NT( self );

  // Commit
  COMMIT_GRAPH_OPERATION_CS( self );

  return n_vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__operation_sync_arcs_CS_NT_NOROG( vgx_Graph_t *self ) {
  int64_t n_arcs = 0;

  n_arcs = __operation_sync_arcs_CS_NT( self );

  // Commit
  COMMIT_GRAPH_OPERATION_CS( self );
   
  return n_arcs;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_vxtable__operation_sync_virtual_vertices_CS_NT_NOROG( vgx_Graph_t *self ) {
  int64_t n_vertices = 0;

  n_vertices = __operation_sync_virtual_vertices_CS_NT( self );

  // Commit
  COMMIT_GRAPH_OPERATION_CS( self );

  return n_vertices;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_vxtable.h"

test_descriptor_t _vgx_vxgraph_vxtable_tests[] = {
  { "VGX Vertex Index Tests", __utest_vxgraph_vxtable },

  {NULL}
};
#endif
