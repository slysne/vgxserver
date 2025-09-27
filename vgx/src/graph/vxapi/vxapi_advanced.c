/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxapi_advanced.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );

#define MAX_ATOMIC_BATCH_SIZE (1ULL << 15)

static int Graph_acquire_graph_readonly( vgx_Graph_t *self, int timeout_ms, bool force, vgx_AccessReason_t *reason );
static int Graph_is_graph_readonly( vgx_Graph_t *self );
static int Graph_release_graph_readonly( vgx_Graph_t *self );
static int Graph_freeze_readonly_CS( vgx_Graph_t *self, CString_t **CSTR__error );
static int Graph_freeze_readonly_OPEN( vgx_Graph_t *self, CString_t **CSTR__error );
static void Graph_unfreeze_readonly_CS( vgx_Graph_t *self );
static void Graph_unfreeze_readonly_OPEN( vgx_Graph_t *self );

static vgx_Vertex_t * Graph_acquire_vertex_readonly( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_by_address_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_writable_nocreate_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_acquire_vertex_writable_nocreate( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_acquire_vertex_by_address_writable_nocreate( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_writable( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );

static vgx_Vertex_t * Graph_escalate_readonly_to_writable( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_relax_writable_to_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );

static vgx_VertexList_t * Graph_atomic_acquire_vertices_readonly( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_VertexList_t * Graph_atomic_acquire_vertices_writable( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int64_t            Graph_atomic_release_vertices( vgx_Graph_t *self, vgx_VertexList_t **vertices_LCK );
static int64_t            Graph_atomic_unlock_writable_by_identifiers( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers_WL );

static bool Graph_has_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );
static bool Graph_has_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );
static vgx_Vertex_t * Graph_get_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid );
static CString_t * Graph_get_vertexid_by_address( vgx_Graph_t *self, QWORD address, vgx_AccessReason_t *reason );
static int Graph_get_vertex_internalid_by_address( vgx_Graph_t *self, QWORD address, objectid_t *obid,vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_object_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_object_writable( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_object_readonly_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_acquire_vertex_object_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason );
static vgx_Vertex_t * Graph_new_vertex( vgx_Graph_t *self,  const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static vgx_Vertex_t * Graph_new_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_create_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_create_return_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, vgx_Vertex_t **ret_vertex_WL, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int64_t Graph_commit_vertex( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL );
static bool Graph_release_vertex_CS( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );
static bool Graph_release_vertex( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK );

static int Graph_delete_vertex( vgx_Graph_t *self, vgx_Vertex_t *vertex_ANY, int timeout_ms, vgx_AccessReason_t *reason );
static int Graph_delete_isolated_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL);

static int Graph_connect_WL( vgx_Graph_t *self, vgx_Relationship_t *relationship, vgx_Arc_t *arc_WL, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_connect_M_INT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, int32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_connect_M_UINT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, uint32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_connect_M_FLT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, float value, vgx_AccessReason_t *reason, CString_t **CSTR__error );
static int Graph_connect_M_INT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, int32_t value, int timeout_ms, CString_t **CSTR__error );
static int Graph_connect_M_UINT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, uint32_t uvalue, int timeout_ms, CString_t **CSTR__error );
static int Graph_connect_M_FLT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, float value, int timeout_ms, CString_t **CSTR__error );

static int64_t Graph_disconnect_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL );

static void Graph_delete_collector( vgx_BaseCollector_context_t **collector );

static int Graph_get_open_vertices( vgx_Graph_t *self, int64_t thread_id_filter, Key64Value56List_t **readonly, Key64Value56List_t **writable );
static int64_t Graph_close_open_vertices( vgx_Graph_t *self );
static int64_t Graph_commit_writable_vertices( vgx_Graph_t *self );
static CString_t * Graph_get_writable_vertices_as_cstring( vgx_Graph_t *self, int64_t *n );

static vgx_MemoryInfo_t Graph_get_memory_info( vgx_Graph_t *self );

static int64_t Graph_reset_serial( vgx_Graph_t *self, int64_t sn );

static void DebugGraph_print_vertex_acquisition_maps( vgx_Graph_t *self );
static void DebugGraph_print_allocators( vgx_Graph_t *self, const char *alloc_name );
static int DebugGraph_check_allocators( vgx_Graph_t *self, const char *alloc_name );
static comlib_object_t * DebugGraph__get_object_at_address( vgx_Graph_t *self, QWORD address );
static comlib_object_t * DebugGraph__find_object_by_identifier( vgx_Graph_t *self, const char *identifier );
static CString_t * Graph__get_vertex_id_by_offset( vgx_Graph_t *self, int64_t *poffset );


static vgx_IGraphAdvanced_t AdvancedMethods = {
  .AcquireGraphReadonly                   = Graph_acquire_graph_readonly,
  .IsGraphReadonly                        = Graph_is_graph_readonly,
  .ReleaseGraphReadonly                   = Graph_release_graph_readonly,
  .FreezeGraphReadonly_CS                 = Graph_freeze_readonly_CS,
  .FreezeGraphReadonly_OPEN               = Graph_freeze_readonly_OPEN,
  .UnfreezeGraphReadonly_CS               = Graph_unfreeze_readonly_CS,
  .UnfreezeGraphReadonly_OPEN             = Graph_unfreeze_readonly_OPEN,
  .AcquireVertexReadonly                  = Graph_acquire_vertex_readonly,
  .AcquireVertexByAddressReadonly         = Graph_acquire_vertex_by_address_readonly,
  .AcquireVertexWritableNocreate_CS       = Graph_acquire_vertex_writable_nocreate_CS,
  .AcquireVertexWritableNocreate          = Graph_acquire_vertex_writable_nocreate,
  .AcquireVertexByAddressWritableNocreate = Graph_acquire_vertex_by_address_writable_nocreate,
  .AcquireVertexWritable                  = Graph_acquire_vertex_writable,
  
  .EscalateReadonlyToWritable             = Graph_escalate_readonly_to_writable,
  .RelaxWritableToReadonly                = Graph_relax_writable_to_readonly,

  .AtomicAcquireVerticesReadonly          = Graph_atomic_acquire_vertices_readonly,
  .AtomicAcquireVerticesWritable          = Graph_atomic_acquire_vertices_writable,
  .AtomicReleaseVertices                  = Graph_atomic_release_vertices,
  .AtomicUnlockByIdentifiers              = Graph_atomic_unlock_writable_by_identifiers,

  .HasVertex                              = Graph_has_vertex,
  .HasVertex_CS                           = Graph_has_vertex_CS,
  .GetVertex_CS                           = Graph_get_vertex_CS,
  .GetVertexIDByAddress                   = Graph_get_vertexid_by_address,
  .GetVertexInternalidByAddress           = Graph_get_vertex_internalid_by_address,
  .AcquireVertexObjectReadonly            = Graph_acquire_vertex_object_readonly,
  .AcquireVertexObjectWritable            = Graph_acquire_vertex_object_writable,
  .AcquireVertexObjectReadonly_CS         = Graph_acquire_vertex_object_readonly_CS,
  .AcquireVertexObjectWritable_CS         = Graph_acquire_vertex_object_writable_CS,
  .NewVertex                              = Graph_new_vertex,
  .NewVertex_CS                           = Graph_new_vertex_CS,
  .CreateVertex_CS                        = Graph_create_vertex_CS,
  .CreateReturnVertex_CS                  = Graph_create_return_vertex_CS,
  .CommitVertex                           = Graph_commit_vertex,
  .ReleaseVertex_CS                       = Graph_release_vertex_CS,
  .ReleaseVertex                          = Graph_release_vertex,

  .DeleteVertex                           = Graph_delete_vertex,
  .DeleteIsolatedVertex_CS_WL             = Graph_delete_isolated_vertex_CS_WL,

  .Connect_WL                             = Graph_connect_WL,
  .Connect_M_INT_WL                       = Graph_connect_M_INT_WL,
  .Connect_M_UINT_WL                      = Graph_connect_M_UINT_WL,
  .Connect_M_FLT_WL                       = Graph_connect_M_FLT_WL,
  .Connect_M_INT                          = Graph_connect_M_INT,
  .Connect_M_UINT                         = Graph_connect_M_UINT,
  .Connect_M_FLT                          = Graph_connect_M_FLT,

  .Disconnect_WL                          = Graph_disconnect_WL,

  .DeleteCollector                        = Graph_delete_collector,

  .GetOpenVertices                        = Graph_get_open_vertices,
  .CloseOpenVertices                      = Graph_close_open_vertices,
  .CommitWritableVertices                 = Graph_commit_writable_vertices,
  .GetWritableVerticesAsCString           = Graph_get_writable_vertices_as_cstring,

  .GetMemoryInfo                          = Graph_get_memory_info,

  .ResetSerial                            = Graph_reset_serial,

  .DebugPrintVertexAcquisitionMaps        = DebugGraph_print_vertex_acquisition_maps,
  .DebugPrintAllocators                   = DebugGraph_print_allocators,
  .DebugCheckAllocators                   = DebugGraph_check_allocators,
  .DebugGetObjectAtAddress                = DebugGraph__get_object_at_address,
  .DebugFindObjectByIdentifier            = DebugGraph__find_object_by_identifier,

  .GetVertexIDByOffset                    = Graph__get_vertex_id_by_offset
};



DLL_HIDDEN vgx_IGraphAdvanced_t * _vxapi_advanced = &AdvancedMethods;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_acquire_graph_readonly( vgx_Graph_t *self, int timeout_ms, bool force, vgx_AccessReason_t *reason ) {
  int ret;
  vgx_ExecutionTimingBudget_t timing_budget;
  uint32_t thread_id = GET_CURRENT_THREAD_ID();
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  GRAPH_LOCK( self ) {
    // SYSTEM is NULL if we are in the process of initializing the SYSTEM graph. If so, allow readonly.
    if( SYSTEM && iSystem.IsSystemGraph( self ) && iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
      ret = -1;
    }
    int64_t W = 0;
    _vxgraph_tracker__num_open_vertices_CS( self, thread_id, NULL, &W );
    if( W == 0 || force == true ) {
      if( W != 0 ) {
        WARN( 0x000, "Forcing readonly transition despite %lld vertices being in a writable state", W );
        if( timeout_ms < 1 ) {
          timeout_ms = 30000;
          WARN( 0x000, "Forcing readonly transition timeout to %d milliseconds", timeout_ms );
        }
      }

      timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
      if( (ret = _vxgraph_state__acquire_graph_readonly_CS( self, force, &timing_budget )) > 0 ) {
        _vxgraph_state__inc_explicit_readonly_CS( &self->readonly );
      }
      __set_access_reason( reason, timing_budget.reason );
    }
    else {
      __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
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
static int Graph_is_graph_readonly( vgx_Graph_t *self ) {
  bool is_RO;
  GRAPH_LOCK( self ) {
    is_RO = _vgx_is_readonly_CS( &self->readonly );
  } GRAPH_RELEASE;
  return is_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_release_graph_readonly( vgx_Graph_t *self ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    if( (ret = _vxgraph_state__release_graph_readonly_CS( self )) > 0 ) {
      _vxgraph_state__dec_explicit_readonly_CS( &self->readonly );
    }
  } GRAPH_RELEASE;
  return ret;
}



/*******************************************************************//**
 * Acquire another graph readonly lock if graph is already readonly.
 * If another lock was acquired it must be released with Graph_unfreeze_readonly() later.
 * If graph is writable, no action.
 ***********************************************************************
 */
static int Graph_freeze_readonly_CS( vgx_Graph_t *self, CString_t **CSTR__error ) {
  vgx_readonly_state_t *ros = &self->readonly;
  if( _vgx_is_readonly_CS( ros ) && !_vgx_has_readonly_transition_CS( ros ) ) {
    if( _vgx_readonly_acquire_CS( ros ) < 0 ) {
      __format_error_string( CSTR__error, "unable to secure readonly graph for query (r:%d ww:%d)", _vgx_get_readonly_readers_CS( ros ), _vgx_get_readonly_writers_waiting_CS( ros ) );
      return -1;
    }
    else {
      _vxgraph_state__inc_explicit_readonly_CS( ros );
      return 1;
    }
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * Acquire another graph readonly lock if graph is already readonly.
 * If another lock was acquired it must be released with Graph_unfreeze_readonly() later.
 * If graph is writable, no action.
 ***********************************************************************
 */
static int Graph_freeze_readonly_OPEN( vgx_Graph_t *self, CString_t **CSTR__error ) {
  int frozen;
  GRAPH_LOCK( self ) {
    frozen = Graph_freeze_readonly_CS( self, CSTR__error );
  } GRAPH_RELEASE;
  return frozen;
}



/*******************************************************************//**
 * Call this exactly once after calling Graph_freeze_readonly() IF AND ONLY IF
 * that method returned 1.
 ***********************************************************************
 */
static void Graph_unfreeze_readonly_CS( vgx_Graph_t *self ) {
  //
  // TODO: Unfreezing may be the last release of the 
  //       readonly lock (if the thread setting readonly in
  //       the first place has released readonly) and
  //       then this call will do all the heavy lifting
  //       associated with leaving readonly mode, such as
  //       resuming event processor thread and operation
  //       processor thread. This may take several seconds
  //       to complete, which is not a good thing when
  //       called by query threads.
  //
  if( _vxgraph_state__release_graph_readonly_CS( self ) > 0 ) {
    _vxgraph_state__dec_explicit_readonly_CS( &self->readonly );
  }
}



/*******************************************************************//**
 * Call this exactly once after calling Graph_freeze_readonly() IF AND ONLY IF
 * that method returned 1.
 ***********************************************************************
 */
static void Graph_unfreeze_readonly_OPEN( vgx_Graph_t *self ) {
  GRAPH_LOCK( self ) {
    Graph_unfreeze_readonly_CS( self );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_readonly( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_RO = NULL;
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  GRAPH_LOCK( self ) {
    vertex_RO = _vxgraph_state__acquire_readonly_vertex_CS( self, vertex_obid, &timing_budget );
  } GRAPH_RELEASE;
  __set_access_reason( reason, timing_budget.reason );
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_by_address_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_RO = NULL;
  if( vertex && COMLIB_OBJECT_ISINSTANCE( vertex, vgx_Vertex_t ) ) {
    vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
    GRAPH_LOCK( self ) {
      vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL ); // (may be NULL if we can't get lock)
    } GRAPH_RELEASE;
    __set_access_reason( reason, timing_budget.reason );
  }
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_INVALID );
  }
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_writable_nocreate_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  vgx_Vertex_t *vertex_WL = _vxgraph_state__acquire_writable_vertex_CS( self, NULL, vertex_obid, VERTEX_STATE_CONTEXT_MAN_NULL, &timing_budget, CSTR__error );
  __set_access_reason( reason, timing_budget.reason );
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_writable_nocreate( vgx_Graph_t *self, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex_WL;
  GRAPH_LOCK( self ) {
    vertex_WL = Graph_acquire_vertex_writable_nocreate_CS( self, vertex_obid, timeout_ms, reason, CSTR__error );
  } GRAPH_RELEASE;
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_by_address_writable_nocreate( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_WL = NULL;
  if( vertex && COMLIB_OBJECT_ISINSTANCE( vertex, vgx_Vertex_t ) ) {
    vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
    GRAPH_LOCK( self ) {
      if( (vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL )) != NULL ) {
        __vertex_set_manifestation_real( vertex_WL );
      }
    } GRAPH_RELEASE;
    __set_access_reason( reason, timing_budget.reason );
  }
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_INVALID );
  }
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_writable( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  vgx_Vertex_t *vertex_WL;
  GRAPH_LOCK( self ) {
    vertex_WL = _vxgraph_state__acquire_writable_vertex_CS( self, CSTR__vertex_idstr, vertex_obid, VERTEX_STATE_CONTEXT_MAN_REAL, &timing_budget, CSTR__error );
  } GRAPH_RELEASE;
  __set_access_reason( reason, timing_budget.reason );
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_escalate_readonly_to_writable( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  vgx_Vertex_t *vertex_WL;
  GRAPH_LOCK( self ) {
    vertex_WL = _vxgraph_state__escalate_to_writable_vertex_CS_RO( self, vertex_RO, &timing_budget, VGX_VERTEX_RECORD_ALL, CSTR__error );
  } GRAPH_RELEASE;
  __set_access_reason( reason, timing_budget.reason );
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_relax_writable_to_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  return _vxgraph_state__relax_to_readonly_vertex_OPEN_LCK( self, vertex_WL, VGX_VERTEX_RECORD_ALL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexList_t * Graph_atomic_acquire_vertices_readonly( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int64_t n_vertices = iVertex.Identifiers.Size( identifiers );

  // Check for duplicate identifiers
  if( !iVertex.Identifiers.CheckUnique( identifiers ) ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
    __set_error_string( CSTR__error, "duplicate vertices in acquisition set" );
    return NULL;
  }

  vgx_VertexList_t *vertices = iVertex.List.New( n_vertices );
  if( vertices == NULL ) {
    return NULL;
  }

  int64_t vidx = 0;
  vgx_Vertex_t *vertex = NULL;
  vgx_vertex_record record = VGX_VERTEX_RECORD_NONE;

  GRAPH_LOCK( self ) {
    XTRY {

      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
      _vgx_start_graph_execution_timing_budget( self, &timing_budget );
      while( vidx < n_vertices ) {
        if( (vertex = iVertex.Identifiers.GetVertex( identifiers, vidx )) != NULL ) {
          // Acquire RO with timing budget
          // NOTE: We don't record acquisition or operation here. It will be done later if mass-acquisition succeeds.
          if( _vxgraph_state__lock_vertex_readonly_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) { // <== NOTHING RECORDED HERE
            iVertex.List.Set( vertices, vidx++, vertex );
          }
          // Readonly acquisition timeout.
          else {
            __set_access_reason( reason, timing_budget.reason );
            __set_error_string( CSTR__error, CALLABLE( vertex )->IDString( vertex ) );
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
          }
        }
        // Terminate. Vertex does not exist.
        else {
          __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST_MSG );
          __set_error_string( CSTR__error, iVertex.Identifiers.GetId( identifiers, vidx ) );
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
        }
      }

      // Success!
      // Now we must track vertices into acquisition map
      record = VGX_VERTEX_RECORD_ALL;
      for( vidx = 0; vidx < n_vertices; vidx++ ) {
        vertex = iVertex.List.Get( vertices, vidx );
        // Track vertex in readonly acquisition map if lock state is readonly
        if( __vertex_is_readonly( vertex ) ) {
          _vxgraph_tracker__register_vertex_RO_for_thread_CS( self, vertex );
        }
        // Track vertex in writelocked acquisition map because it is was already locked writable 
        else {
          _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex );
        }
      }

    }
    XCATCH( errcode ) {
      for( vidx = 0; vidx < n_vertices; vidx++ ) {
        if( (vertex = iVertex.List.Get( vertices, vidx )) != NULL ) {
          _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, record );
        }
      }
      iVertex.List.Delete( &vertices );
      SIGNAL_VERTEX_AVAILABLE( self );
    }
    XFINALLY {
    }
  } GRAPH_RELEASE;

  return vertices;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VertexList_t * Graph_atomic_acquire_vertices_writable( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int64_t n_vertices = iVertex.Identifiers.Size( identifiers );
  if( n_vertices > MAX_ATOMIC_BATCH_SIZE ) {
    __format_error_string( CSTR__error, "Atomic acquisition of %lld vertices failed (max %lld)", n_vertices, MAX_ATOMIC_BATCH_SIZE );
    return NULL;
  }

  // Check for duplicate identifiers
  if( !iVertex.Identifiers.CheckUnique( identifiers ) ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
    __set_error_string( CSTR__error, "duplicate vertices in acquisition set" );
    return NULL;
  }

  vgx_VertexList_t *vertices = iVertex.List.New( n_vertices );
  if( vertices == NULL ) {
    return NULL;
  }

  int64_t n_wl = 0;
  int64_t vidx = 0;
  vgx_Vertex_t *vertex = NULL;
  vgx_vertex_record record = VGX_VERTEX_RECORD_NONE;

  GRAPH_LOCK( self ) {
    XTRY {

      if( _vgx_is_readonly_CS( &self->readonly ) ) {
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
      if( _vgx_has_readonly_request_CS( &self->readonly ) ) {
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_PENDING );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }

      // ---------------------------------------------------
      // Procedure 1: Optimistic non-blocking WL acquisition
      // ---------------------------------------------------
      vgx_ExecutionTimingBudget_t zero_budget = _vgx_get_graph_zero_execution_timing_budget( self );
      while( vidx < n_vertices ) {
        objectid_t obid = iVertex.Identifiers.GetObid( identifiers, vidx );
        if( (vertex = _vxgraph_vxtable__query_CS( self, NULL, &obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL ) {
          // Acquire WL non-blocking.
          // NOTE: We don't record acquisition or operation here. It will be done later if mass-acquisition succeeds.
          if( _vxgraph_state__lock_vertex_writable_CS( self, vertex, &zero_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {  // <== NOTHING RECORDED HERE
            // Got WL. We need to convert to REAL, but only after mass acquisition succeeds.
            iVertex.List.Set( vertices, vidx++, vertex );
            ++n_wl;
          }
          // Acquire RO non-blocking (we will escalate later)
          else if( _vxgraph_state__lock_vertex_readonly_CS( self, vertex, &zero_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {  // <== NOTHING RECORDED HERE
            // Got RO. We need to escalate to WL later, with timing budget
            iVertex.List.Set( vertices, vidx++, vertex );
          }
          // Optimistic acquisition cannot continue. (Vertex is writelocked by another thread.)
          else {
            break;
          }
        }
        // Terminate. Vertex does not exist.
        else {
          __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST_MSG );
          __set_error_string( CSTR__error, iVertex.Identifiers.GetId( identifiers, vidx ) );
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
        }
      }

      // ------------------------------------------------------------------------
      // Procedure 2: Partial acquisition at this point, keep trying with timeout
      // ------------------------------------------------------------------------
      if( n_wl < n_vertices ) {
        bool terminate = false;
        bool relax = false;
        unsigned loops = 0;
        vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
        _vgx_start_graph_execution_timing_budget( self, &timing_budget );
        BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( &timing_budget, self ) {
          BEGIN_EXECUTION_BLOCKED_WHILE( !terminate && n_wl < n_vertices ) {

            // Process next vertex (cycle)
            if( !(++vidx < n_vertices) ) {
              // For the first few iterations keep readonly locks while relaxing
              bool keep_RO = ++loops < 5;
              // We must release all WL and keep trying
              if( relax && n_wl > 0 ) {
                for( vidx = 0; vidx < n_vertices; vidx++ ) {
                  // Vertex is acquired
                  if( (vertex = iVertex.List.Get( vertices, vidx )) != NULL ) {
                    bool W = __vertex_is_locked_writable_by_current_thread( vertex );
                    if( W ) {
                      --n_wl;
                    }
                    // Vertex is WL by current thread and we are relaxing it to RO
                    if( W && keep_RO ) {
                      _vxgraph_state__relax_to_readonly_vertex_CS_LCK( self, vertex, VGX_VERTEX_RECORD_NONE );
                    }
                    // Vertex is either RO or we need to back off, we must fully release
                    else {
                      _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, VGX_VERTEX_RECORD_NONE );
                      iVertex.List.Set( vertices, vidx, NULL ); // Clear slot
                    }
                  }
                }
              }
              // Leave CS, none WL at this time
              SIGNAL_VERTEX_AVAILABLE( self );
              // Start off in a grabby mode
              if( keep_RO ) {
                WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
              }
              // Become nicer if lots of readlock churn
              // (We will not hold any locks during sleep!)
              else {
                GRAPH_SUSPEND_LOCK( self ) {
                  sleep_milliseconds( loops > 10 ? 10 : loops );
                } GRAPH_RESUME_LOCK;
              }
              // Start again
              vidx = 0;
              relax = false;
            }

            // Inspect acquisition list
            vertex = iVertex.List.Get( vertices, vidx );

            // Vertex not acquired, try to get WL
            if( vertex == NULL ) {
              objectid_t obid = iVertex.Identifiers.GetObid( identifiers, vidx );
              if( (vertex = _vxgraph_vxtable__query_CS( self, NULL, &obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL ) {
                int short_ms = n_wl > 0 ? 0 : 10; // acquire next non-blocking if any WL already held
                vgx_ExecutionTimingBudget_t short_budget = _vgx_get_graph_execution_timing_budget( self, short_ms );
                if( _vxgraph_state__lock_vertex_writable_CS( self, vertex, &short_budget, VGX_VERTEX_RECORD_NONE ) != NULL ) {  // <== NOTHING RECORDED HERE
                  // Got WL. We need to convert to REAL, but only after mass acquisition succeeds.
                  iVertex.List.Set( vertices, vidx, vertex );
                  ++n_wl; // got another WL
                }
                else {
                  __set_access_reason( reason, short_budget.reason );
                  relax = true;
                }
              }
              // Terminate. Vertex does not exist.
              else {
                __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST_MSG );
                __set_error_string( CSTR__error, iVertex.Identifiers.GetId( identifiers, vidx ) );
                terminate = true;
              }
            }
            // We have vertex acquired and it is  RO, try to escalate to WL
            else if( __vertex_is_locked_readonly( vertex ) ) {
              int short_ms = n_wl > 0 ? 0 : 10; // acquire next non-blocking if any WL already held
              vgx_ExecutionTimingBudget_t short_budget = _vgx_get_graph_execution_timing_budget( self, short_ms );
              if( _vxgraph_state__escalate_to_writable_vertex_CS_RO( self, vertex, &short_budget, VGX_VERTEX_RECORD_NONE, CSTR__error ) != NULL ) { // <== NOTHING RECORDED HERE
                ++n_wl; // got another WL
              }
              else {
                __set_access_reason( reason, short_budget.reason );
                relax = true;
              }
            }

          } END_EXECUTION_BLOCKED_WHILE;
        } END_EXECUTE_WITH_TIMING_BUDGET_CS;

        if( terminate ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
        }
        else if( n_wl < n_vertices ) {
          __format_error_string( CSTR__error, "failed to acquire %lld of %lld vertices", n_vertices - n_wl, n_vertices );
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
        }
      }

      // Success!
      // Now we must open operation for all vertices and track them into acquisition map
      __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
      record = VGX_VERTEX_RECORD_ALL;
      for( vidx = 0; vidx < n_vertices; vidx++ ) {
        vertex = iVertex.List.Get( vertices, vidx );
        // Ensure REAL vertex
        _vxgraph_state__convert_WL( self, vertex, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_OPERATION );
        // Track vertex in writelocked acquisition map
        _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex );
      }

      // Capture operation
      if( iOperation.Lock_CS.AcquireWL( self, vertices, CSTR__error ) < 0 ) {
        __set_access_reason( reason, VGX_ACCESS_REASON_OPFAIL );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
      }

    }
    XCATCH( errcode ) {
      for( vidx = 0; vidx < n_vertices; vidx++ ) {
        if( (vertex = iVertex.List.Get( vertices, vidx )) != NULL ) {
          _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, record );
        }
      }
      iVertex.List.Delete( &vertices );
      SIGNAL_VERTEX_AVAILABLE( self );
    }
    XFINALLY {
    }
  } GRAPH_RELEASE;

  return vertices;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_atomic_release_vertices( vgx_Graph_t *self, vgx_VertexList_t **vertices_LCK ) {
  int64_t n = 0;
  if( vertices_LCK && *vertices_LCK ) {
    int64_t n_vertices = iVertex.List.Size( *vertices_LCK );
    GRAPH_LOCK( self ) {
      // Ensure all vertices are closable by this thread
      for( int64_t vidx=0; vidx < n_vertices; vidx++ ) {
        vgx_Vertex_t *vertex = iVertex.List.Get( *vertices_LCK, vidx );
        // Only proceed if vertex is locked, and if writable also owned by current thread.
        if( __vertex_is_unlocked( vertex ) || (__vertex_is_writable( vertex ) && !__vertex_is_writer_current_thread( vertex )) ) {
          n = -1;
          break;
        }
      }

      // Vertices are closable
      if( n == 0 ) {
        // Capture operation
        iOperation.Unlock_CS.ReleaseLCK( self, *vertices_LCK );

        // Unlock all and remove from acquisition map
        for( int64_t vidx=0; vidx < n_vertices; vidx++ ) {
          vgx_Vertex_t *vertex = iVertex.List.Get( *vertices_LCK, vidx );
          n += _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, VGX_VERTEX_RECORD_ACQUISITION );
        }
      }
    } GRAPH_RELEASE;
    // Delete the supplied vertex list
    iVertex.List.Delete( vertices_LCK );
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_atomic_unlock_writable_by_identifiers( vgx_Graph_t *self, vgx_VertexIdentifiers_t *identifiers_WL ) {
  int64_t n = 0;
  int64_t n_vertices = iVertex.Identifiers.Size( identifiers_WL );
  uint32_t thread_id = GET_CURRENT_THREAD_ID();

  GRAPH_LOCK( self ) {
    // All supplied vertex identifiers
    for( int64_t i=0; i<n_vertices; i++ ) {
      vgx_Vertex_t *vertex = iVertex.Identifiers.GetVertex( identifiers_WL, i );
      // Vertex exists and is writelocked by current thread
      if( vertex && __vertex_is_locked_writable_by_thread( vertex, thread_id ) ) {
        // then release
        n += _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex, VGX_VERTEX_RECORD_ACQUISITION );
      }
    }
  } GRAPH_RELEASE;

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_has_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid ) {
  return _vxgraph_vxtable__exists_OPEN( self, CSTR__vertex_name, obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_has_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid ) {
  return _vxgraph_vxtable__exists_CS( self, CSTR__vertex_name, obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_get_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const objectid_t *obid ) {
  return _vxgraph_vxtable__query_CS( self, CSTR__vertex_name, obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * Graph_get_vertexid_by_address( vgx_Graph_t *self, QWORD address, vgx_AccessReason_t *reason ) {
  CString_t *CSTR__id = NULL;
  GRAPH_LOCK( self ) {
    comlib_object_t *obj = CALLABLE( self->vertex_allocator )->GetObjectAtAddress( self->vertex_allocator, address );
    if( obj && _cxmalloc_is_object_active( obj ) ) {
      vgx_Vertex_t *vertex = (vgx_Vertex_t*)obj;
      if( COMLIB_OBJECT_ISINSTANCE( vertex, vgx_Vertex_t ) ) {
        if( (CSTR__id = vertex->identifier.CSTR__idstr) != NULL && CSTR__id->allocator_context ) {
          _cxmalloc_object_incref_nolock( CSTR__id );
        }
        else {
          CSTR__id = NewEphemeralCString( self, vertex->identifier.idprefix.data );
        }
      }
      else {
        // Object found but not a valid vertex object
        __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
      }
    }
    else {
      // No object at given address
      __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
    }
  } GRAPH_RELEASE;
  return CSTR__id;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_get_vertex_internalid_by_address( vgx_Graph_t *self, QWORD address, objectid_t *obid, vgx_AccessReason_t *reason ) {
  int ret = 0;
  GRAPH_LOCK( self ) {
    comlib_object_t *obj = CALLABLE( self->vertex_allocator )->GetObjectAtAddress( self->vertex_allocator, address );
    if( obj && _cxmalloc_is_object_active( obj ) ) {
      vgx_Vertex_t *vertex = (vgx_Vertex_t*)obj;
      if( COMLIB_OBJECT_ISINSTANCE( vertex, vgx_Vertex_t ) ) {
        idcpy( obid, __vertex_internalid( vertex ) );
      }
      else {
        // Object found but not a valid vertex object
        __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
        ret = -1;
      }
    }
    else {
      // No object at given address
      __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
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
static vgx_Vertex_t * Graph_acquire_vertex_object_readonly( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_RO;
  GRAPH_LOCK( self ) {
    vertex_RO = Graph_acquire_vertex_object_readonly_CS( self, vertex, timeout_ms, reason );
  } GRAPH_RELEASE;
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_object_writable( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_WL;
  GRAPH_LOCK( self ) {
    vertex_WL = Graph_acquire_vertex_object_writable_CS( self, vertex, timeout_ms, reason );
  } GRAPH_RELEASE;
  return vertex_WL;     
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_object_readonly_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_RO;
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL );
  __set_access_reason( reason, timing_budget.reason );
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_acquire_vertex_object_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  vgx_Vertex_t *vertex_WL;
  vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
  if( (vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL )) != NULL ) {
    // Make sure its REAL. Outsiders (who acquire the vertex) only work with REAL vertices.
    _vxgraph_state__convert_WL( self, vertex_WL, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL );
  }
  __set_access_reason( reason, timing_budget.reason );
  return vertex_WL;     
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_new_vertex( vgx_Graph_t *self, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex_WL = NULL;
  const objectid_t *vertex_obid = CStringObid( CSTR__vertex_name );
  GRAPH_LOCK( self ) {
    _vxgraph_state__create_vertex_CS( self, CSTR__vertex_name, vertex_obid, CSTR__vertex_typename, &vertex_WL, timeout_ms, reason, CSTR__error );
  } GRAPH_RELEASE;
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * Graph_new_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex_WL = NULL;
  _vxgraph_state__create_vertex_CS( self, CSTR__vertex_name, vertex_obid, CSTR__vertex_typename, &vertex_WL, timeout_ms, reason, CSTR__error );
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_create_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  return _vxgraph_state__create_vertex_CS( self, CSTR__vertex_name, vertex_obid, CSTR__vertex_typename, NULL, timeout_ms, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_create_return_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_name, const CString_t *CSTR__vertex_typename, vgx_Vertex_t **ret_vertex_WL, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  return _vxgraph_state__create_vertex_CS( self, CSTR__vertex_name, vertex_obid, CSTR__vertex_typename, ret_vertex_WL, timeout_ms, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_commit_vertex( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  int64_t commit_op;
  GRAPH_LOCK( self ) {
    commit_op = _vxdurable_commit__commit_vertex_CS_WL( self, vertex_WL, false );
  } GRAPH_RELEASE;
  return commit_op;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_release_vertex_CS( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK ) {
  return _vxgraph_state__release_vertex_CS_LCK( self, vertex_LCK );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Graph_release_vertex( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK ) {
  return _vxgraph_state__release_vertex_OPEN_LCK( self, vertex_LCK );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_delete_vertex( vgx_Graph_t *self, vgx_Vertex_t *vertex_ANY, int timeout_ms, vgx_AccessReason_t *reason ) {
  int n_deleted = 0;
  // Hold the state lock and lock the vertex - stay within CS for the duration of deletion
  GRAPH_LOCK( self ) {
    // Yield inarcs of all currently writelocked vertices for current thread, except vertex to be deleted (if already writelocked)
    ENTER_SAFE_MULTILOCK_CS( self, vertex_ANY, reason ) {
      // Obtain exclusive write lock on vertex and own a reference
      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
      vgx_Vertex_t *vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex_ANY, &timing_budget, VGX_VERTEX_RECORD_OPERATION );
      __set_access_reason( reason, timing_budget.reason );
      if( vertex_WL ) {
        if( (n_deleted = _vxdurable_commit__delete_vertex_CS_WL( self, &vertex_WL, &timing_budget, VGX_VERTEX_RECORD_OPERATION )) < 0 ) {
          __set_access_reason( reason, timing_budget.reason );
        }
      }
      else {
        n_deleted = -1;
      }
    } LEAVE_SAFE_MULTILOCK_CS( n_deleted = -1 );
  } GRAPH_RELEASE;
  return n_deleted;
}



/*******************************************************************//**
 *
 * Returns: 1: Vertex deleted
 *          0: Vertex not deleted
 *         -1: Vertex cannot be deleted (not isolated)
 ***********************************************************************
 */
static int Graph_delete_isolated_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL) {
  if( __vertex_is_isolated( vertex_WL ) ) {
    vgx_ExecutionTimingBudget_t zero_budget = _vgx_get_graph_zero_execution_timing_budget( self );
    return _vxdurable_commit__delete_vertex_CS_WL( self, &vertex_WL, &zero_budget, VGX_VERTEX_RECORD_OPERATION );
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 * Returns: >= 1: Created connection
 *             0: Connection already existed
 *            -1: Error
 *
 ***********************************************************************
 */
static int Graph_connect_WL( vgx_Graph_t *self, vgx_Relationship_t *relationship, vgx_Arc_t *arc_WL, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {

  // Finalize the arc relationship
  iRelation.SetStoredRelationship( self, arc_WL, relationship );

  // Check relationship code
  vgx_predicator_rel_enum erel = arc_WL->head.predicator.rel.enc;
  if( !__relationship_enumeration_valid( erel ) ) {
    switch( erel ) {
    case VGX_PREDICATOR_REL_INVALID:
      __format_error_string( CSTR__error, "invalid relationship: '%s'", relationship->CSTR__name ? CStringValue( relationship->CSTR__name ) : "?" );
      return -1;
    case VGX_PREDICATOR_REL_NO_MAPPING:
      __set_access_reason( reason, VGX_ACCESS_REASON_ENUM_NOTYPESPACE );
      return -1;
    default:
      __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
      return -1;
    }
  }

  // Commit vertices if not yet committed
  vgx_Vertex_t *ucA = __vertex_is_suspended_context( arc_WL->tail ) ? arc_WL->tail : NULL;
  vgx_Vertex_t *ucB = __vertex_is_suspended_context( arc_WL->head.vertex ) ? arc_WL->head.vertex : NULL;
  if( ucA || ucB ) {
    GRAPH_LOCK( self ) {
      if( ucA ) {
        _vxdurable_commit__commit_vertex_CS_WL( self, ucA, false );
      }
      if( ucB ) {
        _vxdurable_commit__commit_vertex_CS_WL( self, ucB, false );
      }
    } GRAPH_RELEASE;
  }

  // Create arc
  return _vxgraph_state__create_arc_WL( self, arc_WL, lifespan, arc_condition_set, reason, CSTR__error );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __connect_MOD_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, vgx_predicator_modifier_enum mod, vgx_predicator_val_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int ret;

  vgx_Arc_t Arc = {0};
  vgx_Relationship_t Rel = {0};

  Arc.tail = initial_WL,
  Arc.head.vertex = terminal_WL;
  if( relationship ) {
    if( (Rel.CSTR__name = NewEphemeralCString( self, relationship )) == NULL ) {
      return -1;
    }
  }

  Rel.mod_enum = mod;
  Rel.value = value;

  ret = Graph_connect_WL( self, &Rel, &Arc, -1, NULL, reason, CSTR__error );

  iString.Discard( &Rel.CSTR__name );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_INT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, int32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .integer = value };
  return __connect_MOD_WL( self, initial_WL, terminal_WL, relationship, VGX_PREDICATOR_MOD_INTEGER, predval, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_UINT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, uint32_t value, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .uinteger = value };
  return __connect_MOD_WL( self, initial_WL, terminal_WL, relationship, VGX_PREDICATOR_MOD_UNSIGNED, predval, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_FLT_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal_WL, const char *relationship, float value, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .real = value };
  return __connect_MOD_WL( self, initial_WL, terminal_WL, relationship, VGX_PREDICATOR_MOD_FLOAT, predval, reason, CSTR__error );
}



/*******************************************************************//**
 *
 *
 * Returns: >= 1: Created connection
 *             0: Connection already existed
 *            -1: Error
 *
 ***********************************************************************
 */
static int __connect_MOD( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, vgx_predicator_modifier_enum mod, vgx_predicator_val_t value, int timeout_ms, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_VertexIdentifiers_t *pair = NULL;
  vgx_VertexList_t *vertices = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  XTRY {
    if( (pair = iVertex.Identifiers.NewPair( self, initial, terminal )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
    if( (vertices = Graph_atomic_acquire_vertices_writable( self, pair, timeout_ms, &reason, CSTR__error)) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }
    vgx_Vertex_t *V_init = iVertex.List.Get( vertices, 0 );
    vgx_Vertex_t *V_term = iVertex.List.Get( vertices, 1 );

    if( (ret = __connect_MOD_WL( self, V_init, V_term, relationship, mod, value, &reason, CSTR__error )) < 0 ) { 
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( vertices ) {
      Graph_atomic_release_vertices( self, &vertices );
    }
    if( pair ) {
      iVertex.Identifiers.Delete( &pair );
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_INT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, int32_t value, int timeout_ms, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .integer = value };
  return __connect_MOD( self, initial, terminal, relationship, VGX_PREDICATOR_MOD_INTEGER, predval, timeout_ms, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_UINT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, uint32_t uvalue, int timeout_ms, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .uinteger = uvalue };
  return __connect_MOD( self, initial, terminal, relationship, VGX_PREDICATOR_MOD_UNSIGNED, predval, timeout_ms, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_connect_M_FLT( vgx_Graph_t *self, const char *initial, const char *terminal, const char *relationship, float value, int timeout_ms, CString_t **CSTR__error ) {
  vgx_predicator_val_t predval = { .real = value };
  return __connect_MOD( self, initial, terminal, relationship, VGX_PREDICATOR_MOD_FLOAT, predval, timeout_ms, CSTR__error );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t Graph_disconnect_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL ) {
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Graph_delete_collector( vgx_BaseCollector_context_t **collector ) {
  iGraphCollector.DeleteCollector( collector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Graph_get_open_vertices( vgx_Graph_t *self, int64_t thread_id_filter, VertexAndInt64List_t **readonly, VertexAndInt64List_t **writable ) {
  return _vxgraph_tracker__get_open_vertices_OPEN( self, thread_id_filter, readonly, writable );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_close_open_vertices( vgx_Graph_t *self ) {
  int64_t n;
  uint32_t thread_id = GET_CURRENT_THREAD_ID();
  GRAPH_LOCK( self ) {
    // Close all vertices held by current thread
    n = _vxgraph_tracker__close_open_vertices_CS( self, thread_id );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_commit_writable_vertices( vgx_Graph_t *self ) {
  uint32_t thread_id = GET_CURRENT_THREAD_ID();
  return _vxgraph_tracker__commit_writable_vertices_OPEN( self, thread_id );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * Graph_get_writable_vertices_as_cstring( vgx_Graph_t *self, int64_t *n ) {
  return _vxgraph_tracker__writable_vertices_as_cstring_OPEN( self, n );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AllocatorInfo_t * __set_allocator_info_CS( vgx_Graph_t *self, cxmalloc_family_t *alloc, vgx_AllocatorInfo_t *dest ) {
  GRAPH_SUSPEND_LOCK( self ) {
    dest->bytes = CALLABLE( alloc )->Bytes( alloc );
    dest->utilization = CALLABLE( alloc )->Utilization( alloc );
  } GRAPH_RESUME_LOCK;
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AllocatorInfo_t * __merge_allocator_info( vgx_AllocatorInfo_t *dest, vgx_AllocatorInfo_t *other ) {
  int64_t bytes = dest->bytes + other->bytes;
  double max_bytes = (dest->bytes / (dest->utilization > 0 ? dest->utilization : 1.0)) + (other->bytes / (other->utilization > 0 ? other->utilization : 1.0));
  dest->bytes = bytes;
  dest->utilization = max_bytes > 0 ? bytes / max_bytes : 0.0;
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AllocatorInfo_t * __from_dynamic_CS( vgx_Graph_t *self, framehash_dynamic_t *dyn, vgx_AllocatorInfo_t *dest ) {
  vgx_AllocatorInfo_t tmp;
  vgx_AllocatorInfo_t *info = __merge_allocator_info( __set_allocator_info_CS( self, dyn->balloc, dest ), __set_allocator_info_CS( self, dyn->falloc, &tmp ) );
  return info;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AllocatorInfo_t * __from_framehash_CS( vgx_Graph_t *self, framehash_t *fh, vgx_AllocatorInfo_t *dest ) {
  if( fh ) {
    __from_dynamic_CS( self, &fh->_dynamic, dest );
  }
  else {
    dest->bytes = 0;
    dest->utilization = 0.0;
  }
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_system_meminfo( vgx_MemoryInfo_t *meminfo ) {
  int64_t global_use_pct;
  int mem = get_system_physical_memory(
              &meminfo->system.global.physical.bytes,
              &global_use_pct,
              &meminfo->system.process.use.bytes
            );

  // Failed to get memory, set defaults
  if( mem < 0 ) {
  }
  // Set utilization
  else {
    meminfo->system.global.physical.utilization = (double)global_use_pct / 100.0;
    meminfo->system.process.use.utilization = (double)meminfo->system.process.use.bytes / meminfo->system.global.physical.bytes;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_MemoryInfo_t Graph_get_memory_info( vgx_Graph_t *self ) {
  vgx_MemoryInfo_t meminfo = {0};

#define TOTAL_POOLED( Src ) __merge_allocator_info( &meminfo.pooled.total, (Src) )

  vgx_AllocatorInfo_t tmp;
  GRAPH_LOCK( self ) {

    // vertex.object
    TOTAL_POOLED( __set_allocator_info_CS( self, self->vertex_allocator, &meminfo.pooled.vertex.object ) );
    // vertex.arcvector
    TOTAL_POOLED( __from_dynamic_CS( self, &self->arcvector_fhdyn, &meminfo.pooled.vertex.arcvector ) );
    // vertex.property
    TOTAL_POOLED( __from_dynamic_CS( self, &self->property_fhdyn, &meminfo.pooled.vertex.property ) );

    // string.data
    TOTAL_POOLED( __set_allocator_info_CS( self, (cxmalloc_family_t*)self->property_allocator_context->allocator, &meminfo.pooled.string.data ) );

    // index.global
    TOTAL_POOLED( __from_framehash_CS( self, self->vxtable, &meminfo.pooled.index.global ) );
    // index.type
    for( int vx=0; vx<256; vx++ ) {
      __merge_allocator_info( &meminfo.pooled.index.type, __from_framehash_CS( self, self->vxtypeidx[ vx ], &tmp ) );
    }
    TOTAL_POOLED( &meminfo.pooled.index.type );

    // codec.vxtype
    TOTAL_POOLED( __merge_allocator_info( __from_framehash_CS( self, self->vxtype_encoder, &meminfo.pooled.codec.vxtype ), __from_framehash_CS( self, self->vxtype_decoder, &tmp ) ) );
    // codec.rel
    TOTAL_POOLED( __merge_allocator_info( __from_framehash_CS( self, self->rel_encoder, &meminfo.pooled.codec.rel ), __from_framehash_CS( self, self->rel_decoder, &tmp ) ) );
    // codec.vxprop
    TOTAL_POOLED( __merge_allocator_info( __from_framehash_CS( self, self->vxprop_keymap, &meminfo.pooled.codec.vxprop ), __from_framehash_CS( self, self->vxprop_valmap, &tmp ) ) );
    // codec.dim
    if( self->similarity->dim_encoder && self->similarity->dim_decoder ) {
      TOTAL_POOLED( __merge_allocator_info( __from_framehash_CS( self, self->similarity->dim_encoder, &meminfo.pooled.codec.dim ), __from_framehash_CS( self, self->similarity->dim_decoder, &tmp ) ) );
    }

    // vector.internal
    TOTAL_POOLED( __set_allocator_info_CS( self, self->similarity->int_vector_allocator, &meminfo.pooled.vector.internal ) );
    // vector.external
    TOTAL_POOLED( __set_allocator_info_CS( self, self->similarity->ext_vector_allocator, &meminfo.pooled.vector.external ) );
    // vector.dimension
    if( self->similarity->dimension_allocator_context ) {
      TOTAL_POOLED( __set_allocator_info_CS( self, (cxmalloc_family_t*)self->similarity->dimension_allocator_context->allocator, &meminfo.pooled.vector.dimension ) );
    }

    // schedule.map.evplong
    __from_framehash_CS( self, self->EVP.Schedule.LongTerm, &meminfo.pooled.schedule.map.evplong );
    // schedule.map.evpmedium
    __from_framehash_CS( self, self->EVP.Schedule.MediumTerm, &meminfo.pooled.schedule.map.evpmedium );
    // schedule.map.evpshort
    __from_framehash_CS( self, self->EVP.Schedule.ShortTerm, &meminfo.pooled.schedule.map.evpshort );
    // schedule.total
    __merge_allocator_info( &meminfo.pooled.schedule.total, &meminfo.pooled.schedule.map.evplong );
    __merge_allocator_info( &meminfo.pooled.schedule.total, &meminfo.pooled.schedule.map.evpmedium );
    __merge_allocator_info( &meminfo.pooled.schedule.total, &meminfo.pooled.schedule.map.evpshort );
    TOTAL_POOLED( &meminfo.pooled.schedule.total );

    // ephemeral.string
    TOTAL_POOLED( __set_allocator_info_CS( self, (cxmalloc_family_t*)self->ephemeral_string_allocator_context->allocator, &meminfo.pooled.ephemeral.string ) );
    // ephemeral.vector
    __set_allocator_info_CS( self, self->similarity->int_vector_ephemeral_allocator, &meminfo.pooled.ephemeral.vector );
    __set_allocator_info_CS( self, self->similarity->ext_vector_ephemeral_allocator, &tmp );
    TOTAL_POOLED( __merge_allocator_info( &meminfo.pooled.ephemeral.vector, &tmp ) );
    // ephemeral.vtxmap
    TOTAL_POOLED( __from_dynamic_CS( self, &self->vtxmap_fhdyn, &meminfo.pooled.ephemeral.vtxmap ) );
  } GRAPH_RELEASE;

  // SYSTEM
  __set_system_meminfo( &meminfo );

  return meminfo;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Graph_reset_serial( vgx_Graph_t *self, int64_t sn ) {
  GRAPH_LOCK( self ) {
    int64_t previous_sn = self->tx_serial_in;

    // Negative sn, rewind previous the given number of steps
    if( sn < 0 ) {
      if( (sn = previous_sn + sn) < 0 ) {
        sn = 0;
      }
    }
    // Set new serial number if different from current
    if( sn != previous_sn ) {
      self->tx_serial_in = sn;    // adjust serial
      idunset( &self->tx_id_in ); // clear transaction ID
      VERBOSE( 0x001, "Reset graph '%s' serial number from %lld to %lld", CALLABLE( self )->FullPath( self ), previous_sn, sn );
    }

  } GRAPH_RELEASE;

  return sn;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __print_vertex_acquisition_map_CS( vgx_Graph_t *graph, framehash_cell_t *map, framehash_dynamic_t *dyn, const char *header, const char *prefix ) {
  CStringQueue_t *stream = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
  Key64Value56List_t *threads = iFramehash.simple.IntItems( map, dyn, NULL );
  Key64Value56_t item;
  if( threads ) {
    size_t sz_header = strlen(header);
    char *dashes = calloc( sz_header + 1, 1 );
    if( dashes ) {
      memset( dashes, '-', sz_header );
      printf( "--------%s--------\n", dashes );
      printf( "------- %s -------\n", header );
      printf( "--------%s--------\n", dashes );
      int64_t n_threads = CALLABLE( threads )->Length( threads );
      for( int64_t i=0; i<n_threads; i++ ) {
        CALLABLE( threads )->Get( threads, i, &item.m128 );
        int64_t threadid = item.key;
        framehash_cell_t *vertex_map = (framehash_cell_t*)item.value56;
        if( vertex_map ) {
          Key64Value56List_t *vertex_list = iFramehash.simple.IntItems( vertex_map, dyn, NULL );
          Key64Value56_t vertex_entry;
          if( vertex_list ) {
            int64_t n_vertices = CALLABLE( vertex_list )->Length( vertex_list );
            if( n_vertices > 0 ) {
              printf( "THREAD: %lld\n", threadid );
              for( int64_t vx=0; vx<n_vertices; vx++ ) {
                CALLABLE( vertex_list )->Get( vertex_list, vx, &vertex_entry.m128 );
                vgx_Vertex_t *vertex_LCK = (vgx_Vertex_t*)vertex_entry.key;
                int64_t recursion = vertex_entry.value56;
                CString_t *CSTR__id = Graph_get_vertexid_by_address( graph, (QWORD)vertex_LCK, NULL );
                if( CSTR__id ) {
                  const char *name = CStringValue( CSTR__id );
                  char *descriptor = NULL;
                  CALLABLE( vertex_LCK )->Descriptor( vertex_LCK, stream );
                  CALLABLE( stream )->ReadNolock( stream, (void**)&descriptor, -1 );
                  if( descriptor ) {
                    printf( "  %s=%-3lld : %s  %s\n", prefix, recursion, descriptor, name );
                    ALIGNED_FREE( descriptor );
                  }
                  iString.Discard( &CSTR__id );
                }
                else {
                  printf( "  %s=%-3lld : ?  ?\n", prefix, recursion );
                }
              }
            }
            COMLIB_OBJECT_DESTROY( vertex_list );
          }
          else {
            printf( "  *** error ***\n" );
          }
        }
      }
      free( dashes );
      COMLIB_OBJECT_DESTROY( threads );
    }
  }
  COMLIB_OBJECT_DESTROY( stream );
  printf( "\n" );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void DebugGraph_print_vertex_acquisition_maps( vgx_Graph_t *self ) {
  GRAPH_LOCK( self ) {
    __print_vertex_acquisition_map_CS( self, self->vtxmap_RO, &self->vtxmap_fhdyn, "Readonly vertices", "RO" );
    __print_vertex_acquisition_map_CS( self, self->vtxmap_WL, &self->vtxmap_fhdyn, "Writable vertices", "WL" );
  } GRAPH_RELEASE;
}


#define __match_alloc_name(Name) (alloc_name == NULL || !strcmp( alloc_name, Name ))


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void DebugGraph_print_allocators( vgx_Graph_t *self, const char *alloc_name ) {

  GRAPH_LOCK( self ) {
    // Similarity allocators
    if( __match_alloc_name( "similarity" ) ) {
      CALLABLE( self->similarity )->PrintAllocators( self->similarity );
    }

    // Arcvector framehash dynamic
    if( __match_alloc_name( "arcvector_fhdyn" ) ) {
      iFramehash.dynamic.PrintAllocators( &self->arcvector_fhdyn );
    }

    // Property framehash dynamic
    if( __match_alloc_name( "property_fhdyn" ) ) {
      iFramehash.dynamic.PrintAllocators( &self->property_fhdyn );
    }

    // Vertex acquisition map framehash dynamic
    if( __match_alloc_name( "vtxmap_fhdyn" ) ) {
      iFramehash.dynamic.PrintAllocators( &self->vtxmap_fhdyn );
    }

    // Ephemeral string allocator context
    cxmalloc_family_t *ephemeral_string_allocator = (cxmalloc_family_t*)self->ephemeral_string_allocator_context->allocator;
    if( __match_alloc_name( "ephemeral_string_allocator" ) ) {
      PRINT( ephemeral_string_allocator );
    }

    // Property allocator context
    cxmalloc_family_t *property_allocator = (cxmalloc_family_t*)self->property_allocator_context->allocator;
    if( __match_alloc_name( "property_allocator" ) ) {
      PRINT( property_allocator );
    }

    // Vertex type encoder
    if( __match_alloc_name( "vxtype_encoder" ) ) {
      CALLABLE( self->vxtype_encoder )->PrintAllocators( self->vxtype_encoder );
    }

    // Vertex type decoder
    if( __match_alloc_name( "vxtype_decoder" ) ) {
      CALLABLE( self->vxtype_decoder )->PrintAllocators( self->vxtype_decoder );
    }

    // Relationship type encoder
    if( __match_alloc_name( "rel_encoder" ) ) {
      CALLABLE( self->rel_encoder )->PrintAllocators( self->rel_encoder );
    }

    // Relationship type decoder
    if( __match_alloc_name( "rel_decoder" ) ) {
      CALLABLE( self->rel_decoder )->PrintAllocators( self->rel_decoder );
    }

    // Vertex property keymap
    if( __match_alloc_name( "vxprop_keymap" ) ) {
      CALLABLE( self->vxprop_keymap )->PrintAllocators( self->vxprop_keymap );
    }

    // Vertex property valmap
    if( __match_alloc_name( "vxprop_valmap" ) ) {
      CALLABLE( self->vxprop_valmap )->PrintAllocators( self->vxprop_valmap );
    }

    // Vertex allocator
    if( __match_alloc_name( "vertex_allocator" ) ) {
      PRINT( self->vertex_allocator );
    }

    // Vertex general index
    if( __match_alloc_name( "vxtable" ) ) {
      CALLABLE( self->vxtable )->PrintAllocators( self->vxtable );
    }

    // Vertex type index
    if( __match_alloc_name( "vxtypeidx" ) ) {
      for( int vx=0; vx<256; vx++ ) {
        framehash_t *idx = self->vxtypeidx[ vx ];
        if( idx ) {
          CALLABLE( idx )->PrintAllocators( idx );
        }
      }
    }
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int DebugGraph_check_allocators( vgx_Graph_t *self, const char *alloc_name ) {
  int err = 0;

  GRAPH_LOCK( self ) {

    // Similarity allocators
    if( __match_alloc_name( "similarity" ) ) {
      err += CALLABLE( self->similarity )->CheckAllocators( self->similarity );
    }

    // Arcvector framehash dynamic
    if( __match_alloc_name( "arcvector_fhdyn" ) ) {
      err += iFramehash.dynamic.CheckAllocators( &self->arcvector_fhdyn );
    }

    // Property framehash dynamic
    if( __match_alloc_name( "property_fhdyn" ) ) {
      err += iFramehash.dynamic.CheckAllocators( &self->property_fhdyn );
    }

    // Vertex acquisition map framehash dynamic
    if( __match_alloc_name( "vtxmap_fhdyn" ) ) {
      err += iFramehash.dynamic.CheckAllocators( &self->vtxmap_fhdyn );
    }

    // Ephemeral string allocator context
    if( __match_alloc_name( "ephemeral_string_allocator" ) ) {
      cxmalloc_family_t *ephemeral_string_allocator = (cxmalloc_family_t*)self->ephemeral_string_allocator_context->allocator;
      err += CALLABLE( ephemeral_string_allocator )->Check( ephemeral_string_allocator );
    }

    // Property allocator context
    if( __match_alloc_name( "property_allocator" ) ) {
      cxmalloc_family_t *property_allocator = (cxmalloc_family_t*)self->property_allocator_context->allocator;
      err += CALLABLE( property_allocator )->Check( property_allocator );
    }

    // Vertex type encoder
    if( __match_alloc_name( "vxtype_encoder" ) ) {
      err += CALLABLE( self->vxtype_encoder )->CheckAllocators( self->vxtype_encoder );
    }

    // Vertex type decoder
    if( __match_alloc_name( "vxtype_decoder" ) ) {
      err += CALLABLE( self->vxtype_decoder )->CheckAllocators( self->vxtype_decoder );
    }

    // Relationship type encoder
    if( __match_alloc_name( "rel_encoder" ) ) {
      err += CALLABLE( self->rel_encoder )->CheckAllocators( self->rel_encoder );
    }

    // Relationship type decoder
    if( __match_alloc_name( "rel_decoder" ) ) {
      err += CALLABLE( self->rel_decoder )->CheckAllocators( self->rel_decoder );
    }

    // Vertex property keymap
    if( __match_alloc_name( "vxprop_keymap" ) ) {
      err += CALLABLE( self->vxprop_keymap )->CheckAllocators( self->vxprop_keymap );
    }

    // Vertex property valmap
    if( __match_alloc_name( "vxprop_valmap" ) ) {
      err += CALLABLE( self->vxprop_valmap )->CheckAllocators( self->vxprop_valmap );
    }

    // Vertex allocator
    if( __match_alloc_name( "vertex_allocator" ) ) {
      err += CALLABLE( self->vertex_allocator )->Check( self->vertex_allocator );
    }

    // Vertex general index
    if( __match_alloc_name( "vxtable" ) ) {
      err += CALLABLE( self->vxtable )->CheckAllocators( self->vxtable );
    }

    // Vertex type index
    if( __match_alloc_name( "vxtypeidx" ) ) {
      for( int vx=0; vx<256; vx++ ) {
        framehash_t *idx = self->vxtypeidx[ vx ];
        if( idx ) {
          err += CALLABLE( idx )->CheckAllocators( idx );
        }
      }
    }
  } GRAPH_RELEASE;

  return err;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * DebugGraph__get_object_at_address( vgx_Graph_t *self, QWORD address ) {
  comlib_object_t *obj = NULL;
  // We look in all allocators to see if they cover the given address


  cxmalloc_family_t *property_allocator = (cxmalloc_family_t*)self->property_allocator_context->allocator;
  if( (obj = CALLABLE( property_allocator )->GetObjectAtAddress( property_allocator, address )) != NULL ) {
    return obj;
  }

  if( (obj = CALLABLE( self->vertex_allocator )->GetObjectAtAddress( self->vertex_allocator, address )) != NULL ) {
    return obj;
  }

  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * DebugGraph__find_object_by_identifier( vgx_Graph_t *self, const char *identifier ) {
  comlib_object_t *obj = NULL;

  if( identifier == NULL ) {
    return NULL;
  }

  // TODO: Note that CString objects may have attributes which will alter the
  //       object id so it's not equal to the plain hash digest. Until this is
  //       handled we can't look up CString objects with attributes 
  //       (e.g. non-plain-string vertex string properties.)
  objectid_t obid = smartstrtoid( identifier, -1 );

  // Look in vertex allocator
  if( (obj = CALLABLE( self->vertex_allocator )->FindObjectByObid( self->vertex_allocator, obid )) != NULL ) {
    return obj;
  }

  // TODO: other object types?

  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * Graph__get_vertex_id_by_offset( vgx_Graph_t *self, int64_t *poffset ) {
  CString_t *CSTR__name = NULL;
  GRAPH_LOCK( self ) {
    vgx_Vertex_t *V = (vgx_Vertex_t*)CALLABLE( self->vertex_allocator )->GetObjectByOffset( self->vertex_allocator, poffset );
    if( V ) {
      CSTR__name = CALLABLE( V )->IDCString( V );
    }
  } GRAPH_RELEASE;
  return CSTR__name;
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxapi_advanced.h"

test_descriptor_t _vgx_vxapi_advanced_tests[] = {
  { "VGX Graph Advanced API Tests", __utest_vxapi_advanced },
  {NULL}
};
#endif
