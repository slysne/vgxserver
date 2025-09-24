/*######################################################################
 *#
 *# vxdurable_commit.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __commit_vertex_WL( vgx_Vertex_t *vertex_WL, uint32_t ts ) {
  // ===============================================
  // Consider the object committed when the allocator
  // is notified of the modification.
  //
  _cxmalloc_object_set_modified_nolock( vertex_WL );
  //
  // ===============================================

  // Transition a brand new vertex to the normal context
  if( __vertex_is_suspended_context( vertex_WL ) ) {
    // ???
    // TODO: ACTIONS RELATED TO COMMITTING A VERTEX FOR THE FIRST TIME
    // ???
    __vertex_set_active_context( vertex_WL );
  }

  // set modification time (resolution is 1 second)
  vertex_WL->TMM = ts;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __commit_graph_operation_CS( vgx_Graph_t *self, int64_t opid ) {
  // Assign the operation id to graph
  iOperation.SetId( &self->operation, opid ); // graph -> dirty implied

  // Commit the graph operation
  COMMIT_GRAPH_OPERATION_CS( self );

  return opid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __capture_isolated_virtual_delete( vgx_Vertex_t *vertex_WL ) {
  if( __vertex_owned_by_index_only_LCK( vertex_WL, Vertex_REFCNT_WL( vertex_WL ) - 1 ) ) {
    if( __vertex_is_isolated( vertex_WL ) ) {
      if( __vertex_is_manifestation_virtual( vertex_WL ) ) {
        if( iOperation.Vertex_CS.Delete( vertex_WL ) < 0 ) {
          CRITICAL( 0x001, "Failed to capture delete operation for virtual vertex" );
          return -1;
        }
        return 1;
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 * WARNING TO ALL CALLERS OF THIS FUNCTION:
 *
 * Calling thread may lose CS and go to sleep! Make sure vertex state
 * is such that another thread cannot modify the vertex state while
 * sleeping!
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_commit__close_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {

  // Vertex operation ID before close
  int64_t pre_close_opid = iOperation.GetId_LCK( &vertex_WL->operation );

  // Commit vertex active and modified if dirty
  if( iOperation.IsDirty( &vertex_WL->operation ) ) {
    __commit_vertex_WL( vertex_WL, _vgx_graph_seconds( self ) );
  }

  // Close vertex operation
  //
  // WARNING: THIS CALL MAY RESULT IN TEMPORARY LOSS OF CS!
  //          MAKE SURE VERTEX IS PROTECTED BY A STATE FLAG THAT PREVENTS
  //          ANOTHER THREAD FROM MODIFYING VERTEX STATE!
  int64_t opid = CLOSE_VERTEX_OPERATION_CS_WL( vertex_WL );
  // ^^^^^ DANGER ^^^^^ DANGER ^^^^^ DANGER ^^^^^ DANGER ^^^^^ DANGER ^^^^^

  if( opid > 0 ) {
    // Vertex is now committed and clean
    return __commit_graph_operation_CS( self, opid );
  }
  else {
    return pre_close_opid;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_commit__commit_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, bool hold_CS ) {
  // Vertex operation ID before commit
  int64_t pre_commit_opid = iOperation.GetId_LCK( &vertex_WL->operation );

  // Commit vertex active and modified
  __commit_vertex_WL( vertex_WL, _vgx_graph_seconds( self ) );

  // Commit vertex operation
  int64_t opid = COMMIT_VERTEX_OPERATION_CS_WL( vertex_WL, hold_CS );

  if( opid > 0 ) {
    // Vertex is now committed and clean
    return __commit_graph_operation_CS( self, opid );
  }
  else {
    return pre_commit_opid;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int64_t _vxdurable_commit__commit_inarcs_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL ) {
  //
  // BIG TODO!  Basically everything required to commit modified inarcs
  // to persistent storage. This means everything related to changelogs and writing
  // into the persisten vertex allocator, etc. HUGE job, nothing at all done here yet.
  //
  int64_t op = 0;
  return op;
}



/*******************************************************************//**
 *
 *
 * Returns: A new vertex with specified manifestation (VIRTUAL or REAL),
 *          or NULL on failure or if the NULL manifestation was given.
 *          The new vertex is added to the graph index table(s).
 *
 *          The caller of this function owns a reference to the new vertex.
 *          Additional references are owned internally by the vertex index.
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxdurable_commit__new_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_VertexTypeEnumeration_t vxtype, vgx_Rank_t rank, vgx_VertexStateContext_man_t manifestation, CString_t **CSTR__error ) {

  bool allowed = true;
  if( _vgx_is_readonly_CS( &self->readonly ) || ( _vgx_has_readonly_request_CS( &self->readonly ) && !_vxgraph_tracker__has_writable_locks_CS( self )) ) {
    allowed = false;
  }

  // Abort if readonly graph
  if( allowed == false ) {
    __set_error_string( CSTR__error, "Graph is readonly" );
    return NULL;
  }

  // Make sure vertex type is valid
  if( __vertex_type_enumeration_valid( vxtype ) == false ) {
    __format_error_string( CSTR__error, "Invalid vertex type encoding: %d", (int)vxtype );
    return NULL;
  }

  vgx_Vertex_t *vertex = NULL;
  CString_t *CSTR__autoid = NULL;
  if( manifestation == VERTEX_STATE_CONTEXT_MAN_NULL ) {
    return NULL; // don't create vertex anyway
  }

  if( CSTR__idstr && obid ) {
    // ok
  }
  else if( !CSTR__idstr && !obid ) {
    return NULL; // can't do it
  }
  else if( CSTR__idstr && !obid ) {
    obid = CStringObid( CSTR__idstr );
  }
  else if( !CSTR__idstr && obid ) {
    char buffer[33];  // use if needed
    CSTR__idstr = CSTR__autoid = NewEphemeralCStringLen( self, idtostr( buffer, obid ), 32, 0 ); // populate idstring from obid bytes (becomes hex string)
  }

  // Continue if we have populated idstr and obid
  if( CSTR__idstr && !idnone(obid) ) {
    // Construct vertex with the given ID, and no properties or arcs
    // Vertex has refcnt=1 after this
    if( (vertex = NewVertex_CS( self, obid, CSTR__idstr, vxtype, rank, manifestation, CSTR__error )) != NULL ) {
      // Index vertex into the parent graph's vertex table(s)
      // Index becomes owner of any new references it needs to hold the vertex in its various tables
      if( _vxgraph_vxtable__index_vertex_CS_WL( self, vertex ) < 1 ) { // We expect vertex to be added to at least one table!
        // Indexing failed! Destroy vertex.
        COMLIB_OBJECT_DESTROY( vertex );
        vertex = NULL;
      }
      else {
        // NOTE: The vertex refcount = 3 at this point.
        // Owners are:
        // - The caller of this function ows one reference
        // - The generic vertex index owns one reference
        // - The vertex type index owns one reference
      }
    }
  }

  if( CSTR__autoid ) {
    CStringDelete( CSTR__autoid );
  }

  return vertex;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __get_ondelete_event_CS_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *ondelete_prop ) {
  ondelete_prop->key = g_CSTR__ondelete_key;
  ondelete_prop->keyhash = g_ondelete_keyhash;

  // Retrieve ondelete property if it exists, and own another reference
  if( _vxvertex_property__get_property_RO( self_WL, ondelete_prop ) ) {
    if( ondelete_prop->val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
      return 1;
    }
  }

  // No ondelete property
  return 0;
}




/*******************************************************************//**
 *
 * Delete a vertex.
 * The caller must have obtained a write lock on the vertex before calling
 * this function. This write lock is automatically released (once) by this
 * function before it returns, i.e. the caller gives up one write lock on
 * the vertex by calling this function.
 * 
 * The actions taken depend on the current manifestation of the vertex.
 * 
 * A) Vertex is REAL
 * If the vertex is REAL this function will first remove all properties, 
 * vector, and all outarcs. 
 *   1) If outarc removal cannot complete within the provided timing budget
 *      the vertex will continue to exist in a partial (but valid) state 
 *      and another deletion attempt has to be made again in the future.
 *      Expiration event is not cleared in this case.
 *      Returns: -1
 *   2) If outarc removal completed the vertex will be suspended.
 *      Any expiration event will be cancelled by queuing a event processor
 *      cancellation event. Next action depends on whether inarcs exist.
 *      * If no inarcs exist (degree = 0) the vertex is unindexed from the
 *        graph's vertex index and manifestation set to the NULL state.
 *      * If inarcs still exist (degree > 0) the vertex will remain indexed
 *        and its manifestation set to the VITRUAL state.
 *      Returns: 1
 *
 * B) Vertex is VIRTUAL
 * If the vertex is VIRTUAL this function has no effect on the vertex other
 * than queuing an event cancellation if the vertex has an event scheduled.
 * Returns: 0
 *
 *
 * NOTE ON VERTEX DEALLOCATION
 * ---------------------------
 * Before returning this function releases the write lock held on the
 * vertex by the caller. If this results in the vertex refcount going to
 * zero the vertex is permanently destroyed and can no longer be accessed.
 * If the caller holds recursive write locks or have inflated the refcount
 * of the vertex through other means the vertex is NOT destroyed here.
 * Instead it will be destroyed later if the refcount goes to zero after
 * the last recursive write lock has been released, or when the vertex
 * refcount goes to zero through other means.
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_commit__delete_vertex_CS_WL( vgx_Graph_t *self, vgx_Vertex_t **vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record ) {
  int n_deleted = 0;

  vgx_Vertex_t *v_WL = *vertex_WL;
  vgx_Operation_t *vxop = &v_WL->operation;
  vgx_Operation_t *grop = &self->operation;
  

  BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( self ) {
    // Capture
    // NOTE: This operation will commit the delete to the operation processor, which
    //       internally should ignore further operation captures for this vertex.
    //       How the current graph (i.e. "indexer") manages to handle the delete is
    //       not relevant to the receiver (i.e. "searcher") of the operation. For example
    //       if the current graph cannot complete outarc removal immediately, the vertex
    //       is still considered deleted at this point in the operation stream.
    if( iOperation.Vertex_CS.Delete( v_WL ) < 0 ) {
      printf( "*** VERTEX OPERATION ***\n" );
      iOperation.Dump_CS( vxop );
      printf( "************************\n" );

      printf( "*** GRAPH OPERATION ***\n" );
      iOperation.Dump_CS( grop );
      printf( "************************\n" );

      CALLABLE( self )->Dump( self );
      FATAL( 0x3C1, "negative Delete" );
    }

    // Close vertex operation to prevent further operation output for this vertex
    if( CLOSE_VERTEX_OPERATION_CS_WL( v_WL ) < 0 ) {
      FATAL( 0x3C2, "negative Close_CS" );
    }

    // Commit the graph operation so the delete gets emitted
    if( COMMIT_GRAPH_OPERATION_CS( self ) < 0 ) {
      FATAL( 0xC33, "negative Commit_CS" );
    }

    // Vertex is REAL - remove its properties and outarcs, and delete from graph if no inarcs, otherwise convert to VIRTUAL and keep indexed
    if( __vertex_is_manifestation_real( v_WL ) ) {

      vgx_Vertex_vtable_t *iV = CALLABLE(v_WL);

      // Get ondelete event (if one exists)
      vgx_VertexProperty_t ondelete_prop = {0};
      int has_ondelete = __get_ondelete_event_CS_WL( v_WL, &ondelete_prop );

      // Remove all properties
      if( iV->RemoveProperties( v_WL ) < 0 ) {
        PRINT_VERTEX( v_WL );
        FATAL( 0x3C4, "Unexpected failure to remove vertex properties" );
        // TODO: less severe error handling
      }

      // Re-insert ondelete event if we had one before we removed all properties
      if( has_ondelete ) {
        iV->SetProperty( v_WL, &ondelete_prop );
        icstringobject.DecrefNolock( ondelete_prop.val.data.simple.CSTR__string );
      }

      // Remove vector
      if( iV->RemoveVector( v_WL ) < 0 )  {
        PRINT_VERTEX( v_WL );
        FATAL( 0x3C5, "Unexpected failure to remove vertex vector" );
        // TODO: less severe error handling
      }
      // Remove all outarcs
      vgx_predicator_t probe = VGX_PREDICATOR_ANY_OUT;

      //
      // TODO:  fix_arcdelnoblock  This needs re-work. May return -1 below if removal timed out, which means
      // removal was partial or none at all. Check timing budget/reason to determine cause
      // and next action.
      //
      int64_t narcs = _vxgraph_state__remove_outarcs_CS_WL( self, v_WL, timing_budget, probe );
      // Incomplete, do not proceed with complete deletion. Vertex will remain and deletion needs to be re-attempted.
      if( narcs < 0 ) {
        // Timed out while trying to acquire terminal(s) in order to disconnect reverse
        // arcs. This means the vertex being deleted is only partially deleted at this
        // point and deletion needs to be re-attempted. An expiration event is automatically
        // scheduled.
        if( _vgx_is_execution_halted( timing_budget ) ) {
          // Vertex will be auto-deleted in the background whenever possible.
          // We schedule immediate deletion event unless event already scheduled
          // and the vertex has already expired (which means EVP is working on it and will retry.)
          if( !(__vertex_has_event_scheduled( v_WL ) && __vertex_is_expired( v_WL, _vgx_graph_seconds( self ) )) ) {
            __vertex_set_expiration_ts( v_WL, _vgx_graph_seconds( self ) );
            iGraphEvent.ScheduleVertexEvent.Expiration_WL( self, v_WL, v_WL->TMX.vertex_ts );
          }
          // Caller gets notified of error.
          n_deleted = -1;
        }
        // Other internal error. That's bad.
        else {
          PRINT_VERTEX( v_WL );
          FATAL( 0x3C6, "Unexpected failure to remove vertex outarcs" );
          // TODO: less severe error handling
        }
      }
      // All outarcs removed ok
      else {
        // Vertex will be deleted so we can safely erase any future events scheduled for it
        if( __vertex_has_event_scheduled( v_WL ) ) {
          __vertex_all_clear_expiration( v_WL );
          iGraphEvent.CancelVertexEvent.RemoveSchedule_WL( self, v_WL ); 
        }

        // Isolated vertex owned only by index and a single writelock: remove from graph by unindexing it.
        if( __vertex_is_isolated( v_WL ) && __vertex_is_indexed( v_WL ) && Vertex_REFCNT_WL( v_WL ) == VXTABLE_VERTEX_REFCOUNT + 1 ) {

          // Suspend the vertex
          __vertex_set_suspended_context( v_WL );

          // unindex the isolated vertex
          if( _vxgraph_vxtable__unindex_vertex_CS_WL( self, v_WL ) > 0 ) { // At least one reference given up by unindexing, means success
            n_deleted = 1;
            // unindexed but NOT deallocated: framehash.delete() calls destructor causing one DECREF, but we own another reference locally
            // Mark as empty and retired
            __vertex_set_manifestation_null( v_WL );
          }
          else {
            // This is an interesting situation. Unindexing either failed (-1) or deleted nothing (0), which means
            // our overall vertex deletion did not complete as expected.
            // TODO: look into handling this situation. It's most likely a logical bug somewhere
            PRINT_VERTEX( v_WL );
            FATAL( 0x3C7, "Unexpected failure (%d) to remove vertex from graph index", n_deleted );
          }
        }
        // Other owners or inarcs exist, convert to VIRTUAL and keep in index
        else {
          __vertex_set_manifestation_virtual( v_WL );
          n_deleted = 1;  // Converted to VIRTUAL. (Deleted from caller's point of view.)
        }
      }

    }
    // Vertex is already VIRTUAL - no action
    else if( __vertex_is_manifestation_virtual( v_WL ) ) {
      n_deleted = 0;  // Is VIRTUAL, will continue to exist. (Virtual vertices are removed automatically by other internal mechanisms)
    }

    // Vertex manifestation is now either VIRTUAL or NULL.
    // If VIRTUAL it means vertex has inarcs and will continue to exist in the graph.
    // If NULL it means vertex has no inarcs and is no longer in the graph index. In this case we
    // hold exactly one reference to the vertex (the one from the lock operation), so when we 
    // unlock below the NULL vertex's refcount goes to zero and the vertex is deallocated.
    if( _vxgraph_state__unlock_vertex_CS_LCK( self, vertex_WL, record ) == false ) {
      PRINT_VERTEX( v_WL );
      FATAL( 0x3C8, "Unexpected failure to unlock vertex after deletion from graph." );
      // TODO: less severe handling
    }
  } END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS;

  // If vertex manifestation was NULL right above, that vertex is now deallocated.
  // Exception to the rule: If extra references are held on the outside, then deallocation will
  // take place later when the last of the references has been released.

  return n_deleted;
}



/*******************************************************************//**
 *
 *
 * Returns:   1 : Named vertex was deleted
 *            0 : Named vertex does not exist
 *           -1 : Failed to delete, either because of timeout or other error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_commit__delete_vertex_OPEN( vgx_Graph_t *self, const CString_t *CSTR__idstr, const objectid_t *obid, vgx_ExecutionTimingBudget_t *timing_budget, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int n_deleted = -1;

  // Compute obid if we didn't get one
  if( obid == NULL ) {
    if( CSTR__idstr == NULL ) {
      return -1;
    }
    // Use a local copy for object id, since things may happen to the 
    obid = CStringObid( CSTR__idstr );

    // Check
    if( idnone(obid) ) {
      return -1;
    }
  }

  // Hold the state lock and lock the vertex - stay within CS for the duration of deletion
  // vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
  // TODO: Is there a way to avoid staying in CS for the entire delete?
  // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  if( timing_budget->t0_ms == 0 ) {
    _vgx_start_graph_execution_timing_budget( self, timing_budget );
  }

  GRAPH_LOCK( self ) {

    XTRY {

      if( _vgx_is_readonly_CS( &self->readonly ) ) {
        _vgx_request_write_CS( &self->readonly );
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
        THROW_SILENT( CXLIB_ERR_API, 0x3C6 );
      }

      vgx_Vertex_t *vertex = _vxgraph_vxtable__query_CS( self, NULL, obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
      // Look up vertex by obid in index
      if( vertex != NULL ) {

        // TODO: If current thread holds readonly lock to vertex we need to attempt read->write escalation before continuing.
        // Otherwise we may inadvertently call delete on a vertex we hold readonly lock for, with infinite timeout, and we have deadlock.
        // For now we disallow infinite timeout when the vertex is locked readonly, to avoid this deadlock scenario.
        if( __vertex_is_locked_readonly( vertex ) && timing_budget->timeout_ms < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_LOCKED );
          THROW_SILENT( CXLIB_ERR_MUTEX, 0x3C7 );
        }
        else {
          cxlib_exc_subcategory_t errcat = CXLIB_ERR_GENERAL;
          int err = 0;
          // Yield inarcs of all currently writelocked vertices for current thread, except vertex to be deleted (if already writelocked)
          ENTER_SAFE_MULTILOCK_CS( self, vertex, reason ) {
            // Obtain exclusive write lock on vertex and own a reference
            vgx_Vertex_t *vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, timing_budget, VGX_VERTEX_RECORD_OPERATION );
            __set_access_reason( reason, timing_budget->reason );
            if( vertex_WL ) {
              // Delete vertex
              if( (n_deleted = _vxdurable_commit__delete_vertex_CS_WL( self, &vertex_WL, timing_budget, VGX_VERTEX_RECORD_OPERATION )) < 0 ) {
                __set_error_string( CSTR__error, "Outarcs removal is incomplete, try again." );
                __set_access_reason( reason, timing_budget->reason );
              }
            }
            else {
              errcat = CXLIB_ERR_MUTEX;
              err = 0x3C9;
            }
          } LEAVE_SAFE_MULTILOCK_CS( err = 0x3CA );

          if( err ) {
            THROW_SILENT( errcat, err );
          }
        }
      }
      else {
        __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
        n_deleted = 0;
      }
    }
    XCATCH( errcode ) {
      n_deleted = -1;
    }
    XFINALLY {
    }
  } GRAPH_RELEASE;

  return n_deleted;
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_commit.h"
  
test_descriptor_t _vgx_vxdurable_commit_tests[] = {
  { "VGX Graph Durable Commit Tests", __utest_vxdurable_commit },
  {NULL}
};
#endif
