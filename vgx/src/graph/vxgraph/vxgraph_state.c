/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_state.c
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
#include "_vxarcvector.h"
/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int _vxgraph_state__create_arc_timestamps_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL, int lifespan, uint32_t *tmx );

//
static vgx_Vertex_t * __create_and_own_vertex_writable_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexTypeEnumeration_t vxtype, vgx_VertexStateContext_man_t manifestation, vgx_vertex_record record, CString_t **CSTR__error );


static bool __unlock_vertex_writable_CS_WL( vgx_Graph_t *self, vgx_Vertex_t **vertex_WL, vgx_vertex_record record );
static bool __unlock_vertex_readonly_CS_RO( vgx_Graph_t *self, vgx_Vertex_t **vertex_RO, vgx_vertex_record record );



// Inarc management
static vgx_Vertex_t * __borrow_vertex_inarcs_CS( vgx_Vertex_t *vertex );
static void __return_vertex_inarcs_CS_iWL( vgx_Graph_t *graph, vgx_Vertex_t *vertex_iWL );
static vgx_Vertex_t * __lock_terminal_writable_yield_initial_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record );



// Readonly mode
static int __set_graph_readonly_CS( vgx_Graph_t *self, bool force, bool tx_input_suspended, vgx_ExecutionTimingBudget_t *timing_budget );
static int __clear_graph_readonly_CS( vgx_Graph_t *self );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "VX::STE(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXGRAPH_STATE_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_STATE_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_STATE_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_STATE_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_STATE_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_STATE_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )




__inline static bool __vertex_pointer_valid_active_CS( cxmalloc_family_t *valloc, cxmalloc_handle_t vhandle, vgx_Vertex_t *vertex ) {
  vgx_Vertex_t *obj = CALLABLE( valloc )->HandleAsObjectNolock( valloc, vhandle );
  if( vertex == obj && _cxmalloc_is_object_active( vertex ) && __vertex_is_active_context( vertex ) ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __WAIT_VERTEX_CS( Graph, VertexPtrPtr )                             \
  do {                                                                      \
    vgx_Vertex_t **__vertex__ = VertexPtrPtr;                               \
    cxmalloc_handle_t __vhandle__ = ivertexobject.AsHandle( *__vertex__ );  \
    cxmalloc_family_t *__valloc__ = (Graph)->vertex_allocator;              \
    do

#define __VERIFY_ACTIVE_VERTEX_CONTINUE_CS                                  \
    WHILE_ZERO;                                                             \
    if( !__vertex_pointer_valid_active_CS( __valloc__, __vhandle__, *__vertex__ ) ) {  \
      *__vertex__ = NULL;                                                   \
    }                                                                       \
  } WHILE_ZERO





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN void __check_arc_balance( vgx_Graph_t *self, const char *funcname, size_t linenum ) {
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __no_writer_access_reason( vgx_Vertex_t *vertex, const int timeout_ms, vgx_AccessReason_t *reason ) {
  if( vertex == NULL ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }
  else if( !__vertex_is_semaphore_writer_reentrant( vertex ) ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_SEMAPHORE );
  }
  else if( timeout_ms > 0 ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_TIMEOUT );
  }
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_LOCKED );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __no_reader_access_reason( vgx_Vertex_t *vertex, int timeout_ms, vgx_AccessReason_t *reason ) {
  if( vertex == NULL ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }
  else if( !__vertex_has_semaphore_reader_capacity( vertex ) ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_SEMAPHORE );
  }
  else if( timeout_ms > 0 ) {
    __set_access_reason( reason, VGX_ACCESS_REASON_TIMEOUT );
  }
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_LOCKED );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define __BEGIN_CHECK_VERTEX_ACCESS( VertexPtr, ReasonPtr ) \
  do {                                                      \
    vgx_AccessReason_t *__reason__ = ReasonPtr;       \
    if( VertexPtr )


#define __END_CHECK_VERTEX_ACCESS                           \
    else {                                                  \
      __set_access_reason( __reason__, VGX_ACCESS_REASON_NOEXIST ); \
    }                                                       \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vertex_id_valid( const CString_t *CSTR__id, BYTE *illegal, int *pos ) {
  if( CSTR__id ) {
    const BYTE *id = (BYTE*)CStringValue( CSTR__id );
    int sz = CStringLength( CSTR__id );
    const BYTE *cursor = id;
    const BYTE *end = id + sz;
    BYTE b;
    while( cursor < end ) {
      if( (b = *cursor++) < 0x20 ) {
        *illegal = b;
        *pos = (int)(cursor - id) - 1;
        return 0;
      }
    }
  }
  return 1;
}



/*******************************************************************//**
 * Create a new vertex of the specified manifestation (VIRTUAL or REAL).
 *
 * Returns: Pointer to vertex on success, NULL on failure of if NULL
 *          manifestation was specified.
 *
 * NOTE: Several references to the vertex exist after this call
 *       completes: One owned by the caller of this function, and the
 *       others by the graph index.
 ***********************************************************************
 */
static vgx_Vertex_t * __create_and_own_vertex_writable_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexTypeEnumeration_t vxtype, vgx_VertexStateContext_man_t manifestation, vgx_vertex_record record, CString_t **CSTR__error) {
  __assert_state_lock( self );
  // Validate vertex id
  BYTE bad_byte;
  int bad_pos;
  if( CStringLength( CSTR__vertex_idstr ) == 0 ) {
    __set_error_string( CSTR__error, "empty vertex identifier" );
    return NULL;
  }
  else if( !__vertex_id_valid( CSTR__vertex_idstr, &bad_byte, &bad_pos ) ) {
    __format_error_string( CSTR__error, "illegal vertex identifier, char %02xh at position %d", bad_byte, bad_pos );
    return NULL;
  }

  vgx_Rank_t rank = vgx_Rank_INIT();

  // Create a new vertex of the specified manifestation (REAL or VIRTUAL)
  vgx_Vertex_t *vertex_WL = _vxdurable_commit__new_vertex_CS( self, CSTR__vertex_idstr, vertex_obid, vxtype, rank, manifestation, CSTR__error );
  if( vertex_WL ) {
    // Track acquisition
    if( __vgx_vertex_record_acquisition( record ) ) {
      _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_WL );
    }
  }
  return vertex_WL;
}



/*******************************************************************//**
 * Perform the appropriate actions to guarantee exclusive access to 
 * the vertex inarcs. Access to inarcs can be granted when inarcs are
 * available. 
 * Available = /Unavailable = /L + (Y * /B)
 *
 * Returns true if inarcs were borrowed, otherwise false
 ***********************************************************************
 */
__inline static vgx_Vertex_t * __borrow_vertex_inarcs_CS( vgx_Vertex_t *vertex ) {
  __assert_state_lock( vertex->graph );
  vgx_Vertex_t *vertex_iWL = NULL;
  // /L
  if( __vertex_is_unlocked( vertex ) ) {
    // Effectively lock the vertex - a normal write lock, which includes access to inarcs
    vertex_iWL = __vertex_lock_writable_CS( vertex );
    Vertex_INCREF_WL( vertex_iWL );

    // Operation NOT opened since we are modifying inarcs, and that doesn't count as an operation on the vertex

    //
    // NOTE: This function is only used in a transient manner so we don't track vertex acquisitions
    //
  }
  // + (Y * /B)
  else if( __vertex_is_inarcs_yielded( vertex ) && !__vertex_is_borrowed_inarcs_busy( vertex ) ) {
    // Borrow the inarcs
    vertex_iWL = __vertex_set_borrowed_inarcs_busy( vertex );
    Vertex_INCREF_WL( vertex_iWL );
    //
    // NOTE: We don't track the borrowing of inarcs
    //
  }

  // Will be non-NULL if we were able to borrow vertex inarcs
  return vertex_iWL;
}



/*******************************************************************//**
 * Clear the yielded inarcs busy flag and signal vertex availability
 *
 ***********************************************************************
 */
__inline static void __return_vertex_inarcs_CS_iWL( vgx_Graph_t *graph, vgx_Vertex_t *vertex_iWL ) {
  __assert_state_lock( graph );
  // We are the ones who borrowed the inarcs so let's return them now
  __vertex_clear_borrowed_inarcs_busy( vertex_iWL );
  //
  // NOTE: This function is called only in a transient manner so we don't track vertex acquisitions
  //
  // Give up ownership
  Vertex_DECREF_WL( vertex_iWL );
  // Signal the return of the inarcs to a non-busy state (others may now borrow if waiting, or the vertex may be able to re-acquire its inarcs if waiting)
  SIGNAL_INARCS_AVAILABLE( graph );
}



/*******************************************************************//**
 *
 * Returns: 1 : inarcs transitioned to yielded
 *          0 : inarcs already yielded, no change
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__yield_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL ) {
  __assert_state_lock( self );

  int ret = 0;

  // Inarcs are not yielded
  if( !__vertex_is_inarcs_yielded( vertex_WL ) ) {
    __vertex_set_yield_inarcs( vertex_WL );
    ret = 1;
  }

  // Broadcast an availability signal
  SIGNAL_VERTEX_AVAILABLE( self );

  return ret;
}



/*******************************************************************//**
 *
 * Returns: 1 : inarcs reclaimed
 *          0 : inarcs were not yielded
 *         -1 : inarcs are busy and cannot be reclaimed at this time
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__reclaim_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );

  int ret = 0;

  // Check if inarcs are actually yielded
  if( __vertex_is_inarcs_yielded( vertex_WL ) ) {

    // Retract the yielding of inarcs of our originally writelocked vertex
    __vertex_clear_yield_inarcs( vertex_WL );

    // yield cleared
    ret = 1;

    // Check if inarcs have been borrowed before entering expensive wait loop
    if( __vertex_is_borrowed_inarcs_busy( vertex_WL ) ) {
      BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
        BEGIN_EXECUTION_BLOCKED_WHILE( __vertex_is_borrowed_inarcs_busy( vertex_WL ) ) {
          BEGIN_WAIT_FOR_EXECUTION_RESOURCE( vertex_WL ) {
            WAIT_FOR_INARCS_AVAILABLE( self, 1 );
          } END_WAIT_FOR_EXECUTION_RESOURCE;
        } END_EXECUTION_BLOCKED_WHILE;

        // Give up, re-yield inarcs and return timeout
        if( __vertex_is_borrowed_inarcs_busy( vertex_WL ) ) {
          __vertex_set_yield_inarcs( vertex_WL );
          ret = -1;
          __no_writer_access_reason( vertex_WL, EXECUTION_BUDGET_TIMEOUT, &timing_budget->reason );
        }

      } END_EXECUTE_WITH_TIMING_BUDGET_CS;
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxgraph_state__yield_vertex_inarcs_and_wait_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, int sleep_ms ) {
  __assert_state_lock( self );
  // Yield the inarcs of our writelocked vertex
  vgx_Vertex_t *vertex = __vertex_set_yield_inarcs( vertex_WL );

  // Broadcast an availability signal
  SIGNAL_VERTEX_AVAILABLE( self );

  // Optional sleep
  if( sleep_ms > 0 ) {
    GRAPH_SUSPEND_LOCK( self ) {
      sleep_milliseconds( sleep_ms );
    } GRAPH_RESUME_LOCK;
  }

  // Wait for signal
  WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
  
  // Retract the yielding of inarcs of our originally writelocked vertex
  __vertex_clear_yield_inarcs( vertex );
  
  // Wait until the inarcs are not borrowed by anyone else, restoring WL of vertex
  while( __vertex_is_borrowed_inarcs_busy( vertex ) ) {
    // Someone is borrowing the inarcs, wait for signal to re-check
    WAIT_FOR_INARCS_AVAILABLE( self, 1 );
  }

  // At this point vertex is not yielding its inarcs and no other thread
  // is borrowing the inarcs.
}



/*******************************************************************//**
 *
 *
 *
 * After yielding its inarcs the vertex offers temporary exclusive
 * access to a subset of its resources to anyone who acquires the lock
 * using __lock_terminal_inarcs_writable(). The subset of vertex
 * resources that are yielded includes exactly the following members
 *  - inarcs
 *  - operation.id
 *  - descriptor.access.bib
 *
 *
 *
 * NOTE about commit consistency: If someone needs to modify another
 * vertex's inarcs they first call __lock_terminal_inarcs_writable()
 * to guarantee exclusive access to that vertex's inarcs. Depending
 * on the state of that vertex the lock may be a full lock or it may
 * be a limited lock for inarcs resources only. The caller has no way
 * of knowing whether a full lock or a limited lock was used, so from
 * the callers point of view only the inarcs have been locked. After
 * the caller is done it calls __unlock_terminal_inarcs_writable() to
 * return the inarcs back to the vertex it borrowed them from. That
 * function will know what lock was being held and perform release
 * actions accordingly. If the inarcs were being borrowed we know that
 * the vertex is back to being locked fully and will see a full
 * release in the future via __unlock_vertex_writable() with a full
 * commit and operation update. In that case the returning of the 
 * borrowed inarcs will result in only a partial commit (of inarcs
 * resources only) with associated operation number. If on the other
 * hand the vertex was fully locked by the inarcs modifier, then
 * __unlock_terminal_inarcs_writable() will perform a full unlock 
 * with a full commit and associated operation number and clear
 * the operation dirty bit.
 * 
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __lock_terminal_writable_yield_initial_inarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *initial_WL, vgx_Vertex_t *terminal, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record ) {
  __assert_state_lock( self );
  // This is the vertex we want to return in WL state
  vgx_Vertex_t *terminal_WL = NULL;
  vgx_AccessReason_t *reason = &timing_budget->reason;

  // Make sure initial and terminal are different
  if( initial_WL == terminal ) {
    VXGRAPH_STATE_FATAL( self, 0xC01, "Illegal vertex synchronization: initial == terminal" );
  }
  // Make sure the initial inarcs are ours to yield
  else if( !__vertex_is_locked_writable_by_current_thread( initial_WL ) ) {
    // SANITY CHECK FOR NOW WHILE WE WORK OUT THE DETAILS
    PRINT_VERTEX( initial_WL );
    VXGRAPH_STATE_FATAL( self, 0xC02, "Invalid attempt to yield inarcs without holding write lock!" ); // <= yes
  }

  // Commit terminal if brand new and owned by this thread
  if( __vertex_is_suspended_context( terminal ) && __vertex_is_locked_writable_by_current_thread( terminal ) ) { 
    if( _vxdurable_commit__commit_vertex_CS_WL( self, terminal, false ) < 0 ) {
      return NULL;
    }
  }

  // Only enter acquisition loop if the terminal is active
  if( __vertex_is_active_context( terminal ) ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
      // Inspect the terminal's state to determine if we can acquire it immediately for both CX_WRITE and CX_READ
      BEGIN_EXECUTION_BLOCKED_WHILE( terminal && !__vertex_is_lockable_as_writable( terminal ) ) { // <= while locked by some other thread
        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( terminal ) {
          __WAIT_VERTEX_CS( self, &terminal ) {
            // Mark our request to write to the terminal so subsequent read attempts of the same terminal will yield
            __vertex_set_write_requested( terminal ); // <= will only have effect if currently readonly locked and no other wrq has been made
            // Allow other threads to modify our initial's inarcs while we're blocked on terminal
            _vxgraph_state__yield_vertex_inarcs_and_wait_CS_WL( self, initial_WL, 0 );
          } __VERIFY_ACTIVE_VERTEX_CONTINUE_CS;
        } END_WAIT_FOR_EXECUTION_RESOURCE;
      } END_EXECUTION_BLOCKED_WHILE;

      // Lock the terminal if it still exists and we didn't time out
      __BEGIN_CHECK_VERTEX_ACCESS( terminal, reason ) {
        // It's lockable, do it
        if( __vertex_is_lockable_as_writable( terminal ) ) {
          vgx_VertexDescriptor_t pre_lock = terminal->descriptor;
          // Lock the terminal
          terminal_WL = __vertex_lock_writable_CS( terminal );
          // Open terminal operation for capture
          if( __vgx_vertex_record_operation( record ) && iOperation.Open_CS( self, &terminal_WL->operation, COMLIB_OBJECT( terminal_WL ), false ) < 0 ) {
            // Failed
            __set_access_reason( reason, VGX_ACCESS_REASON_OPFAIL );
            terminal_WL->descriptor = pre_lock;
            terminal_WL = NULL;
          }
          // Success
          else {
            // Become independent owner of locked terminal
            Vertex_INCREF_WL( terminal_WL );
            // Track
            if( __vgx_vertex_record_acquisition( record ) ) {
              _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, terminal_WL );
            }
            __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
          }
        }
        // Not lockable
        else {
          // No longer interested
          __vertex_clear_write_requested( terminal );
          __no_writer_access_reason( terminal, EXECUTION_BUDGET_TIMEOUT, reason );
        }
      } __END_CHECK_VERTEX_ACCESS;
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  }
  // Terminal suspended
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }

  // May be NULL if terminal disappeared while waiting, or if we timed out while waiting
  return terminal_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_vertex_readonly_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_RO = NULL;
  vgx_AccessReason_t *reason = &timing_budget->reason;
  // Only enter acquisition loop if the vertex is active
  if( __vertex_is_active_context( vertex ) ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
      // Wait for the vertex to be lockable as readonly, but no longer than given timeout
      BEGIN_EXECUTION_BLOCKED_WHILE( vertex && !__vertex_is_lockable_as_readonly( vertex ) && !__vertex_is_locked_writable_by_current_thread( vertex ) ) {
        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( vertex ) {
          __WAIT_VERTEX_CS( self, &vertex ) {
            WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
         } __VERIFY_ACTIVE_VERTEX_CONTINUE_CS;
        } END_WAIT_FOR_EXECUTION_RESOURCE;
      } END_EXECUTION_BLOCKED_WHILE;

      // Mark vertex for readonly access if it still exists and it is lockable (we may have timed out or vertex may have been deleted)
      __BEGIN_CHECK_VERTEX_ACCESS( vertex, reason ) {
        if( __vertex_is_lockable_as_readonly( vertex ) ) {
          // Lock vertex as readonly (increment the readers count)
          vertex_RO = __vertex_lock_readonly_CS( vertex );
          // Become independent owner of locked vertex
          Vertex_INCREF_CS_RO( vertex_RO ); // We don't have exclusive vertex access so we use the locked allocator incref to be safe
          if( __vgx_vertex_record_acquisition( record ) ) {
            _vxgraph_tracker__register_vertex_RO_for_thread_CS( self, vertex_RO );
          }
          __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
        }
        else if( __vertex_is_locked_writable_by_current_thread( vertex ) && __vertex_is_writer_reentrant( vertex ) ) {
          vertex_RO = vertex; // it's really writable, which is a superset of readonly so it's ok!
          // Already writable by current thread, 
          __vertex_inc_writer_recursion_CS( vertex_RO );
          // Become independent owner of locked vertex
          Vertex_INCREF_CS_RO( vertex_RO ); // We don't have exclusive vertex access so we use the locked allocator incref to be safe
          if( __vgx_vertex_record_acquisition( record ) ) {
            _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_RO );
          }
          __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
        }
        else {
          __no_reader_access_reason( vertex, EXECUTION_BUDGET_TIMEOUT, reason );
        }
      } __END_CHECK_VERTEX_ACCESS;
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  }
  // Vertex suspended
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }
  // May be NULL if vertex was deleted or we timed out while waiting
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_vertex_readonly_OPEN( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record ) {
  vgx_Vertex_t *vertex_RO = NULL;
  GRAPH_LOCK_SPIN( self, 2 ) {
    vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, timing_budget, record );
  } GRAPH_RELEASE;
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_vertex_readonly_multi_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, int8_t n_locks ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_RO = NULL;
  if( n_locks > 0 ) {
    if( (vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, timing_budget, VGX_VERTEX_RECORD_NONE )) != NULL && --n_locks > 0 ) {
      if( __vertex_get_semaphore_count( vertex_RO ) + n_locks < VERTEX_SEMAPHORE_COUNT_READERS_LIMIT ) {
        if( __vertex_is_readonly( vertex_RO ) ) {
          Vertex_INCREF_DELTA_CS_RO( vertex_RO, n_locks );
          __vertex_inc_readonly_recursion_delta_CS( vertex_RO, n_locks );
        }
        // Vertex is actually writable - OK
        else {
          Vertex_INCREF_DELTA_WL( vertex_RO, n_locks );
          __vertex_inc_writer_recursion_delta_CS( vertex_RO, n_locks );
        }
      }
      else {
        _vxgraph_state__unlock_vertex_CS_LCK( self, &vertex_RO, VGX_VERTEX_RECORD_NONE );
        __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_SEMAPHORE );
      }
    }
  }
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
//DLL_HIDDEN bool _vxgraph_state__unlock_vertex_readonly_CS_RO( vgx_Graph_t *self, vgx_Vertex_t **vertex_RO, bool track ) {
static bool __unlock_vertex_readonly_CS_RO( vgx_Graph_t *self, vgx_Vertex_t **vertex_RO, vgx_vertex_record record ) {
  __assert_state_lock( self );
  bool released = false;
  // Decrement reader count (and unlock when reader count goes to zero)
  if( __vertex_unlock_readonly_CS( *vertex_RO ) > 0 ) {
    // Give up vertex ownership and sanity check
    if( Vertex_DECREF_CS_RO( *vertex_RO ) < VXTABLE_VERTEX_REFCOUNT ) { 
      PRINT_VERTEX( *vertex_RO );
      VXGRAPH_STATE_FATAL( self, 0xC11, "Unexpected drop in vertex refcount after readonly unlock" );
    }
    released = true;
  }
  // Recursion count has gone to zero
  else {
    int64_t refcnt = Vertex_DECREF_CS_RO( *vertex_RO );
    if( __vertex_owned_by_index_only_LCK( *vertex_RO, refcnt ) ) {
      // All vertex references left should be held by vxtable and vertex is therefore ISOLATED in graph
      if( __vertex_is_isolated( *vertex_RO ) ) {
        // Isolated VIRTUAL vertex cannot exist in graph and will be destroyed
        if( __vertex_is_manifestation_virtual( *vertex_RO ) ) {
          if( _vgx_is_writable_CS( &self->readonly ) ) {
            // Unindex virtual vertex - this will trigger vertex destructor since framehash releases its ownership and it will be the last reference
            // We don't open an operation here since the vertex will be deleted.
            // We need to temporarily become WL to unindex
            vgx_Vertex_t *vertex_WL = __vertex_lock_writable_CS( *vertex_RO );
            if( _vxgraph_vxtable__unindex_vertex_CS_WL( self, vertex_WL ) < 1 ) { // At least one reference should be given up by unindexing from vxtable
              // error
              PRINT_VERTEX( vertex_WL );
              VXGRAPH_STATE_CRITICAL( self, 0xC12, "Unexpected failure to remove readonly isolated virtual vertex" );
              // TODO: find a less severe way to handle this error
            }
          }
          else {
            // SPECIAL CASE: Isolated virtual vertex coming out of readonly mode cannot be destroyed while graph is readonly!
            // We will convert the vertex to real (violating the readonly state of the graph but we can't leave it virtual)
            __vertex_set_manifestation_real( *vertex_RO ); // <= Technically, access violation but we have no choice.
            VXGRAPH_STATE_WARNING( self, 0xC13, "Isolated, VIRTUAL vertex '%s' converted to REAL", CALLABLE( *vertex_RO )->IDPrefix( *vertex_RO ) );
          }
        }
        // Isolated REAL vertex is valid in graph, no further action
        else { /* ok */ }
      }
    }
    released = true;
  }

  if( released ) {
    if( __vgx_vertex_record_acquisition( record ) ) {
      _vxgraph_tracker__unregister_vertex_RO_for_thread_CS( self, *vertex_RO );
    }
    // Invalidate caller's vertex reference
    *vertex_RO = NULL;
  }

  return released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__unlock_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK, vgx_vertex_record record ) {
  __assert_state_lock( self );
  bool released = false;
  // Vertex is locked as WRITABLE by current thread
  if( __vertex_is_locked_writable_by_current_thread( *vertex_LCK ) ) {
    released = __unlock_vertex_writable_CS_WL( self, vertex_LCK, record );
  }
  // Vertex is locked as READABLE
  else if( __vertex_is_locked_readonly( *vertex_LCK ) ) {
    released = __unlock_vertex_readonly_CS_RO( self, vertex_LCK, record );
  }
  return released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__unlock_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK, vgx_vertex_record record ) {
  bool released;
  GRAPH_LOCK( self ) {
    // Wake other threads that may be waiting for vertex availability
    if( (released = _vxgraph_state__unlock_vertex_CS_LCK( self, vertex_LCK, record )) == true ) {
      SIGNAL_VERTEX_AVAILABLE( self );
    }
  } GRAPH_RELEASE;
  return released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_arc_head_inarcs_writable_yield_tail_inarcs_CS_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL_to_ANY, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  vgx_Vertex_t *tail_WL = arc_WL_to_ANY->tail;
  vgx_Vertex_t *head_ANY = arc_WL_to_ANY->head.vertex;
  vgx_Vertex_t *head_iWL = NULL;

  // Make sure tail and head are different
  if( tail_WL == head_ANY ) {
    VXGRAPH_STATE_FATAL( self, 0xC21, "Illegal vertex synchronization: tail == head" );
  }
  // Make sure the tail inarcs are ours to yield
  else if( !__vertex_is_locked_writable_by_current_thread( tail_WL ) ) {
    // SANITY CHECK FOR NOW WHILE WE WORK OUT THE DETAILS
    PRINT_VERTEX( tail_WL );
    VXGRAPH_STATE_FATAL( self, 0xC22, "Invalid attempt to yield inarcs without holding write lock!" ); // <= yes
  }

  // Only enter acquisition loop if the head vertex is active
  if( __vertex_is_active_context( head_ANY ) ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
      // Block as long as the head exists and we don't already own it writable or we can't obtain its inarcs by borrowing them
      // H: head vertex exists
      // W: current thread owns the vertex writable
      // B: the vertex inarcs are available to be borrowed
      // BLOCK WHILE TRUE: H * /W * /B = H * /(W + B)
      BEGIN_EXECUTION_BLOCKED_WHILE( head_ANY && !( __vertex_is_locked_writable_by_current_thread( head_ANY ) || __vertex_is_inarcs_available( head_ANY ) ) ) {
        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( head_ANY ) {
          __WAIT_VERTEX_CS( self, &head_ANY ) {
            // Mark our request to write to vertex so subsequent readers will yield
            __vertex_set_write_requested( head_ANY ); // <= will request only if locked readonly and no previous wrq set
            // Allow other threads to modify the tail's inarcs while we're blocked on head
            _vxgraph_state__yield_vertex_inarcs_and_wait_CS_WL( self, tail_WL, 0 );
          } __VERIFY_ACTIVE_VERTEX_CONTINUE_CS;
        } END_WAIT_FOR_EXECUTION_RESOURCE;
      } END_EXECUTION_BLOCKED_WHILE;

      __BEGIN_CHECK_VERTEX_ACCESS( head_ANY, &timing_budget->reason ) {
        // We already own write access to the head, increment the recursion count
        if( __vertex_is_locked_writable_by_current_thread( head_ANY ) ) {
          if( !__vertex_is_semaphore_writer_reentrant( head_ANY ) ) {
            PRINT_VERTEX( head_ANY );
            VXGRAPH_STATE_FATAL( self, 0xC23, "Vertex write lock recursion overflow!" ); // die instead of deadlock
          }
          __vertex_inc_writer_recursion_CS( head_ANY );
          head_iWL = head_ANY;
          Vertex_INCREF_WL( head_iWL ); // Guaranteed exclusive vertex access since we already own WL so we use the nolock incref
          //
          // NOTE: It is assumed this function is transient and we're not tracking vertex acquisition
          //
          __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
        }
        // We don't already own write access to head, but we can borrow the inarcs or become writer
        else if( __vertex_is_inarcs_available( head_ANY ) ) {
          // When we're not blocked we borrow the terminal inarcs
          head_iWL = __borrow_vertex_inarcs_CS( head_ANY );
          __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
        }
        // Timed out
        else {
          // No longer interested
          __vertex_clear_write_requested( head_ANY );
          __no_writer_access_reason( head_ANY, EXECUTION_BUDGET_TIMEOUT, &timing_budget->reason );
        }
      } __END_CHECK_VERTEX_ACCESS;
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  }
  // Head vertex is suspended
  else {
    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_NOEXIST );
  }

  // not NULL if we managed to lock the head inarcs
  return head_iWL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxgraph_state__create_arc_timestamps_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL, int lifespan, uint32_t *tmx ) {
  int n_added = 0;
  vgx_predicator_t pred = arc_WL->head.predicator;
  uint32_t now = _vgx_graph_seconds( self );

  XTRY {
    int n;
    // Copy the arc so we can override modifier and value for setting timestamps for this relationship
    vgx_predicator_t tm_pred = {
      .val = { .uinteger = now },
      .rel = arc_WL->head.predicator.rel,
      .mod = VGX_PREDICATOR_MOD_TIME_CREATED,
      .eph = VGX_PREDICATOR_EPH_TYPE_NONE
    };

    // Override predicator
    arc_WL->head.predicator = tm_pred;

    // No creation time exists, add TMC arc
    if( _vgx_predicator_is_none( iarcvector.GetArcValue( &self->arcvector_fhdyn, &arc_WL->tail->outarcs, &arc_WL->head ) ) ) {
      if( (n = iarcvector.Add( &self->arcvector_fhdyn, arc_WL, _vxgraph_arc__connect_WL_reverse_WL )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC31 );
      }
      n_added += n;
    }
    
    // Add/update TMM arc
    arc_WL->head.predicator.mod.bits = VGX_PREDICATOR_MOD_TIME_MODIFIED;
    if( (n = iarcvector.Add( &self->arcvector_fhdyn, arc_WL, _vxgraph_arc__connect_WL_reverse_WL )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC32 );
    }
    n_added += n;

    // Add/update TMX arc if lifespan specified
    if( lifespan > -1 ) {
      arc_WL->head.predicator.mod.bits = VGX_PREDICATOR_MOD_TIME_EXPIRES;
      *tmx = arc_WL->head.predicator.val.uinteger = now + lifespan;
      if( (n = iarcvector.Add( &self->arcvector_fhdyn, arc_WL, _vxgraph_arc__connect_WL_reverse_WL )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC33 );
      }
      n_added += n;
    }
  }
  XCATCH( errcode ) {
    n_added = -1;
  }
  XFINALLY {
    // Restore predicator
    arc_WL->head.predicator = pred;
  }

  return n_added;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __match_target( vgx_Graph_t *self, const vgx_ArcVector_cell_t *arcvector, vgx_ArcHead_t *head_probe, vgx_predicator_t probeB ) {
  vgx_predicator_t target = iarcvector.GetArcValue( &self->arcvector_fhdyn, arcvector, head_probe );
  switch( iArcFilter.LogicFromPredicators( head_probe->predicator, probeB ) ) {
  case VGX_LOGICAL_NO_LOGIC:
    return __predicator_has_relationship( target );
  case VGX_LOGICAL_AND:
    // hit = A * B 
    return __predicator_has_relationship( target ) && predmatchfunc.Generic( probeB, target );
  case VGX_LOGICAL_OR:
    // hit = A + B
    return __predicator_has_relationship( target ) || predmatchfunc.Generic( probeB, target );
  default:
    return false; // unsupported logic error, miss
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __exists_arc( vgx_Graph_t *self, vgx_Vertex_t *tail_WL, vgx_Vertex_t *head_WL, vgx_ArcConditionSet_t *arc_condition_set, CString_t **CSTR__error ) {
  if( !arc_condition_set || !arc_condition_set->set || !*arc_condition_set->set ) {
    return 1;
  }

  vgx_ArcHead_t head_probe = {
    .vertex = head_WL,
    .predicator = VGX_PREDICATOR_NONE
  };
  vgx_predicator_t probeB = VGX_PREDICATOR_NONE;
  if( iArcFilter.ConfigurePredicatorsFromArcConditionSet( self, arc_condition_set, &head_probe.predicator, &probeB ) < 0 ) {
    if( CSTR__error ) {
      *CSTR__error = arc_condition_set->CSTR__error;
      arc_condition_set->CSTR__error = NULL;
    }
    return -1; // error
  }
  else {
    bool match;
    switch( arc_condition_set->arcdir ) {
    case VGX_ARCDIR_ANY:
      // OUT or IN
      if( (match = __match_target( self, &tail_WL->outarcs, &head_probe, probeB )) == false ) {
        match = __match_target( self, &tail_WL->inarcs, &head_probe, probeB );
      }
      break;
    case VGX_ARCDIR_IN:
      // IN
      match = __match_target( self, &tail_WL->inarcs, &head_probe, probeB );
      break;
    case VGX_ARCDIR_OUT:
      // OUT
      match = __match_target( self, &tail_WL->outarcs, &head_probe, probeB );
      break;
    case VGX_ARCDIR_BOTH:
      // OUT and IN
      if( (match = __match_target( self, &tail_WL->outarcs, &head_probe, probeB )) == true ) {
        match = __match_target( self, &tail_WL->inarcs, &head_probe, probeB );
      }
      break;
    default:
      match = false;
    }
    int neg = !(*arc_condition_set->set)->positive;
    return match ^ neg;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__create_arc_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL, int lifespan, vgx_ArcConditionSet_t *arc_condition_set, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int n_added = 0;
  int n;

  if( arc_condition_set ) {
    int match_result = __exists_arc( self, arc_WL->tail, arc_WL->head.vertex, arc_condition_set, CSTR__error );
    if( match_result <= 0 ) {
      return match_result;
    }
  }

  vgx_predicator_modifier_enum mod_enum = _vgx_predicator_as_modifier_enum( arc_WL->head.predicator );
  uint32_t autotmx = TIME_EXPIRES_NEVER;

  // Auto timestamps?
  if( arc_WL->head.predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_AUTO_TM ) {
    if( mod_enum == VGX_PREDICATOR_MOD_TIME_CREATED 
        ||
        mod_enum == VGX_PREDICATOR_MOD_TIME_MODIFIED
        ||
        (mod_enum == VGX_PREDICATOR_MOD_TIME_EXPIRES && lifespan > -1 ) )
    {
      __set_access_reason( reason, VGX_ACCESS_REASON_VERTEX_ARC_ERROR );
      __set_error_string( CSTR__error, "Arc timestamps are automatic, manual timestamp assignment not allowed" );
      return -1; // Can't set timestamps when auto timestamps are active
    }
    if( (n_added = _vxgraph_state__create_arc_timestamps_WL( self, arc_WL, lifespan, &autotmx )) < 0 ) {
      return -1;      
    }
  }
  // Check if we're setting TMC manually that it doesn't already exist
  else if( mod_enum == VGX_PREDICATOR_MOD_TIME_CREATED ) {
    if( !_vgx_predicator_is_none( iarcvector.GetArcValue( &self->arcvector_fhdyn, &arc_WL->tail->outarcs, &arc_WL->head ) ) ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_VERTEX_ARC_ERROR );
      __set_error_string( CSTR__error, "Arc creation time already set" );
      return -1; // Can't set TMC when already set
    }
  }

  //
  // Add the arc
  //
  
  // Extract the terminal's inarcs arcvector type
  _vgx_ArcVector_cell_type term_avtype =  __arcvector_cell_type( &arc_WL->head.vertex->inarcs );

  if( arc_WL->head.predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_FWDARCONLY ) {
    // Verify the terminal has no normal arcs. Forward-only arcs and normal arcs are mutually exclusive for any terminal.
    if( term_avtype != VGX_ARCVECTOR_NO_ARCS && term_avtype != VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_VERTEX_ARC_ERROR );
      __set_error_string( CSTR__error, "Forward-only arc not allowed when terminal already has regular inarc(s)" );
      return -1;
    }
    n = iarcvector.Add( &self->arcvector_fhdyn, arc_WL, _vxgraph_arc__connect_WL_to_WL_no_reverse );
  }
  else {
    // Verify the terminal has no forward-only arcs. Forward-only arcs and normal arcs are mutually exclusive for any terminal.
    if( term_avtype == VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
      __set_access_reason( reason, VGX_ACCESS_REASON_VERTEX_ARC_ERROR );
      __set_error_string( CSTR__error, "Regular arc not allowed when terminal already has forward-only inarc(s)" );
      return -1;
    }
    n = iarcvector.Add( &self->arcvector_fhdyn, arc_WL, _vxgraph_arc__connect_WL_reverse_WL );
  }

  if( n < 0 ) {
    VXGRAPH_STATE_CRITICAL( self, 0xC41, "Internal arcvector error" );
    __set_access_reason( reason, VGX_ACCESS_REASON_VERTEX_ARC_ERROR );
    __set_error_string( CSTR__error, "Internal arcvector error" );
    return -1;
  }

  n_added += n;

  __check_vertex_consistency_WL( arc_WL->tail );
  __check_vertex_consistency_WL( arc_WL->head.vertex );

  // If expiration was set, make sure event is scheduled for vertex
  uint32_t tmx = TIME_EXPIRES_NEVER;
  if( _vgx_predicator_is_expiration( arc_WL->head.predicator ) ) {
    tmx = arc_WL->head.predicator.val.uinteger; 
  }
  if( autotmx < tmx ) {
    tmx = autotmx;
  }

  if( tmx < TIME_EXPIRES_NEVER ) {
    vgx_Vertex_t *vertex_WL = arc_WL->tail;
    // Update the vertex with the new arc tmx if smaller than a previous arc tmx
    if( tmx < __vertex_arcvector_get_expiration_ts( vertex_WL ) ) {
      __vertex_arcvector_set_expiration_ts( vertex_WL, tmx );
      // The new arc tmx is also earlier than vertex tmx: schedule the event to trigger for this new earliest tmx in the vertex
      if( tmx < __vertex_get_expiration_ts( vertex_WL ) ) {
        iGraphEvent.ScheduleVertexEvent.Expiration_WL( self, vertex_WL, tmx );
        __check_vertex_consistency_WL( vertex_WL );
      }
    }
  }

  __CHECK_ARC_BALANCE( self )
  return n_added;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__remove_arc_WL( vgx_Graph_t *self, vgx_Arc_t *arc_WL ) {
  f_Vertex_disconnect_event disconnect_event;

  switch( arc_WL->head.predicator.rel.dir ) {
  case VGX_ARCDIR_IN:
    disconnect_event = _vxgraph_arc__disconnect_WL_forward_WL;
    break;
  case VGX_ARCDIR_OUT:
    disconnect_event = _vxgraph_arc__disconnect_WL_reverse_WL;
    break;
  default:
    return -1;
  }

  vgx_ExecutionTimingBudget_t zero_budget = _vgx_get_zero_execution_timing_budget();
  int64_t n_removed = iarcvector.Remove( &self->arcvector_fhdyn, arc_WL, &zero_budget, disconnect_event );

  __CHECK_ARC_BALANCE( self )

  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __remove_outarcs_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_predicator_t probe, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event reverse_disconnect ) {
  vgx_Arc_t forward_WL_to_ANY = {
    .tail = vertex_WL,
    .head = {
      .vertex     = NULL,
      .predicator = probe
    }
  };

  forward_WL_to_ANY.head.predicator.rel.dir = VGX_ARCDIR_OUT; // just to be clear

  int64_t n_removed = iarcvector.Remove( &self->arcvector_fhdyn, &forward_WL_to_ANY, timing_budget, reverse_disconnect );

  __CHECK_ARC_BALANCE( self )

  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __remove_inarcs_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL, vgx_predicator_t probe, vgx_ExecutionTimingBudget_t *timing_budget, f_Vertex_disconnect_event forward_disconnect ) {
  vgx_Arc_t reverse_iWL_to_ANY = {
    .tail = vertex_iWL,
    .head = {
      .vertex     = NULL,
      .predicator = probe
    }
  };

  reverse_iWL_to_ANY.head.predicator.rel.dir = VGX_ARCDIR_IN; // just to be clear

  int64_t n_removed = iarcvector.Remove( &self->arcvector_fhdyn, &reverse_iWL_to_ANY, timing_budget, forward_disconnect );

  __CHECK_ARC_BALANCE( self )

  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__remove_outarcs_CS_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe ) {
  __assert_state_lock( self );
  return __remove_outarcs_WL( self, vertex_WL, probe, timing_budget, _vxgraph_arc__disconnect_WL_reverse_CS );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__remove_outarcs_OPEN_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe ) {
  return __remove_outarcs_WL( self, vertex_WL, probe, timing_budget, _vxgraph_arc__disconnect_WL_reverse_GENERAL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__remove_inarcs_CS_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe ) {
  __assert_state_lock( self );
  return __remove_inarcs_iWL( self, vertex_iWL, probe, timing_budget, _vxgraph_arc__disconnect_iWL_forward_CS );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__remove_inarcs_OPEN_iWL( vgx_Graph_t *self, vgx_Vertex_t *vertex_iWL, vgx_ExecutionTimingBudget_t *timing_budget, vgx_predicator_t probe ) {
  return __remove_inarcs_iWL( self, vertex_iWL, probe, timing_budget, _vxgraph_arc__disconnect_iWL_forward_GENERAL );
}



/*******************************************************************//**
 * Obtain an exclusive write lock for vertex and own a reference.
 *
 * Returns: Pointer to write locked vertex or NULL if vertex was deleted
 *          from graph or we timed out while waiting for lock.
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_vertex_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *vertex, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_WL = NULL;
  vgx_AccessReason_t *reason = &timing_budget->reason;

  // Recursive write lock
  if( __vertex_is_locked_writable_by_current_thread( vertex ) ) {
    vertex_WL = vertex;
    // Commit if brand new vertex
    if( __vertex_is_suspended_context( vertex_WL ) ) {
      if( _vxdurable_commit__commit_vertex_CS_WL( self, vertex_WL, false ) < 0 ) {
        return NULL;
      }
    }

    // Acquire another lock and reference
    if( __vertex_is_semaphore_writer_reentrant( vertex_WL ) ) {
      __vertex_inc_writer_recursion_CS( vertex_WL );
      Vertex_INCREF_WL( vertex_WL );
      __check_vertex_consistency_WL( vertex_WL );
      if( __vgx_vertex_record_acquisition( record ) ) {
        _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_WL );
      }
      __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
      return vertex_WL;
    }
    else {
      __set_access_reason( reason, VGX_ACCESS_REASON_SEMAPHORE );
      return NULL;
    }
  }

  // Only enter acquisition loop if the vertex is active
  if( __vertex_is_active_context( vertex ) ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {

      // Graph is not readonly locked
      if( _vgx_is_writable_CS( &self->readonly ) ) {
        // Graph readonly is not pending, or if it is this thread already has WL locks so we are allowed to proceed
        if( _vgx_readonly_not_requested_CS( &self->readonly ) || _vxgraph_tracker__has_writable_locks_CS( self ) ) {
          // Inspect the existing vertex's state to determine if we can acquire it for both CX_WRITE and CX_READ
          BEGIN_EXECUTION_BLOCKED_WHILE( vertex && !__vertex_is_lockable_as_writable(vertex) ) { // <= while locked by some other thread
            BEGIN_WAIT_FOR_EXECUTION_RESOURCE( vertex ) {
              __WAIT_VERTEX_CS( self, &vertex ) {
                // Mark our request to write so subsequent readers will yield
                __vertex_set_write_requested( vertex ); // <= will only have effect if vertex is readonly locked and no previous wrq set 
                // We have to wait for a signal to retry the acquisition
                WAIT_FOR_VERTEX_AVAILABLE( self, 1 ); // this releases the graph lock and sleeps here until the signal to continue, then tries to re-acquire
              } __VERIFY_ACTIVE_VERTEX_CONTINUE_CS;
            } END_WAIT_FOR_EXECUTION_RESOURCE;
          } END_EXECUTION_BLOCKED_WHILE;

          // Now we'll be able to lock vertex unless it disappeared or we timed out while waiting
          __BEGIN_CHECK_VERTEX_ACCESS( vertex, reason ) {
            if( __vertex_is_lockable_as_writable(vertex) ) {
              vgx_VertexDescriptor_t pre_lock = vertex->descriptor;
              // We now hold a lock to the vertex and own a reference
              vertex_WL = __vertex_lock_writable_CS( vertex );
              // Open vertex operation for capture
              if( __vgx_vertex_record_operation( record ) && iOperation.Open_CS( self, &vertex_WL->operation, COMLIB_OBJECT( vertex_WL ), false ) < 0 ) {
                // Failed
                __set_access_reason( reason, VGX_ACCESS_REASON_OPFAIL );
                vertex_WL->descriptor = pre_lock;
                vertex_WL = NULL;
              }
              // Success
              else {
                // Become independent owner of locked vertex
                Vertex_INCREF_WL( vertex_WL ); // Guaranteed exclusive vertex access since we got WL so we used the nolock incref
                __check_vertex_consistency_WL( vertex_WL );
                if( __vgx_vertex_record_acquisition( record ) ) {
                  _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_WL );
                }
                __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
              }
            }
            // Timed out
            else {
              // No longer interested
              __vertex_clear_write_requested( vertex );
              __no_writer_access_reason( vertex, EXECUTION_BUDGET_TIMEOUT, reason );
            }
          } __END_CHECK_VERTEX_ACCESS;
        }
        // A graph readonly request is currently pending and this thread does not have WL vertices
        else {
          __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_PENDING );
        }
      }
      else {
        _vgx_request_write_CS( &self->readonly );
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
      }

      // Caller of function is CS locked and may continue to try locking indefinitely.
      // If graph is (or is trying to become) readonly, suspend CS here to ensure
      // progress in other parts of the program.
      if( vertex_WL == NULL && __is_access_reason_readonly( *reason ) ) {
        WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
      }
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  }
  // Vertex suspended
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }

  // Not NULL if we got the lock
  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __trap_vertex_isolation_error( vgx_Graph_t *self, vgx_Vertex_t *vertex, int64_t refcnt ) {
#ifdef VGX_CONSISTENCY_CHECK
          PRINT_VERTEX( vertex );
          vgx_Vertex_t *indexed_vertex = _vxgraph_vxtable__query_CS( self, NULL, COMLIB_OBJECT_GETID( vertex ), VERTEX_TYPE_ENUMERATION_WILDCARD );
          if( indexed_vertex == vertex ) {
            VXGRAPH_STATE_INFO( self, 0xC53, "Vertex still indexed" );
          }
          else if( indexed_vertex == NULL ) {
            VXGRAPH_STATE_INFO( self, 0xC54, "Vertex not indexed!" );
          }
          else {
            VXGRAPH_STATE_INFO( self, 0xC55, "Incorrect result from index lookup: vertex pointer: 0x%llp, expected: 0x%llp", indexed_vertex, vertex );
          }
          iarcvector.PrintDebugDump( &vertex->outarcs, "Vertex outarcs" );
          iarcvector.PrintDebugDump( &vertex->inarcs, "Vertex inarcs" );

          // Restore FATAL since we don't coredump in release mode anymore
          VXGRAPH_STATE_FATAL( self, 0xC56, "Vertex isolation assertion error (refc=%lld)", refcnt );
#else
          // In release mode, we do best effort to correct the refcount.
          // Add references to safeguard the vertex from premature invalidation. (This may not be the correct refcnt.)
          // Justification: We confirmed above that the refcount was equal to the number of indexes containing the vertex AND that the vertex is indeed indexed.
          //                This should mean that the vertex has zero degree and not referenced by any other tables (like event processor). We also know the
          //                vertex was WL when entering this function and that the recursive WL has gone to zero. But we arrived here in the error condition
          //                because the vertex is not isolated, meaning it has degree > 0. It may or may not have additional referrers. The correct refcount
          //                is therefore AT LEAST the number of indices plus the degree. We may have to correct refcount again in the future.
          int64_t corrected_refcnt = VXTABLE_VERTEX_REFCOUNT + CALLABLE( vertex )->Degree( vertex );  // minimal refcount
          cxmalloc_linehead_t *vertex_linehead = _cxmalloc_linehead_from_object( vertex );
          vertex_linehead->data.refc = (int)corrected_refcnt;
          const char *prefix = CALLABLE( vertex )->IDPrefix( vertex );
          VXGRAPH_STATE_CRITICAL( self, 0xC57, "Vertex isolation assertion error for vertex: '%s'. Correcting refcount: %lld -> %lld", prefix, refcnt, corrected_refcnt );
#endif
}





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
//DLL_HIDDEN bool _vxgraph_state__unlock_vertex_writable_CS_WL( vgx_Graph_t *self, vgx_Vertex_t **vertex_WL, bool track ) {
static bool __unlock_vertex_writable_CS_WL( vgx_Graph_t *self, vgx_Vertex_t **vertex_WL, vgx_vertex_record record ) {
  __assert_state_lock( self );
  bool released = false;
  vgx_Vertex_t *vertex = *vertex_WL;
  // Recursion will not go to zero
  if( __vertex_get_writer_recursion( *vertex_WL ) > 1 ) {
    // Give up vertex ownership
    __vertex_dec_writer_recursion_CS( vertex );
    if( Vertex_DECREF_WL( vertex ) < 1 ) { // Guaranteed exclusive vertex access since we still own WL so use the nolock decref
      PRINT_VERTEX( vertex );
      VXGRAPH_STATE_FATAL( self, 0xC51, "Unexpected drop in vertex refcount after writable unlock" );
    }
    released = true;
  }
  // Recursion count will go to zero, now we unlock and commit
  else {
    // Persist all modifications to vertex object and close the operation
    int64_t opid = _vxdurable_commit__close_vertex_CS_WL( self, *vertex_WL );

    // Operation ok, now release the vertex
    if( opid >= 0 ) {

      // Writer recursion is exactly ONE here.
      // Refcount is at least ONE, but could be higher.
      //
      int64_t refcnt = Vertex_REFCNT_WL( vertex ); // 1 or more


      // The last reference now be released, which means vertex is not indexed and not
      // connected to any other vertices, not scheduled for events or used by any other
      // parts of the system.
      if( refcnt == 1 ) {
        // This will deconstruct the vertex, unlock writelock/counter, and deallocate
        Vertex_DECREF_WL( vertex );
      }
      // Other parts of the system reference the vertex.
      else if( refcnt > 1 ) {
        
        // If AFTER decref of the ONE writer recursion the vertex refcnt is:
        //  0: Then the vertex is automatically destroyed.
        //  1: Then the vertex is not indexed but some thread still holds a reference
        //  2: Then the vertex is either 
        //        a) only referenced by the index (most likely), or
        //        b) held by other thread(s)
        // >2: Then multiple references are left and several should be held by vxtable and the others by arcs (either inbound arc(s) or reverse arc(s) from outbound), and 
        //     possibly additional references from other system components.

        // Remain WRITABLE until we're done
        //

        // Scenario 2a: Vertex owned by index only
        if( __vertex_owned_by_index_only_LCK( vertex, refcnt-1 ) ) {
          // All vertex references left should be held by vxtable and vertex is therefore ISOLATED in graph
          if( __vertex_is_isolated( vertex ) ) {
            // Isolated VIRTUAL (or NULL) vertex cannot exist in graph and will be destroyed
            if( __vertex_is_manifestation_virtual( vertex ) || __vertex_is_manifestation_null( vertex ) ) {
              // Unindex virtual vertex - this will trigger vertex destructor since framehash releases its ownership and it will be the last reference
              if( _vxgraph_vxtable__unindex_vertex_CS_WL( self, vertex ) < 1 ) { // At least one reference should be given up by unindexing from vxtable
                // error
                PRINT_VERTEX( vertex );
                VXGRAPH_STATE_CRITICAL( self, 0xC55, "Unexpected failure to remove isolated virtual vertex" );
                // TODO: find a less severe way to handle this error
              }
            }
            // Isolated, REAL, indexed, refcnt-1 = 2
            else {
              /* ok */
            }
          }
          // Vertex confirmed indexed only but is also NOT isolated. This is a refcount error.
          else {
            __trap_vertex_isolation_error( self, vertex, refcnt );
          }
        }

        // Current thread who is giving up writelock is the LAST owner
        if( Vertex_REFCNT_WL( vertex ) == 1 ) {
          // This will deconstruct the vertex, unlock writelock/counter, and deallocate
#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
          int recursion = __vertex_get_writer_recursion( vertex );
          int64_t WL_cnt = _vgx_graph_get_vertex_WL_count_CS( self );
          if( recursion > WL_cnt ) {
            VXGRAPH_STATE_FATAL( self, 0xEEE, "Recursion %d > WL_cnt %lld", recursion, WL_cnt );
          }
#endif
          Vertex_DECREF_WL( vertex );
        }
        // Vertex still in use by other parts of the system
        else {
          // This will NOT bring refcount to zero
          refcnt = Vertex_DECREF_WL( vertex );

          // Unlock vertex
          if( __vertex_unlock_writable_CS( vertex ) != 0 ) {
            // error
            PRINT_VERTEX( vertex );
            VXGRAPH_STATE_CRITICAL( self, 0xC52, "Non-zero writer recursion after unlock" );
          }
#ifdef VGX_CONSISTENCY_CHECK
          __check_vertex_consistency_WL( vertex );
#endif
        }
      }
      // Refcount was ZERO!
      else {
        VXGRAPH_STATE_FATAL( self, 0xC56, "Unexpected vertex refcount: %lld", refcnt );
      }
      released = true;
    }
  }

  // Invalidate caller's vertex reference if vertex was released
  if( released ) {
    if( __vgx_vertex_record_acquisition( record ) ) {
      _vxgraph_tracker__unregister_vertex_WL_for_thread_CS( self, vertex );
    }
    *vertex_WL = NULL;
  }

  // true or false
  return released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__lock_terminal_inarcs_writable_CS( vgx_Graph_t *self, vgx_Vertex_t *terminal, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  vgx_Vertex_t *terminal_iWL = NULL;
  vgx_AccessReason_t *reason = &timing_budget->reason;

  // TODO dev_roreq
  // Look at this function to determine the roreq rules. Not sure yet.
  //

  // Only enter acquisition loop if the terminal is active
  if( __vertex_is_active_context( terminal ) ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {

      if( _vgx_is_writable_CS( &self->readonly ) ) {
        // Graph readonly is not pending, or if it is this thread already has WL locks so we are allowed to proceed
        if( _vgx_readonly_not_requested_CS( &self->readonly ) || _vxgraph_tracker__has_writable_locks_CS( self ) ) {
          // Block as long as the terminal exists and we don't already own it writable or we can't obtain its inarcs by borrowing them
          // H: terminal vertex exists
          // W: current thread owns the vertex writable
          // B: the vertex inarcs are available to be borrowed
          // BLOCK WHILE TRUE: H * /W * /B = H * /(W + B)
          BEGIN_EXECUTION_BLOCKED_WHILE( terminal && !( __vertex_is_locked_writable_by_current_thread( terminal ) || __vertex_is_inarcs_available( terminal ) ) ) {
            BEGIN_WAIT_FOR_EXECUTION_RESOURCE( terminal ) {
              __WAIT_VERTEX_CS( self, &terminal ) {
                // Mark our request to write to vertex so subsequent readers will yield
                __vertex_set_write_requested( terminal ); // <= will only have effect if terminal is currently readonly and wrq not already set
                // Wait for other thread to signal vertex availability (may be fully unlocked, or the inarcs may be yielded), or timeout to check again
                WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
              } __VERIFY_ACTIVE_VERTEX_CONTINUE_CS;
            } END_WAIT_FOR_EXECUTION_RESOURCE;
          } END_EXECUTION_BLOCKED_WHILE;

          // We can now lock the terminal inarcs if terminal still exists and we didn't time out
          __BEGIN_CHECK_VERTEX_ACCESS( terminal, reason ) {
            // We already own write access to the terminal, increment the recursion count
            if( __vertex_is_locked_writable_by_current_thread( terminal ) ) {
              if( !__vertex_is_semaphore_writer_reentrant( terminal ) ) {
                PRINT_VERTEX( terminal );
                VXGRAPH_STATE_FATAL( self, 0xC61, "Vertex write lock recursion overflow!" ); // die instead of deadlock
              }
              __vertex_inc_writer_recursion_CS( terminal );
              terminal_iWL = terminal;
              Vertex_INCREF_WL( terminal_iWL ); // Guaranteed exclusive vertex access since we already own WL so we use the nolock incref
              //
              // NOTE: It is assumed this function call is transient so we don't track vertex acquisitions
              //
              __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
            }
            // We don't already own write access to terminal, but we can borrow the inarcs or become writer
            else if( __vertex_is_inarcs_available( terminal ) ) {
              // When we're not blocked we borrow the inarcs
              terminal_iWL = __borrow_vertex_inarcs_CS( terminal ); // either borrow inarcs or become writer
              __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
            }
            // Timed out
            else {
              // No longer interested
              __vertex_clear_write_requested( terminal );
              __no_writer_access_reason( terminal, EXECUTION_BUDGET_TIMEOUT, reason );
            }
          } __END_CHECK_VERTEX_ACCESS;
        }
        else {
          __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_PENDING );
        }
      }
      else {
        _vgx_request_write_CS( &self->readonly );
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
      }
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  }
  // Terminal suspended
  else {
    __set_access_reason( reason, VGX_ACCESS_REASON_NOEXIST );
  }

  // Not NULL if we were able to borrow the terminal inarcs
  return terminal_iWL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( vgx_Graph_t *self, vgx_Vertex_t **terminal_iWL ) {
  __assert_state_lock( self );
  bool released = false;

  // If we got the inarcs because they were yielded by the terminal while it was itself waiting for another vertex,
  // then we only return the busy flag to zero and commit the inarcs, and rely on the terminal to perform further commits
  // and cleanup when it eventually releases its fully locked state.
  if( __vertex_is_borrowed_inarcs_busy( *terminal_iWL ) ) {
    // Persist inarcs modifications only
    if( _vxdurable_commit__commit_inarcs_iWL( self, *terminal_iWL ) >= 0 ) {
      // We are the ones who borrowed the inarcs so let's return them now
      __return_vertex_inarcs_CS_iWL( self, *terminal_iWL );
      released = true;
    }
  }
  // Otherwise we got the inarcs because we managed to get the full lock, and if so are responsible for committing and cleaning up flags.
  else {
    vgx_Vertex_t **terminal_WL = terminal_iWL; // just to be clear - the at-least-inarc-locked terminal is actually fully locked, which is ok
    released = __unlock_vertex_writable_CS_WL( self, terminal_WL, VGX_VERTEX_RECORD_OPERATION ); // <- was acquired without tracking due to transient nature, so don't track release either
  }

  // true or false
  return released;
}



/*******************************************************************//**
 *
 * Change vertex manifestation
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxgraph_state__convert_WL( vgx_Graph_t *self, vgx_Vertex_t *vertex_WL, vgx_VertexStateContext_man_t man, vgx_vertex_record record ) {

  if( __vertex_get_manifestation( vertex_WL ) != man ) {
    // Becomes REAL or VIRTUAL
    __vertex_set_manifestation( vertex_WL, man );
    // Capture
    if( __vgx_vertex_record_operation( record ) ) {
      iOperation.Vertex_WL.Convert( vertex_WL, man );
    }
    // Graph -> dirty
    GRAPH_LOCK( self ) {
      iOperation.Graph_CS.SetModified( self );
    } GRAPH_RELEASE;
  }
}



/*******************************************************************//**
 *
 * NOTE: if ret_vertex_WL is not NULL, the created (or existing) vertex
 *       will be returned in this pointer, locked writable.
 *
 * Returns: 1: Vertex created
 *          0: Vertex already exists
 *         -1: Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__create_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, const CString_t *CSTR__vertex_typename, vgx_Vertex_t **ret_vertex_WL, int timeout_ms, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  __assert_state_lock( self );
  int n_created = -1;

  // TODO dev_roreq: 
  // If roreq flag is set:
  //   Allow operation if current thread already has WL vertices
  //   Disallow operation if current thread does not already have WL vertices

  vgx_vertex_record record = VGX_VERTEX_RECORD_OPERATION;
  if( ret_vertex_WL != NULL ) {
    record |= VGX_VERTEX_RECORD_ACQUISITION;
  }


  if( _vgx_is_writable_CS( &self->readonly ) ) {
    // Graph readonly is not pending, or if readonly is pending this thread already has WL locks so we are allowed to proceed
    if( _vgx_readonly_not_requested_CS( &self->readonly ) || _vxgraph_tracker__has_writable_locks_CS( self ) ) {

      // Map vertex type
      CString_t *CSTR__mapped_instance = NULL;
      vgx_vertex_type_t new_vertex_type = iEnumerator_CS.VertexType.Encode( self, CSTR__vertex_typename, &CSTR__mapped_instance, true );
      if( !__vertex_type_enumeration_valid( new_vertex_type ) ) {
        if( new_vertex_type == VERTEX_TYPE_ENUMERATION_INVALID ) {
          if( CSTR__error && !*CSTR__error ) {
            *CSTR__error = CStringNewFormat( "invalid vertex type: '%s'", CStringValue( CSTR__vertex_typename ) );
          }
        }
        else {
          __set_access_reason( reason, VGX_ACCESS_REASON_ENUM_NOTYPESPACE );
        }
        return -1;
      }

      vgx_Vertex_t *vertex = NULL;
      vgx_Vertex_t *vertex_WL = NULL;
      bool exists = false;
      bool is_virtual = false;
      vgx_vertex_type_t existing_type = VERTEX_TYPE_ENUMERATION_NONE;

      BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( self ) {
        // Look up vertex by obid in index and create vertex if it does not exist
        if( (vertex = _vxgraph_vxtable__query_CS( self, NULL, vertex_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) == NULL ) { // we need to look in the global table
          BEGIN_DISALLOW_READONLY_CS( &self->readonly ) {
            if( (vertex_WL = __create_and_own_vertex_writable_CS( self, CSTR__vertex_idstr, vertex_obid, new_vertex_type, VERTEX_STATE_CONTEXT_MAN_REAL, record, CSTR__error )) == NULL ) {
              __set_access_reason( reason, VGX_ACCESS_REASON_NOCREATE );
            }
          } END_DISALLOW_READONLY_CS;
        }
        else {
          exists = true;
          existing_type = __vertex_get_type( vertex );
          is_virtual = __vertex_is_manifestation_virtual( vertex );
        }
      } END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS;

      // Virtual vertex exists, update type and manifestation
      if( exists && is_virtual ) {
        BEGIN_DISALLOW_READONLY_CS( &self->readonly ) {
          // Virtual vertex already exists: Convert to REAL and update type if needed
          vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
          if( (vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &timing_budget, record )) != NULL ) {
            __set_access_reason( reason, timing_budget.reason );
            // Update type if needed
            if( new_vertex_type != VERTEX_TYPE_ENUMERATION_NONE && existing_type != new_vertex_type ) {
              _vxgraph_vxtable__reindex_vertex_type_CS_WL( self, vertex_WL, new_vertex_type );
            }
            // Set REAL
            _vxgraph_state__convert_WL( self, vertex_WL, VERTEX_STATE_CONTEXT_MAN_REAL, record );
          }
          else {
          }
        } END_DISALLOW_READONLY_CS;
      }

      // Vertex did not previously exist, or it existed in a VIRTUAL state
      if( vertex == NULL || is_virtual ) {
        // Assign vertex to return variable, or if not provided release and commit
        if( vertex_WL ) {
          // Vertex will be returned - if commit requested commit if dirty
          if( __vgx_vertex_record_acquisition( record ) ) {
            if( iOperation.IsDirty( &vertex_WL->operation ) ) {
              if( _vxdurable_commit__commit_vertex_CS_WL( self, vertex_WL, false ) < 0 ) {
                // TODO: handle error
                VXGRAPH_STATE_CRITICAL( self, 0xC71, "Failed to commit vertex" );
              }
            }
            *ret_vertex_WL = vertex_WL; // never NULL when record acquisition bit is set
            n_created = 1;
          }
          // Vertex will not be returned - release and commit
          else {
            if( __unlock_vertex_writable_CS_WL( self, &vertex_WL, VGX_VERTEX_RECORD_OPERATION ) ) {
              n_created = 1; // success (created new or converted virtual to real)
            }
            // Error
            else {
              // TODO: handle error
              VXGRAPH_STATE_CRITICAL( self, 0xC72, "Failed to release and commit vertex" );
            }
          }
        }
      }
      // REAL Vertex already exists
      else {
        // Not allowed to switch type of existing REAL vertex - flag the error to ensure called does not get a misleading result when
        // opening existing vertex when a different type was used
        if( CSTR__vertex_typename && existing_type != new_vertex_type ) {
          if( CSTR__error && *CSTR__error == NULL ) {
            *CSTR__error = CStringNewFormat( "existing type: %s", CStringValue( CALLABLE(vertex)->TypeName(vertex) ) );
          }
          __set_access_reason( reason, VGX_ACCESS_REASON_TYPEMISMATCH );
        }
        // Type ok
        else {
          // Vertex will be returned - acquire write lock
          if( __vgx_vertex_record_acquisition( record ) ) {
            vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( self, timeout_ms );
            SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
            if( (*ret_vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL )) != NULL ) {
              n_created = 0;
            }
            __set_access_reason( reason, timing_budget.reason );
          }
          else {
            n_created = 0;
          }
        }
      }
    }
    // A graph readonly request is currently pending and this thread does not have WL vertices
    else {
      __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_PENDING );
      n_created = -1;
    }
  }
  // READONLY GRAPH
  else {
    _vgx_request_write_CS( &self->readonly );
    __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
    n_created = -1;
  }

  return n_created;
}



/*******************************************************************//**
 * This function acquires the vertex (V) for full write/read access.
 *
 * If (V) identified by vertex_obid does not exist, it is created in
 * accordance with the supplied default_manifestation (NULL, REAL or VIRTUAL)
 * using the (optional) vertex_idstr as the friendly vertex ID string.
 *
 * Use __release_writable_vertex() later to release the acquired vertex (V).
 *
 * Returns vertex (V) on success, NULL if vertex is not created or acquired.
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__acquire_writable_vertex_CS( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexStateContext_man_t default_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error ) {
  __assert_state_lock( self );

  vgx_Vertex_t *vertex_WL = NULL;

  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
    bool wait = true;
    BEGIN_EXECUTION_BLOCKED_WHILE( vertex_WL == NULL && wait ) {
      vgx_Vertex_t *vertex;
      bool exists = false;
      BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( self ) {
        // Look up vertex by obid in index
        if( (vertex = _vxgraph_vxtable__query_CS( self, NULL, vertex_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) == NULL ) {
          // Vertex does not exist - create new vertex if requested as default action.
          if( default_manifestation != VERTEX_STATE_CONTEXT_MAN_NULL ) {
            // Create and own a new vertex of the default VERTEX type, will be in the locked readwrite incomplete state, preparation context
            CString_t *CSTR__mapped_instance = NULL;
            if( (vertex_WL = __create_and_own_vertex_writable_CS( self, CSTR__vertex_idstr, vertex_obid, iEnumerator_CS.VertexType.Encode( self, NULL, &CSTR__mapped_instance, true ), default_manifestation, VGX_VERTEX_RECORD_ALL, CSTR__error )) != NULL ) {
              __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_OBJECT_CREATED );
            }
            // Vertex creation failed for some reason - exit wait loop
            else {
              wait = false;
            }
          }
          else {
            wait = false;
            __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_NOEXIST );
          }
        }
        else {
          exists = true;
        }
      } END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS;

      // Vertex exists - try to acquire it for writing
      if( vertex && exists ) {
        // Quick attempt
        vgx_ExecutionTimingBudget_t quick = _vgx_get_graph_execution_timing_budget( self, EXECUTION_BUDGET_IS_NONE ? 0 : 10 );
        vertex_WL = _vxgraph_state__lock_vertex_writable_CS( self, vertex, &quick, VGX_VERTEX_RECORD_ALL );
        __set_access_reason( &timing_budget->reason, quick.reason );
        if( vertex_WL != NULL ) {
          // Make sure its REAL. Outsiders (who acquire the vertex) only work with REAL vertices.
          _vxgraph_state__convert_WL( self, vertex_WL, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL );
        }
        // Timeout
        else {
          SET_EXECUTION_RESOURCE_BLOCKED( CSTR__vertex_idstr );
          if( __is_access_reason_readonly( timing_budget->reason ) ) {
            wait = false; // even if timeout not expired, we break out when the graph is (or can become) readonly
          }
          else if( __vertex_is_suspended_context( vertex ) ) {
            wait = false; // vertex is not acquirable due to suspended context
          }
        }
      }
    } END_EXECUTION_BLOCKED_WHILE;
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;

  return vertex_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__acquire_writable_vertex_OPEN( vgx_Graph_t *self, const CString_t *CSTR__vertex_idstr, const objectid_t *vertex_obid, vgx_VertexStateContext_man_t default_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error ) {

  if( timing_budget->t0_ms == 0 ) {
    _vgx_start_graph_execution_timing_budget( self, timing_budget );
  }

  vgx_Vertex_t *vertex_WL = NULL;
  GRAPH_LOCK( self ) {
    vertex_WL = _vxgraph_state__acquire_writable_vertex_CS( self, CSTR__vertex_idstr, vertex_obid, default_manifestation, timing_budget, CSTR__error );
  } GRAPH_RELEASE;

  return vertex_WL;
}



/*******************************************************************//**
 * Escalate existing readonly access for vertex (V) to full writable access.
 *
 * Returns WL vertex (V) on success, NULL if vertex could not be escalated.
 *
 * ASSUMPTION / WARNING:
 *  - The caller of this function MUST HOLD READONLY EXACTLY ONCE for the
 *    vertex, or the escalation will never succeeed. (Infinite loop will
 *    happen if timeout=-1 and the assumption is broken.)
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__escalate_to_writable_vertex_CS_RO( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record, CString_t **CSTR__error ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_WL = NULL;
  vgx_AccessReason_t *reason = &timing_budget->reason;

  // TODO dev_roreq: 
  // If roreq flag is set:
  //   Allow operation if current thread already has WL vertices
  //   Disallow operation if current thread does not already have WL vertices
  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
    if( _vgx_is_writable_CS( &self->readonly ) ) {
      // Graph readonly is not pending, or if it is this thread already has WL locks so we are allowed to proceed
      if( _vgx_readonly_not_requested_CS( &self->readonly ) || _vxgraph_tracker__has_writable_locks_CS( self ) ) {
        // Caller holds exactly one readlock (see ASSUMPTION above)
        // WARNING !!!
        // WARNING !!! If caller breaks the rules and calls this function while holding MULTIPLE READONLY
        // WARNING !!! locks or NO LOCK AT ALL we're going to have problems. With multiple readonly locks the
        // WARNING !!! loop below will be stuck forever (or until timeout occurs). If no lock is held by current
        // WARNING !!! thread there may still be other readers (so vertex will be marked as readonly!) and we'll
        // WARNING !!! incorrectly enter the loop below waiting until the reader count goes to 1 (one other thread)
        // WARNING !!! and then STEAL away that thread's lock and completely destroy the system.
        //
        //              ***************************************************************************************
        // WARNING !!!  ***** DO NOT CALL THIS FUNCTION UNLESS YOU HOLD EXACTLY ONE READONLY LOCK !!!!!!! *****
        //              ***************************************************************************************
        //
        if( __vertex_is_locked_readonly( vertex_RO ) ) {
          BEGIN_TIME_LIMITED_WHILE( vertex_WL == NULL, EXECUTION_TIME_LIMIT, EXECUTION_HALTED ) {
            if( __vertex_is_readonly_lockable_as_writable( vertex_RO ) ) { // <- ASSUME this thread already holds READONLY exactly once.
              vgx_VertexDescriptor_t pre_lock = vertex_RO->descriptor;
              // Lock
              __vertex_unlock_readonly_CS( vertex_RO );            // unlock, semcnt=0
              vertex_WL = __vertex_lock_writable_CS( vertex_RO );  // lock, semcnt=1

              // Open vertex operation for capture
              SUPPRESS_WARNING_DEREFERENCING_NULL_POINTER
              if( __vgx_vertex_record_operation( record ) && iOperation.Open_CS( self, &vertex_WL->operation, COMLIB_OBJECT( vertex_WL ), true ) < 0 ) {
                // Failed
                __set_access_reason( reason, VGX_ACCESS_REASON_OPFAIL );
                vertex_WL->descriptor = pre_lock; // back to readonly
                vertex_WL = NULL;
              }
              // Success
              else {
                if( __vgx_vertex_record_acquisition( record ) ) {
                  _vxgraph_tracker__unregister_vertex_RO_for_thread_CS( self, vertex_RO );
                  _vxgraph_tracker__register_vertex_WL_for_thread_CS( self, vertex_WL );
                }
                // Make sure it's REAL. Outsiders (who acquire the vertex) only work with REAL vertices.
                if( vertex_WL ) {
                  _vxgraph_state__convert_WL( self, vertex_WL, VERTEX_STATE_CONTEXT_MAN_REAL, record );
                  __set_access_reason( reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
                }
                else {
                  // Can't happen. Remove this.
                  iString.SetNew( NULL, CSTR__error, "internal error, vertex NULL pointer" );
                  __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
                }
              }
            }
            else {
              BEGIN_WAIT_FOR_EXECUTION_RESOURCE( vertex_RO ) {
                __vertex_set_write_requested( vertex_RO );
                __set_access_reason( reason, VGX_ACCESS_REASON_TIMEOUT );
                WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
              } END_WAIT_FOR_EXECUTION_RESOURCE;
            }
          } END_TIME_LIMITED_WHILE;

          if( vertex_WL == NULL ) {
            __vertex_clear_write_requested( vertex_RO );
          }

        }
        else {
          iString.SetNew( NULL, CSTR__error, "vertex is not readonly" );
          __set_access_reason( reason, VGX_ACCESS_REASON_BAD_CONTEXT );
        }
      }
      // A graph readonly request is currently pending and this thread does not have WL vertices
      else {
        __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_PENDING );
      }
    }
    else {
      _vgx_request_write_CS( &self->readonly );
      __set_access_reason( reason, VGX_ACCESS_REASON_READONLY_GRAPH );
    }
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  return vertex_WL;
}



/*******************************************************************//**
 * Escalate existing readonly access for vertex (V) to full writable access.
 *
 * Returns WL vertex (V) on success, NULL if vertex could not be escalated.
 *
 * ASSUMPTION / WARNING:
 *  - The caller of this function MUST HOLD READONLY EXACTLY ONCE for the
 *    vertex, or the escalation will never succeeed. (Infinite loop will
 *    happen if timeout=-1 and the assumption is broken.)
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__escalate_to_writable_vertex_OPEN_RO( vgx_Graph_t *self, vgx_Vertex_t *vertex_RO, vgx_ExecutionTimingBudget_t *timing_budget, vgx_vertex_record record, CString_t **CSTR__error ) {
  vgx_Vertex_t *vertex_WL = NULL;
  GRAPH_LOCK( self ) {
    vertex_WL = _vxgraph_state__escalate_to_writable_vertex_CS_RO( self, vertex_RO, timing_budget, record, CSTR__error );
  } GRAPH_RELEASE;
  return vertex_WL;
}



/*******************************************************************//**
 * Relax existing writable access for vertex (V) to readonly access.
 *
 * Returns:
 *  - If vertex has non-recursive WL, relax to RO and return vertex
 *  - If vertex has recursive WL, normal WL release and return WL vertex
 *  - If WL release fails do to commit error, return NULL
 *  - If vertex has RO already, no action
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__relax_to_readonly_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t *vertex_LCK, vgx_vertex_record record ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_RLX = vertex_LCK;
  if( __vertex_is_locked_writable_by_current_thread( vertex_LCK ) ) {
    // If writer recursion, normal release! (it is NOT relaxed to RO)
    if( __vertex_get_writer_recursion( vertex_LCK ) > 1 ) {
      __unlock_vertex_writable_CS_WL( self, &vertex_LCK, record );
    }
    // No WL recursion: We will now release WL and replace with RO
    else {
      vgx_Vertex_t *vertex_WL = vertex_LCK; // save the pointer
      
      // Commit before transition WL -> RO
      _vxdurable_commit__close_vertex_CS_WL( self, vertex_WL );

      // Unlock WL
      if( __vertex_unlock_writable_CS( vertex_WL ) != 0 ) {
        VXGRAPH_STATE_FATAL( self, 0x001, "Code error." );
      }

      // Lock RO
      vertex_RLX = __vertex_lock_readonly_CS( vertex_WL );

      // Change tracking from WL to RO
      if( __vgx_vertex_record_acquisition( record ) ) {
        _vxgraph_tracker__unregister_vertex_WL_for_thread_CS( self, vertex_WL );
        _vxgraph_tracker__register_vertex_RO_for_thread_CS( self, vertex_RLX );
      }
    }
  }

  return vertex_RLX;
}



/*******************************************************************//**
 * Relax existing writable access for vertex (V) to readonly access.
 *
 * Returns:
 *  - If vertex has non-recursive WL, relax to RO and return vertex
 *  - If vertex has recursive WL, normal WL release and return WL vertex
 *  - If WL release fails due to commit error, return NULL
 *  - If vertex has RO already, no action
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__relax_to_readonly_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t *vertex_LCK, vgx_vertex_record record ) {
  vgx_Vertex_t *vertex_RLX;
  if( __vertex_is_locked_readonly( vertex_LCK ) ) {
    vertex_RLX = vertex_LCK; // no action, already RO
  }
  else {
    GRAPH_LOCK( self ) {
      vertex_RLX = _vxgraph_state__relax_to_readonly_vertex_CS_LCK( self, vertex_LCK, record );
    } GRAPH_RELEASE;
  }

  return vertex_RLX;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__acquire_readonly_vertex_CS( vgx_Graph_t *self, const objectid_t *vertex_obid, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  vgx_Vertex_t *vertex_RO = NULL;
  // Look up vertex by obid in index
  vgx_Vertex_t *vertex = _vxgraph_vxtable__query_CS( self, NULL, vertex_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
  if( vertex != NULL ) {
    // Acquire the readonly lock
    vertex_RO = _vxgraph_state__lock_vertex_readonly_CS( self, vertex, timing_budget, VGX_VERTEX_RECORD_ALL ); // (may be NULL if we can't get lock)
  }
  else if( vertex_obid ) {
    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_NOEXIST );
  }
  else {
    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_NOEXIST_MSG );
  }

  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_Vertex_t * _vxgraph_state__acquire_readonly_vertex_OPEN( vgx_Graph_t *self, const objectid_t *vertex_obid, vgx_ExecutionTimingBudget_t *timing_budget ) {
  vgx_Vertex_t *vertex_RO = NULL;
  GRAPH_LOCK( self ) {
    vertex_RO = _vxgraph_state__acquire_readonly_vertex_CS( self, vertex_obid, timing_budget );
  } GRAPH_RELEASE;
  return vertex_RO;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__release_vertex_CS_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK ) {
  __assert_state_lock( self );
  bool released = false;

  released = _vxgraph_state__unlock_vertex_CS_LCK( self, vertex_LCK, VGX_VERTEX_RECORD_ALL );
  
  // Wake other threads that may be waiting for vertex availability
  if( released ) {
    SIGNAL_VERTEX_AVAILABLE( self );
  }

  return released;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__release_vertex_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **vertex_LCK ) {
  bool released = false;
  GRAPH_LOCK( self ) {
    released = _vxgraph_state__release_vertex_CS_LCK( self, vertex_LCK );
  } GRAPH_RELEASE;
  return released;
}



/*******************************************************************//**
 * This function acquires both initial (A) and terminal (B) for full
 * write/read access atomically.
 * 
 * If initial (A) identified by initial_obid does not exist it is
 * created as a REAL vertex using the (optional) initial_idstr as the
 * friendly vertex ID string.
 *
 * If terminal (B) identified by terminal_obid does not exist it is created
 * in accordance with the supplied default_terminal_manifestation
 * (NULL, REAL or VIRTUAL) using the (optional) terminal_idstr as the friendly
 * vertex ID string. If the NULL manifestation is supplied and the terminal
 * does not exist then the operation will be aborted and no vertices are acquired.
 *
 * On success the acquired vertices are placed in *ret_initial and *ret_terminal.
 * 
 * On failure to acquire both vertices *ret_initial and *ret_terminal are not
 * modified.
 *
 * Use _vxgraph_state__release_writable_initial_and_terminal_OPEN_WL() later to release both
 * initial (A) and terminal (B) atomically.
 *
 * Return codes:  2=both vertices acquired - this is the only successful retcode
 *               -1=can't acquire both vertices atomically
 *
 * NOTE 1: Initial vertex may be converted from VIRTUAL to REAL and remain
 *         REAL if we fail to obtain a lock on terminal.
 * NOTE 2: Initial or terminal may be created and still remain in graph
 *         even if we failed to acquire a lock on the other vertex.
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( vgx_Graph_t *self, vgx_Vertex_t **ret_initial, const CString_t *CSTR__initial_idstr, const objectid_t *initial_obid, vgx_Vertex_t **ret_terminal, const CString_t *CSTR__terminal_idstr, const objectid_t *terminal_obid, vgx_VertexStateContext_man_t default_terminal_manifestation, vgx_ExecutionTimingBudget_t *timing_budget, CString_t **CSTR__error ) {
  int n_acquired = 0;

  vgx_Vertex_t *initial_WL = NULL;
  vgx_Vertex_t *terminal_WL = NULL;

  if( initial_obid == NULL || terminal_obid == NULL ) {
    return -1;  // must supply obid for both
  }
  else if( idmatch( initial_obid, terminal_obid ) ) {
    return -1;  // can't acquire same ID as a pair!
  }
  if( timing_budget->t0_ms == 0 ) {
    _vgx_start_graph_execution_timing_budget( self, timing_budget );
  }

  GRAPH_LOCK( self ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
      // TODO dev_roreq: 
      // If roreq flag is set:
      //   Allow operation if current thread already has WL vertices
      //   Disallow operation if current thread does not already have WL vertices

      if( _vgx_is_writable_CS( &self->readonly ) ) {
        // Graph readonly is not pending, or it is pending but this thread already has WL locks so we are allowed to proceed
        if( _vgx_readonly_not_requested_CS( &self->readonly ) || _vxgraph_tracker__has_writable_locks_CS( self ) ) {

          BEGIN_EXECUTION_BLOCKED_WHILE( n_acquired == 0 ) {
            BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( self ) {

              // Look up vertices by obid in index
              vgx_Vertex_t *initial = _vxgraph_vxtable__query_CS( self, NULL, initial_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
              vgx_Vertex_t *terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );

              vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_graph_execution_timing_budget( self, EXECUTION_BUDGET_IS_NONE ? 0 : 10 );

              // --------------------------------
              // Situation 1: Both vertices exist
              // --------------------------------
              if( initial && terminal ) {
                // First we try for a short time to get a lock on the initial vertex
                initial_WL = _vxgraph_state__lock_vertex_writable_CS( self, initial, &short_timeout, VGX_VERTEX_RECORD_ALL );
                __set_access_reason( &timing_budget->reason, short_timeout.reason );
                if( initial_WL != NULL ) {
                  ++n_acquired;
                  // Make sure the initial vertex is REAL - (note: it will remain real even if we fail later!)
                  _vxgraph_state__convert_WL( self, initial_WL, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL );
                  // Next we try for a short time to get a lock on the terminal vertex while yielding the initial's inarcs while we wait for the terminal.
                  // Before attempting the lock we need to re-verify that terminal is still indexed since it may have been deleted if we were blocked above
                  // waiting for the initial.
                  _vgx_reset_execution_timing_budget( &short_timeout ); // reset before next brief lock attempt
                  terminal_WL = NULL;
                  if( (terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL ) {
                    terminal_WL = __lock_terminal_writable_yield_initial_inarcs_CS_WL( self, initial, terminal, &short_timeout, VGX_VERTEX_RECORD_ALL );
                    __set_access_reason( &timing_budget->reason, short_timeout.reason );
                  }
                  if( terminal_WL ) {
                    // Both initial and terminal now acquired
                    ++n_acquired;
                    // <<< SUCCESS >>>
                  }
                  // Could not acquire terminal, release initial and start over
                  else {
                    SET_EXECUTION_RESOURCE_BLOCKED( CSTR__terminal_idstr );
                    if( __unlock_vertex_writable_CS_WL( self, &initial_WL, VGX_VERTEX_RECORD_ALL ) == false ) {
                      // Internal problems.
                      PRINT_VERTEX( initial_WL );
                      VXGRAPH_STATE_FATAL( self, 0xC91, "Internal error - unlock vertex failed" );
                    }
                    --n_acquired;
                  }
                }
                else {
                  SET_EXECUTION_RESOURCE_BLOCKED( CSTR__initial_idstr );
                }
              }
              // ----------------------------------------------------------------------------------------------
              // Situation 2: Initial exists, terminal does not exist, REAL or VIRTUAL terminal will be created
              // ----------------------------------------------------------------------------------------------
              else if( initial && !terminal && default_terminal_manifestation != VERTEX_STATE_CONTEXT_MAN_NULL ) {
                // First we create the terminal using caller's requested default manifestation before we lock the initial
                CString_t *CSTR__mapped_instance = NULL;
                vgx_vertex_type_t vertex_type = iEnumerator_CS.VertexType.Encode( self, NULL, &CSTR__mapped_instance, true );
                if( (terminal_WL = __create_and_own_vertex_writable_CS( self, CSTR__terminal_idstr, terminal_obid, vertex_type, default_terminal_manifestation, VGX_VERTEX_RECORD_ALL, CSTR__error )) != NULL ) {
                  ++n_acquired;
                  // Next we try for a short time to get a lock on the initial 
                  initial_WL = NULL;
                  if( (initial = _vxgraph_vxtable__query_CS( self, NULL, initial_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL ) {
                    initial_WL = _vxgraph_state__lock_vertex_writable_CS( self, initial, &short_timeout, VGX_VERTEX_RECORD_ALL );
                    __set_access_reason( &timing_budget->reason, short_timeout.reason );
                  }
                  // Both initial and terminal now acquired
                  if( initial_WL ) {
                    ++n_acquired;
                    _vxgraph_state__convert_WL( self, initial_WL, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL );
                    // <<< SUCCESS >>>
                  }
                  // Could not acquire initial, release terminal and start over
                  else {
                    SET_EXECUTION_RESOURCE_BLOCKED( CSTR__initial_idstr );
                    // TODO: the created terminal should possibly be deleted here to leave the graph as it was before in case we ultimately fail
                    if( __unlock_vertex_writable_CS_WL( self, &terminal_WL, VGX_VERTEX_RECORD_ALL ) == false ) {
                      // Internal problems.
                      PRINT_VERTEX( terminal_WL );
                      VXGRAPH_STATE_FATAL( self, 0xC92, "Internal error - unlock vertex failed" );
                    }
                    --n_acquired;
                  }
                }
                else {
                  // Failed, we're done.
                  VXGRAPH_STATE_REASON( self, 0xC93, "Failed to create terminal vertex" );
                  __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_ERROR );
                  n_acquired = -1;
                }
              }
              // ----------------------------------------------------------------------------------
              // Situation 3: Terminal exists, initial does not exist, REAL initial will be created
              // ----------------------------------------------------------------------------------
              else if( !initial && terminal ) {
                // First we create the initial as a REAL vertex before we lock the terminal
                CString_t *CSTR__mapped_instance = NULL;
                if( (initial_WL = __create_and_own_vertex_writable_CS( self, CSTR__initial_idstr, initial_obid, iEnumerator_CS.VertexType.Encode( self, NULL, &CSTR__mapped_instance, true ), VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL, CSTR__error )) != NULL ) {
                  ++n_acquired;
                  // Next we try for a short time to get a lock on the terminal
                  terminal_WL = NULL;
                  if( (terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL ) {
                    terminal_WL = _vxgraph_state__lock_vertex_writable_CS( self, terminal, &short_timeout, VGX_VERTEX_RECORD_ALL );
                    __set_access_reason( &timing_budget->reason, short_timeout.reason );
                  }
                  // Both initial and terminal now acquired
                  if( terminal_WL ) {
                    ++n_acquired;
                    // <<< SUCCESS >>>
                  }
                  // Could not acquire terminal, release initial and start over
                  else {
                    SET_EXECUTION_RESOURCE_BLOCKED( CSTR__terminal_idstr );
                    // TODO: the created initial should possibly be deleted here to leave the graph as it was before in case we ultimately fail
                    if( __unlock_vertex_writable_CS_WL( self, &initial_WL, VGX_VERTEX_RECORD_ALL ) == false ) {
                      // Internal problems.
                      PRINT_VERTEX( initial_WL );
                      VXGRAPH_STATE_FATAL( self, 0xC94, "Internal error - unlock vertex failed" );
                    }
                    --n_acquired;
                  }
                }
                else {
                  // Failed, we're done.
                  VXGRAPH_STATE_REASON( self, 0xC95, "Failed to create initial vertex" );
                  __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_ERROR );
                  n_acquired = -1;
                }
              }
              // ---------------------------------------------------------------------------------------------
              // Situation 4: Neither vertex exists, REAL initial and REAL or VIRTUAL terminal will be created
              // ---------------------------------------------------------------------------------------------
              else if( !initial && !terminal && default_terminal_manifestation != VERTEX_STATE_CONTEXT_MAN_NULL ) {
                // First we create and own initial as a REAL vertex
                CString_t *CSTR__mapped_instance = NULL;
                if( (initial_WL = __create_and_own_vertex_writable_CS( self, CSTR__initial_idstr, initial_obid, iEnumerator_CS.VertexType.Encode( self, NULL, &CSTR__mapped_instance, true ), VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_ALL, CSTR__error )) != NULL ) {
                  ++n_acquired;
                  // Next we create and own terminal using caller's requested default manifestation (REAL or VIRTUAL)
                  if( (terminal_WL = __create_and_own_vertex_writable_CS( self, CSTR__terminal_idstr, terminal_obid, iEnumerator_CS.VertexType.Encode( self, NULL, &CSTR__mapped_instance, true ), default_terminal_manifestation, VGX_VERTEX_RECORD_ALL, CSTR__error )) != NULL ) {
                    // Both initial and terminal now acquired
                    ++n_acquired;
                    // <<< SUCCESS >>>
                  }
                  // Terminal creation failed, drop the initial we just created above
                  else {
                    _vxgraph_vxtable__unindex_vertex_CS_WL( self, initial_WL ); // remove from graph index (and give up all internal references)
                    _vxgraph_tracker__unregister_vertex_WL_for_thread_CS( self, initial_WL );
                    Vertex_DECREF_WL( initial_WL );                               // give up our own reference (then auto dealloc)
                    initial_WL = NULL;
                    // Failed, we're done.
                    VXGRAPH_STATE_REASON( self, 0xC96, "Terminal creation failed after new initial created" );
                    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_ERROR );
                    n_acquired = -1;
                  }
                }
                else {
                  // Failed, we're done.
                  VXGRAPH_STATE_REASON( self, 0xC97, "Failed to create initial vertex" );
                  __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_ERROR );
                  n_acquired = -1;
                }
              }
              // ------------------------------------------------------------------------
              // Situation 5: One or both vertices not existing and should not be created
              // ------------------------------------------------------------------------
              else {
                n_acquired = -1;
                __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_NOEXIST_MSG );
                char buf[33];
                if( !initial ) {
                  __set_error_string( CSTR__error, CSTR__initial_idstr ? CStringValue(CSTR__initial_idstr) : idtostr( buf, initial_obid ) );
                }
                else {
                  __set_error_string( CSTR__error, CSTR__terminal_idstr ? CStringValue(CSTR__terminal_idstr) : idtostr( buf, terminal_obid ) );
                }
              }

              // Exit loop if graph is (or is trying to become) readonly
              if( n_acquired != 2 && __is_access_reason_readonly( timing_budget->reason ) ) {
                n_acquired = -1;
              }

            } END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS;
          } END_EXECUTION_BLOCKED_WHILE;
        }
        else {
          __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_READONLY_PENDING );
          n_acquired = -1;
        }
      }
      else {
        _vgx_request_write_CS( &self->readonly );
        __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_READONLY_GRAPH );
        n_acquired = -1;
      }
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  } GRAPH_RELEASE;

  if( n_acquired == 2 ) {
    *ret_initial = initial_WL;
    *ret_terminal = terminal_WL;
    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
  }
  else if( n_acquired == 0 ) {
    if( !__has_access_reason( &timing_budget->reason ) ) {
      if( timing_budget->timeout_ms > 0 ) {
        __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_TIMEOUT );
      }
      else {
        __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_LOCKED );
      }
    }
    n_acquired = -1;
  }

  return n_acquired;
}



/*******************************************************************//**
 * This function acquires both initial (A) and terminal (B) for readonly
 * access atomically.
 * 
 * Both initial (A) and terminal (B) must exist.
 *
 * On success the acquired vertices are placed in *ret_initial and *ret_terminal.
 * 
 * On failure to acquire both vertices *ret_initial and *ret_terminal are not
 * modified.
 *
 * Use _vxgraph_state__release_readonly_initial_and_terminal_OPEN_RO() later to release both
 * initial (A) and terminal (B) atomically.
 *
 * Return codes:  2=both vertices acquired - this is the only successful retcode
 *               -1=can't acquire both vertices atomically
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( vgx_Graph_t *self, vgx_Vertex_t **ret_initial, const objectid_t *initial_obid, vgx_Vertex_t **ret_terminal, const objectid_t *terminal_obid, vgx_ExecutionTimingBudget_t *timing_budget ) {

  vgx_Vertex_t *initial_LCK = NULL;
  vgx_Vertex_t *terminal_LCK = NULL;

  int n_acquired = 0;

  if( initial_obid == NULL || terminal_obid == NULL ) {
    return -1;  // must supply obid for both
  }

  if( idmatch( initial_obid, terminal_obid ) ) {
    return -1;  // can't acquire same ID as a pair!
  }

  GRAPH_LOCK( self ) {
    BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
      BEGIN_EXECUTION_BLOCKED_WHILE( n_acquired == 0 ) {
        // Look up vertices by obid in index
        vgx_Vertex_t *initial = _vxgraph_vxtable__query_CS( self, NULL, initial_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
        vgx_Vertex_t *terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD );

        vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_graph_execution_timing_budget( self, EXECUTION_BUDGET_IS_NONE ? 0 : 10 );

        // Both must exist
        if( initial && terminal ) {
          // First we get a lock (at least RO, maybe WL) on the initial vertex with a quick timeout
          initial_LCK = _vxgraph_state__lock_vertex_readonly_CS( self, initial, &short_timeout, VGX_VERTEX_RECORD_ALL );
          __set_access_reason( &timing_budget->reason, short_timeout.reason );
          if( initial_LCK != NULL ) {
            ++n_acquired;
            // Then we check if terminal is readable before we try to get the lock immediately
            // We have to check because we can't allow blocking, since we can't yield the inarcs of the readonly initial!
            // In case we left CS while waiting for initial above we have to re-check that terminal still exists as it could 
            // have been deleted (and therefore invalidated) while we waited.
            if( (terminal = _vxgraph_vxtable__query_CS( self, NULL, terminal_obid, VERTEX_TYPE_ENUMERATION_WILDCARD )) != NULL
                &&
                (
                  __vertex_is_lockable_as_readonly( terminal ) 
                  ||
                  (__vertex_is_locked_writable_by_current_thread( terminal ) && __vertex_is_writer_reentrant( terminal ) ) )
                )
            {
              vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( self );
              // We SHOULD get the terminal now (at least RO, maybe WL)
              terminal_LCK = _vxgraph_state__lock_vertex_readonly_CS( self, terminal, &zero_timeout, VGX_VERTEX_RECORD_ALL );
              __set_access_reason( &timing_budget->reason, zero_timeout.reason );
              if( terminal_LCK != NULL ) {
                // Now we have both!
                ++n_acquired;  // 2 vertices acquired
              }
              else {
                PRINT_VERTEX( terminal );
                VXGRAPH_STATE_FATAL( self, 0xCA1, "Internal error, can't acquire readonly vertex although marked as lockable" );
              }
            }
            // Can't block, we have to release initial and try again after a quick sleep
            else {
              BEGIN_WAIT_FOR_EXECUTION_RESOURCE( terminal ) {
                _vxgraph_state__unlock_vertex_CS_LCK( self, &initial_LCK, VGX_VERTEX_RECORD_ALL );
                --n_acquired;
                __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_TIMEOUT );
                WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
              } END_WAIT_FOR_EXECUTION_RESOURCE;
            }
          }
          else {
            SET_EXECUTION_RESOURCE_BLOCKED( initial_obid );
          }
        }
        else {
          n_acquired = -1;
        }
      } END_EXECUTION_BLOCKED_WHILE;
    } END_EXECUTE_WITH_TIMING_BUDGET_CS;
  } GRAPH_RELEASE;

  if( n_acquired == 2 ) {
    // Place the acquired vertices in the output pointers
    *ret_initial = initial_LCK;
    *ret_terminal = terminal_LCK;
    __set_access_reason( &timing_budget->reason, VGX_ACCESS_REASON_OBJECT_ACQUIRED );
  }
  else if( n_acquired == 0 ) {
    n_acquired = -1;
  }

  return n_acquired;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxgraph_state__release_initial_and_terminal_OPEN_LCK( vgx_Graph_t *self, vgx_Vertex_t **initial_LCK, vgx_Vertex_t **terminal_LCK ) {
  if( initial_LCK == terminal_LCK ) {
    VXGRAPH_STATE_FATAL( self, 0xCB1, "Illegal vertex synchronization: initial == terminal" );
  }

  bool released;

  GRAPH_LOCK( self ) {
    // First we relax the initial vertex if it is WL
    bool initial_was_RO = __vertex_is_locked_readonly( *initial_LCK );
    if( _vxgraph_state__relax_to_readonly_vertex_CS_LCK( self, *initial_LCK, VGX_VERTEX_RECORD_ALL ) ) {
      // WL -> RO, lets broadcast
      if( !initial_was_RO && __vertex_is_locked_readonly( *initial_LCK ) ) {
        SIGNAL_VERTEX_AVAILABLE( self );
      }
      // Next we relax the terminal vertex if it is WL
      bool terminal_was_RO = __vertex_is_locked_readonly( *terminal_LCK );
      if( _vxgraph_state__relax_to_readonly_vertex_CS_LCK( self, *terminal_LCK, VGX_VERTEX_RECORD_ALL ) ) {
        // WL -> RO, lets broadcast
        if( !terminal_was_RO && __vertex_is_locked_readonly( *terminal_LCK ) ) {
          SIGNAL_VERTEX_AVAILABLE( self );
        }

        // Both vertices have now been committed if their original state required commit, 
        // i.e. if a vertex originally had exactly one WL it is now committed.
        // If a vertex had multi WL, one recursion level has been released.
        // If a vertex had RO it is in its original state and should be released.

        // Initial is currently readonly
        if( __vertex_is_locked_readonly( *initial_LCK ) ) {
          // It may have been WL or RO originally, but it doesn't matter - release RO and signal
          __unlock_vertex_readonly_CS_RO( self, initial_LCK, VGX_VERTEX_RECORD_ALL );
          SIGNAL_VERTEX_AVAILABLE( self );
        }
        // Initial is still WL after recursive release from multi-WL
        else {
          *initial_LCK = NULL;
        }

        // Terminal is currently readonly
        if( __vertex_is_locked_readonly( *terminal_LCK ) ) {
          // It may have been WL or RO originally, but it doesn't matter - release RO and signal
          __unlock_vertex_readonly_CS_RO( self, terminal_LCK, VGX_VERTEX_RECORD_ALL );
          SIGNAL_VERTEX_AVAILABLE( self );
        }
        // Terminal is still WL after recursive release from multi-WL
        else {
          *terminal_LCK = NULL;
        }

        released = true;
      }
      // Relaxation of terminal failed because it was WL and the commit failed!
      else {
        // We need to restore original WL state of initial
        if( !initial_was_RO ) {
          // Initial is RO now because it was relaxed from WL to RO -- we hold one RO lock
          if( __vertex_is_locked_readonly( *initial_LCK ) ) {
            // We will eventually get the WL if there are other readers currently -- BLOCK for a long while but not indefinitely to avoid deadlock disaster if other bugs exist
            vgx_ExecutionTimingBudget_t long_timeout = _vgx_get_graph_execution_timing_budget( self, 30000 );
            if( _vxgraph_state__escalate_to_writable_vertex_CS_RO( self, *initial_LCK, &long_timeout, VGX_VERTEX_RECORD_ALL, NULL ) == NULL ) {
              VXGRAPH_STATE_CRITICAL( self, 0xCB2, "Failed to re-acquire '%s' as writable - it is READONLY", CALLABLE(*initial_LCK)->IDString(*initial_LCK) );
            }
          }
          // Initial is WL now because it was recursive WL
          else {
            // This is guaranteed to succeed
            vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( self );
            if( _vxgraph_state__lock_vertex_writable_CS( self, *initial_LCK, &zero_timeout, VGX_VERTEX_RECORD_ALL ) == NULL ) {
              VXGRAPH_STATE_FATAL( self, 0xCB3, "Failed to acquire guaranteed recursive writelock" );
            }
          }
        }
        released = false;
      }

    }
    // Relaxation of initial failed because it was WL and the commit failed!
    else {
      released = false;
    }
  } GRAPH_RELEASE;

  return released;
}



/*******************************************************************//**
 * Returns:  n  : Current number of writable vertices
 *          -1  : Error
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_state__wait_max_writable_CS( vgx_Graph_t *self, int64_t max_writable, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  int64_t n_writable = _vgx_graph_get_vertex_WL_count_CS( self );

  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
    // Enter a timeout loop waiting for graph to have no more than max_writable writable vertices
    BEGIN_EXECUTION_BLOCKED_WHILE( n_writable > max_writable ) {
      // Check current number of writable vertices
      if( (n_writable = _vgx_graph_get_vertex_WL_count_CS(self)) > 0 ) {
        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
          WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
        } END_WAIT_FOR_EXECUTION_RESOURCE;
      }
    } END_EXECUTION_BLOCKED_WHILE;
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;

  return n_writable;
}



/*******************************************************************//**
 * Returns:  >= 0  : Graph is readonly 
 *          -1     : Error
 ***********************************************************************
 */
static int __set_graph_readonly_CS( vgx_Graph_t *self, bool force, bool tx_input_suspended, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  int err = 0;

  vgx_readonly_state_t *ros = &self->readonly;

  // No action if already readonly
  if( _vgx_is_readonly_CS( ros ) ) {
    return 0;
  }

  vgx_AccessReason_t *reason = &timing_budget->reason;

  framehash_vtable_t *iFH = (framehash_vtable_t*)COMLIB_CLASS_VTABLE( framehash_t );

  int eventproc_disabled_here = 0;
  int operation_suspended_here = 0;
  bool readonly_here = false;

  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
    XTRY {
      //  ------------------------------------------------------------------
      //  Hold here for as long as readonly is disallowed
      //  ------------------------------------------------------------------
      BEGIN_EXECUTION_BLOCKED_WHILE( _vgx_is_readonly_disallowed_CS( ros ) ) {
        GRAPH_SUSPEND_LOCK( self ) {
          sleep_milliseconds( 10 );
        } GRAPH_RESUME_LOCK;
      } END_EXECUTION_BLOCKED_WHILE;
      if( _vgx_is_readonly_disallowed_CS( ros ) ) {
        if( force == true ) {
          VXGRAPH_STATE_WARNING( self, 0x000, "Forcing readonly transition despite disallow recursion = %d", _vgx_get_disallow_readonly_recursion_CS( ros ) );
        }
        else {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
        }
      }

      //  ------------------------------------------------------------------
      //  Suspend Event Processor if running
      //  ------------------------------------------------------------------
      if( iGraphEvent.IsEnabled( self ) ) {
        GRAPH_SUSPEND_LOCK( self ) {
          eventproc_disabled_here = iGraphEvent.NOCS.Disable( self, timing_budget );
        } GRAPH_RESUME_LOCK;
        // Could not disable event processor
        if( eventproc_disabled_here <= 0 ) {
          if( force == true ) {
            VXGRAPH_STATE_WARNING( self, 0x000, "Forcing readonly transition despite being unable to suspend event processor" );
          }
          else {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
          }
        }
      }

      // Enter a timeout loop waiting for graph to have no writable vertices
      int64_t n_writable = -1;
      int ro_allowed = 0;

      // While( /L * /D * /ZW ):  keep trying as long as not yet RO, RO allowed and writable vertices exist
      //
      BEGIN_EXECUTION_BLOCKED_WHILE( _vgx_is_writable_CS( ros ) && _vgx_is_readonly_allowed_CS( ros ) && n_writable != 0 ) {
        // Check current number of writable vertices
        if( (n_writable = _vgx_graph_get_vertex_WL_count_CS(self)) > 0 ) {
          BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
            // Writable vertices exist, request readonly mode and sleep for while waiting for vertex condition signal
            WAIT_FOR_VERTEX_AVAILABLE( self, 1 );
          } END_WAIT_FOR_EXECUTION_RESOURCE;
        }
      } END_EXECUTION_BLOCKED_WHILE;

      // Became readonly while we slept 
      if( _vgx_is_readonly_CS( ros ) ) {
        // No further action
      }
      // No writable (or forced RO), we can enter readonly mode
      else if( (n_writable == 0 && (ro_allowed = _vgx_is_readonly_allowed_CS( ros )) > 0) || force == true ) {
        if( n_writable != 0 || !ro_allowed ) {
          VXGRAPH_STATE_WARNING( self, 0x000, "Forcing readonly transition despite %lld writable vertices, readonly allowed=%d", n_writable, ro_allowed );
        }

        // Operation processor running, let's capture and suspend
        if( iOperation.Emitter_CS.IsReady( self ) ) {
          // Capture readonly operation
          if( iOperation.IsOpen( &self->operation ) ) {
            if( iOperation.State_CS.Readonly( self ) < 0 ) {
              VXGRAPH_STATE_WARNING( self, 0x004, "Failed to capture readonly operation when entering readonly mode" );
            }
          }

          // Close the operation
          CLOSE_GRAPH_OPERATION_CS( self );

          // Suspend operation processor
          if( (operation_suspended_here = iOperation.Emitter_CS.Suspend( self, 120000 )) < 1 ) {
            if( force == true ) {
              VXGRAPH_STATE_WARNING( self, 0x000, "Forcing readonly transition despite being unable to suspend operation emitter" );
            }
            else {
              // Re-open graph operation
              if( OPEN_GRAPH_OPERATION_CS( self ) < 0 ) {
                VXGRAPH_STATE_FATAL( self, 0x005, "Failed to re-open graph operation" );
              }
              THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
            }
          }
        }
        else {
          // Close the operation
          CLOSE_GRAPH_OPERATION_CS( self );
        }

        // Lock graph readonly and set flag
        _vgx_lock_readonly_CS( &self->readonly, eventproc_disabled_here, operation_suspended_here, tx_input_suspended );

        //  ------------------------------------------------------------------
        //  Graph is now READONLY
        //  ------------------------------------------------------------------

        // Operation processor is now readonly
        iOperation.Readonly_CS.Enter( self );

        // Readonly now
        readonly_here = true;

        //  ------------------------------------------------------------------
        //  Now we have to freeze all internal structures
        //  ------------------------------------------------------------------

        // 1. Set similarity object readonly
        // [11]
        if( CALLABLE( self->similarity )->SetReadonly( self->similarity ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
        }

        // 2. Set arcvector allocators readonly. These are used by the framehash derivate arcvectors
        // and need to be set readonly here.
        // [13.3.2]
        if( CALLABLE( self->arcvector_fhdyn.falloc )->SetReadonly( self->arcvector_fhdyn.falloc ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
        }
        // [13.3.3]
        if( CALLABLE( self->arcvector_fhdyn.balloc )->SetReadonly( self->arcvector_fhdyn.balloc ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
        }

        // 3. Set property allocators readonly. These are used by the simple framehash property maps
        // and need to be set readonly here.
        // [14.5.2]
        if( CALLABLE( self->property_fhdyn.falloc )->SetReadonly( self->property_fhdyn.falloc ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
        }
        // [14.5.3]
        if( CALLABLE( self->property_fhdyn.balloc )->SetReadonly( self->property_fhdyn.balloc ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00B );
        }

        // NOTE: vtxmap_fhdyn is not serialized (and needed for queries) so we CANNOT make it readonly.

        // 4. Set property string allocator context readonly. This is used by many internal maps to encode
        // string values. Protect it by setting readonly here.
        // [17]
        if( icstringalloc.SetReadonly( self->property_allocator_context ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00C );
        }

        // 5. Set vxtype enumerator readonly
        // [18 + 19]
        if( iFH->SetReadonly( self->vxtype_encoder ) < 0 ||
            iFH->SetReadonly( self->vxtype_decoder ) < 0 )
        {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00D );
        }

        // 6. Set rel enumerator readonly
        // [20 + 21]
        if( iFH->SetReadonly( self->rel_encoder ) < 0 ||
            iFH->SetReadonly( self->rel_decoder ) < 0 )
        {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00E );
        }

        // 7. Set property key/val enumerators readonly
        // [22 + 23]
        if( iFH->SetReadonly( self->vxprop_keymap ) < 0 ||
            iFH->SetReadonly( self->vxprop_valmap ) < 0 )
        {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x00F );
        }

        // 8. Set the vertex allocator readonly
        // [24]
        if( CALLABLE( self->vertex_allocator )->SetReadonly( self->vertex_allocator ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x010 );
        }

        // 9. Set vxtable readonly
        // [25 + 26]
        if( _vxgraph_vxtable__set_readonly_CS( self ) < 0 ) {
          __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x011 ); // ??
        }

      }
      // Writable vertices still exist, we can't become readonly
      else if( n_writable > 0 ) {
        __set_access_reason( reason, VGX_ACCESS_REASON_TIMEOUT );
        THROW_SILENT( CXLIB_ERR_MUTEX, 0x012 ); // timeout
      }
      // Readonly not allowed at this point
      else if( _vgx_is_readonly_disallowed_CS( ros ) ) {
        __set_access_reason( reason, VGX_ACCESS_REASON_RO_DISALLOWED );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x013 );
      }
      // Unknown error
      else {
        __set_access_reason( reason, VGX_ACCESS_REASON_ERROR );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x014 );
      }

      VXGRAPH_STATE_INFO( self, 0x015, "READONLY (%d)", _vgx_get_readonly_readers_CS( &self->readonly ) );

    }
    XCATCH( errcode ) {
      // Revert to writable if we got as far as becoming readonly
      if( readonly_here ) {
        if( __clear_graph_readonly_CS( self ) < 0 ) {
          VXGRAPH_STATE_FATAL( self, 0x016, "Unrecoverable: Failed to clean up unsuccesful attempt to enter readonly mode" );
        }
      }
      // If only part of the way to readonly, we need to re-enable things we may have disabled
      else {
        // Resume event processor if we weren't fully readonly graph
        if( eventproc_disabled_here ) {
          vgx_ExecutionTimingBudget_t re_enable_timeout = _vgx_get_graph_execution_timing_budget( self, 30000 );
          GRAPH_SUSPEND_LOCK( self ) {
            iGraphEvent.NOCS.Enable( self, &re_enable_timeout );
          } GRAPH_RESUME_LOCK;
        }
      }
      err = -1;
    }
    XFINALLY {
    }
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;

  if( err < 0 ) {
    return -1;
  }
  else {
    return _vgx_get_readonly_readers_CS( &self->readonly );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __clear_graph_readonly_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );
  int ret = 0;

  //  ------------------------------------------------------
  //  Now we have to unfreeze all internal structures
  //  ------------------------------------------------------

  vgx_readonly_state_t *ros = &self->readonly;
  framehash_vtable_t *iFH = (framehash_vtable_t*)COMLIB_CLASS_VTABLE( framehash_t );

  if( _vgx_is_readonly_CS( ros ) ) {
    // Sanity check, proceed only if no readers
    if( !_vgx_has_readonly_readers_CS( ros ) ) {

      // 9. Set vxtable writable
      // [25 + 26]
      _vxgraph_vxtable__clear_readonly_CS( self );

      // 8. Set the vertex allocator writable
      // [24]
      CALLABLE( self->vertex_allocator )->ClearReadonly( self->vertex_allocator );

      // 7. Set property key/val enumerators writable
      // [22 + 23]
      iFH->ClearReadonly( self->vxprop_keymap );
      iFH->ClearReadonly( self->vxprop_valmap );

      // 6. Set rel enumerator writable
      // [20 + 21]
      iFH->ClearReadonly( self->rel_encoder );
      iFH->ClearReadonly( self->rel_decoder );

      // 5. Set vxtype enumerator writable
      // [18 + 19]
      iFH->ClearReadonly( self->vxtype_encoder );
      iFH->ClearReadonly( self->vxtype_decoder );

      // 4. Set property string allocator context writable.
      // [17]
      icstringalloc.ClearReadonly( self->property_allocator_context );

      // 3. Set property allocators writable.
      // [14.5.2]
      CALLABLE( self->property_fhdyn.falloc )->ClearReadonly( self->property_fhdyn.falloc );
      // [14.5.3]
      CALLABLE( self->property_fhdyn.balloc )->ClearReadonly( self->property_fhdyn.balloc );

      // 2. Set arcvector allocators writable.
      // [13.3.2]
      CALLABLE( self->arcvector_fhdyn.falloc )->ClearReadonly( self->arcvector_fhdyn.falloc );
      // [13.3.3]
      CALLABLE( self->arcvector_fhdyn.balloc )->ClearReadonly( self->arcvector_fhdyn.balloc );

      // 1. Set similarity object writable
      // [11]
      CALLABLE( self->similarity )->ClearReadonly( self->similarity );
      
      // Resume operation processor
      if( _vgx_readonly_resume_OP_on_unlock_CS( ros ) ) {
        if( iOperation.Emitter_CS.IsRunning( self ) && iOperation.Emitter_CS.IsSuspended( self ) ) {
          if( iOperation.Emitter_CS.Resume( self ) < 0 ) {
            VXGRAPH_STATE_CRITICAL( self, 0x001, "Cannot resume operation processor, future operations will be lost" );
          }
        }
        else {
          VXGRAPH_STATE_CRITICAL( self, 0x002, "Operation processor not running after graph writable resumed" );
        }
      }

      // Re-open graph operation
      if( !iOperation.IsOpen( &self->operation ) ) {
        if( OPEN_GRAPH_OPERATION_CS( self ) < 0 ) {
          VXGRAPH_STATE_CRITICAL( self, 0x003, "Cannot re-open operation capture, future operations may be lost" );
        }
      }
      else {
        VXGRAPH_STATE_WARNING( self, 0x004, "Operation processor had open graph operation during readonly mode" );
      }

      // Leave operation readonly
      iOperation.Readonly_CS.Leave( self );

      // Capture readwrite operation now that operation processor has been resumed, graph operation re-opened
      // and operation readonly mode no longer in effect.
      if( iOperation.IsOpen( &self->operation ) ) {
        if( iOperation.State_CS.Readwrite( self ) < 0 ) {
          VXGRAPH_STATE_CRITICAL( self, 0x005, "Cannot capture readwrite operation after graph writable resumed" );
        }
      }

      // Graph readonly can now be unlocked
      bool resume_EVP = false;
      bool resume_TX_in = false;
      _vgx_unlock_readonly_CS( ros, &resume_EVP, NULL, &resume_TX_in );

      //  ------------------------------------------------------------------
      //  Graph is now WRITABLE
      //  ------------------------------------------------------------------

      VXGRAPH_STATE_INFO( self, 0x006, "WRITABLE" );

      // Resume the event processor if suspended by readonly mode
      if( resume_EVP ) {
        VXGRAPH_STATE_VERBOSE( self, 0x007, "Will resume event processor" );
        vgx_ExecutionTimingBudget_t enable_timeout = _vgx_get_graph_execution_timing_budget( self, 30000 );
        GRAPH_SUSPEND_LOCK( self ) {
          iGraphEvent.NOCS.Enable( self, &enable_timeout );
        } GRAPH_RESUME_LOCK;
      }

      // If no graphs are readonly at this point, resume the transaction input parser if suspended by readonly mode
      if( resume_TX_in ) {
        vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
        if( SYSTEM != NULL ) {
          GRAPH_SUSPEND_LOCK( self ) {
            if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
              if( _vxgraph_state__get_num_readonly_graphs_OPEN() == 0 ) {
                if( iOperation.System_OPEN.ConsumerService.ResumeExecution( SYSTEM, 15000 ) < 0 ) {
                  VXGRAPH_STATE_REASON( self, 0x008, "Failed to resume transaction consumer service" );
                }
              }
            }
          } GRAPH_RESUME_LOCK;
        }
      }

      // OK
      ret = 1;
    }
    else {
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
DLL_HIDDEN int _vxgraph_state__acquire_graph_readonly_CS( vgx_Graph_t *self, bool force, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __assert_state_lock( self );
  int acquired_one = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  vgx_readonly_state_t *ros = &self->readonly;

  int was_tx_suspended = 0;
  int tx_input_suspended_here = 0;

  

  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( timing_budget, self ) {
    // Stay in loop until we can get readonly, or fail, or timeout
    BEGIN_EXECUTION_BLOCKED_WHILE( acquired_one == 0 ) {

      // Another thread is already in the middle of trying to switch graph to readonly - wait
      if( _vgx_has_readonly_transition_CS( ros ) ) {
        // keep waiting in outer loop
        BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
          GRAPH_SUSPEND_LOCK( self ) {
            sleep_milliseconds( 100 ); // relatively long sleep since readonly graph is likely overloaded at this point
          } GRAPH_RESUME_LOCK;
        } END_WAIT_FOR_EXECUTION_RESOURCE;
      }
      // No other thread is in the middle of a readonly request - proceed
      else {

        // ENTER
        // Ensure exclusive access to the readonly state from here on
        _vgx_enter_readonly_transition_CS( ros );


        //  ------------------------------------------------------------------
        //  Suspend transaction input consumer
        //  ------------------------------------------------------------------
        if( SYSTEM != NULL ) {
          int consumer_service_running = 0;
          GRAPH_SUSPEND_LOCK( self ) {
            if( (consumer_service_running = (iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) > 0)) > 0 ) {
              if( (was_tx_suspended = iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM )) == 0 ) {
                tx_input_suspended_here = iOperation.System_OPEN.ConsumerService.SuspendExecution( SYSTEM, (int)timing_budget->t_remain_ms );
              }
            }
          } GRAPH_RESUME_LOCK;
          // We failed to suspend consumer service (and we are not forcing readonly)
          if( consumer_service_running && !was_tx_suspended && tx_input_suspended_here < 1 ) {
            if( force == true ) {
              VXGRAPH_STATE_WARNING( self, 0x000, "Forcing readonly transition despite being unable to suspend transaction input consumer service" );
            }
            else {
              acquired_one = -1;
            }
          }
        }

        if( acquired_one == 0 ) {
          // Record the readonly request for other threads to inspect
          _vgx_enter_readonly_request_CS( ros );
          // ----

          // Transition graph into readonly mode if not already done
          if( _vgx_is_writable_CS( ros ) ) {
            if( __set_graph_readonly_CS( self, force, tx_input_suspended_here, timing_budget ) == 0 ) {
              acquired_one = _vgx_readonly_acquire_CS( ros ) > 0;
            }
            // Timeout or error
            else {
              acquired_one =  -1;
            }
          }
          // If already readonly, acquire another ownership of the readonly lock
          else {
            int readers = 0;
            // Try to become another reader, may fail if too many readers or too many writers waiting to write
            if( (readers = _vgx_readonly_acquire_CS( ros )) < 0 ) {
              // keep waiting in outer loop
              BEGIN_WAIT_FOR_EXECUTION_RESOURCE( NULL ) {
                GRAPH_SUSPEND_LOCK( self ) {
                  sleep_milliseconds( 100 ); // relatively long sleep since readonly graph is likely overloaded at this point
                } GRAPH_RESUME_LOCK;
              } END_WAIT_FOR_EXECUTION_RESOURCE;
            }
            // 
            else {
              acquired_one = readers > 0;
            }
          }

          // LEAVE
          // Give up the readonly request
          _vgx_leave_readonly_request_CS( ros );
        }

        // Readonly attempt failed
        //
        if( acquired_one < 0 ) {
          // Resume transaction input parser if we suspended it
          if( SYSTEM && tx_input_suspended_here ) {
            GRAPH_SUSPEND_LOCK( self ) {
              // Sanity check that no other graphs have become readonly.
              // Only resume tx consumer service if no graphs are readonly.
              //
              // WARNING: This is not 100% synchronized. 
              // TODO:    Make this 100% reliable.
              //
              if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
                if( _vxgraph_state__get_num_readonly_graphs_OPEN() == 0 ) {
                  if( iOperation.System_OPEN.ConsumerService.ResumeExecution( SYSTEM, 10000 ) != 1 ) {
                    VXGRAPH_STATE_CRITICAL( self, 0x001, "Failed to re-enable transaction consumer service after failed readonly attempt" );
                  }
                }
              }
            } GRAPH_RESUME_LOCK;
          }
        }

        // Give up exclusive access to the readonly state
        _vgx_leave_readonly_transition_CS( ros );
        // -----
      }

    } END_EXECUTION_BLOCKED_WHILE;
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;


  return acquired_one;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__acquire_graph_readonly_OPEN( vgx_Graph_t *self, bool force, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int ret_readers;

  GRAPH_LOCK( self ) {
    ret_readers = _vxgraph_state__acquire_graph_readonly_CS( self, force, timing_budget );
  } GRAPH_RELEASE;

  return ret_readers;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__release_graph_readonly_CS( vgx_Graph_t *self ) {
  __assert_state_lock( self );

  int released_one = 0;

  vgx_readonly_state_t *ros = &self->readonly;

  // Let's not risk deadlock
  vgx_ExecutionTimingBudget_t safety_budget = _vgx_get_graph_execution_timing_budget( self, 30000 );

  BEGIN_EXECUTE_WITH_TIMING_BUDGET_CS( &safety_budget, self ) {
    // Another thread is already in the middle of trying to switch graph to readonly - wait
    BEGIN_EXECUTION_BLOCKED_WHILE( _vgx_has_readonly_transition_CS( ros ) ) {
      GRAPH_SUSPEND_LOCK( self ) {
        sleep_milliseconds( 10 ); 
      } GRAPH_RESUME_LOCK;
    } END_EXECUTION_BLOCKED_WHILE;
  } END_EXECUTE_WITH_TIMING_BUDGET_CS;

  if( _vgx_has_readonly_transition_CS( ros ) ) {
    VXGRAPH_STATE_WARNING( self, 0x001, "Failed to release readonly graph, another thread is attempting readonly transition" );
  }

  // No other thread holds readonly state transition at this point, so we can enter it now
  // ENTER transition
  _vgx_enter_readonly_transition_CS( ros );
  //

  int count = _vgx_get_readonly_readers_CS( ros );

  // Proceed with release if locked
  if( _vgx_is_readonly_CS( ros ) ) {
    // Readers exist, decrement counter
    if( count > 0 ) {
      count = _vgx_readonly_release_CS( ros );
      released_one = 1;
    }
    // Reader count is zero, clear readonly mode
    if( count == 0 ) {
      if( __clear_graph_readonly_CS( self ) != 1 ) {
        released_one = -1;
        CALLABLE( self )->Dump( self );
        VXGRAPH_STATE_FATAL( self, 0x002, "Readonly state corruption." );
      }
    }
  }
  else {
    // Sanity check
    if( count > 0 ) {
      released_one = -1;
      VXGRAPH_STATE_FATAL( self, 0x003, "Readonly recursion %d without readonly state!", count );
    }
  }

  // Finally leave readonly state transition
  // LEAVE transition
  _vgx_leave_readonly_transition_CS( ros);
  //

  return released_one;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__release_graph_readonly_OPEN( vgx_Graph_t *self ) {
  int released_one;
  GRAPH_LOCK( self ) {
    released_one = _vxgraph_state__release_graph_readonly_CS( self );
  } GRAPH_RELEASE;

  return released_one;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__get_num_readonly_graphs_OPEN( void ) {

  int n = 0;

  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  if( SYSTEM == NULL ) {
    return -1;
  }

  if( CALLABLE( SYSTEM )->advanced->IsGraphReadonly( SYSTEM ) ) {
    ++n;
  }

  GRAPH_FACTORY_ACQUIRE {
    const vgx_Graph_t **graphs = NULL;
    if( (graphs = igraphfactory.ListGraphs( NULL )) != NULL ) { 
      vgx_Graph_t **cursor = (vgx_Graph_t**)graphs;
      vgx_Graph_t *graph;
      while( (graph = *cursor++) != NULL ) {
        if( CALLABLE( graph )->advanced->IsGraphReadonly( graph ) ) {
          ++n;
        }
      }
      free( (void*)graphs );
      graphs = NULL;
    }
  } GRAPH_FACTORY_RELEASE;

  return n;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__inc_explicit_readonly_CS( vgx_readonly_state_t *state_CS ) {
  if( state_CS->__n_explicit < READONLY_MAX_EXPLICIT ) {
    return ++state_CS->__n_explicit;
  }
  else {
    VXGRAPH_STATE_FATAL( NULL, 0x001, "BEYOND MAX EXPLICIT READONLY!" );
    return -1;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_state__dec_explicit_readonly_CS( vgx_readonly_state_t *state_CS ) {
  if( state_CS->__n_explicit > 0 ) { //&& gt_explicit_readonly > 0 ) {
    return --state_CS->__n_explicit;
  }
  else {
    VXGRAPH_STATE_FATAL( NULL, 0x001, "BELOW ZERO EXPLICIT READONLY!" );
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void __trap_fatal_vertex_corruption( const vgx_Vertex_t *vertex ) {
  if( vertex ) {
    PRINT_VERTEX( vertex );
  }
  VXGRAPH_STATE_FATAL( NULL, 0x001, "Vertex state corrupted." );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void __trap_fatal_state_corruption( vgx_Graph_t *graph ) {
  if( graph ) {
    CALLABLE( graph )->Dump( graph );
  }
  VXGRAPH_STATE_FATAL( graph, 0x001, "Graph state corrupted." );
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_state.h"

test_descriptor_t _vgx_vxgraph_state_tests[] = {
  { "VGX Graph State Tests", __utest_vxgraph_state },
  {NULL}
};
#endif
