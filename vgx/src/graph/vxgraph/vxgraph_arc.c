/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_arc.c
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



static int __vertex_WL_incref_delta( vgx_Vertex_t *vertex_WL, int inc_delta );
static int64_t __vertex_WL_decref_delta( vgx_Vertex_t *vertex_WL, int64_t dec_delta );
static int64_t __vertex_iWL_decref_delta( vgx_Vertex_t *vertex_iWL, int64_t dec_delta );

static int __vertex_WL_incref_event( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int head_refdelta );
static int64_t __vertex_WL_decref_event( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int64_t head_refdelta, vgx_ExecutionTimingBudget_t *timing_budget );




/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_Graph_t *graph ) {
  return graph ? CALLABLE( graph )->FullPath( graph ) : "";
}

#define __MESSAGE( LEVEL, Graph, Code, Format, ... ) LEVEL( Code, "VX::ARC(%s): " Format, __full_path( Graph ), ##__VA_ARGS__ )

#define VXGRAPH_ARC_VERBOSE( Graph, Code, Format, ... )   __MESSAGE( VERBOSE, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_ARC_INFO( Graph, Code, Format, ... )      __MESSAGE( INFO, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_ARC_WARNING( Graph, Code, Format, ... )   __MESSAGE( WARN, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_ARC_REASON( Graph, Code, Format, ... )    __MESSAGE( REASON, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_ARC_CRITICAL( Graph, Code, Format, ... )  __MESSAGE( CRITICAL, Graph, Code, Format, ##__VA_ARGS__ )
#define VXGRAPH_ARC_FATAL( Graph, Code, Format, ... )     __MESSAGE( FATAL, Graph, Code, Format, ##__VA_ARGS__ )






/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __vertex_WL_incref_delta( vgx_Vertex_t *vertex_WL, int inc_delta ) {
  Vertex_INCREF_DELTA_WL( vertex_WL, inc_delta );
  return inc_delta;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __try_vertex_unindex( vgx_Vertex_t *vertex ) {
  int unindexed;
  vgx_Graph_t *graph = vertex->graph;
  GRAPH_LOCK( graph ) {
    // Unindex vertex
    if( (unindexed = _vxgraph_vxtable__unindex_vertex_CS_WL( graph, vertex )) == VXTABLE_VERTEX_REFCOUNT ) {
      // OK!
    }
    // Vertex was not owned by index after all, but that's ok too
    else if( unindexed == 0 ) {
      VXGRAPH_ARC_VERBOSE( graph, 0x5E1, "Arc disconnect of unindexed virtual vertex: %s", CALLABLE( vertex )->IDPrefix( vertex ) );
    }
    // Unindexing failed, not ok
    else if( unindexed < 0 ) {
      VXGRAPH_ARC_CRITICAL( graph, 0x5E2, "Unindexing of virtual node failed after arc disconnect" );
    }
    // Partial unindex, not ok
    else {
      VXGRAPH_ARC_CRITICAL( graph, 0x5E3, "Partial unindexing of virtual node after arc disconnect" );
    }
  } GRAPH_RELEASE;



  return unindexed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __vertex_WL_decref_delta( vgx_Vertex_t *vertex_WL, int64_t dec_delta ) {
  int64_t ret_delta = dec_delta;
  for( int64_t i=0; i<dec_delta; i++ ) {
    // High likelihood the only remaining owner is the index
    if( Vertex_DECREF_WL( vertex_WL ) == VXTABLE_VERTEX_REFCOUNT ) { // Guaranteed exclusive access to vertex since we own WL, so we use the nolock decref
      // Further evidence the only owner is the index
      if( __vertex_is_manifestation_virtual( vertex_WL ) ) {
        // We have a virtual, isolated vertex: try to unindex it and then it disappears from graph.
        __try_vertex_unindex( vertex_WL );
      }
    }
    // TODO: add ability in the allocator to call a different decref once with a >1 refcount
  }
  return ret_delta;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __vertex_iWL_decref_delta( vgx_Vertex_t *vertex_iWL, int64_t dec_delta ) {
  int64_t ret = dec_delta;
  for( int64_t i=0; i<dec_delta; i++ ) {
    // High likelihood the only remaining owner is the index
    if( Vertex_DECREF_WL( vertex_iWL ) == VXTABLE_VERTEX_REFCOUNT ) { // We are not guaranteed exclusive vertex access with iWL so we use the locked version of decref
      //^^^^^^^^^^^^^^^^
      // SL20250411: I thought we were supposed to use the locked version of decref above.
      //             Is this safe?
      // 
      // Further evidence the only owner is the index
      if( __vertex_is_manifestation_virtual( vertex_iWL ) ) {
        // We have a virtual, isolated vertex: try to unindex it and then it disappears from graph.
        __try_vertex_unindex( vertex_iWL );
      }
    }
    // TODO: add ability in the allocator to call a different decref once with a >1 refcount
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __vertex_WL_incref_event( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int head_refdelta ) {
  return __vertex_WL_incref_delta( arc_WL->head.vertex, head_refdelta );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __vertex_WL_decref_event( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL, int64_t head_refdelta, vgx_ExecutionTimingBudget_t *__na_budget ) {
  return __vertex_WL_decref_delta( arc_WL->head.vertex, head_refdelta );
}



/*******************************************************************//**
 * Callback in response to creating an arc (INITIAL)-->(TERMINAL) that will
 * create a reverse arc (INITIAL)<--(TERMINAL).
 *
 * This version expects exclusive write locks on both vertices and is
 * agnostic with respect to the state lock mutex.
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_arc__connect_WL_reverse_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_WL, int refdelta ) {
  // Add the REV reverse connection from TERMINAL back to INITIAL
  //    INITIAL     TERMINAL           
  //     (tail)---->(head)
  //       ^          Vin
  //        \___REV___/
  //

#ifndef NDEBUG
  {
    vgx_Vertex_t *t = arc_WL_to_WL->tail;
    vgx_Vertex_t *h = arc_WL_to_WL->head.vertex;
    GRAPH_LOCK( t->graph ) {
      int status;
      if( (status = __expect_vertex_WL_CS( t )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x501, "%s tail not writable, status=%d", __FUNCTION__, status );
      }
      if( (status = __expect_vertex_WL_CS( h )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x502, "%s head not writable, status=%d", __FUNCTION__, status );
      }
    } GRAPH_RELEASE;
  }
#endif
  vgx_Vertex_t *head_WL = arc_WL_to_WL->head.vertex;

  // Safeguard against mixing regular and forward-only arcs
  if( __arcvector_cell_is_indegree_counter_only( &head_WL->inarcs ) ) {
    return __ARCVECTOR_ERROR( arc_WL_to_WL, NULL, NULL, NULL );
  }

  vgx_Arc_t reverse_WL = {
    .tail = head_WL,
    .head = {
      .vertex     = arc_WL_to_WL->tail,
      .predicator = arc_WL_to_WL->head.predicator
    }
  };

  reverse_WL.head.predicator.rel.dir = VGX_ARCDIR_IN;

  __vertex_WL_incref_delta( head_WL, refdelta );

  int n_added = iarcvector.Add( dynamic, &reverse_WL, __vertex_WL_incref_event ); // <= note the reversal of tail/head and no callback event
  if( n_added == refdelta ) {
    // Capture
    iOperation.Arc_WL.Connect( arc_WL_to_WL );
    return n_added;
  }
  else {
    // TODO: handle the discrepancy, rollback. Right now we're inconsistent.
    return __ARCVECTOR_ERROR( &reverse_WL, NULL, NULL, NULL );
  }
}



/*******************************************************************//**
 * Callback in response to creating an arc (INITIAL)-->(TERMINAL) that
 * will NOT create a reverse arc.
 *
 * This version expects exclusive write locks on both vertices and is
 * agnostic with respect to the state lock mutex.
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _vxgraph_arc__connect_WL_to_WL_no_reverse( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_WL, int refdelta ) {

#ifndef NDEBUG
  {
    vgx_Vertex_t *t = arc_WL_to_WL->tail;
    vgx_Vertex_t *h = arc_WL_to_WL->head.vertex;
    GRAPH_LOCK( t->graph ) {
      int status;
      if( (status = __expect_vertex_WL_CS( t )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x501, "%s tail not writable, status=%d", __FUNCTION__, status );
      }
      if( (status = __expect_vertex_WL_CS( h )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x502, "%s head not writable, status=%d", __FUNCTION__, status );
      }
    } GRAPH_RELEASE;
  }
#endif

  vgx_Vertex_t *head_WL = arc_WL_to_WL->head.vertex;
  vgx_ArcVector_cell_t *Vin = &head_WL->inarcs;

  // Check terminal's inarcs vector: Forward-only arc mutually exclusive with regular arc
  switch( __arcvector_cell_type( Vin ) ) {
  case VGX_ARCVECTOR_NO_ARCS:
    // No inarcs, convert cell to indegree counter for forward-only arcs
    __arcvector_cell_set_indegree_counter_only( Vin, 1 );
    __vertex_set_has_inarcs( head_WL );
    break;
  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    __arcvector_inc_degree( Vin, refdelta );
    break;
  default:
    return __ARCVECTOR_ERROR( arc_WL_to_WL, NULL, NULL, NULL );
  }

 
  // Artificially incref tail refcount even though it will have no incoming reverse arc. (Needed to pass consistency checks.)
  __vertex_WL_incref_delta( arc_WL_to_WL->tail, refdelta );

  // Inc head refcount 
  __vertex_WL_incref_delta( head_WL, refdelta );

  // Capture
  iOperation.Arc_WL.Connect( arc_WL_to_WL );
  return refdelta;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_disconnect_decref_error( const char *funcname, vgx_Arc_t *arc, int64_t refdelta, int64_t n_removed ) {
  const char *tail_id = CALLABLE( arc->tail )->IDString( arc->tail );
  const char *head_id = CALLABLE( arc->head.vertex )->IDString( arc->head.vertex );
  vgx_predicator_t pred = arc->head.predicator;
  vgx_Graph_t *graph = arc->tail->graph;
  if( graph ) {
    VXGRAPH_ARC_CRITICAL( graph, 0x511, "Inconsistent vertex refcount after disconnect in %s: refdelta=%lld n_removed=%lld", funcname, refdelta, n_removed );
    VXGRAPH_ARC_CRITICAL( graph, 0x512, "  %s-[r=%u,d=%u,m=%u,v=%lu]->%s", tail_id, pred.rel.enc, pred.rel.dir, pred.mod.bits, pred.val.bits, head_id );
    int64_t size = GraphSize( graph );
    int64_t rev_size = GraphRevSize( graph );
    VXGRAPH_ARC_CRITICAL( graph, 0x513, "  graph size: (fwd=%lld rev%lld)", size, rev_size );
  }
  else {
    CRITICAL( 0x514, "arc->tail->graph was null" );
  }
  COMLIB_OBJECT_PRINT( arc->tail );
  COMLIB_OBJECT_PRINT( arc->head.vertex );

  VXGRAPH_ARC_FATAL( graph, 0, "System corruption." );
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __handle_disconnect_unlock_error( const char *funcname, vgx_Arc_t *arc ) {
  const char *tail_id = CALLABLE( arc->tail )->IDString( arc->tail );
  const char *head_id = CALLABLE( arc->head.vertex )->IDString( arc->head.vertex );
  vgx_predicator_t pred = arc->head.predicator;
  vgx_Graph_t *graph = arc->tail->graph;
  VXGRAPH_ARC_CRITICAL( graph, 0x521, "Unlock error after disconnect in %s", funcname );
  VXGRAPH_ARC_CRITICAL( graph, 0x522, "  %s-[r=%u,d=%u,m=%u,v=%lu]->%s", tail_id, pred.rel.enc, pred.rel.dir, pred.mod.bits, pred.val.bits, head_id );
  COMLIB_OBJECT_PRINT( arc->tail );
  COMLIB_OBJECT_PRINT( arc->head.vertex );
  return -1;
}



/*******************************************************************//**
 * Callback in response to removing an arc (INITIAL)-X->(TERMINAL) that will
 * remove the reverse arc (INITIAL)<-X-(TERMINAL).
 *
 * This version expects exclusive write locks on both vertices and is
 * state lock mutex agnostic.
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_WL_reverse_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_WL, int64_t head_refdelta, vgx_ExecutionTimingBudget_t *__na_budget ) {
  // Remove the REV reverse connection from TERMINAL back to INITIAL
  //    INITIAL     TERMINAL           
  //     (tail)---->(head)
  //       ^          Vin
  //        \...REV.../
  //
  // EXPECT EXCLUSIVE CX_WRITE LOCK ON BOTH VERTICES BUT NO GRAPH STATE LOCK
#ifndef NDEBUG
  {
    vgx_Vertex_t *t = arc_WL_to_WL->tail;
    vgx_Vertex_t *h = arc_WL_to_WL->head.vertex;
    GRAPH_LOCK( t->graph ) {
      int status;
      if( (status = __expect_vertex_WL_CS( t )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x531, "%s tail not writable, status=%d", __FUNCTION__, status );
      }
      if( (status = __expect_vertex_WL_CS( h )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x532, "%s head not writable, status=%d", __FUNCTION__, status );
      }
    } GRAPH_RELEASE;
  }
#endif

  vgx_Vertex_t *head_WL = arc_WL_to_WL->head.vertex;
  vgx_ArcVector_cell_t *Vin = &head_WL->inarcs;
  int64_t n_removed;

  // Terminal indicates this is a forward-only arc
  if( __arcvector_cell_is_indegree_counter_only( Vin ) ) {
    // Sanity check
    int64_t ideg_pre = __arcvector_get_degree( Vin );
    if( ideg_pre < head_refdelta ) {
      __ARCVECTOR_ERROR( arc_WL_to_WL, NULL, NULL, __na_budget );
      return __handle_disconnect_decref_error( __FUNCTION__, arc_WL_to_WL, head_refdelta, ideg_pre );
    }
    // No reverse disconnect, adjust terminal's indegree
    if( __arcvector_dec_degree( Vin, head_refdelta ) == 0 ) {
      __vertex_clear_has_inarcs( head_WL );
    }
    // Decref tail's artificially incref'ed refcount at connect
    __vertex_WL_decref_delta( arc_WL_to_WL->tail, head_refdelta );
    n_removed = head_refdelta;
  }
  // Normal arc
  else {
    vgx_Arc_t reverse_WL = {
      .tail = head_WL,
      .head = {
        .vertex     = arc_WL_to_WL->tail,
        .predicator = arc_WL_to_WL->head.predicator
      }
    };
    reverse_WL.head.predicator.rel.dir = VGX_ARCDIR_IN;
    n_removed = iarcvector.Remove( dynamic, &reverse_WL, __na_budget, __vertex_WL_decref_event );
  }

  // Adjust terminal's refcount and capture successful operation
  if( n_removed >= 0 && __vertex_WL_decref_delta( head_WL, head_refdelta ) == n_removed ) {
    // Capture
    iOperation.Arc_WL.Disconnect( arc_WL_to_WL, n_removed );
    return n_removed;
  }
  else {
    // TODO: handle the discrepancy, rollback. Right now we're inconsistent.
    __ARCVECTOR_ERROR( arc_WL_to_WL, NULL, NULL, __na_budget );
    return __handle_disconnect_decref_error( __FUNCTION__, arc_WL_to_WL, head_refdelta, n_removed );
  }
}



/*******************************************************************//**
 * Callback in response to removing an arc (INITIAL)-X->(TERMINAL) that will
 * remove the reverse arc (INITIAL)<-X-(TERMINAL).
 *
 * This version expects exclusive write locks on both vertices and is
 * state lock mutex agnostic.
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_WL_reverse_iWL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_iWL, int64_t head_refdelta, vgx_ExecutionTimingBudget_t *__na_budget ) {
  // Remove the REV reverse connection from TERMINAL back to INITIAL
  //    INITIAL     TERMINAL           
  //     (tail)---->(head)
  //       ^          Vin
  //        \...REV.../
  //
  // EXPECT EXCLUSIVE CX_WRITE LOCK ON TAIL AND AT LEAST INARCS LOCK ON HEAD, BUT NO GRAPH STATE LOCK
#ifndef NDEBUG
  {
    vgx_Vertex_t *t = arc_WL_to_iWL->tail;
    vgx_Vertex_t *h = arc_WL_to_iWL->head.vertex;
    GRAPH_LOCK( t->graph ) {
      int status;
      if( (status = __expect_vertex_WL_CS( t )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x531, "%s tail not writable, status=%d", __FUNCTION__, status );
      }
      if( (status = __expect_vertex_iWL_CS( h )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x532, "%s head inarcs not writable, status=%d", __FUNCTION__, status );
      }
    } GRAPH_RELEASE;
  }
#endif

  vgx_Vertex_t *head_iWL = arc_WL_to_iWL->head.vertex;
  vgx_ArcVector_cell_t *Vin = &head_iWL->inarcs;
  int64_t n_removed;

  // Terminal indicates this is a forward-only arc
  if( __arcvector_cell_is_indegree_counter_only( Vin ) ) {
    // Sanity check
    int64_t ideg_pre = __arcvector_get_degree( Vin );
    if( ideg_pre < head_refdelta ) {
      __ARCVECTOR_ERROR( arc_WL_to_iWL, NULL, NULL, __na_budget );
      return __handle_disconnect_decref_error( __FUNCTION__, arc_WL_to_iWL, head_refdelta, ideg_pre );
    }
    // No reverse disconnect, adjust terminal's indegree
    if( __arcvector_dec_degree( Vin, head_refdelta ) == 0 ) {
      __vertex_clear_has_inarcs( head_iWL );
    }
    // Decref tail's artificially incref'ed refcount at connect
    __vertex_WL_decref_delta( arc_WL_to_iWL->tail, head_refdelta );
    n_removed = head_refdelta;
  }
  // Normal arc
  else {
    vgx_Arc_t reverse_iWL_to_WL = {
      .tail = head_iWL,
      .head = {
        .vertex     = arc_WL_to_iWL->tail,
        .predicator = arc_WL_to_iWL->head.predicator
      }
    };
    reverse_iWL_to_WL.head.predicator.rel.dir = VGX_ARCDIR_IN;
    n_removed = iarcvector.Remove( dynamic, &reverse_iWL_to_WL, __na_budget, __vertex_WL_decref_event );
  }

  // Adjust terminal's refcount and capture successful operation
  if( n_removed >= 0 && __vertex_iWL_decref_delta( head_iWL, head_refdelta ) == n_removed ) {
    // Capture
    iOperation.Arc_WL.Disconnect( arc_WL_to_iWL, n_removed );
    return n_removed;
  }
  else {
    // TODO: handle the discrepancy, rollback. Right now we're inconsistent.
    __ARCVECTOR_ERROR( arc_WL_to_iWL, NULL, NULL, __na_budget );
    return __handle_disconnect_decref_error( __FUNCTION__, arc_WL_to_iWL, head_refdelta, n_removed );
  }
}



/*******************************************************************//**
 * Callback in response to removing an arc (INITIAL)-X->(TERMINAL) that will
 * remove the reverse arc (INITIAL)<-X-(TERMINAL).
 *
 * This version expects the graph state lock to be held but makes no
 * assumption about the head vertex write lock. The head may or may
 * not be write locked. If head is not write locked we obtain the lock
 * and proceed with removal of reverse arc. If head is write locked
 * we will yield the tail's inarcs and sleep until awoken by signal to
 * repeat the attempt to lock the head.
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_WL_reverse_CS( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget ) {
  // Remove the REV reverse connection from TERMINAL back to INITIAL
  //    INITIAL     TERMINAL           
  //     (tail)---->(head)
  //       ^          Vin
  //        \...REV.../
  //
  // EXPECT GRAPH STATE LOCK TO BE HELD AND EXCLUSIVE CX_WRITE LOCK ON TAIL (the INITIAL)
#ifndef NDEBUG
  {
    vgx_Vertex_t *v = arc_WL_to_ANY->tail;
    int status = __expect_vertex_WL_CS( v );
    if( status < 0 ) {
      VXGRAPH_ARC_FATAL( v->graph, 0x541, "%s tail not writable, status=%d", __FUNCTION__, status );
    }
  }
#endif


  int64_t n_removed;
  vgx_Graph_t *graph = arc_WL_to_ANY->tail->graph;
  vgx_Vertex_t *head_ANY = arc_WL_to_ANY->head.vertex;

  // Loop
  if( arc_WL_to_ANY->tail == head_ANY ) {
    vgx_Arc_t *arc_WL = arc_WL_to_ANY;
    n_removed = _vxgraph_arc__disconnect_WL_reverse_WL( dynamic, arc_WL, refdelta, timing_budget );
  }
  // Normal
  else {
    vgx_Vertex_t *head_iWL;
    bool external_lock = __vertex_is_locked_writable_by_current_thread( head_ANY );
    int inarcs_reclaimed = 0;
    if( external_lock ) {
      head_iWL = head_ANY;
      // Reclaim inarcs if yielded
      if( __vertex_is_inarcs_yielded( head_iWL ) && (inarcs_reclaimed = _vxgraph_state__reclaim_inarcs_CS_WL( graph, head_iWL, timing_budget )) < 0 ) {
        // could not reclaim inarcs because they are busy
        return 0;
      }
    }
    else if( (head_iWL = _vxgraph_state__lock_arc_head_inarcs_writable_yield_tail_inarcs_CS_WL( graph, arc_WL_to_ANY, timing_budget )) == NULL ) {
      // could not lock head because 
      // a) it disappeared while we blocked waiting for it and so it's gone along with the inarc we set out to remove; or
      // b) timeout because we're not blocking (arc expiration scenario)
      return 0;
    }

    // Disconnect
    GRAPH_SUSPEND_LOCK( graph ) {
      vgx_Arc_t *arc_WL_to_iWL = arc_WL_to_ANY; // be clear
      n_removed = _vxgraph_arc__disconnect_WL_reverse_iWL( dynamic, arc_WL_to_iWL, refdelta, timing_budget );
    } GRAPH_RESUME_LOCK;

    // Unlock head
    if( external_lock == false ) {
      if( _vxgraph_state__unlock_terminal_inarcs_writable_CS_iWL( graph, &head_iWL ) == false ) {
        // Operation failed and we are possibly in an inconsistent state
        // TODO: investigate inconsistency scenarios and fix.
        vgx_Arc_t reverse_iWL_to_WL = { .tail = arc_WL_to_ANY->head.vertex, .head = { .vertex = arc_WL_to_ANY->tail, .predicator = arc_WL_to_ANY->head.predicator } };
        reverse_iWL_to_WL.head.predicator.rel.dir = VGX_ARCDIR_IN;
        n_removed = __handle_disconnect_unlock_error( __FUNCTION__, &reverse_iWL_to_WL );
      }
    }
    // return to inarcs yielded state
    else if( inarcs_reclaimed ) {
      _vxgraph_state__yield_inarcs_CS_WL( graph, head_iWL );
    }
  }
  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_WL_reverse_GENERAL( framehash_dynamic_t *dynamic, vgx_Arc_t *arc_WL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int64_t n;
  // NOTE: The mutex must support recursion for this to be safe!
  GRAPH_LOCK(  arc_WL_to_ANY->tail->graph ) {
    n = _vxgraph_arc__disconnect_WL_reverse_CS( dynamic, arc_WL_to_ANY, refdelta, timing_budget );
  } GRAPH_RELEASE;

  return n;
}



/*******************************************************************//**
 * Callback in response to removing a reverse arc (INITIAL)<-X-(TERMINAL) that will
 * remove the forward arc (INITIAL)-X->(TERMINAL).
 *
 * This version expects exclusive write locks on both vertices and is
 * state lock mutex agnostic.
 *
 *
        vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_WL_forward_WL( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_WL_to_WL, int64_t refdelta, vgx_ExecutionTimingBudget_t *__na_budget ) {
  // Remove the FWD forward connection from INITIAL to TERMINAL
  //    INITIAL       TERMINAL           
  //     (head)--FWD-->(tail)    <---- note how (head) and (tail) have the reverse meaning in this context
  //       ^             |
  //        \.........../
  //

  // EXPECT EXCLUSIVE CX_WRITE LOCK ON BOTH VERTICES BUT NO GRAPH STATE LOCK
#ifndef NDEBUG
  {
    vgx_Vertex_t *t = reverse_WL_to_WL->tail;
    vgx_Vertex_t *h = reverse_WL_to_WL->head.vertex;
    GRAPH_LOCK( t->graph ) {
      int status;
      if( (status = __expect_vertex_WL_CS( t )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x551, "%s tail not writable, status=%d", __FUNCTION__, status );
      }
      if( (status = __expect_vertex_WL_CS( h )) < 0 ) {
        VXGRAPH_ARC_FATAL( t->graph, 0x552, "%s head not writable, status=%d", __FUNCTION__, status );
      }
    } GRAPH_RELEASE;
  }
#endif
  
  vgx_Arc_t forward_WL_to_WL = {
    .tail = reverse_WL_to_WL->head.vertex,
    .head = {
      .vertex     = reverse_WL_to_WL->tail,
      .predicator = reverse_WL_to_WL->head.predicator
    }
  };
  forward_WL_to_WL.head.predicator.rel.dir = VGX_ARCDIR_OUT;
  int64_t n_removed = iarcvector.Remove( dynamic, &forward_WL_to_WL, __na_budget, __vertex_WL_decref_event );
  if( n_removed >= 0 && __vertex_WL_decref_delta( reverse_WL_to_WL->head.vertex, refdelta ) == n_removed ) {
    // Capture
    iOperation.Arc_WL.Disconnect( &forward_WL_to_WL, n_removed );

    return n_removed;
  }
  else {
    // TODO: handle the discrepancy, rollback. Right now we're inconsistent.
    __ARCVECTOR_ERROR( &forward_WL_to_WL, NULL, NULL, __na_budget );
    return __handle_disconnect_decref_error( __FUNCTION__, &forward_WL_to_WL, refdelta, n_removed );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_iWL_forward_CS( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_iWL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget ) {
  // NOTE: in this FORWARD disconnect context "tail-->head" represents the 
  // arc for the reverse connection, so the actual graph TERMINAL is the tail and INITIAL is the head!

  // Remove the FWD forward connection from INITIAL to TERMINAL
  //    INITIAL        TERMINAL
  //     (head)--FWD-->(tail)    <---- note how (head) and (tail) have the reverse meaning in this context
  //       ^             |
  //        \.........../
  //
  // EXPECT GRAPH STATE LOCK TO BE HELD AND EXCLUSIVE CX_WRITE ACCESS TO TERMINAL (here tail) INARCS
#ifndef NDEBUG
  {
    vgx_Vertex_t *v = reverse_iWL_to_ANY->tail;
    int status = __expect_vertex_iWL_CS( v );
    if( status < 0 ) {
      PRINT_VERTEX( v );
      VXGRAPH_ARC_FATAL( v->graph, 0x561, "%s tail inarcs not writable, status=%d", __FUNCTION__, status );
    }
  }
#endif

  int64_t n_removed;
  vgx_Graph_t *graph = reverse_iWL_to_ANY->tail->graph;
  vgx_Vertex_t *head_ANY = reverse_iWL_to_ANY->head.vertex;

  // Loop 
  if( reverse_iWL_to_ANY->tail == head_ANY ) {
    // !! WARNING !!
    // TODO: WARNING - the vertex is technically only inarc-writelocked. We may have to revisit this.
    // !! WARNING !!
    // We are calling a WL function with iWL vertex, it's the SAME vertex so may be ok but TODO is to validate this in all cases
    vgx_Arc_t reverse_iWL = {
      .tail = reverse_iWL_to_ANY->tail,
      .head = {
        .vertex     = reverse_iWL_to_ANY->head.vertex,
        .predicator = reverse_iWL_to_ANY->head.predicator
      }
    };
    reverse_iWL.head.predicator.rel.dir = VGX_ARCDIR_OUT;
    
    // We are assumed to hold WL since this is a self-reference arc, but check anyway
    if( __vertex_is_locked_writable_by_current_thread( head_ANY ) ) {
      // Ensure the vertex operation is open so we can capture the disconnect (normally not open when iWL locked)
      vgx_Vertex_t *head_WL = head_ANY;
      if( iOperation.Open_CS( graph, &head_WL->operation, COMLIB_OBJECT(head_WL), true ) < 0 ) {
        return 0;
      }
    }

    // Disconnect
    n_removed = _vxgraph_arc__disconnect_WL_forward_WL( dynamic, &reverse_iWL, refdelta, timing_budget );
  }
  // Normal
  else {
    vgx_Vertex_t *head_WL;
    bool external_lock = __vertex_is_locked_writable_by_current_thread( head_ANY );
    int inarcs_reclaimed = 0;
    if( external_lock ) {
      head_WL = head_ANY;
      // Reclaim inarcs if yielded
      if( __vertex_is_inarcs_yielded( head_WL ) && (inarcs_reclaimed = _vxgraph_state__reclaim_inarcs_CS_WL( graph, head_WL, timing_budget )) < 0 ) {
        // could not reclaim inarcs because they are busy
        return 0;
      }
    }
    else if( (head_WL = _vxgraph_state__lock_vertex_writable_CS( graph, head_ANY, timing_budget, VGX_VERTEX_RECORD_OPERATION )) == NULL ) {
      // could not lock reverse head because 
      // a) it disappeared while we blocked waiting for it and so it's gone along with the outarc we set out to remove; or
      // b) timeout because we're not blocking (arc expiration scenario)
      return 0;
    }

    // reverse head has now been writelocked
    vgx_Arc_t forward_WL_to_iWL = {
      .tail = head_WL,
      .head = {
        .vertex     = reverse_iWL_to_ANY->tail,
        .predicator = reverse_iWL_to_ANY->head.predicator
      }
    };
    forward_WL_to_iWL.head.predicator.rel.dir = VGX_ARCDIR_OUT;
    GRAPH_SUSPEND_LOCK( graph) {
      n_removed = iarcvector.Remove( dynamic, &forward_WL_to_iWL, timing_budget, __vertex_WL_decref_event ); // <= note the reversal of tail/head and no callback event
      if( n_removed < 0 || __vertex_WL_decref_delta( forward_WL_to_iWL.tail, refdelta ) != n_removed ) {
        // TODO: handle the discrepancy, rollback. Right now we're inconsistent.
        n_removed = __handle_disconnect_decref_error( __FUNCTION__, &forward_WL_to_iWL, refdelta, n_removed );
      }
      else {
        // Capture
        iOperation.Arc_WL.Disconnect( &forward_WL_to_iWL, n_removed );
      }
    } GRAPH_RESUME_LOCK;

    if( external_lock == false ) {
      if( _vxgraph_state__unlock_vertex_CS_LCK( graph, &forward_WL_to_iWL.tail, VGX_VERTEX_RECORD_OPERATION ) == false ) {
        // Operation failed and we are possibly in an inconsistent state
        // TODO: investigate inconsistency scenarios and fix.
        n_removed = __handle_disconnect_unlock_error( __FUNCTION__, &forward_WL_to_iWL );
      }
    }
    // return to inarcs yielded state
    else if( inarcs_reclaimed ) {
      _vxgraph_state__yield_inarcs_CS_WL( graph, head_WL );
    }
  }

  return n_removed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxgraph_arc__disconnect_iWL_forward_GENERAL( framehash_dynamic_t *dynamic, vgx_Arc_t *reverse_iWL_to_ANY, int64_t refdelta, vgx_ExecutionTimingBudget_t *timing_budget ) {
  int64_t n;
  // NOTE: The mutex must support recursion for this to be safe!
  GRAPH_LOCK( reverse_iWL_to_ANY->tail->graph ) {
    n = _vxgraph_arc__disconnect_iWL_forward_CS( dynamic, reverse_iWL_to_ANY, refdelta, timing_budget );
  } GRAPH_RELEASE;
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_arc.h"

test_descriptor_t _vgx_vxgraph_arc_tests[] = {
  { "VGX Graph Event Tests", __utest_vxgraph_arc },
  {NULL}
};
#endif
