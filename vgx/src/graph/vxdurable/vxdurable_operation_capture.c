/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdurable_operation_capture.c
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

#include "_vxoperation.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


#include "op/opcodes.h"



static int __acquire_explicit( vgx_Operation_t *operation );
static int __release_explicit( vgx_Operation_t *operation );
static int __acquire_vertex_explicit( vgx_Vertex_t *vertex );
static int64_t __submit_operation_CS( vgx_Operation_t *operation, bool hold_CS );
static int64_t __commit_operation_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, bool close, bool hold_CS );
static int __emitter_fence_CS( vgx_Graph_t *graph, int64_t opid, int timeout_ms );
static int _vxdurable_operation__vertex_acquire_LCK( vgx_Operation_t *operation );
static int _vxdurable_operation__vertex_release_LCK( vgx_Operation_t *operation );



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int __acquire_explicit( vgx_Operation_t *operation ) {
  vgx_OperatorCapture_t *capture = OPERATION_GET_CAPTURE( operation );
  return ++(capture->inheritable.xrecursion);
}



/*******************************************************************//**
 * Unwind one level of explicit acquisition in operation stream
 *
 * Return: > 0: Still explicitly acquired
 *         = 0: Was explicitly acquired but has gone to zero
 *         < 0: Was not explicitly acquired
 ***********************************************************************
 */
__inline static int __release_explicit( vgx_Operation_t *operation ) {
  vgx_OperatorCapture_t *capture = OPERATION_GET_CAPTURE( operation );
  if( capture->inheritable.xrecursion > 0 ) {
    --(capture->inheritable.xrecursion);
    return capture->inheritable.xrecursion;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static int __acquire_vertex_explicit( vgx_Vertex_t *vertex ) {
  vgx_Operation_t *op = &vertex->operation;

  if( OPERATION_IS_CLOSED( op ) ) {
    // Crucial to hold on to CS when opening vertex operation. We can't allow emitter throttle
    // to release CS since this call is one of several that all need to complete as a group
    // without losing CS (which may result in loss of open graph operation.)
    if( iOperation.Open_CS( vertex->graph, op, COMLIB_OBJECT( vertex ), true ) < 0 ) {
      return _vxdurable_operation__trap_error( vertex->graph, false, 0, "failed to open vertex operation" );
    }
  }

  return __acquire_explicit( op );
}



/*******************************************************************//**
 *
 * Returns:   > 0: New operation ID
 *           == 0: No operation
 *            < 0: Error
 *
 *
 ***********************************************************************
 */
static int64_t __submit_operation_CS( vgx_Operation_t *operation, bool hold_CS ) {
  // Yank the capture object out of the operation.
  // Operation is defunct at this point and must be reverted to closed state
  // explicitly before we return.
  vgx_OperatorCapture_t *capture = OPERATION_POP_CAPTURE( operation );
  vgx_Operation_t *defunct_operation = operation;

  vgx_Graph_t *graph = capture->graph;
  int64_t prev_opid = capture->opid;
  ALIGNED_VAR( vgx_OperationCaptureInheritable_t, inheritable, 32 ) = capture->inheritable;

  // SUBMIT (capture object will be consumed and eventually returned to pool)
  int64_t opid = _vxdurable_operation_emitter__submit_to_pending_CS( &capture );

  if( opid < 0 ) {
    OPERATION_CLOSE( defunct_operation, prev_opid );
    return _vxdurable_operation__trap_error( graph, true, 0x001, "failed to submit capture object" );
  }

  // Close defunct operation and set new opid
  vgx_Operation_t *closed_operation = OPERATION_CLOSE( defunct_operation, opid );

  // Re-open operation with new capture object
  if( _vxdurable_operation_emitter__next_operation_CS( graph, closed_operation, &inheritable, hold_CS ) == NULL ) {
    return _vxdurable_operation__trap_error( graph, true, 0x002, "failed to get new capture object" );
  }

  if( OPERATION_IS_OPEN( operation ) == false ) {
    return _vxdurable_operation__trap_error( graph, true, 0x003, "operation closed" );
  }

  return opid;
}



/*******************************************************************//**
 *
 * Returns:   > 0: New operation ID
 *           == 0: No operation
 *            < 0: Error
 *
 *
 ***********************************************************************
 */
static int64_t __commit_operation_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, bool close, bool hold_CS ) {

  int64_t opid = 0;

  // EMITTABLE VIA CAPTURE OBJECT
  if( OPERATION_IS_EMITTABLE( operation ) ) {
    if( OPERATION_IS_OPEN( operation ) ) {
      // COMMIT AND CX_CLOSE
      if( close ) {
        // Emit one explicit release operator for each lock recursion (zero ops if no xrecursion)
        while( __release_explicit( operation ) >= 0 ) {
          if( _vxdurable_operation__vertex_release_LCK( operation ) < 0 ) {
            return _vxdurable_operation__trap_error( graph, true, 0x001, "vertex release capture failed" );
          }
          OPERATION_SET_DIRTY( operation );
        }

        // Yank the capture object out of the operation.
        // Operation is defunct at this point and must be reverted to closed state
        // explicitly before we return.
        vgx_OperatorCapture_t *capture = OPERATION_POP_CAPTURE( operation );

        int64_t prev_opid = capture->opid;

        // Only commit if operation is marked dirty or capture object has uncommitted data
        if( OPERATION_IS_DIRTY( operation ) || ComlibSequenceLength( capture->opdatabuf ) > 0 ) {
          // SUBMIT
          if( (opid = _vxdurable_operation_emitter__submit_to_pending_CS( &capture )) < 0 ) {
            OPERATION_CLOSE( operation, prev_opid );
            return _vxdurable_operation__trap_error( graph, true, 0x002, "failed to submit capture object" );
          }
          else {
            // Committed and closed
            OPERATION_CLOSE( operation, opid );
          }
        }
        // Clean operation
        else {
          // Close operation with the original opid
          OPERATION_CLOSE( operation, prev_opid );
          // Return the unused capture object to the pool
          if( _vxdurable_operation_emitter__return_operator_capture_to_pool_OPEN( &capture ) < 0 ) {
            return _vxdurable_operation__trap_error( graph, true, 0x003, "failed to return capture object to pool" );
          }
        }
      }
      // COMMIT AND KEEP OPEN
      else if( OPERATION_IS_DIRTY( operation ) ) {
        if( (opid = __submit_operation_CS( operation, hold_CS )) < 0 ) {
          return _vxdurable_operation__trap_error( graph, true, 0x004, "failed to submit operation" );
        }
        OPERATION_SET_CLEAN( operation );
      }
    }
  }
  // LOCAL ONLY
  else {
    // Dirty operation that is local only gets new opid
    if( OPERATION_IS_DIRTY( operation ) ) {
      opid = _vxdurable_operation_emitter__next_operation_id_CS( &graph->OP.emitter );
      OPERATION_OPID_SET_INTEGER( operation, opid );
      OPERATION_SET_CLEAN( operation );
    }
  }

  return opid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __emitter_fence_CS( vgx_Graph_t *graph, int64_t opid, int timeout_ms ) {
  const char *path = CALLABLE( graph )->FullPath( graph );
  if( graph->OP.emitter.control.flag_CS.suspended ) {
    return 0;
  }

  // Flush
  if( iOperation.Emitter_CS.Fence( graph, opid, timeout_ms ) < 0 ) {
    if( iOperation.Emitter_CS.IsRunning( graph ) ) {
      if( iOperation.Emitter_CS.IsSuspended( graph ) ) {
        WARN( 0x001, "Emitter suspended (%s) - operation deferred", path );
      }
      else {
        CRITICAL( 0x001, "Emitter (%s) fence timeout at opid:%lld", path, opid );
      }
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 * Returns:   > 0: New operation ID
 *           == 0: No operation
 *            < 0: Error
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_capture__commit_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, bool hold_CS ) {
  // Commits temporarily deferred (used to group operators when the caller knows what they're doing.)
  if( graph->OP.emitter.control.defercommit_CS > 0 ) {
    return 0;
  }

  int64_t next;

  if( iSystem.IsSystemGraph( graph ) ) {
    int64_t opid = iOperation.GetId_LCK( operation );
    next = __commit_operation_CS( graph, operation, false, false );
    __emitter_fence_CS( graph, opid, 60000 );
  }
  else {
    next = __commit_operation_CS( graph, operation, false, hold_CS );
  }

  return next;
}



/*******************************************************************//**
 *
 * Returns:   > 0: New operation ID
 *           == 0: No operation
 *            < 0: Error
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_capture__close_CS( vgx_Graph_t *graph, vgx_Operation_t *operation, bool hold_CS ) {
  int64_t next;

  if( iSystem.IsSystemGraph( graph ) ) {
    int64_t opid = iOperation.GetId_LCK( operation );
    next = __commit_operation_CS( graph, operation, true, false );
    __emitter_fence_CS( graph, opid, 60000 );
  }
  else {
    next = __commit_operation_CS( graph, operation, true, hold_CS );
  }

  return next;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_capture__graph_commit_CS( vgx_Graph_t *graph ) {
  return _vxdurable_operation_capture__commit_CS( graph, &(graph)->operation, false );
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxdurable_operation_capture__graph_close_CS( vgx_Graph_t *graph ) {
  _vxdurable_operation_emitter__set_opmuted_CS( graph );
  return _vxdurable_operation_capture__close_CS( graph, &(graph)->operation, false );
}




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static CQwordBuffer_t * __get_operation_buffer( vgx_Operation_t *operation ) {
#define __OPDATABUF_MAX (1<<16)
  static const int OPDATABUF_MAX = __OPDATABUF_MAX;
  vgx_OperatorCapture_t *capture = OPERATION_GET_CAPTURE( operation );
  // Auto-commit if capture buffer has grown too large
  if( ComlibSequenceLength( capture->opdatabuf ) > OPDATABUF_MAX ) {
    GRAPH_LOCK( capture->graph ) {
      iOperation.Commit_CS( capture->graph, operation, true );
    } GRAPH_RELEASE;
    // Refresh
    capture = OPERATION_GET_CAPTURE( operation );
  }
  return capture->opdatabuf;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __capture_opdata( vgx_Operation_t *operation, QWORD *qwords, int64_t nqwords, bool dirty ) {
  if( OPERATION_IS_OPEN( operation ) ) {
    CQwordBuffer_t *buf = __get_operation_buffer( operation );
    if( CALLABLE( buf )->Write( buf, qwords, nqwords ) == nqwords ) {
      if( dirty ) {
        _vxdurable_operation__set_dirty( operation );
      }
      return 1;
    }
    else {
      return _vxdurable_operation__trap_error( NULL, false, 0x001, "write to operation buffer failed" );
    }
  }
  return _vxdurable_operation__trap_error( NULL, false, 0x002, "operation was closed" );
}




#define CAPTURE( Op, OpData )             __capture_opdata( Op, (OpData).qwords, qwsizeof( OpData ), true )
#define CAPTURE_CLEAN( Op, OpData )       __capture_opdata( Op, (OpData).qwords, qwsizeof( OpData ), false )



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static bool __graph_operation_capture_check_available_CS( vgx_Graph_t *graph ) {
  // Normal case: graph operation is open, proceed
  if( OPERATION_IS_OPEN( &graph->operation ) ) {
    return true;
  }
  // Graph operation is closed. Should we wait?
  else if( iOperation.Emitter_CS.IsRunning( graph ) ) {
    return _vxdurable_operation_emitter__wait_available_graph_operation_CS( graph );
  }
  // Emitter not running
  else {
    return false;
  }
}




#define CAPTURE_VERTEX_OPERATION( Vertex )                        \
if( iSystem.IsSystemGraph( (Vertex)->graph ) || OPERATION_IS_LOCAL_ONLY( &(Vertex)->operation ) ) { \
  return _vxdurable_operation__set_dirty( &(Vertex)->operation ); \
}                                                                 \
else if( OPERATION_IS_CLOSED( &(Vertex)->operation ) ) {          \
  return 0;                                                       \
}                                                                 \
else /* {
  CODE
} */

#define CAPTURE_ARC_OPERATION( Arc )                                    \
if( iSystem.IsSystemGraph( (Arc)->tail->graph ) || OPERATION_IS_LOCAL_ONLY( &((Arc)->tail)->operation ) ) { \
  _vxdurable_operation__set_dirty( &((Arc)->tail)->operation );         \
  _vxdurable_operation__set_dirty( &((Arc)->head.vertex)->operation );  \
  return 1;                                                             \
}                                                                       \
else if( OPERATION_IS_CLOSED( &((Arc)->tail)->operation ) ) {           \
  return 0;                                                             \
}                                                                       \
else /* {
  CODE
} */


#define CAPTURE_GRAPH_OPERATION_CS( Graph, DirtyImplied )         \
if( iSystem.IsSystemGraph( Graph ) || !OPERATION_CAPTURE_ENABLED_CS( Graph ) ) { \
  if( DirtyImplied ) {                                            \
    _vxdurable_operation__set_dirty( &(Graph)->operation  );      \
  }                                                               \
  return 1;                                                       \
}                                                                 \
else if( !(Graph) || __graph_operation_capture_check_available_CS( Graph ) == false ) { \
  return _vxdurable_operation__trap_error( Graph, false, 0, "no available capture object for graph operation" ); \
}                                                                 \
else /* {
  CODE
} */


#define CAPTURE_SYSTEM_OPERATION_SYS_CS( SystemGraph, DirtyImplied  )   \
if( !OPERATION_CAPTURE_ENABLED_CS( SystemGraph ) ) {                    \
  if( DirtyImplied ) {                                                  \
    _vxdurable_operation__set_dirty( &(SystemGraph)->operation  );      \
  }                                                                     \
  return 1;                                                             \
}                                                                       \
else if( !(SystemGraph) || __graph_operation_capture_check_available_CS( SystemGraph ) == false ) { \
  return _vxdurable_operation__trap_error( SystemGraph, false, 0, "no available capture object for system graph operation" ); \
}                                                                       \
else /* {
  CODE
} */



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_new_CS( vgx_Vertex_t *vertex, vgx_Vertex_constructor_args_t *args ) {
  vgx_Graph_t *graph = vertex->graph;

  vgx_Operation_t *vertex_operation = &vertex->operation;
  vgx_Operation_t *graph_operation = &vertex->graph->operation;

  if( iSystem.IsSystemGraph( graph ) || OPERATION_IS_LOCAL_ONLY( vertex_operation ) ) {
    _vxdurable_operation__set_dirty( vertex_operation );
    _vxdurable_operation__set_dirty( graph_operation );
    return 1;
  }

  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    CAPTURE_VERTEX_OPERATION( vertex ) {

      _vxdurable_operation__set_dirty( vertex_operation );

      op_vertex_new opdata = get__op_vertex_new( graph, args->vxtype, __vertex_internalid( vertex ), args->ts, args->rank, args->CSTR__idstring );

      int ret = CAPTURE( graph_operation, opdata );
      if( ret < 0 ) {
        icstringobject.DecrefNolock( opdata.CSTR__id );
      }
      return ret;
    }
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_delete_CS( vgx_Vertex_t *vertex ) {
  vgx_Graph_t *graph = vertex->graph;

  vgx_Operation_t *vertex_operation = &vertex->operation;
  vgx_Operation_t *graph_operation = &vertex->graph->operation;

  if( iSystem.IsSystemGraph( graph ) || OPERATION_IS_LOCAL_ONLY( vertex_operation ) ) {
    _vxdurable_operation__set_dirty( vertex_operation );
    _vxdurable_operation__set_dirty( graph_operation );
    return 1;
  }

  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    CAPTURE_VERTEX_OPERATION( vertex ) {
      _vxdurable_operation__set_dirty( vertex_operation );
      op_vertex_delete opdata = get__op_vertex_delete( vertex );
      return CAPTURE( graph_operation, opdata );
    }
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_set_rank_WL( vgx_Vertex_t *vertex, const vgx_Rank_t *rank ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_set_rank opdata = get__op_vertex_set_rank( rank );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_change_type_WL( vgx_Vertex_t *vertex, vgx_vertex_type_t new_type ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_set_type opdata = get__op_vertex_set_type( new_type );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_set_tmx_WL( vgx_Vertex_t *vertex, uint32_t tmx ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_set_tmx opdata = get__op_vertex_set_tmx( tmx );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_convert_WL( vgx_Vertex_t *vertex, vgx_VertexStateContext_man_t manifestation ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_convert opdata = get__op_vertex_convert( manifestation );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_set_property_WL( vgx_Vertex_t *vertex, const vgx_VertexProperty_t *prop ) {
  static const char sysprefix[] = VGX_SYSTEM_PROPERTY_PREFIX;
  vgx_Operation_t *operation = &vertex->operation;

  // System properties are never emitted
  if( prop->key && CALLABLE( prop->key )->StartsWith( prop->key, sysprefix ) ) {
    _vxdurable_operation__set_dirty( operation  );
    return 1;
  }

  CAPTURE_VERTEX_OPERATION( vertex ) {
    op_vertex_set_property opdata = get__op_vertex_set_property( vertex->graph, prop );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_del_property_WL( vgx_Vertex_t *vertex, shortid_t key ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_delete_property opdata = get__op_vertex_delete_property( key );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_del_properties_WL( vgx_Vertex_t *vertex ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_clear_properties opdata = get__op_vertex_clear_properties();
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_set_vector_WL( vgx_Vertex_t *vertex, const vgx_Vector_t *vector ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    int ret;
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_set_vector opdata = get__op_vertex_set_vector( vector );
    if( opdata.CSTR__vector ) {
      if( (ret = CAPTURE( operation, opdata )) < 0 ) {
        iString.Discard( &opdata.CSTR__vector );
      }
    }
    else {
      ret = -1;
    }
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_del_vector_WL( vgx_Vertex_t *vertex ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_delete_vector opdata = get__op_vertex_delete_vector();
    return CAPTURE( operation, opdata );
  }
}


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_remove_outarcs_WL( vgx_Vertex_t *vertex, int64_t n_removed ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_delete_outarcs opdata = get__op_vertex_delete_outarcs( n_removed );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertex_remove_inarcs_WL( vgx_Vertex_t *vertex, int64_t n_removed ) {
  CAPTURE_VERTEX_OPERATION( vertex ) {
    vgx_Operation_t *operation = &vertex->operation;
    op_vertex_delete_inarcs opdata = get__op_vertex_delete_inarcs( n_removed );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__arc_connect_WL( vgx_Arc_t *arc ) {
  CAPTURE_ARC_OPERATION( arc ) {
    op_arc_connect opdata = get__op_arc_connect( arc );
    vgx_Operation_t *optail = &arc->tail->operation;
    vgx_Operation_t *ophead = &arc->head.vertex->operation;
    _vxdurable_operation__set_dirty( ophead );
    return CAPTURE( optail, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__arc_disconnect_WL( vgx_Arc_t *arc, int64_t n_removed ) {
  CAPTURE_ARC_OPERATION( arc ) {
    if( n_removed > 0 ) {
      op_arc_disconnect opdata = get__op_arc_disconnect( arc, n_removed );
      vgx_Operation_t *optail = &arc->tail->operation;
      vgx_Operation_t *ophead = &arc->head.vertex->operation;
      _vxdurable_operation__set_dirty( ophead );
      return CAPTURE( optail, opdata );
    }
    else {
      return 0;
    }
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__vertex_acquire_LCK( vgx_Operation_t *operation ) {
  op_vertex_acquire opdata = get__op_vertex_acquire();
  return CAPTURE_CLEAN( operation, opdata );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxdurable_operation__vertex_release_LCK( vgx_Operation_t *operation ) {
  op_vertex_release opdata = get__op_vertex_release();
  return CAPTURE_CLEAN( operation, opdata );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_attach_SYS_CS( vgx_Graph_t *SYSTEM ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    int ret;
    vgx_Operation_t *operation = &SYSTEM->operation;

    op_system_attach opdata = get__op_system_attach( SYSTEM );

    if( opdata.CSTR__via_uri && opdata.CSTR__origin_host && opdata.CSTR__origin_version ) {
      ret = CAPTURE_CLEAN( operation, opdata );
    }
    else {
      ret = -1;
    }

    if( ret < 0 ) {
      iString.Discard( &opdata.CSTR__via_uri );
      iString.Discard( &opdata.CSTR__origin_host );
      iString.Discard( &opdata.CSTR__origin_version );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_detach_SYS_CS( vgx_Graph_t *SYSTEM ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    int ret;
    vgx_Operation_t *operation = &SYSTEM->operation;

    op_system_detach opdata = get__op_system_detach( SYSTEM );

    if( opdata.CSTR__origin_host && opdata.CSTR__origin_version ) {
      ret = CAPTURE_CLEAN( operation, opdata );
    }
    else {
      ret = -1;
    }

    if( ret < 0 ) {
      iString.Discard( &opdata.CSTR__origin_host );
      iString.Discard( &opdata.CSTR__origin_version );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_clear_registry_SYS_CS( vgx_Graph_t *SYSTEM ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, true ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
    op_system_clear_registry opdata = get__op_system_clear_registry();
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_create_graph_SYS_CS( vgx_Graph_t *SYSTEM, const objectid_t *obid, const char *name, const char *path, int vertex_block_order, uint32_t graph_t0, int64_t start_opcount, vgx_Similarity_config_t *simconfig ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, true ) {
    int ret;
    vgx_Operation_t *operation = &SYSTEM->operation;

    op_system_similarity opdata_sim = get__op_system_similarity( simconfig );
    op_none noop = get__op_none();
    op_system_create_graph opdata = get__op_system_create_graph( SYSTEM, vertex_block_order, graph_t0, start_opcount, obid, name, path ); 

    if( opdata.CSTR__name == NULL || opdata.CSTR__path == NULL ) {
      iString.Discard( &opdata.CSTR__name );
      iString.Discard( &opdata.CSTR__path );
      return _vxdurable_operation__trap_error( SYSTEM, false, 0x001, "out of memory" );
    }

    CAPTURE( operation, opdata_sim );
    CAPTURE( operation, noop );

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__path );
      CStringDelete( opdata.CSTR__name );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_delete_graph_SYS_CS( vgx_Graph_t *SYSTEM, const objectid_t *obid ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, true ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
    op_system_delete_graph opdata = get__op_system_delete_graph( obid );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_tick_SYS_CS( vgx_Graph_t *SYSTEM, int64_t tms ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    vgx_Operation_t *operation = &SYSTEM->operation;

    op_graph_tic opdata = get__op_graph_tic( tms );

    int ret = CAPTURE_CLEAN( operation, opdata );

    if( ret < 0 ) {
      return ret;
    }

    __submit_operation_CS( operation, false );
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_send_comment_SYS_CS( vgx_Graph_t *SYSTEM, CString_t *CSTR__comment ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    int ret;
    vgx_Operation_t *operation = &SYSTEM->operation;

    op_system_send_comment opdata = get__op_system_send_comment( SYSTEM, CSTR__comment );

    if( opdata.CSTR__comment == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE_CLEAN( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__comment );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_send_simple_aux_command_SYS_CS( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, CString_t *CSTR__command ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
    // Max 32kB
    const int maxpart = 1 << 15;

    int64_t dlen = CStringLength( CSTR__command );
    if( dlen > maxpart ) {
      return -1;
    }

    // Generate a unique object id for the data
    objectid_t cmd_id;
    idcpy( &cmd_id, CALLABLE( CSTR__command )->Obid( CSTR__command ) );  //obid_from_string_len( data, dlen > maxdigest ? maxdigest : (int)dlen );
   
    op_system_send_raw_data opdata = get__op_system_send_raw_data( SYSTEM, cmd, cmd_id, 1, 0, CSTR__command );

    if( opdata.CSTR__datapart == NULL ) {
      return -1;
    }

    if( CAPTURE_CLEAN( operation, opdata ) < 0 ) {
      CStringDelete( opdata.CSTR__datapart );
      return -1;
    }

    return 1;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _vxdurable_operation_capture__system_forward_aux_command_SYS_CS( struct s_vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, objectid_t cmd_id, vgx_StringList_t *cmd_data ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
   
    int64_t n_parts = iString.List.Size( cmd_data );

    for( int64_t part_id=0; part_id<n_parts; part_id++ ) {
      CString_t *CSTR__datapart = iString.List.GetItem( cmd_data, part_id );
      if( CSTR__datapart ) {

        op_system_send_raw_data opdata = get__op_system_send_raw_data( SYSTEM, OPAUX_RAW_DATA, cmd_id, n_parts, part_id, CSTR__datapart );
        if( opdata.CSTR__datapart == NULL ) {
          return -1;
        }

        if( CAPTURE_CLEAN( operation, opdata ) < 0 ) {
          CStringDelete( opdata.CSTR__datapart );
          return -1;
        }
      }
      else {
        return -1;
      }
    }

    iOperation.Graph_CS.SetModified( SYSTEM );
    COMMIT_GRAPH_OPERATION_CS( SYSTEM );

    return 1;
  }

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_send_raw_data_SYS_CS( vgx_Graph_t *SYSTEM, const char *data, int64_t dlen ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, false ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
   
    // Generate a unique object id for the data
    const int maxdigest = 1 << 30;
    objectid_t baseid = obid_from_string_len( data, dlen > maxdigest ? maxdigest : (int)dlen );
    QWORD base[] = {
      __MILLISECONDS_SINCE_1970(),
      baseid.H,
      baseid.L,
      dlen
    };
    objectid_t obid = obid_from_string_len( (const char*)base, sizeof(base) );

    // Max 32kB per chunk
    const int maxpart = 1 << 15;

    // Send data in chunks
    const char *cursor = data;
    const char *end = data + dlen;
    int64_t n_parts = ((dlen - 1) >> 15) + 1;
    int64_t part_id = 0;

    while( cursor < end ) {
      int64_t remain = (int64_t)(end - cursor);
      int len = remain > maxpart ? maxpart : (int)remain;
      CString_t *CSTR__datapart = NewEphemeralCStringLen( SYSTEM, cursor, len, 0 ); // new instance
      if( CSTR__datapart == NULL ) {
        return -1;
      }
      op_system_send_raw_data opdata = get__op_system_send_raw_data( SYSTEM, OPAUX_RAW_DATA, obid, n_parts, part_id, CSTR__datapart );
      iString.Discard( &CSTR__datapart ); // decref (opdata has its own ref)
      if( opdata.CSTR__datapart == NULL ) {
        return -1;
      }

      if( CAPTURE_CLEAN( operation, opdata ) < 0 ) {
        CStringDelete( opdata.CSTR__datapart );
        return -1;
      }

      cursor += len;
      ++part_id;
    }

    return 1;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__system_clone_graph_SYS_CS( vgx_Graph_t *SYSTEM, vgx_Graph_t *source ) {
  CAPTURE_SYSTEM_OPERATION_SYS_CS( SYSTEM, true ) {
    vgx_Operation_t *operation = &SYSTEM->operation;
    op_system_clone_graph opdata = get__op_system_clone_graph( source );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_truncate_CS( vgx_Graph_t *graph, vgx_vertex_type_t vxtype, int64_t n_discarded ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    vgx_Operation_t *operation = &graph->operation;
    op_graph_truncate opdata = get__op_graph_truncate( vxtype, n_discarded );
    return CAPTURE( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_persist_CS( vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts, bool force ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    op_graph_persist opdata = get__op_graph_persist( counts, force );
    return CAPTURE_CLEAN( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_state_CS( vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    op_graph_state opdata = get__op_graph_state( counts, false );
    return CAPTURE_CLEAN( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_assert_state_CS( vgx_Graph_t *graph, const vgx_graph_base_counts_t *counts ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    op_graph_state opdata = get__op_graph_state( counts, true );
    return CAPTURE_CLEAN( operation, opdata );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_readonly_CS( vgx_Graph_t *graph ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    
    op_graph_readonly opdata = get__op_graph_readonly();

    int ret = CAPTURE_CLEAN( operation, opdata );
    
    if( ret < 0 ) {
      return ret;
    }
    
    __submit_operation_CS( operation, false );
    
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_readwrite_CS( vgx_Graph_t *graph ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    
    op_graph_readwrite opdata = get__op_graph_readwrite();

    int ret = CAPTURE_CLEAN( operation, opdata );
    
    if( ret < 0 ) {
      return ret;
    }
    
    __submit_operation_CS( operation, false );
    
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_events_CS( vgx_Graph_t *graph ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    
    op_graph_events opdata = get__op_graph_events();

    int ret = CAPTURE_CLEAN( operation, opdata );
    
    if( ret < 0 ) {
      return ret;
    }
    
    __submit_operation_CS( operation, false );
    
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_noevents_CS( vgx_Graph_t *graph ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    
    op_graph_noevents opdata = get__op_graph_noevents();

    int ret = CAPTURE_CLEAN( operation, opdata );
    
    if( ret < 0 ) {
      return ret;
    }
    
    __submit_operation_CS( operation, false );
    
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__graph_event_exec_CS( vgx_Graph_t *graph, uint32_t ts, uint32_t tmx ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    vgx_Operation_t *operation = &graph->operation;
    
    op_graph_event_exec opdata = get__op_graph_event_exec( ts, tmx );

    int ret = CAPTURE_CLEAN( operation, opdata );
    
    if( ret < 0 ) {
      return ret;
    }
    
    __submit_operation_CS( operation, false );
    
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertices_acquire_CS_WL( vgx_Graph_t *graph, vgx_VertexList_t *vertices_WL, CString_t **CSTR__error ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    int ret = 0;
    vgx_Operation_t *operation = &graph->operation;

    int sz = (int)iVertex.List.Size( vertices_WL );

    if( sz > _vxdurable_operation_emitter__inflight_capacity_CS( graph ) ) {
      const char *serr = "too many inflight operations";
      __set_error_string( CSTR__error, serr );
      return _vxdurable_operation__trap_error( graph, false, 0, serr ); // code=0 -> not critical
    }

    op_vertices_lockstate opdata = {
      .op        = OPERATOR_VERTICES_ACQUIRE_WL,
      .count     = 0, // set later
      .obid_list = calloc( sz, sizeof( objectid_t ) )
    };

    if( opdata.obid_list == NULL ) {
      const char *serr = "out of memory";
      __set_error_string( CSTR__error, serr );
      return _vxdurable_operation__trap_error( graph, false, 0x002, serr );
    }

    objectid_t *dest = opdata.obid_list;
    vgx_Vertex_t *vertex;

    for( int i=0; i<sz; i++ ) {
      vertex = iVertex.List.Get( vertices_WL, i );
      idcpy( dest++, __vertex_internalid( vertex ) );
      if( __acquire_vertex_explicit( vertex ) < 0 ) {
        free( opdata.obid_list );
        const char *serr = "explicit vertex acquisition failed";
        __set_error_string( CSTR__error, serr );
        return _vxdurable_operation__trap_error( graph, false, 0x003, serr );
      }
      opdata.count++;
    }

    // Capture
    if( iVertex.List.Truncate( vertices_WL, opdata.count ) > 0 && (ret = CAPTURE_CLEAN( operation, opdata )) > 0 ) {
      __submit_operation_CS( operation, false );
    }
    else {
      // If we failed to capture we need to release what we acquired above
      for( int i=0; i<opdata.count; i++ ) {
        vertex = iVertex.List.Get( vertices_WL, i );
        __release_explicit( &vertex->operation );
      }

      free( opdata.obid_list );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertices_release_CS_LCK( vgx_Graph_t *graph, vgx_VertexList_t *vertices_LCK ) {
  int ret = 0;
  int err = 0;
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {

    int sz = (int)iVertex.List.Size( vertices_LCK );

    op_vertices_lockstate opdata = {
      .op        = OPERATOR_VERTICES_RELEASE,
      .count     = 0, // set later
      .obid_list = calloc( sz, sizeof( objectid_t ) )
    };

    if( opdata.obid_list == NULL ) {
      return _vxdurable_operation__trap_error( graph, false, 0x001, "out of memory" );
    }

    objectid_t *dest = opdata.obid_list;
    vgx_Vertex_t *vertex;
    for( int i=0; i<sz; i++ ) {
      vertex = iVertex.List.Get( vertices_LCK, i );
      vgx_Operation_t *op_vertex = &vertex->operation;
      // Only writable vertices are considered (readonly locks are never part of the operation stream)
      if( __vertex_is_locked_writable( vertex ) && OPERATION_IS_OPEN( op_vertex ) ) {
        
        // Release any explicit acquisition (recursively) and set flag to true
        // if vertex was explicitly acquired.
        bool explicit = __release_explicit( op_vertex ) >= 0;
        
        // Set flag to true if vertex was modified and therefore has
        // an operation in the stream. It needs to be included in multi-release
        // to ensure consistent locking semantics.
        bool dirty = _vxdurable_operation__is_dirty( op_vertex );

        // Explicitly acquired or modified vertex.
        // Queue vertex for multi-release.
        if( explicit || dirty ) {
          idcpy( dest++, __vertex_internalid( vertex ) );
          opdata.count++;
        }

        // Modified vertex that was not explicitly acquired.
        // Inject one explicit acquisition for this vertex operation to balance
        // the multi-release we are about to issue.
        if( dirty && !explicit ) {
          if( _vxdurable_operation__vertex_acquire_LCK( op_vertex ) < 0 ) {
            --err;
            _vxdurable_operation__trap_error( graph, false, 0x002, "failed to capture explicit vertex acquisition before multi-release" );
          }
        }

        // We need to commit all vertices before they are released below
        // to preserve the intended order of operations.
        // NOTE: Hold CS flag to avoid risk of losing CS due to emitter throttle
        _vxdurable_commit__commit_vertex_CS_WL( graph, vertex, true );

      }
    }

    // Capture
    vgx_Operation_t *op_graph = &graph->operation;
    if( iVertex.List.Truncate( vertices_LCK, opdata.count ) > 0 && (ret = CAPTURE_CLEAN( op_graph, opdata )) > 0 ) {
      __submit_operation_CS( op_graph, false );
      opdata.obid_list = NULL; // stolen by capture object
    }

    free( opdata.obid_list );
  }

  if( err < 0 ) {
    return err;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__vertices_release_all_CS( vgx_Graph_t *graph ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, false ) {
    int ret = 0;
    vgx_Operation_t *operation = &graph->operation;

    op_vertices_lockstate opdata = {
      .op        = OPERATOR_VERTICES_RELEASE_ALL,
      .count     = (int)_vxgraph_tracker__num_writable_locks_CS( graph ), // HINT only
      .obid_list = NULL
    };

    // Capture
    if( (ret = CAPTURE_CLEAN( operation, opdata )) > 0 ) {
      __submit_operation_CS( operation, true );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_add_vertextype_CS( vgx_Graph_t *graph, QWORD typehash, const CString_t *CSTR__vertex_type_name, vgx_vertex_type_t typecode ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_add_string64 opdata = get__op_vea( graph, typehash, CSTR__vertex_type_name, typecode );

    if( opdata.CSTR__value == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__value );
    }

    return ret;
  }
}
 


/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_remove_vertextype_CS( vgx_Graph_t *graph, QWORD typehash, vgx_vertex_type_t typecode ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_delete_string64 opdata = get__op_ved( typehash, typecode );

    ret = CAPTURE( operation, opdata );

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_add_relationship_CS( vgx_Graph_t *graph, QWORD relhash, const CString_t *CSTR__relationship, vgx_predicator_rel_enum relcode ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_add_string64 opdata = get__op_rea( graph, relhash, CSTR__relationship, relcode );

    if( opdata.CSTR__value == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__value );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_remove_relationship_CS( vgx_Graph_t *graph, QWORD relhash, vgx_predicator_rel_enum relcode ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_delete_string64 opdata = get__op_red( relhash, relcode );

    ret = CAPTURE( operation, opdata );

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_add_dimension_CS( vgx_Similarity_t *similarity, QWORD dimhash, const CString_t *CSTR__dimension, feature_vector_dimension_t dimcode ) {
  vgx_Graph_t *graph = similarity->parent;
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;
    
    op_enum_add_string64 opdata = get__op_dea( graph, dimhash, CSTR__dimension, dimcode );

    if( opdata.CSTR__value == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__value );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_remove_dimension_CS( vgx_Similarity_t *similarity, QWORD dimhash, feature_vector_dimension_t dimcode ) {
  vgx_Graph_t *graph = similarity->parent;
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;
    op_enum_delete_string64 opdata = get__op_ded( dimhash, dimcode );
    ret = CAPTURE( operation, opdata );
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_add_propertykey_CS( vgx_Graph_t *graph, shortid_t keyhash, const CString_t *CSTR__key ) {
  static const char sysprefix[] = VGX_SYSTEM_PROPERTY_PREFIX;
  vgx_Operation_t *operation = &graph->operation;

  // System properties are never emitted
  if( CALLABLE( CSTR__key )->StartsWith( CSTR__key, sysprefix ) ) {
    _vxdurable_operation__set_dirty( operation  );
    return 1;
  }

  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;

    op_enum_add_string64 opdata = get__op_kea( graph, keyhash, CSTR__key );

    if( opdata.CSTR__value == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__value );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_remove_propertykey_CS( vgx_Graph_t *graph, shortid_t keyhash ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_delete_string64 opdata = get__op_ked( keyhash );
    ret = CAPTURE( operation, opdata );
    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_add_stringvalue_CS( vgx_Graph_t *graph, const objectid_t *obid, const CString_t *CSTR__string ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;

    op_enum_add_string128 opdata = get__op_sea( graph, obid, CSTR__string );

    if( opdata.CSTR__value == NULL ) {
      return -1;
    }

    if( (ret = CAPTURE( operation, opdata )) < 0 ) {
      CStringDelete( opdata.CSTR__value );
    }

    return ret;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxdurable_operation_capture__enumerator_remove_stringvalue_CS( vgx_Graph_t *graph, const objectid_t *obid ) {
  CAPTURE_GRAPH_OPERATION_CS( graph, true ) {
    int ret;
    vgx_Operation_t *operation = &graph->operation;
    op_enum_delete_string128 opdata = get__op_sed( obid );
    ret = CAPTURE( operation, opdata );
    return ret;
  }
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxdurable_operation_capture.h"
  
test_descriptor_t _vgx_vxdurable_operation_capture_tests[] = {
  { "VGX Graph Durable Operation Capture Tests", __utest_vxdurable_operation_capture },
  {NULL}
};
#endif
