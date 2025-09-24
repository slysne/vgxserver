/*
###################################################
#
# File:   oputil.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXDURABLE_OPUTIL_H
#define _VGX_VXDURABLE_OPUTIL_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __parser_reason( vgx_OperationParser_t *parser, vgx_ExecutionTimingBudget_t *timing_budget ) {
  __set_access_reason( &parser->reason, timing_budget->reason );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t *__acquire_arc_head( vgx_OperationParser_t *parser, const objectid_t *obid ) {
  vgx_Vertex_t *head_WL = NULL;
  // Acquire head
  vgx_Graph_t *graph = parser->op_graph;
  GRAPH_LOCK( graph ) {
    BEGIN_EXECUTION_ATTEMPT( parser, head_WL != NULL ) {
      vgx_Vertex_t *head = _vxgraph_vxtable__query_CS( graph, NULL, obid, VERTEX_TYPE_ENUMERATION_WILDCARD );
      if( head ) {
        vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( graph, LONG_TIMEOUT );
        if( (head_WL = _vxgraph_state__lock_vertex_writable_CS( graph, head, &timing_budget, VGX_VERTEX_RECORD_ACQUISITION )) != NULL ) {
          PARSER_INC_VERTEX_LOCKS( parser, 1 );
        }
        __parser_reason( parser, &timing_budget );
      }
      else {
        PARSER_SET_REASON( parser, VGX_ACCESS_REASON_NOEXIST );
      }
    } END_EXECUTION_ATTEMPT;
  } GRAPH_RELEASE;
  return head_WL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __delete_graph( vgx_Graph_t *graph, CString_t **CSTR__error ) {
  objectid_t obid;
  char idbuf[33];
  char path[255];
  objectid_t *graphkey = idcpy( &obid, COMLIB_OBJECT_GETID( graph ) );
  idtostr( idbuf, graphkey );
  strncpy( path, CALLABLE( graph )->FullPath( graph ), 254 );

  unsigned int owner;
  GRAPH_LOCK( graph ) {
    owner = _vgx_graph_owner_CS( graph );
  } GRAPH_RELEASE;

  // No owner, delete graph
  if( owner == 0 ) {
    if( igraphfactory.DelGraphByObid( graphkey ) < 0 ) {
      __format_error_string( CSTR__error, "Failed to delete graph %s (%s): registry error", idbuf, path );
      return -1;
    }
  }
  // Parser thread owns graph, this should never happen
  else if( owner == GET_CURRENT_THREAD_ID() ) {
    __format_error_string( CSTR__error, "Failed to delete graph %s (%s): operation parser is registered as graph owner", idbuf, path );
    return -1;
  }
  // Other thread owns graph, we can't delete it
  else {
    __format_error_string( CSTR__error, "Failed to delete graph %s (%s): thread %u registered as graph owner", idbuf, path, owner );
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool __operation_parser_gate_check_CS( vgx_OperationParser_t *parser, vgx_Graph_t *graph_CS ) {
  __assert_state_lock( graph_CS );
  // Make sure graph is writable
  if( _vgx_is_readonly_CS( &graph_CS->readonly ) ) {
    // READONLY graph. We cannot use this graph to process operations at this time.
    PARSER_SET_ERROR_REASON( parser, "Cannot apply operation to readonly graph", VGX_ACCESS_REASON_READONLY_GRAPH );
    return false;
  }

  // Verify serial number allowed
  // Regression ?
  if( PARSER_CONTROL_CHECK_REGRESSION( parser ) && parser->sn < graph_CS->tx_serial_in ) {
    // We will log error and prevent parser from doing any more work until a recovery point is reached
    if( PARSER_CONTROL_CATCH_REGRESSION( parser ) ) {
      PARSER_SET_ERROR_REASON( parser, "Regression", VGX_ACCESS_REASON_BAD_CONTEXT );
      int64_t regress = parser->sn - graph_CS->tx_serial_in;
      OPEXEC_REASON( parser, "Regression at sn=%lld (%lld steps)", parser->sn, regress );
      return false;
    }

    // Otherwise we will allow parsing to silently ignore the regression and continue without executing any opcodes
    if( parser->control.exe == OPEXEC_NORMAL ) {
      PARSER_CONTROL_SKIP_OPERATION( parser );
    }
  }

  // Graph cleared for use
  return true;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __operation_parser_gate_exit_CS( vgx_OperationParser_t *parser, vgx_Graph_t *graph_CS ) {
  __assert_state_lock( graph_CS );
  // Update graph with current transaction ID and serial number
  if( parser->sn > graph_CS->tx_serial_in ) {
    graph_CS->tx_serial_in = parser->sn;
    idcpy( &graph_CS->tx_id_in, &parser->transid );
    graph_CS->tx_count_in++;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __operation_parser_open_vertex( vgx_OperationParser_t *parser, objectid_t *vertex_obid ) {
  vgx_Graph_t *graph = parser->op_graph;
  if( graph == NULL ) {
    return NULL;
  }

  // Vertex already open, acquire recursive writelock
  if( parser->op_vertex_WL ) {
    vgx_Vertex_t *vertex_WL;
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_graph_zero_execution_timing_budget( graph );
    GRAPH_LOCK( graph ) {
      vertex_WL = _vxgraph_state__lock_vertex_writable_CS( graph, parser->op_vertex_WL, &zero_timeout, VGX_VERTEX_RECORD_ACQUISITION );
    } GRAPH_RELEASE;
    if( vertex_WL ) {
      PARSER_INC_VERTEX_LOCKS( parser, 1 );
    }
    else {
      PARSER_SET_ERROR_REASON( parser, "recursion error", VGX_ACCESS_REASON_NOEXIST );
    }
  }
  // Acquire vertex WRITABLE before we can proceed.
  else {
    GRAPH_LOCK( graph ) {
      if( __operation_parser_gate_check_CS( parser, graph ) ) {
        BEGIN_EXECUTION_ATTEMPT( parser, parser->op_vertex_WL != NULL ) {
          if( (parser->op_vertex_WL = CALLABLE( graph )->advanced->AcquireVertexWritableNocreate_CS( graph, vertex_obid, LONG_TIMEOUT, &parser->reason, &parser->CSTR__error )) != NULL ) {
            PARSER_INC_VERTEX_LOCKS( parser, 1 );
          }
        } END_EXECUTION_ATTEMPT;
      }
    } GRAPH_RELEASE;
  }

  return parser->op_vertex_WL;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool __operation_parser_close_vertex( vgx_OperationParser_t *parser ) {
  bool released = false;
  // Release exactly one vertex lock recursion, to balance out __operation_parser_open_vertex
  if( parser->op_vertex_WL ) {
    vgx_Graph_t *graph = parser->op_vertex_WL->graph;
    GRAPH_LOCK( graph ) {
      released = _vxgraph_state__release_vertex_CS_LCK( graph, &parser->op_vertex_WL );
      __operation_parser_gate_exit_CS( parser, graph );
    } GRAPH_RELEASE;
    if( released ) {
      PARSER_DEC_VERTEX_LOCKS( parser, 1 );
    }
  }
  return released;
}




#endif
