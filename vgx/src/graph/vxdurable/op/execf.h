/*
###################################################
#
# File:   execf.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXDURABLE_EXECF_H
#define _VGX_VXDURABLE_EXECF_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_execf_entry {
  struct {
    f_execute_operator patched;
    f_execute_operator normal;
  } execf;
  OperationProcessorOperator_t op; 
} __execf_entry;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct __s_execf_map {
  __execf_entry operator__op_none;
  __execf_entry operator__op_vertex_new;
  __execf_entry operator__op_vertex_delete;
  __execf_entry operator__op_vertex_set_rank;
  __execf_entry operator__op_vertex_set_type;
  __execf_entry operator__op_vertex_set_tmx;
  __execf_entry operator__op_vertex_convert;
  __execf_entry operator__op_vertex_set_property;
  __execf_entry operator__op_vertex_delete_property;
  __execf_entry operator__op_vertex_clear_properties;
  __execf_entry operator__op_vertex_set_vector;
  __execf_entry operator__op_vertex_delete_vector;
  __execf_entry operator__op_vertex_delete_outarcs;
  __execf_entry operator__op_vertex_delete_inarcs;
  __execf_entry operator__op_vertex_acquire;
  __execf_entry operator__op_vertex_release;
  __execf_entry operator__op_arc_connect;
  __execf_entry operator__op_arc_disconnect;
  __execf_entry operator__op_system_similarity;
  __execf_entry operator__op_system_attach;
  __execf_entry operator__op_system_detach;
  __execf_entry operator__op_system_clear_registry;
  __execf_entry operator__op_system_create_graph;
  __execf_entry operator__op_system_delete_graph;
  __execf_entry operator__op_system_send_comment;
  __execf_entry operator__op_system_send_raw_data;
  __execf_entry operator__op_graph_truncate;
  __execf_entry operator__op_graph_persist;
  __execf_entry operator__op_graph_state;
  __execf_entry operator__op_graph_readonly;
  __execf_entry operator__op_graph_readwrite;
  __execf_entry operator__op_graph_events;
  __execf_entry operator__op_graph_noevents;
  __execf_entry operator__op_graph_tic;
  __execf_entry operator__op_graph_event_exec;
  __execf_entry operator__op_vertices_acquire_wl;
  __execf_entry operator__op_vertices_release;
  __execf_entry operator__op_vertices_release_all;
  __execf_entry operator__op_enum_add_vxtype;
  __execf_entry operator__op_enum_add_rel;
  __execf_entry operator__op_enum_add_dim;
  __execf_entry operator__op_enum_add_key;
  __execf_entry operator__op_enum_delete_vxtype;
  __execf_entry operator__op_enum_delete_rel;
  __execf_entry operator__op_enum_delete_dim;
  __execf_entry operator__op_enum_delete_key;
  __execf_entry operator__op_enum_add_string;
  __execf_entry operator__op_enum_delete_string;
  __execf_entry operator__op_null;
  __execf_entry __end;
} __execf_map;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __is_execution_allowed( vgx_OperationParser_t *parser ) {
  bool allowed = false;
  vgx_Graph_t *graph = parser->op_graph;
  if( graph ) {
    bool force_exit = COMLIB_TASK__IsRequested_ForceExit( parser->TASK );
    GRAPH_LOCK( graph ) {
      if( _vgx_is_writable_CS( &graph->readonly ) && force_exit == false ) {
        allowed = true;
      }
      else {
        allowed = false;
      }
    } GRAPH_RELEASE;
  }
  return allowed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define BEGIN_EXECUTION_ATTEMPT( Parser, SuccessCondition ) \
do {                                                        \
  vgx_OperationParser_t *__p__ = Parser;                    \
  __p__->reason = VGX_ACCESS_REASON_NONE;            \
  while( !(SuccessCondition) && __is_access_reason_none_or_transient( __p__->reason ) && __is_execution_allowed( __p__ ) ) /* {
    code here
  } */

#define END_EXECUTION_ATTEMPT   \
} WHILE_ZERO




#endif
