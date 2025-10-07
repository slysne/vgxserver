/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_ard.h
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

#ifndef _VXDURABLE_OP_ARD_H
#define _VXDURABLE_OP_ARD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_arc_disconnect get__op_arc_disconnect( const vgx_Arc_t *arc, int64_t n_removed ) {
  int eventexec = arc->tail->descriptor.writer.threadid == iGraphEvent.ExecutorThreadId( arc->tail->graph );
  op_arc_disconnect opdata = {
    .op        = OPERATOR_ARC_DISCONNECT,
    .eventexec = eventexec,
    .n_removed = n_removed,
    .pred      = arc->head.predicator
  };
  idcpy( &opdata.headobid, __vertex_internalid( arc->head.vertex ) );
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_arc_disconnect( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_arc_disconnect, parser ) {

    QWORD qw;
    BYTE b;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // eventexec
    case 1:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->eventexec = (int)b;
      OPERATOR_CONTINUE( op );

    // n_removed
    case 2:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->n_removed = (int64_t)qw;
      OPERATOR_CONTINUE( op );

    // pred
    case 3:
      if( !hex_to_QWORD( parser->token.data, &op->pred.data ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // headobid
    case 4:
      op->headobid = strtoid( parser->token.data );
      if( idnone( &op->headobid ) ) {
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        vgx_Vertex_t *tail = parser->op_vertex_WL;
        vgx_Vertex_t *head = _vxgraph_vxtable__query_OPEN( parser->op_graph, NULL, &op->headobid, VERTEX_TYPE_ENUMERATION_WILDCARD );
        const char *s_tail = tail ? CALLABLE( tail )->IDString( tail ) : "???";
        const char *s_head = head ? CALLABLE( head )->IDString( head ) : "???";
        const char *s_rel = CStringValue( iEnumerator_OPEN.Relationship.Decode( parser->op_graph, op->pred.rel.enc ) );
        const char *s_mod = _vgx_modifier_as_string( op->pred.mod );
        if( _vgx_predicator_value_is_float( op->pred ) ) {
          PARSER_VERBOSE( parser, 0x001, "op_arc_disconnect: %s %08x [%s]-( %s, %s, %g )->[%s] eventexec=%d n_removed=%lld", op->op.name, op->op.code, s_tail, s_rel, s_mod, op->pred.val.real, s_head, op->eventexec, op->n_removed );
        }
        else {
          PARSER_VERBOSE( parser, 0x002, "op_arc_disconnect: %s %08x [%s]-( %s, %s, %d )->[%s] eventexec=%d n_removed=%lld", op->op.name, op->op.code, s_tail, s_rel, s_mod, op->pred.val.integer, s_head, op->eventexec, op->n_removed );
        }
      }
  #endif
      // TODO
      OPERATOR_COMPLETE( op );

    // ERROR
    default:
      OPERATOR_ERROR( op );
    }
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_arc_disconnect( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_arc_disconnect, parser ) {
    int result = 0;

    vgx_Vertex_t *head_WL = NULL;
    vgx_Graph_t *graph = parser->op_graph;

    XTRY {
      // Acquire head
      if( (head_WL = __acquire_arc_head( parser, &op->headobid )) == NULL ) {
        // If head does not exist arc doesn't exist either: no action
        if( __is_access_reason_noexist( parser->reason ) ) {
          XBREAK;
        }
        // Timeout or other acquisition error
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      //
      // DISCONNECT ( tail )-[ pred ]->( head )
      //
      vgx_Arc_t arc_WL = {
        .tail = vertex_WL,
        .head = {
          .vertex = head_WL,
          .predicator = op->pred
        }
      };
      
      if( _vxgraph_state__remove_arc_WL( graph, &arc_WL ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      result = 0;

    }
    XCATCH( errcode ) {
      result = -1;
    }
    XFINALLY {
      // Release head
      if( head_WL ) {
        if( _vxgraph_state__unlock_vertex_OPEN_LCK( graph, &head_WL, VGX_VERTEX_RECORD_ALL ) ) {
          PARSER_DEC_VERTEX_LOCKS( parser, 1 );
        }
      }
    }
    OPERATOR_RETURN( op, result );
  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_arc_disconnect( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_arc_disconnect, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
