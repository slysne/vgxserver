/*######################################################################
 *#
 *# _op_arc.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_ARC_H
#define _VXDURABLE_OP_ARC_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_arc_connect get__op_arc_connect( const vgx_Arc_t *arc ) {
  op_arc_connect opdata = {
    .op   = OPERATOR_ARC_CONNECT,
    .pred = arc->head.predicator
  };
  idcpy( &opdata.headobid, __vertex_internalid( arc->head.vertex ) );
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_arc_connect( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_arc_connect, parser ) {

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // pred
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->pred.data ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // headobid
    case 2:
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
          PARSER_VERBOSE( parser, 0x001, "op_arc_connect: %s %08x [%s]-( %s, %s, %g )->[%s]", op->op.name, op->op.code, s_tail, s_rel, s_mod, op->pred.val.real, s_head );
        }
        else {
          PARSER_VERBOSE( parser, 0x002, "op_arc_connect: %s %08x [%s]-( %s, %s, %d )->[%s]", op->op.name, op->op.code, s_tail, s_rel, s_mod, op->pred.val.integer, s_head );
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
static int __execute_op_arc_connect( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_arc_connect, parser ) {
    int result = 0;
    vgx_Vertex_t *head_WL = NULL;
    vgx_Graph_t *graph = parser->op_graph;

    XTRY {
      // Acquire head
      if( (head_WL = __acquire_arc_head( parser, &op->headobid )) == NULL ) {
        // Head does not exist
        if( __is_access_reason_noexist( parser->reason ) ) {
          const char *tail_id = VertexIdPrefix( vertex_WL );
          char head_id[33];
          if( warn_ignore_head_count++ < MAX_IGNORE_WARNINGS ) {
            OPEXEC_WARNING( parser, "(%s)-[%016llX]->(%s) ignored, head vertex no longer exists", tail_id, op->pred.data, idtostr( head_id, &op->headobid ) );
          }
          XBREAK; // don't throw error
        }
        // Timeout or other acquisition error
        else {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
        }
      }

      //
      // CONNECT ( tail )-[ pred ]->( head )
      //
      vgx_Arc_t arc_WL = {
        .tail = vertex_WL,
        .head = {
          .vertex = head_WL,
          .predicator = op->pred
        }
      };

      // We need to set the eph type for the arc creation to recognize the forward only arc
      // TODO: Remove the need to set this
      if( (op->pred.data & __VGX_PREDICATOR_FWD_MASK) ) {
        arc_WL.head.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_FWDARCONLY;
      }

      result = _vxgraph_state__create_arc_WL( graph, &arc_WL, -1, NULL, &parser->reason, &parser->CSTR__error );
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
static int __retire_op_arc_connect( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_arc_connect, parser ) {
  } END_OPERATOR_RETURN;
}




#endif
