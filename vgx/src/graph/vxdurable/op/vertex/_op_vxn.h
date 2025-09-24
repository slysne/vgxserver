/*######################################################################
 *#
 *# _op_vxn.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VXN_H
#define _VXDURABLE_OP_VXN_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_new get__op_vertex_new( vgx_Graph_t *graph, vgx_VertexTypeEnumeration_t vxtype, const objectid_t *obid, uint32_t tmc, vgx_Rank_t rank, const CString_t *CSTR__id ) {
  object_allocator_context_t *ephemeral = graph->ephemeral_string_allocator_context;

  vgx_vertex_expiration_t NEVER = {
    .vertex_ts = TIME_EXPIRES_NEVER,
    .arc_ts    = TIME_EXPIRES_NEVER
  };
  
  op_vertex_new opdata = {
    .op       = OPERATOR_VERTEX_NEW,
    .vxtype   = vxtype,
    .obid     = *obid,
    .TMC      = tmc,
    .TMX      = NEVER,
    .rank     = rank,
    .CSTR__id = OwnOrCloneCString( CSTR__id, ephemeral )
  };

  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_new( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_vertex_new, parser ) {

    BYTE b;
    DWORD dw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // obid
    case 1:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // vxtype
    case 2:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->vxtype = b;
      OPERATOR_CONTINUE( op );

    // TMC
    case 3:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->TMC = (uint32_t)dw;
      OPERATOR_CONTINUE( op );

    // TMX.vertex_ts
    case 4:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->TMX.vertex_ts = (uint32_t)dw;
      OPERATOR_CONTINUE( op );

    // TMX.arc_ts
    case 5:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->TMX.arc_ts = (uint32_t)dw;
      OPERATOR_CONTINUE( op );

    // rank
    case 6:
      if( !hex_to_QWORD( parser->token.data, &op->rank.bits ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__id
    case 7:
      // Lock the graph for the remainder of this operator if graph is set
      if( parser->op_graph ) {
        if( __operation_parser_lock_graph( parser ) == NULL ) {
          OPERATOR_ERROR( NULL );
        }
      }

      if( (op->CSTR__id = icstringobject.Deserialize( parser->token.data, parser->property_allocator_ref )) == NULL ) {
        PARSER_SET_ERROR( parser, "identifier string deserializer error" );
        OPERATOR_ERROR( op );
      }
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
          char buf[33];
          PARSER_VERBOSE( parser,
                          0x001,
                          "op_vertex_new: %s %08x type=%s obid=%s tmc=%u tmx=(%u,%u) rank=(%g,%g) id=\"%s\"",
                                          op->op.name,
                                             op->op.code, 
                                                       CStringValue( iEnumerator_CS.VertexType.Decode( cs_graph, op->vxtype ) ),
                                                               idtostr( buf, &op->obid ),
                                                                      op->TMC,
                                                                              op->TMX.vertex_ts,
                                                                                 op->TMX.arc_ts,
                                                                                           op->rank.c1,
                                                                                               op->rank.c0,
                                                                                                       CStringValue( op->CSTR__id ) );
      }
  #endif
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
static int __execute_op_vertex_new( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_vertex_new, parser ) {
    vgx_Vertex_t *vertex = NULL;
    vgx_Vertex_t *vertex_WL = NULL;

    int created = 0;

    BEGIN_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS( graph_CS ) {
      // NORMAL CASE - VERTEX DOES NOT EXIST
      if( (vertex = CALLABLE( graph_CS )->advanced->GetVertex_CS( graph_CS, NULL, &op->obid )) == NULL ) {

        // Ensure default vertex type mapping
        if( __vertex_type_enumeration_default( op->vxtype ) && !iEnumerator_CS.VertexType.Exists( graph_CS, NULL ) ) {
          iEnumerator_CS.VertexType.Encode( graph_CS, NULL, NULL, true );
        }

        // Create new vertex and release the new instance immediately (no tracking!)
        if( (vertex_WL = _vxdurable_commit__new_vertex_CS( graph_CS, op->CSTR__id, &op->obid, op->vxtype, op->rank, VERTEX_STATE_CONTEXT_MAN_REAL, &parser->CSTR__error )) != NULL ) {
          
          // TMX should be set to NEVER

          // Unlock vertex (no tracking!)
          if( _vxgraph_state__unlock_vertex_CS_LCK( graph_CS, &vertex_WL, VGX_VERTEX_RECORD_NONE ) ) {
            // SUCCESS
            created = 1;
          }
          // Failed to unlock
          else {
            PARSER_FORMAT_ERROR_REASON( parser, VGX_ACCESS_REASON_ERROR, "Failed to unlock new vertex: \"%s\"", CStringValue( op->CSTR__id ) );
            created = -1;
          }
        }
        // Failed to create new vertex
        else {
          PARSER_SET_REASON( parser, VGX_ACCESS_REASON_NOCREATE );
          created = -1;
        }
      }
    } END_SYNCHRONIZE_VERTEX_CONSTRUCTOR_CS;

    // VERTEX CREATED
    if( created > 0 ) {
      OPERATOR_SUCCESS( op );
    }
    // ERROR
    else if( created < 0 ) {
      OPERATOR_ERROR( op );
    }
    // VERTEX ALREADY EXISTS
    else {
      int result;
      // Open existing vertex to verify type and ensure REAL
      //
      // TODO: First open readonly since that is all we need to
      //       verify type. Then if type differs, escalate to WL
      //       and change type to the new type.
      //
      vgx_ExecutionTimingBudget_t timing_budget = _vgx_get_graph_execution_timing_budget( graph_CS, LONG_TIMEOUT );
      if( (vertex_WL = _vxgraph_state__lock_vertex_writable_CS( graph_CS, vertex, &timing_budget, VGX_VERTEX_RECORD_ALL )) != NULL ) {
        PARSER_INC_VERTEX_LOCKS( parser, 1 );
        int existing_type = __vertex_get_type( vertex_WL );
        // Type match
        if( existing_type == op->vxtype ) {
          // Ensure REAL
          _vxgraph_state__convert_WL( graph_CS, vertex_WL, VERTEX_STATE_CONTEXT_MAN_REAL, VGX_VERTEX_RECORD_OPERATION );
          // SUCCESS
          result = 0;
        }
        // Type mismatch
        else {
          PARSER_FORMAT_ERROR_REASON( parser, VGX_ACCESS_REASON_TYPEMISMATCH, "Type mismatch (%x != %x) for existing vertex: \"%s\"", op->vxtype, existing_type, CStringValue( op->CSTR__id ) );
          result = -1;
        }
      }
      // Failed to open and verify existing vertex
      else {
        parser->reason = timing_budget.reason;
        result = -1;
      }

      if( vertex_WL ) {
        if( _vxgraph_state__unlock_vertex_CS_LCK( graph_CS, &vertex_WL, VGX_VERTEX_RECORD_ALL ) ) {
          PARSER_DEC_VERTEX_LOCKS( parser, 1 );
        }
        else {
          PARSER_FORMAT_ERROR_REASON( parser, VGX_ACCESS_REASON_ERROR, "Failed to unlock new vertex: \"%s\"", CStringValue( op->CSTR__id ) );
          result = -1;
        }
      }

      OPERATOR_RETURN( op, result );
    }
  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_new( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_new, parser ) {
    iString.Discard( &op->CSTR__id );
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}





#endif
