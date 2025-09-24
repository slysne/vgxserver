/*######################################################################
 *#
 *# _op_syd.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_SYD_H
#define _VXDURABLE_OP_SYD_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_detach get__op_system_detach( vgx_Graph_t *SYSTEM ) {
  op_system_detach opdata = {
    .op                   = OPERATOR_SYSTEM_DETACH,
    .tms                  = _vgx_graph_milliseconds( SYSTEM ),
    .CSTR__origin_host    = iURI.NewFqdn(),
    .CSTR__origin_version = igraphinfo.Version( 1 ),
    .status               = 0
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_detach( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_detach, parser ) {

    DWORD dw;
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // tms
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->tms = (int64_t)qw;
      OPERATOR_CONTINUE( op );

    // CSTR__origin_host
    case 2:
      if( (op->CSTR__origin_host = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__origin_version
    case 3:
      if( (op->CSTR__origin_version = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // status
    case 4:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->status = (int)dw;

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_detach: %s %08x tms=%lld originHost=\"%s\" originVersion=\"%s\" status=%d",
                                                        op->op.name,
                                                           op->op.code,
                                                                    op->tms,
                                                                                      CStringValue( op->CSTR__origin_host ),
                                                                                                           CStringValue( op->CSTR__origin_version ),
                                                                                                                        op->status
      );
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
static int __execute_op_system_detach( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_detach, parser ) {
    // NOTE: graph_CS should be SYSTEM
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }

    // Now detached
    if( SYSTEM_CS->OP.system.consumer ) {
      vgx_TransactionalConsumerService_t *consumer_service = SYSTEM_CS->OP.system.consumer;
      consumer_service->provider_attached_SYS_CS = false;
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_detach( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_detach, parser ) {
    //op->op.data;
    //op->tms;
    iString.Discard( &op->CSTR__origin_host );
    iString.Discard( &op->CSTR__origin_version );
    //op->status;
  } END_OPERATOR_RETURN;
}




#endif
