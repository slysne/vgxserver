/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_sya.h
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

#ifndef _VXDURABLE_OP_SYA_H
#define _VXDURABLE_OP_SYA_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_attach get__op_system_attach( vgx_Graph_t *SYSTEM ) {
  op_system_attach opdata = {
    .op                   = OPERATOR_SYSTEM_ATTACH,
    .tms                  = _vgx_graph_milliseconds( SYSTEM ),
    .CSTR__via_uri        = CStringNew( "" ),
    .CSTR__origin_host    = iURI.NewFqdn(),
    .CSTR__origin_version = igraphinfo.Version( 1 ),
    .status               = 1
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_attach( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_attach, parser ) {

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

    // CSTR__via_uri
    case 2:
      if( (op->CSTR__via_uri = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__origin_host
    case 3:
      if( (op->CSTR__origin_host = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__origin_version
    case 4:
      if( (op->CSTR__origin_version = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // status
    case 5:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->status = (int)dw;

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_attach: %s %08x tms=%lld viaURI=\"%s\" originHost=\"%s\" originVersion=\"%s\" status=%d",
                                                        op->op.name,
                                                           op->op.code,
                                                                    op->tms,
                                                                                 CStringValue( op->CSTR__via_uri ),
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
static int __execute_op_system_attach( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_attach, parser ) {
    // NOTE: graph_CS should be SYSTEM
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }

    // Now attached
    if( SYSTEM_CS->OP.system.consumer ) {
      vgx_TransactionalConsumerService_t *consumer_service = SYSTEM_CS->OP.system.consumer;
      consumer_service->provider_attached_SYS_CS = true;
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_attach( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_attach, parser ) {
    //op->op.data;
    //op->tms;
    iString.Discard( &op->CSTR__via_uri );
    iString.Discard( &op->CSTR__origin_host );
    iString.Discard( &op->CSTR__origin_version );
    //op->status;
  } END_OPERATOR_RETURN;
}





#endif
