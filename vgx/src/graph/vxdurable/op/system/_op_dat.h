/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_dat.h
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

#ifndef _VXDURABLE_OP_DAT_H
#define _VXDURABLE_OP_DAT_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_send_raw_data get__op_system_send_raw_data( vgx_Graph_t *SYSTEM, OperationProcessorAuxCommand cmd, objectid_t cmd_id, int64_t n_parts, int64_t part_id, CString_t *CSTR__datapart ) {

  op_system_send_raw_data opdata = {
    .op             = OPERATOR_SYSTEM_SEND_RAW_DATA,
    .n_parts        = n_parts,
    .part_id        = part_id,
    .CSTR__datapart = OwnOrCloneCString( CSTR__datapart, SYSTEM->ephemeral_string_allocator_context ),
    .sz_datapart    = CStringLength( CSTR__datapart ),
    .cmd            = cmd,
    ._rsv           = 0,
    .obid           = cmd_id
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_send_raw_data( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_system_send_raw_data, parser ) {

    QWORD qw;
    DWORD dw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // n_parts
    case 1:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->n_parts = qw;
      OPERATOR_CONTINUE( op );

    // part_id
    case 2:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->part_id = qw;
      OPERATOR_CONTINUE( op );

    // CSTR__datapart
    case 3:
      if( (op->CSTR__datapart = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // sz_datapart
    case 4:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->sz_datapart = qw;
      OPERATOR_CONTINUE( op );

    // cmd
    case 5:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->cmd = (OperationProcessorAuxCommand)dw;
      OPERATOR_CONTINUE( op );

    // _rsv
    case 6:
      OPERATOR_CONTINUE( op );

    // obid
    case 7:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op );
      }
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
static int __execute_op_system_send_raw_data( vgx_OperationParser_t *parser ) {
  // TODO: Implement correctly
  //       For now we assume single execution thread, and no overlapping messages
  static objectid_t cmd_id = {0};
  static vgx_StringList_t *cmd_data = NULL;

  BEGIN_GRAPH_OPERATOR( op_system_send_raw_data, parser ) {

    if( (op->cmd & __OPAUX_MASK__SYSTEM) ) {
      vgx_Graph_t *SYSTEM_CS = graph_CS;
      if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
        OPEXEC_REASON( parser, "Expected SYSTEM graph, got %s", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
        OPERATOR_ERROR( op );
      }

      char idbuf1[33];
      char idbuf2[33];

      // First part
      if( op->part_id == 0 ) {
        // Assert no other command in progress
        if( cmd_data != NULL || !idnone( &cmd_id ) ) {
          idunset( &cmd_id );
          iString.List.Discard( &cmd_data );
          OPEXEC_REASON( parser, "Unexpected part id: %lld", op->part_id );
          OPERATOR_ERROR( op );
        }

        // Create the command data container
        if( (cmd_data = iString.List.New( parser->string_allocator, op->n_parts )) == NULL ) {
          OPEXEC_REASON( parser, "Memory error" );
          OPERATOR_ERROR( op );
        }

        // Set current command id
        idcpy( &cmd_id, &op->obid );

        // Set datapart
        iString.List.SetItemSteal( cmd_data, 0, &op->CSTR__datapart );
      }
      // Next part
      else {
        if( !idmatch( &cmd_id, &op->obid ) ) {
          OPEXEC_REASON( parser, "Unexpected command id %s, expected %s", idtostr(idbuf1,&cmd_id), idtostr(idbuf2,&op->obid) );
          OPERATOR_ERROR( op );
        }
        if( cmd_data == NULL || op->part_id >= iString.List.Size( cmd_data ) ) {
          OPEXEC_REASON( parser, "Unexpected part id: %lld", op->part_id );
          OPERATOR_ERROR( op );
        }
        // Set datapart
        iString.List.SetItemSteal( cmd_data, op->part_id, &op->CSTR__datapart );
      }

      // We have all dataparts
      if( op->part_id == op->n_parts-1 ) {
        // Execute
        int ret = SYSTEM_CS->sysaux_cmd_callback( SYSTEM_CS, op->cmd, cmd_data, &parser->CSTR__error );
        if( (op->cmd & __OPAUX_MASK__FORWARD) ) {
          if( iOperation.System_SYS_CS.ForwardAuxCommand( SYSTEM_CS, op->cmd, cmd_id, cmd_data ) < 0 ) {
            OPEXEC_REASON( parser, "Failed to forward command id %s", idtostr(idbuf1, &cmd_id) );
          }
        }
        // Clean up
        iString.List.Discard( &cmd_data );
        idunset( &cmd_id );
        // Check
        if( ret < 0 ) {
          OPEXEC_REASON( parser, "Command error" );
          OPERATOR_ERROR( op );
        }
        else if( ret == 0 ) {

        }
      }
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_send_raw_data( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_send_raw_data, parser ) {
    //op->op.data;
    //op->n_parts;
    //op->part_id;
    iString.Discard( &op->CSTR__datapart );
    //op->sz_datapart;
    //op->obid;
  } END_OPERATOR_RETURN;
}



#endif
