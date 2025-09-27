/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_grn.h
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

#ifndef _VXDURABLE_OP_GRN_H
#define _VXDURABLE_OP_GRN_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_system_create_graph get__op_system_create_graph( vgx_Graph_t *SYSTEM, int vertex_block_order, uint32_t graph_t0, int64_t start_opcount, const objectid_t *obid, const char *name, const char *path ) {
  op_system_create_graph opdata = {
    .op                 = OPERATOR_SYSTEM_CREATE_GRAPH,
    .vertex_block_order = vertex_block_order,
    .graph_t0           = graph_t0,
    .start_opcount      = start_opcount,
    .obid               = *obid,
    .CSTR__path         = NewEphemeralCString( SYSTEM, path ),
    .CSTR__name         = NewEphemeralCString( SYSTEM, name )
  };
  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_system_create_graph( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_system_create_graph, parser ) {

    DWORD dw;
    QWORD qw;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // vertex_block_order
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->vertex_block_order = (int)dw;
      OPERATOR_CONTINUE( op );

    // graph_t0
    case 2:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->graph_t0 = (uint32_t)dw;
      OPERATOR_CONTINUE( op );

    // start_opcount
    case 3:
      if( !hex_to_QWORD( parser->token.data, &qw ) ) {
        OPERATOR_ERROR( op );
      }
      op->start_opcount = (int64_t)qw;
      OPERATOR_CONTINUE( op );

    // obid
    case 4:
      op->obid = strtoid( parser->token.data );
      if( idnone( &op->obid ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__path
    case 5:
      if( (op->CSTR__path = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // CSTR__name
    case 6:
      if( (op->CSTR__name = icstringobject.Deserialize( parser->token.data, parser->string_allocator )) == NULL ) {
        PARSER_SET_ERROR( parser, "string deserializer error" );
        OPERATOR_ERROR( op );
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      PARSER_VERBOSE( parser, 0x001, "op_system_create_graph: %s %08x block=%d t0=%u opcnt=%lld obid=%016llx%016llx path=\"%s\" name=\"%s\"",
                                                op->op.name,
                                                   op->op.code,
                                                             op->vertex_block_order,
                                                                   op->graph_t0,
                                                                            op->start_opcount,
                                                                                       op->obid.H, op->obid.L,
                                                                                                              CStringValue( op->CSTR__path ),
                                                                                                                          CStringValue( op->CSTR__name ) );
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
static int __execute_op_system_create_graph( vgx_OperationParser_t *parser ) {
  BEGIN_GRAPH_OPERATOR( op_system_create_graph, parser ) {
    // NOTE: graph_CS should be SYSTEM
    vgx_Graph_t *SYSTEM_CS = graph_CS;
    if( !iSystem.IsSystemGraph( SYSTEM_CS ) ) {
      OPEXEC_REASON( parser, "Expected SYSTEM graph, got %s", CALLABLE( SYSTEM_CS )->FullPath( SYSTEM_CS ) );
      OPERATOR_ERROR( op );
    }

    const char *path = op->CSTR__path ? CStringValue( op->CSTR__path ) : "";
    const char *name = op->CSTR__name ? CStringValue( op->CSTR__name ) : "";

    // System graph not allowed to be created here
    if( CharsEqualsConst( path, VGX_SYSTEM_GRAPH_PATH )
        &&
        CharsEqualsConst( name, VGX_SYSTEM_GRAPH_NAME )
      )
    {
      OPEXEC_REASON( parser, "Explicit creation of graph '%s/%s' not allowed", path, name );
      OPERATOR_ERROR( op );
    }

    vgx_Graph_t *opgraph = NULL;
    int err = 0;

    // Temporarily allow readonly since graph constructor requires it
    vgx_readonly_state_t *ro_SYS = &SYSTEM_CS->readonly;
    int pre_recursion = _vgx_get_disallow_readonly_recursion_CS( ro_SYS );
    _vgx_set_readonly_allowed_CS( ro_SYS );

    GRAPH_SUSPEND_LOCK( SYSTEM_CS ) {
      XTRY {
        // Graph does not exist: Create and register new graph
        if( igraphfactory.HasGraph( op->CSTR__path, op->CSTR__name ) == false ) {

          // Configure graph constructor args from operation data
          vgx_Graph_constructor_args_t opgraph_args = {
            .CSTR__graph_path     = op->CSTR__path,
            .CSTR__graph_name     = op->CSTR__name,
            .vertex_block_order   = op->vertex_block_order,
            .graph_t0             = op->graph_t0,
            .start_opcount        = op->start_opcount,
            .simconfig            = NULL,   // TODO: Use config from simconfig operator
            .with_event_processor = false,  // Graph fully operated via remote stream, NO LOCAL EVENT PROCESSOR
            .idle_event_processor = false,
            .force_readonly       = false, 
            .force_writable       = true,   // Ignore any readonly state that may have been previously persisted
            .local_only           = false   // Allow chaining
          };

          // Create graph
          if( (opgraph = COMLIB_OBJECT_NEW( vgx_Graph_t, NULL, &opgraph_args )) == NULL ) {
            OPEXEC_REASON( parser, "Failed to create graph '%s/%s'", path, name );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
          }

          // Add new graph to registry
          if( igraphfactory.RegisterGraph( opgraph, NULL ) < 0 ) {
            OPEXEC_REASON( parser, "Failed to register graph '%s/%s'", path, name );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
          }

          // Make sure we can open (and own) the graph we just created
          if( (opgraph = igraphfactory.OpenGraph( op->CSTR__path, op->CSTR__name, opgraph_args.local_only, NULL, 1000 )) == NULL ) {
            OPEXEC_REASON( parser, "Failed to open registered graph '%s/%s'", path, name );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
          }

          // Get the obid of new graph
          char obid_local[33];
          objectid_t *opgraph_obid = COMLIB_OBJECT_GETID( opgraph );
          idtostr( obid_local, opgraph_obid );

          // We successfully opened (and owned) the new graph by its path and name.
          // Now close graph.
          uint32_t owner = 0;
          if( igraphfactory.CloseGraph( &opgraph, &owner ) != 0 ) {
            OPEXEC_REASON( parser, "Failed to close registered graph '%s/%s', owned by thread %u", path, name, owner );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
          }

          // Verify obid match
          if( !idmatch( opgraph_obid, &op->obid ) ) {
            char obid_expected[33];
            idtostr( obid_expected, &op->obid );
            OPEXEC_REASON( parser, "Graph object identifier mismatch for graph '%s/%s', expected=%s got=%s", path, name, obid_expected, obid_local );
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
          }
        }

        // Graph exists at this point
        // Retrieve the graph object by obid without owning
        if( (opgraph = iSystem.GetGraph( &op->obid )) == NULL ) {
          OPEXEC_REASON( parser, "Failed to retrieve graph object %s '%s/%s'", &op->obid, path, name );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }

        // Graph creation successful, either by constructing and registering new graph object
        // or by opening existing graph object
        //
      }
      XCATCH( errcode ) {
        err = -1;
      }
      XFINALLY {
      }
    } GRAPH_RESUME_LOCK;

    // Restore disallow readonly
    int post_recursion = _vgx_get_disallow_readonly_recursion_CS( ro_SYS );
    _vgx_set_readonly_disallowed_CS( ro_SYS, pre_recursion + post_recursion );

    if( err < 0 ) {
      OPERATOR_ERROR( op );
    }

  } END_GRAPH_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_system_create_graph( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_system_create_graph, parser ) {
    //op->op.data;
    //op->vertex_block_order;
    //op->graph_t0;
    //op->start_opcount;
    //op->obid;
    iString.Discard( &op->CSTR__path );
    iString.Discard( &op->CSTR__name );
  } END_OPERATOR_RETURN;
}



#endif
