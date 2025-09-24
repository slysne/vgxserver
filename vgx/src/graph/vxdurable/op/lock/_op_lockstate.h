/*######################################################################
 *#
 *# _op_lockstate.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_LOCKSTATE_H
#define _VXDURABLE_OP_LOCKSTATE_H



static int __parse_op_vertices_lockstate( vgx_OperationParser_t *parser );
static int __retire_op_vertices_lockstate( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertices_lockstate( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {

    switch( parser->field++ ) {

    DWORD dw;
    int idx;

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // count
    case 1:
      if( !hex_to_DWORD( parser->token.data, &dw ) ) {
        OPERATOR_ERROR( op );
      }
      op->count = (int)dw;
      if( op->count > 0 ) {
        if( (op->obid_list = calloc( op->count, sizeof( objectid_t ) )) == NULL ) {
          OPERATOR_ERROR( op );
        }
      }
      OPERATOR_CONTINUE( op );

    // obid
    default:
      if( op->count > 0 ) {
        idx = parser->field - 3;
        if( idx >= op->count ) {
          OPERATOR_ERROR( op );
        }
        op->obid_list[ idx ] = strtoid( parser->token.data );
        if( idnone( &op->obid_list[ idx ] ) ) {
          OPERATOR_ERROR( op );
        }
        if( idx < op->count - 1 ) {
          OPERATOR_CONTINUE( op );
        }
      }
      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        char *buf = NULL;
        if( op->count > 0 ) {
          size_t sz = (sizeof( vgx_VertexIdentifierPrefix_t ) + 2) * op->count;
          if( (buf = malloc( sz + 1 )) != NULL ) {
            buf[0] = '\0';
            buf[sz] = '\0';
            char *end = buf + sz;
            char *p = buf;
            const char *s;
            const char *err = "<unknown>";
            for( int i=0; i<op->count; i++ ) {
              vgx_Vertex_t *V = _vxgraph_vxtable__query_OPEN( parser->op_graph, NULL, &op->obid_list[i], VERTEX_TYPE_ENUMERATION_WILDCARD );
              if( V ) {
                s = CALLABLE( V )->IDPrefix( V );
              }
              else {
                s = err;
              }
              while( p < end && (*p = *s++) != '\0' ) {
                ++p;
              }
              *p++ = ',';
              *p++ = ' ';
              *p = '\0'; 
            }
            p -= 2;
            *p = '\0';
          }
        }

        const char *name_acquire = "op_vertices_acquire_wl";
        const char *name_release = "op_vertices_release";
        const char *name_unknown = "unknown";
        const char *name;
        switch( op->op.code ) {
        case OPCODE_VERTICES_ACQUIRE_WL:
          name = name_acquire;
          break;
        case OPCODE_VERTICES_RELEASE:
          name = name_release;
          break;
        default:
          name = name_unknown;
        }

        if( buf ) {
          PARSER_VERBOSE( parser, 0x001, "%s: %s %08x list=\"%s\"", name, op->op.name, op->op.code, buf );
          free( buf );
        }
        else {
          PARSER_VERBOSE( parser, 0x002, "%s: %s %08x <!!!ERROR!!!>", name, op->op.name, op->op.code );
        }
      }
  #endif

      // TODO
      OPERATOR_COMPLETE( op );

    }
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertices_lockstate( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertices_lockstate, parser ) {
    if( op->obid_list ) {
      free( op->obid_list );
      op->obid_list = NULL;
    }
  } END_OPERATOR_RETURN;
}




#endif
