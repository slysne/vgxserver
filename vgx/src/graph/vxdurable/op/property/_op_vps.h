/*######################################################################
 *#
 *# _op_vps.h
 *#
 *#
 *######################################################################
 */


#ifndef _VXDURABLE_OP_VPS_H
#define _VXDURABLE_OP_VPS_H



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static op_vertex_set_property get__op_vertex_set_property( vgx_Graph_t *graph, const vgx_VertexProperty_t *prop) {
  op_vertex_set_property opdata = {
    .op     = OPERATOR_VERTEX_SET_PROPERTY,
    .key    = prop->keyhash,
    .vtype  = prop->val.type
  };

  switch( prop->val.type ) {
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
    opdata.stringid = _vxenum_propval__obid( prop->val.data.simple.CSTR__string );
    opdata.CSTR__value = NULL;
    break;
  case VGX_VALUE_TYPE_CSTRING:
    opdata.dataL = 0;
    opdata.dataH = 0;
    opdata.CSTR__value = OwnOrCloneCString( prop->val.data.simple.CSTR__string, graph->ephemeral_string_allocator_context );
    break;
  default:
    opdata.dataL = prop->val.data.bits;
    opdata.dataH = 0;
    opdata.CSTR__value = NULL;
  }

  return opdata;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __parse_op_vertex_set_property( vgx_OperationParser_t *parser ) {

  BEGIN_OPERATOR( op_vertex_set_property, parser ) {

    BYTE b;

    switch( parser->field++ ) {

    // opcode
    case 0:
      OPERATOR_CONTINUE( op );

    // key
    case 1:
      if( !hex_to_QWORD( parser->token.data, &op->key ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // vtype
    case 2:
      if( !hex_to_BYTE( parser->token.data, &b ) ) {
        OPERATOR_ERROR( op );
      }
      op->vtype = b;
      OPERATOR_CONTINUE( op );

    // dataH
    case 3:
      if( !hex_to_QWORD( parser->token.data, &op->dataH ) ) {
        OPERATOR_ERROR( op );
      }
      OPERATOR_CONTINUE( op );

    // dataL
    case 4:
      if( !hex_to_QWORD( parser->token.data, &op->dataL ) ) {
        OPERATOR_ERROR( op );
      }
      if( op->vtype == VGX_VALUE_TYPE_CSTRING ) {
        OPERATOR_CONTINUE( op );
      }

    // CSTR__value
    case 5:
      if( op->vtype == VGX_VALUE_TYPE_CSTRING ) {
        if( (op->CSTR__value = icstringobject.Deserialize( parser->token.data, parser->property_allocator_ref )) == NULL ) {
          PARSER_SET_ERROR( parser, "string deserializer error" );
          OPERATOR_ERROR( op );
        }
      }
      else {
        op->CSTR__value = NULL;
      }

      // *** EXECUTE ***
  #ifdef PARSE_DUMP
      if( parser->op_graph ) {
        const char *s_key = CStringValue( iEnumerator_OPEN.Property.Key.Decode( parser->op_graph, op->key ) );
        const char *s_val = NULL;
        objectid_t obid;
        vgx_value_t value = {
          .data.bits = op->dataL
        };
        switch( op->vtype ) {
        case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
          s_val = CStringValue( iEnumerator_OPEN.Property.Value.Get( parser->op_graph, idset( &obid, op->dataH, op->dataL ) ) );
          PARSER_VERBOSE( parser, 0x001, "op_vertex_set_property: %s %08x %s:%s", op->op.name, op->op.code, s_key, s_val );
          break;
        case VGX_VALUE_TYPE_CSTRING:
          s_val = CStringValue( op->CSTR__value );
          PARSER_VERBOSE( parser, 0x002, "op_vertex_set_property: %s %08x %s:%s", op->op.name, op->op.code, s_key, s_val );
          break;
        case VGX_VALUE_TYPE_INTEGER:
          PARSER_VERBOSE( parser, 0x003, "op_vertex_set_property: %s %08x %s:%lld", op->op.name, op->op.code, s_key, value.data.simple.integer );
          break;
        case VGX_VALUE_TYPE_REAL:
          PARSER_VERBOSE( parser, 0x004, "op_vertex_set_property: %s %08x %s:%g", op->op.name, op->op.code, s_key, value.data.simple.real );
          break;
        default:
          PARSER_VERBOSE( parser, 0x005, "op_vertex_set_property: %s %08x %s:%llu", op->op.name, op->op.code, s_key, value.data.bits );
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
static int __execute_op_vertex_set_property( vgx_OperationParser_t *parser ) {
  BEGIN_VERTEX_OPERATOR( op_vertex_set_property, parser ) {
    vgx_Graph_t *cs_graph = __operation_parser_lock_graph( parser );
    if( cs_graph == NULL ) {
      OPERATOR_ERROR( NULL );
    }

    // Special property
    if( IsPropertyKeyHashVertexEnum( op->key ) ) {
      if( Vertex_SetEnum( vertex_WL, (int32_t)op->dataL ) < 0 ) {
        OPERATOR_IGNORE( op );
      }
    }
    // Normal property
    else {
      vgx_VertexProperty_t vertex_property = {
        .val.type = op->vtype
      };

      // Retrieve key string from enumerator
      // If key does not exist we ignore the operator. Property most likely deleted later within the same operation before commit.
      if( (vertex_property.key = iEnumerator_CS.Property.Key.Get( cs_graph, op->key )) == NULL ) {
        OPERATOR_IGNORE( op );
      }

      // Value type
      switch( vertex_property.val.type ) {
      case VGX_VALUE_TYPE_BOOLEAN:
      case VGX_VALUE_TYPE_INTEGER:
      case VGX_VALUE_TYPE_REAL:
        vertex_property.val.data.bits = op->dataL;
        break;
      case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
        // Retrieve value string from enumerator
        // If the value does not exist we ignore the operator. Property most likely overwritten later within the same operation before commit.
        if( (vertex_property.val.data.simple.CSTR__string = iEnumerator_CS.Property.Value.Get( cs_graph, &op->stringid )) == NULL ) {
          OPERATOR_IGNORE( op );
        }
        break;
      case VGX_VALUE_TYPE_CSTRING:
        if( (vertex_property.val.data.simple.CSTR__string = op->CSTR__value) == NULL ) {
          OPERATOR_IGNORE( op );
        }
        break;
      default:
        // error: invalid value type
        OPEXEC_REASON( parser, "invalid value type %02X", op->vtype );
        vertex_property.val.type = VGX_VALUE_TYPE_NULL;
      }

      // Set property
      if( vertex_property.val.type != VGX_VALUE_TYPE_NULL ) {
        if( VertexSetProperty( vertex_WL, vertex_property ) < 0 ) {
          OPERATOR_ERROR( op );
        }
      }
    }

  } END_VERTEX_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_vertex_set_property( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_vertex_set_property, parser ) {
    iString.Discard( &op->CSTR__value );
    __operation_parser_release_graph( parser );
  } END_OPERATOR_RETURN;
}



#endif
