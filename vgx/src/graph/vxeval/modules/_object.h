/*
###################################################
#
# File:   _object.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_OBJECT_H
#define _VGX_VXEVAL_MODULES_OBJECT_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_object_len( vgx_Evaluator_t *self );
static void __eval_object_strlen( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_object_len( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int64_t sz = 0;
  int64_t errpos;
  CString_attr attr;
  switch( px->type ) {
  case STACK_ITEM_TYPE_CSTRING:
    attr = CStringAttributes( px->CSTR__str );
    switch( attr & __CSTRING_ATTR_ARRAY_MASK ) {
      case CSTRING_ATTR_ARRAY_INT:
      case CSTRING_ATTR_ARRAY_FLOAT:
        sz = VGX_CSTRING_ARRAY_LENGTH( px->CSTR__str );
        break;
      case CSTRING_ATTR_ARRAY_MAP:
        sz = vgx_cstring_array_map_len( (QWORD*)CStringValue( px->CSTR__str ) );
        break;
      default:
      {
        int32_t rsz, rucsz;
        CALLABLE( px->CSTR__str )->GetTrueLength( px->CSTR__str, &rsz, &rucsz, &attr );
        if( (attr & CSTRING_ATTR_BYTEARRAY) || (attr & CSTRING_ATTR_BYTES) ) {
          sz = rsz;
        }
        else if( attr == CSTRING_ATTR_NONE ) {
          if( rucsz == 0 ) {
            rucsz = CALLABLE( px->CSTR__str )->Codepoints( px->CSTR__str );
          }
          sz = rucsz;
        }
      }
    }
    SET_INTEGER_PITEM_VALUE( px, sz );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    SET_INTEGER_PITEM_VALUE( px, CALLABLE( px->vector )->Length( px->vector ) );
    return;
  case STACK_ITEM_TYPE_BITVECTOR:
    SET_INTEGER_PITEM_VALUE( px, __popcnt64( px->bits ) );
    return;
  case STACK_ITEM_TYPE_VERTEX:
    SET_INTEGER_PITEM_VALUE( px, __get_vertex_property_count( px->vertex ) );
    return;
  case STACK_ITEM_TYPE_VERTEXID:
    if( px->vertexid ) {
      const vgx_VertexIdentifier_t *pid = px->vertexid;
      if( pid->CSTR__idstr ) {
        sz = CALLABLE( pid->CSTR__idstr )->Codepoints( pid->CSTR__idstr );
      }
      else {
        sz = COMLIB_length_utf8( (const BYTE*)pid->idprefix.data, -1, NULL, &errpos );
      }
      SET_INTEGER_PITEM_VALUE( px, sz );
      return;
    }
  default:
    break;
  }
  SET_INTEGER_PITEM_VALUE( px, 0 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_object_strlen( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  int64_t sz = 0;
  const vgx_VertexIdentifier_t *pid;
  if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    CString_attr attr;
    int32_t rsz, rucsz;
    CALLABLE( px->CSTR__str )->GetTrueLength( px->CSTR__str, &rsz, &rucsz, &attr );
    sz = rsz;
  }
  else if( px->type == STACK_ITEM_TYPE_VERTEXID && (pid = px->vertexid) != NULL ) {
    if( pid->CSTR__idstr ) {
      sz = CStringLength( pid->CSTR__idstr );
    }
    else {
      sz = strlen( pid->idprefix.data );
    }
  }
  SET_INTEGER_PITEM_VALUE( px, sz );
}




#endif
