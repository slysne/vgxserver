/*
###################################################
#
# File:   _vertex.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_VERTEX_H
#define _VGX_VXEVAL_MODULES_VERTEX_H



/*******************************************************************//**
 *
 ***********************************************************************
 */
// Push Tail
static void __stack_push_TAIL( vgx_Evaluator_t *self );
static void __stack_push_TAIL_prop( vgx_Evaluator_t *self );
static void __stack_push_TAIL_prop_dflt( vgx_Evaluator_t *self );
static void __stack_push_TAIL_propif_dflt( vgx_Evaluator_t *self );
static void __stack_push_TAIL_c1( vgx_Evaluator_t *self );
static void __stack_push_TAIL_c0( vgx_Evaluator_t *self );
static void __stack_push_TAIL_virtual( vgx_Evaluator_t *self );
static void __stack_push_TAIL_id( vgx_Evaluator_t *self );
static void __stack_push_TAIL_obid( vgx_Evaluator_t *self );
static void __stack_push_TAIL_typ( vgx_Evaluator_t *self );
static void __stack_push_TAIL_deg( vgx_Evaluator_t *self );
static void __stack_push_TAIL_ideg( vgx_Evaluator_t *self );
static void __stack_push_TAIL_odeg( vgx_Evaluator_t *self );
static void __stack_push_TAIL_tmc( vgx_Evaluator_t *self );
static void __stack_push_TAIL_tmm( vgx_Evaluator_t *self );
static void __stack_push_TAIL_tmx( vgx_Evaluator_t *self );
static void __stack_push_TAIL_vec( vgx_Evaluator_t *self );
static void __stack_push_TAIL_op( vgx_Evaluator_t *self );
static void __stack_push_TAIL_refc( vgx_Evaluator_t *self );
static void __stack_push_TAIL_bidx( vgx_Evaluator_t *self );
static void __stack_push_TAIL_oidx( vgx_Evaluator_t *self );
static void __stack_push_TAIL_handle( vgx_Evaluator_t *self );
static void __stack_push_TAIL_enum( vgx_Evaluator_t *self );
static void __stack_push_TAIL_locked( vgx_Evaluator_t *self );
static void __stack_push_TAIL_addr( vgx_Evaluator_t *self );
static void __stack_push_TAIL_index( vgx_Evaluator_t *self );
static void __stack_push_TAIL_bitindex( vgx_Evaluator_t *self );
static void __stack_push_TAIL_bitvector( vgx_Evaluator_t *self );

// Push This
static void __stack_push_VERTEX( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_prop( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_prop_dflt( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_propif_dflt( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_c1( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_c0( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_virtual( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_id( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_obid( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_typ( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_deg( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_ideg( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_odeg( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_tmc( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_tmm( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_tmx( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_vec( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_op( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_refc( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_bidx( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_oidx( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_handle( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_enum( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_locked( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_addr( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_index( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_bitindex( vgx_Evaluator_t *self );
static void __stack_push_VERTEX_bitvector( vgx_Evaluator_t *self );

// Push Head
static void __stack_push_HEAD( vgx_Evaluator_t *self );
static void __stack_push_HEAD_prop( vgx_Evaluator_t *self );
static void __stack_push_HEAD_prop_dflt( vgx_Evaluator_t *self );
static void __stack_push_HEAD_propif_dflt( vgx_Evaluator_t *self );
static void __stack_push_HEAD_c1( vgx_Evaluator_t *self );
static void __stack_push_HEAD_c0( vgx_Evaluator_t *self );
static void __stack_push_HEAD_virtual( vgx_Evaluator_t *self );
static void __stack_push_HEAD_id( vgx_Evaluator_t *self );
static void __stack_push_HEAD_obid( vgx_Evaluator_t *self );
static void __stack_push_HEAD_typ( vgx_Evaluator_t *self );
static void __stack_push_HEAD_deg( vgx_Evaluator_t *self );
static void __stack_push_HEAD_ideg( vgx_Evaluator_t *self );
static void __stack_push_HEAD_odeg( vgx_Evaluator_t *self );
static void __stack_push_HEAD_tmc( vgx_Evaluator_t *self );
static void __stack_push_HEAD_tmm( vgx_Evaluator_t *self );
static void __stack_push_HEAD_tmx( vgx_Evaluator_t *self );
static void __stack_push_HEAD_vec( vgx_Evaluator_t *self );
static void __stack_push_HEAD_op( vgx_Evaluator_t *self );
static void __stack_push_HEAD_refc( vgx_Evaluator_t *self );
static void __stack_push_HEAD_bidx( vgx_Evaluator_t *self );
static void __stack_push_HEAD_oidx( vgx_Evaluator_t *self );
static void __stack_push_HEAD_handle( vgx_Evaluator_t *self );
static void __stack_push_HEAD_enum( vgx_Evaluator_t *self );
static void __stack_push_HEAD_locked( vgx_Evaluator_t *self );
static void __stack_push_HEAD_addr( vgx_Evaluator_t *self );
static void __stack_push_HEAD_index( vgx_Evaluator_t *self );
static void __stack_push_HEAD_bitindex( vgx_Evaluator_t *self );
static void __stack_push_HEAD_bitvector( vgx_Evaluator_t *self );

// Vertex Type Enumeration
static void __eval_string_typeenc( vgx_Evaluator_t *self );
static void __eval_string_typedec( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_VertexTypeEnumeration_t __encode_vertextype( vgx_Evaluator_t *self, const CString_t *CSTR__type, vgx_ExpressEvalTypeEncCache_t *cache );
static vgx_EvalStackItem_t * __get_vertex_property( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache, shortid_t keyhash );
static int __has_vertex_property( const vgx_Vertex_t *vertex, shortid_t keyhash );
static int64_t __get_vertex_property_count( const vgx_Vertex_t *vertex );
static vgx_StackItemType_t __push_vertex_property( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VertexTypeEnumeration_t __encode_vertextype( vgx_Evaluator_t *self, const CString_t *CSTR__type, vgx_ExpressEvalTypeEncCache_t *cache ) {
  // Match on string instance
  if( cache->CSTR__type != CSTR__type ) {
    // Compute hash
    QWORD typehash = CStringHash64( CSTR__type );
    // Update cached string
    cache->CSTR__type = CSTR__type;
    // Match on string value
    if( cache->typehash != typehash ) {
      // Encode
      vgx_VertexTypeEnumeration_t typeenc = (vgx_VertexTypeEnumeration_t)iEnumerator_OPEN.VertexType.GetEnum( self->graph, CSTR__type );
      if( typeenc == VERTEX_TYPE_ENUMERATION_NONEXIST || !__vertex_type_enumeration_valid( typeenc ) ) {
        typeenc = VERTEX_TYPE_ENUMERATION_NONE;
      }
      // Update cache
      cache->typehash = typehash;
      cache->vtx = typeenc;
    }
  }
  return cache->vtx;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_EvalStackItem_t * __get_vertex_property( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache, shortid_t keyhash ) {

  vgx_EvalStackItem_t *citem = &cache->item;

  // Perform lookup if vertex or key are different then last lookup
  if( vertex != cache->vertex || keyhash != cache->keyhash ) {
    cache->vertex = vertex;
    cache->keyhash = keyhash;
    if( vertex->properties ) {
      framehash_value_t fvalue = 0;
      framehash_valuetype_t vtype = iFramehash.simple.GetHash64( vertex->properties, keyhash, &fvalue );
      switch( vtype ) {
      // Virtual property offset
      case CELL_VALUE_TYPE_UNSIGNED:
        if( (citem->CSTR__str = _vxvertex_property__read_virtual_property( vertex->graph, (uint64_t)fvalue, NULL, NULL )) != NULL ) {
          citem->type = STACK_ITEM_TYPE_CSTRING;
          if( iEvaluator.LocalAutoScopeObject( self, citem, true ) == 1 ) {
            return citem;
          }
        }
        /* FALLTHRU */
      // Integer
      case CELL_VALUE_TYPE_INTEGER:
        citem->integer = (int64_t)fvalue;
        citem->type = STACK_ITEM_TYPE_INTEGER;
        return citem;
      // Real
      case CELL_VALUE_TYPE_REAL:
        citem->real = *(double*)&fvalue;
        citem->type = STACK_ITEM_TYPE_REAL;
        return citem;
      // String
      case CELL_VALUE_TYPE_OBJECT64:
        citem->CSTR__str = (CString_t*)fvalue;
        citem->type = STACK_ITEM_TYPE_CSTRING;
        return citem;
      default:
        break;
      }
    }
    // Property does not exist, or not a numeric type
    SET_NONE( citem );
  }

  // Return cached item pointer
  return citem;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __has_vertex_property( const vgx_Vertex_t *vertex, shortid_t keyhash ) {
  if( vertex->properties ) {
    framehash_value_t fvalue = 0;
    framehash_valuetype_t vtype = iFramehash.simple.GetHash64( vertex->properties, keyhash, &fvalue );
    switch( vtype ) {
    case CELL_VALUE_TYPE_MEMBER:
    case CELL_VALUE_TYPE_UNSIGNED:
    case CELL_VALUE_TYPE_INTEGER:
    case CELL_VALUE_TYPE_REAL:
    case CELL_VALUE_TYPE_OBJECT64:
      return 1;
    default:
      return 0;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __get_vertex_property_count( const vgx_Vertex_t *vertex ) {
  if( vertex->properties ) {
    return iFramehash.simple.Length( vertex->properties ) - Vertex_HasEnum( vertex );
  }
  else {
    return 0;
  }
}





#include "_conditional.h"



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_StackItemType_t __push_vertex_property( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache ) {

  vgx_EvalStackItem_t *px = GET_PITEM( self );
  shortid_t keyhash = px->integer;

  // Try to get property
  vgx_EvalStackItem_t *item = __get_vertex_property( self, vertex, cache, keyhash );
  // Exists
  if( item->type != STACK_ITEM_TYPE_NONE ) {
    *px = *item;
  }
  // Does not exist (push default)
  else {
    px->type = self->context.default_prop.type;
    px->bits = self->context.default_prop.bits;
  }
  return px->type;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->vertex = vertex;
  item->type = STACK_ITEM_TYPE_VERTEX;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_prop( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache ) {
  __push_vertex_property( self, vertex, cache );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_prop_dflt( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache ) {
  vgx_EvalStackItem_t dflt = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // Try to get property, or use default value if it does not exist
  vgx_EvalStackItem_t *item = __get_vertex_property( self, vertex, cache, px->integer );
  if( item->type != STACK_ITEM_TYPE_NONE ) {
    *px = *item;
  }
  else {
    *px = dflt;
  }
}



/*******************************************************************//**
 * propertyif( key, dflt, cond )
 ***********************************************************************
 */
__inline static void __stack_push_template_propif_dflt( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache ) {
  vgx_EvalStackItem_t cond = POP_ITEM( self );
  vgx_EvalStackItem_t dflt = POP_ITEM( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  vgx_EvalStackItem_t *item; 
  if( __condition( &cond ) && (item = __get_vertex_property( self, vertex, cache, px->integer ))->type != STACK_ITEM_TYPE_NONE ) {
    *px = *item;
  }
  else {
    *px = dflt;
  }
}



/*******************************************************************//**
 * property( key[, dflt[, cond]] )
 ***********************************************************************
 */
__inline static void __stack_push_template_fancyprop( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex, vgx_ExpressEvalPropertyCache_t *cache ) {
  int64_t nargs = self->op->arg.integer;
  vgx_EvalStackItem_t *pcond = NULL;
  vgx_EvalStackItem_t *pdflt = NULL;
  vgx_EvalStackItem_t *px = NULL;

  switch( nargs ) {
  // condition as 3rd arg
  case 3:
    pcond = POP_PITEM( self );
    /* FALLTHRU */
  // default as 2nd arg
  case 2:
    pdflt = POP_PITEM( self );
    // Condition not met, push default
    if( pcond && !__condition( pcond ) ) {
      *GET_PITEM( self ) = *pdflt;
      return;
    }
    /* FALLTHRU */
  //
  case 1:
    px = GET_PITEM( self );
    // Lookup
    vgx_EvalStackItem_t *item = __get_vertex_property( self, vertex, cache, px->integer );
    // Exists
    if( item->type != STACK_ITEM_TYPE_NONE ) {
      *px = *item;
    }
    // Push default
    else if( pdflt ) {
      *px = *pdflt;
    }
    // No default, push context default
    else {
      px->type = self->context.default_prop.type;
      px->bits = self->context.default_prop.bits;
    }
    return;
  default:
    DISCARD_ITEMS( self, nargs );
  }
}



/*******************************************************************//**
 * propcount
 ***********************************************************************
 */
__inline static void __stack_push_template_propcount( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t n = __get_vertex_property_count( vertex );
  SET_INTEGER_PITEM_VALUE( item, n );
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_c1( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  double c1 = vgx_RankGetC1( &vertex->rank );
  SET_REAL_PITEM_VALUE( item, c1 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_c0( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  double c0 = vgx_RankGetC0( &vertex->rank );
  SET_REAL_PITEM_VALUE( item, c0 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_virtual( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int v = vertex->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_VIRTUAL;
  SET_INTEGER_PITEM_VALUE( item, v );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_id( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->vertexid = &vertex->identifier;
  item->type = STACK_ITEM_TYPE_VERTEXID;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_obid( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->vertex = vertex;
  item->type = STACK_ITEM_TYPE_VERTEX;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_typ( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t type = vertex->descriptor.type.enumeration;
  SET_INTEGER_PITEM_VALUE( item, type );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_deg( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t deg = iarcvector.Degree( &vertex->inarcs ) + iarcvector.Degree( &vertex->outarcs );
  SET_INTEGER_PITEM_VALUE( item, deg );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_ideg( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t ideg = iarcvector.Degree( &vertex->inarcs );
  SET_INTEGER_PITEM_VALUE( item, ideg );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_odeg( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t odeg = iarcvector.Degree( &vertex->outarcs );
  SET_INTEGER_PITEM_VALUE( item, odeg );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_tmc( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t tmc = vertex->TMC;
  SET_INTEGER_PITEM_VALUE( item, tmc );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_tmm( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t tmm = vertex->TMM;
  SET_INTEGER_PITEM_VALUE( item, tmm );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_tmx( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t tmx = vertex->TMX.vertex_ts;
  SET_INTEGER_PITEM_VALUE( item, tmx );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_vec( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  if( (item->vector = vertex->vector) == NULL ) {
    item->vector = self->graph->similarity->nullvector;
  }
  item->type = STACK_ITEM_TYPE_VECTOR;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_op( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t op = iOperation.GetId_LCK( &vertex->operation );
  SET_INTEGER_PITEM_VALUE( item, op );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_refc( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t refc = _cxmalloc_linehead_from_object( vertex )->data.refc;
  SET_INTEGER_PITEM_VALUE( item, refc );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_bidx( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t bidx = _cxmalloc_linehead_from_object( vertex )->data.bidx;
  SET_INTEGER_PITEM_VALUE( item, bidx );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_oidx( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t offset = _cxmalloc_linehead_from_object( vertex )->data.offset;
  SET_INTEGER_PITEM_VALUE( item, offset );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_handle( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t handle = _cxmalloc_linehead_from_object( vertex )->data.handle;
  SET_INTEGER_PITEM_VALUE( item, handle );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_enum( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t vertex_enum32 = Vertex_GetEnum( vertex );
  SET_INTEGER_PITEM_VALUE( item, vertex_enum32 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_locked( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t locked = __vertex_is_locked_writable_by_other_thread( vertex, self->context.threadid );
  SET_INTEGER_PITEM_VALUE( item, locked );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_addr( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  int64_t addr = (intptr_t)vertex;
  SET_INTEGER_PITEM_VALUE( item, addr );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_index( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  uint64_t index = __vertex_get_index( ivertexobject.AsAllocatedVertex(vertex) );
  SET_INTEGER_PITEM_VALUE( item, index );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_bitindex( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  uint64_t bitindex = __vertex_get_bitindex( ivertexobject.AsAllocatedVertex(vertex) );
  SET_INTEGER_PITEM_VALUE( item, bitindex );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void __stack_push_template_bitvector( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  item->bits = __vertex_get_bitvector( ivertexobject.AsAllocatedVertex(vertex) );
  item->type = STACK_ITEM_TYPE_BITVECTOR;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#ifdef VGX_CONSISTENCY_CHECK
#define ASSERT_VERTEX_LOCK __assert_vertex_lock
#else
#define ASSERT_VERTEX_LOCK( LockedVertex ) LockedVertex
#endif




#define __DEFINE_VERTEX_FUNCTIONS( Vertex )                                                                                                                                             \
static void __stack_push_##Vertex (             vgx_Evaluator_t *self ) { __stack_push_template(            self, self->context.Vertex ); }                                             \
static void __stack_push_##Vertex##_prop(       vgx_Evaluator_t *self ) { __stack_push_template_prop(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ), &self->cache.Vertex ); }  \
static void __stack_push_##Vertex##_prop_dflt(  vgx_Evaluator_t *self ) { __stack_push_template_prop_dflt(  self, ASSERT_VERTEX_LOCK( self->context.Vertex ), &self->cache.Vertex ); }  \
static void __stack_push_##Vertex##_propif_dflt(vgx_Evaluator_t *self ) { __stack_push_template_propif_dflt(self, ASSERT_VERTEX_LOCK( self->context.Vertex ), &self->cache.Vertex ); }  \
static void __stack_push_##Vertex##_fancyprop(  vgx_Evaluator_t *self ) { __stack_push_template_fancyprop(  self, ASSERT_VERTEX_LOCK( self->context.Vertex ), &self->cache.Vertex ); }  \
static void __stack_push_##Vertex##_propcount(  vgx_Evaluator_t *self ) { __stack_push_template_propcount(  self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_c1(         vgx_Evaluator_t *self ) { __stack_push_template_c1(         self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_c0(         vgx_Evaluator_t *self ) { __stack_push_template_c0(         self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_virtual(    vgx_Evaluator_t *self ) { __stack_push_template_virtual(    self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_id(         vgx_Evaluator_t *self ) { __stack_push_template_id(         self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_obid(       vgx_Evaluator_t *self ) { __stack_push_template_obid(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_typ(        vgx_Evaluator_t *self ) { __stack_push_template_typ(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_deg(        vgx_Evaluator_t *self ) { __stack_push_template_deg(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_ideg(       vgx_Evaluator_t *self ) { __stack_push_template_ideg(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_odeg(       vgx_Evaluator_t *self ) { __stack_push_template_odeg(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_tmc(        vgx_Evaluator_t *self ) { __stack_push_template_tmc(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_tmm(        vgx_Evaluator_t *self ) { __stack_push_template_tmm(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_tmx(        vgx_Evaluator_t *self ) { __stack_push_template_tmx(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_vec(        vgx_Evaluator_t *self ) { __stack_push_template_vec(        self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_op(         vgx_Evaluator_t *self ) { __stack_push_template_op(         self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_refc(       vgx_Evaluator_t *self ) { __stack_push_template_refc(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_bidx(       vgx_Evaluator_t *self ) { __stack_push_template_bidx(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_oidx(       vgx_Evaluator_t *self ) { __stack_push_template_oidx(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_handle(     vgx_Evaluator_t *self ) { __stack_push_template_handle(     self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_enum(       vgx_Evaluator_t *self ) { __stack_push_template_enum(       self, ASSERT_VERTEX_LOCK( self->context.Vertex ) ); }                       \
static void __stack_push_##Vertex##_locked(     vgx_Evaluator_t *self ) { __stack_push_template_locked(     self, self->context.Vertex ); }                                             \
static void __stack_push_##Vertex##_addr(       vgx_Evaluator_t *self ) { __stack_push_template_addr(       self, self->context.Vertex ); }                                             \
static void __stack_push_##Vertex##_index(      vgx_Evaluator_t *self ) { __stack_push_template_index(      self, self->context.Vertex ); }                                             \
static void __stack_push_##Vertex##_bitindex(   vgx_Evaluator_t *self ) { __stack_push_template_bitindex(   self, self->context.Vertex ); }                                             \
static void __stack_push_##Vertex##_bitvector(  vgx_Evaluator_t *self ) { __stack_push_template_bitvector(  self, self->context.Vertex ); }




__DEFINE_VERTEX_FUNCTIONS( TAIL )
__DEFINE_VERTEX_FUNCTIONS( VERTEX )
__DEFINE_VERTEX_FUNCTIONS( HEAD )



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_string_typeenc( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // Already encoded literal during parsing, or argument is zero( same as no type)
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    return;
  }
  // Dynamic encoding during evaluation (slow!)
  else if( px->type == STACK_ITEM_TYPE_CSTRING ) {
    SET_INTEGER_PITEM_VALUE( px, __encode_vertextype( self, px->CSTR__str, &self->cache.vertextype ) );
    return;
  }
  // Invalid
  SET_INTEGER_PITEM_VALUE( px, VERTEX_TYPE_ENUMERATION_NONE );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_string_typedec( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  // Dynamic encoding during evaluation (slow!)
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    if( (px->CSTR__str = _vxenum_vtx__decode_OPEN( self->graph, (vgx_vertex_type_t)px->integer )) != NULL ) {
      px->type = STACK_ITEM_TYPE_CSTRING;
    }
  }
}





#endif
