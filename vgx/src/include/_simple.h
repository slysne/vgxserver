/*
###################################################
#
# File:   _simple.h
#
###################################################
*/

#ifndef _SIMPLE_H
#define _SIMPLE_H

/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_slot_t * __frame_slots_from_top( const framehash_cell_t *top ) {
  return (framehash_slot_t*)(((char*)top) + sizeof( cxmalloc_linehead_t ) + sizeof( framehash_halfslot_t ));
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __frame_top_from_slots( const framehash_slot_t *slots ) {
  return (framehash_cell_t*)(((char*)slots) - sizeof( cxmalloc_linehead_t ) - sizeof( framehash_halfslot_t ));
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_cell_t * __set_ephemeral_top( framehash_cell_t *eph_top, const framehash_cell_t *entrypoint ) {
  // Copy Metas
  eph_top->annotation = entrypoint->annotation;
  // Pointer to slot array
  APTR_SET_POINTER( eph_top, __frame_slots_from_top( entrypoint ) );
  return eph_top;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_context_t * __init_key( framehash_context_t *context, framehash_key_t fkey ) {
  switch( context->ktype ) {
  // 64-bit shortid key. No plain key.
  case CELL_KEY_TYPE_HASH64:
    context->key.plain = FRAMEHASH_KEY_NONE;
    context->obid = NULL;
    context->key.shortid = *((shortid_t*)fkey);
    return context;
  // Plain 64-bit integer key. Compute the shortid from plain key using hash function.
  case CELL_KEY_TYPE_PLAIN64:
    context->key.plain = *((QWORD*)fkey);
    context->obid = NULL;
    context->key.shortid = context->dynamic->hashf( context->key.plain );
    return context;
  // 128-bit obid key. No plain key. Set the shortid as the lower 64-bit of obid.
  case CELL_KEY_TYPE_HASH128:
    context->key.plain = FRAMEHASH_KEY_NONE;
    context->obid = (objectid_t*)fkey;
    context->key.shortid = context->obid->L;
    return context;
  default:
    return context;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_context_t * __init_value( framehash_context_t *context, const framehash_value_t fvalue ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_MEMBER:
    context->value.idH56 = context->obid ? context->obid->H : 0;
    return context;
  case CELL_VALUE_TYPE_BOOLEAN:
    /* FALLTHRU */
  case CELL_VALUE_TYPE_UNSIGNED:
    /* FALLTHRU */
  case CELL_VALUE_TYPE_INTEGER:
    context->value.raw56 = (QWORD)fvalue;
    return context;
  case CELL_VALUE_TYPE_REAL:
    context->value.real56 = *((double*)&fvalue);
    return context;
  case CELL_VALUE_TYPE_POINTER:
    context->value.ptr56 = (char*)fvalue;
    return context;
  case CELL_VALUE_TYPE_OBJECT64:
    context->value.obj56 = (comlib_object_t*)fvalue;
    return context->ktype != CELL_KEY_TYPE_HASH128 ? context : NULL;  // disallow HASH128 for OBJECT64
  case CELL_VALUE_TYPE_OBJECT128:
    context->value.pobject = (comlib_object_t*)fvalue;
    return context->ktype != CELL_KEY_TYPE_HASH64 ? context : NULL;   // disallow HASH64 for OBJECT128
  default:
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_context_t * __init_key_and_value( framehash_context_t *context, framehash_key_t fkey, const framehash_value_t fvalue ) {
  return __init_value( __init_key( context, fkey ), fvalue );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static framehash_valuetype_t __return_value( framehash_context_t *context, framehash_value_t *pfvalue ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    *(QWORD*)pfvalue = 0;
    return context->vtype;
  case CELL_VALUE_TYPE_MEMBER:
    *(QWORD*)pfvalue = 1;
    return context->vtype;
  case CELL_VALUE_TYPE_BOOLEAN:
    *(QWORD*)pfvalue = context->value.raw56 != 0;
    return context->vtype;
  case CELL_VALUE_TYPE_UNSIGNED:
    *(uint64_t*)pfvalue = (uint64_t)context->value.raw56;
    return context->vtype;
  case CELL_VALUE_TYPE_INTEGER:
    *(int64_t*)pfvalue = (int64_t)context->value.raw56;
    return context->vtype;
  case CELL_VALUE_TYPE_REAL:
    *(double*)pfvalue = context->value.real56;
    return context->vtype;
  case CELL_VALUE_TYPE_POINTER:
    *(char**)pfvalue = context->value.ptr56;
    return context->vtype;
  case CELL_VALUE_TYPE_OBJECT64:
    *(comlib_object_t**)pfvalue = (comlib_object_t*)context->value.obj56;
    return context->vtype;
  case CELL_VALUE_TYPE_OBJECT128:
    *(comlib_object_t**)pfvalue = (comlib_object_t*)context->value.pobject;
    return context->vtype;
  default:
    return CELL_VALUE_TYPE_ERROR;
  }
}



#endif




