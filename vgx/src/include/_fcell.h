/*
###################################################
#
# File:   _fcell.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _FCELL_H
#define _FCELL_H


#include "_fmacro.h"
#include "framehash.h"




/*******************************************************************//**
 * __set_cell
 *
 ***********************************************************************
 */
__inline static void __set_cell( framehash_context_t * const context, framehash_cell_t * cell ) {
  switch( context->vtype ) {
  case CELL_VALUE_TYPE_NULL:
    // NOOP for now, in future consider this to mean delete
    return;
  case CELL_VALUE_TYPE_MEMBER:
    _STORE_MEMBERSHIP( context, cell );
    return;
  case CELL_VALUE_TYPE_BOOLEAN:
    _STORE_BOOLEAN( context, cell );
    return;
  case CELL_VALUE_TYPE_UNSIGNED:
    _STORE_UNSIGNED( context, cell );
    return;
  case CELL_VALUE_TYPE_INTEGER:
    _STORE_INTEGER( context, cell );
    return;
  case CELL_VALUE_TYPE_REAL:
    _STORE_REAL( context, cell );
    return;
  case CELL_VALUE_TYPE_POINTER:
    _STORE_POINTER( context, cell );
    return;
  case CELL_VALUE_TYPE_OBJECT64:
    // any previous object is NOT destroyed
    _STORE_OBJECT64_POINTER( context, cell );
    return;
  case CELL_VALUE_TYPE_OBJECT128: /* COMLIB_VS_POINTER */
    // If a different object already exists in the target cell make sure we destroy it first
    _DESTROY_PREVIOUS_OBJECT128( context, cell );
    _STORE_OBJECT128_POINTER( context, cell );
    return;
  default:
    // Unknown vtype, do nothing.
    return;
  }
}




#endif




