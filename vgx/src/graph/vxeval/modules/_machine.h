/*
###################################################
#
# File:   _machine.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_MACHINE_H
#define _VGX_VXEVAL_MODULES_MACHINE_H




/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_cpukill( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = px->integer;
    double y = 0.0;
    int64_t i=0;
    while( i++ < x ) {
      y += (double)i;
    }
    SET_REAL_PITEM_VALUE( px, y );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_memkill( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = px->integer;
    QWORD *data = calloc( x, sizeof( QWORD ) );
    if( data ) {
      int64_t i=0;
      while( i < x ) {
        data[i] = i;
        ++i;
      }
      i = 0;
      while( i < x ) {
        data[ ihash64( i + 1 ) % x ] = data[ ihash64( i + x ) % x ];
        ++i;
      }
      free( data );
    }
  }
}



#endif
