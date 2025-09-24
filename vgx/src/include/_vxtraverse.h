/*
###################################################
#
# File:   _vxtraverse.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXTRAVERSE_H
#define _VGX_VXTRAVERSE_H

#include "_vgx.h"



static int64_t __get_total_neighborhood_size( const vgx_Vertex_t *vertex_RO, vgx_arc_direction direction );



 /*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __get_total_neighborhood_size( const vgx_Vertex_t *vertex_RO, vgx_arc_direction direction ) {
  switch( direction ) {
  case VGX_ARCDIR_ANY:
    /* FALLTHRU */
  case VGX_ARCDIR_BOTH:
    return CALLABLE( vertex_RO )->Degree( vertex_RO );
  case VGX_ARCDIR_IN:
    return iarcvector.Degree( &vertex_RO->inarcs ); 
  case VGX_ARCDIR_OUT:
    return iarcvector.Degree( &vertex_RO->outarcs );
  default:
    return -1;
  }
}






#endif
