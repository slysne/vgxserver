/*
###################################################
#
# File:   vxalloc.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef VGX_VXALLOC_H
#define VGX_VXALLOC_H

#include "cxmalloc.h"
#include "_vxprivate.h"
#include "vxbase.h"



CXMALLOC_HEADER_TEMPLATE( AptrAllocator, aptr_t, ap_vector )


CXMALLOC_HEADER_TEMPLATE( TptrAllocator, tptr_t, tp_vector )


CXMALLOC_HEADER_TEMPLATE( OffsetAllocator, weighted_offset_t, o_vector )



/*******************************************************************//**
 * Vertex Allocator
 ***********************************************************************
 */


#endif




