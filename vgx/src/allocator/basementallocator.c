/*######################################################################
 *#
 *# basementallocator.c
 *#
 *#
 *######################################################################
 */


#include "_cxmalloc.h"
#include "basementallocator.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );

#ifndef MINIMAL_FOOTPRINT
#define MINIMAL_FOOTPRINT 0
#endif

/*******************************************************************//**
 * BasementAllocator
 ***********************************************************************
 */
#if defined FRAMEHASH_BASEMENT_BLOCK_SIZE_MB
#define __BLOCK_BYTES ((size_t)FRAMEHASH_BASEMENT_BLOCK_SIZE_MB << 20)
#else
#define __BLOCK_BYTES (1ULL<<21)  // number of basement cells (incl. alloc overhead) per data block in allocators (2MB = 131072 slots = 1 PD entry = 512 full frames)
#endif



CXMALLOC_TEMPLATE( _framehash_basementallocator__,  /* ATYPE:           generated type is BasementAllocator                 */
                   framehash_cell_t,                /* UTYPE:           sizeof(framehash_cell_t) = 16                       */
                   cells,                           /* Array:           array will be known as "cells" in method signatures */
                   __BLOCK_BYTES,                   /* BlockBytes:                                                          */
                   _FRAMEHASH_BASEMENT_SIZE,        /* MaxArraySize:    aidx=6 => size=63 with S=0                          */
                   0,                               /* HeaderSize:      no header                                           */
                   0,                               /* Growth:          S=0 => 0, 2, 6                                      */
                   0,                               /* AllowOversized:  disallow oversized                                  */
                   3                                /* MaxAllocators:   3 => aidx 0 - 2 (assumes _FRAMEHASH_BASEMENT_SIZE=6)*/
                 )


/*******************************************************************//**
 * Together M1 and M2 make up the annotated pointer cell referencing
 * the next basement.
 ***********************************************************************
 */
#define BASEMENT_HEADER_NEXT_REFERENCE( lineptr )   ((framehash_cell_t*)(CXMALLOC_META_FROM_ARRAY( _framehash_basementallocator__, 1, lineptr )))
// NOTE: The header has this layout:
//     M1       M2      (internal)        data ...
// |--------|--------|----------------|--------...
//
// M1 = metas
// M2 = cell reference
// M1+M2 is interpreted as an annotated pointer where M1 is the annotation and M2 is the pointer



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_basementallocator__initialize( framehash_cell_t *basement_ref ) {
  framehash_cell_t *chaincell = _framehash_basementallocator__get_chain_cell( basement_ref );
  /* we are not referring to any next basement, so set our chain to nothing */
  CELL_GET_FRAME_METAS( chaincell )->QWORD = FRAMEHASH_REF_NULL;
  DELETE_CELL_REFERENCE( chaincell );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _framehash_basementallocator__get_chain_cell( const framehash_cell_t *basement_ref ) {
  return BASEMENT_HEADER_NEXT_REFERENCE( CELL_GET_FRAME_CELLS(basement_ref) );
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_basementallocator.h"

DLL_HIDDEN test_descriptor_t _framehash_basementallocator_tests[] = {
  { "basementallocator",   __utest_framehash_basementallocator },
  {NULL}
};
#endif
