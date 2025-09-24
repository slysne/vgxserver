/*
###################################################
#
# File:   basementallocator.h
# Author: Stian Lysne
#
#
###################################################
*/

#ifndef BASEMENTALLOCATOR_H
#define BASEMENTALLOCATOR_H

#include "cxmalloc.h"
#include "_fhbase.h"

/* Declare BasementAllocator and accessors */
CXMALLOC_HEADER_TEMPLATE( _framehash_basementallocator__, framehash_cell_t, cells )  // allocation unit is CELL

DLL_HIDDEN extern void _framehash_basementallocator__initialize( framehash_cell_t *basement_ref );
DLL_HIDDEN extern framehash_cell_t * _framehash_basementallocator__get_chain_cell( const framehash_cell_t *basement_ref );

#endif




