/*
###################################################
#
# File:   frameallocator.h
# Author: Stian Lysne
#
#
###################################################
*/

#ifndef FRAMEALLOCATOR_H
#define FRAMEALLOCATOR_H

#include "cxmalloc.h"
#include "_fhbase.h"

/* Declare FrameAllocator and accessors */
CXMALLOC_HEADER_TEMPLATE( _framehash_frameallocator__, framehash_slot_t, slots )        // allocation unit is SLOT

DLL_HIDDEN extern void _framehash_frameallocator__initialize( framehash_cell_t *frameref );
DLL_HIDDEN extern framehash_metas_t _framehash_frameallocator__get_metas( const framehash_cell_t *frameref );
DLL_HIDDEN extern framehash_cell_t * _framehash_frameallocator__top_cell_from_slot_array( const framehash_slot_t *slots );
DLL_HIDDEN extern framehash_cell_t * _framehash_frameallocator__update_top_cell( framehash_cell_t *frameref );
DLL_HIDDEN extern uint16_t _framehash_frameallocator__get_chainbits( const framehash_cell_t *frameref );
DLL_HIDDEN extern uint16_t _framehash_frameallocator__set_chainbits( framehash_cell_t *frameref, const uint16_t chainbits );
DLL_HIDDEN extern uint16_t _framehash_frameallocator__get_fileno( const framehash_cell_t *frameref );
DLL_HIDDEN extern uint16_t _framehash_frameallocator__set_fileno( framehash_cell_t *frameref, const uint16_t fileno );
DLL_HIDDEN extern framehash_ftype_t _framehash_frameallocator__get_ftype( const framehash_cell_t *frameref );
DLL_HIDDEN extern framehash_ftype_t _framehash_frameallocator__set_ftype( framehash_cell_t *frameref, const framehash_ftype_t ftype );
DLL_HIDDEN extern int _framehash_frameallocator__get_domain( const framehash_cell_t *frameref );
DLL_HIDDEN extern int _framehash_frameallocator__set_domain( framehash_cell_t *frameref, const int domain );
DLL_HIDDEN extern int _framehash_frameallocator__get_order( const framehash_cell_t *frameref );
DLL_HIDDEN extern int _framehash_frameallocator__set_order( framehash_cell_t *frameref, const int order );
DLL_HIDDEN extern framehash_halfslot_t * _framehash_frameallocator__header_half_slot( framehash_cell_t *frameref );
DLL_HIDDEN extern bool _framehash_frameallocator__is_frame_valid( const framehash_cell_t * const frameref );

#endif




