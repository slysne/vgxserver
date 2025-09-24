/*######################################################################
 *#
 *# default_cxmalloc.c
 *#
 *#
 *######################################################################
 */

#include "_cxmalloc.h"
#include "default_allocator.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );

/*******************************************************************//**
 * TestAllocator
 ***********************************************************************
 */
#define BLOCK_BYTES (1<<21)
CXMALLOC_TEMPLATE( DefaultAllocator, /* ATYPE:          type name */
                   char,             /* UTYPE:          unit type */
                   memory,           /* ARRAY:          array name */
                   BLOCK_BYTES,      /* BlockBytes:     bytes per block */
                   8192,             /* MaxArraySize:   max array size */
                   0,                /* HeaderSize:     extra header bytes for custom metas */
                   0,                /* Growth:         growth factor S */
                   1,                /* AllowOversized: oversized */
                   100               /* MaxAllocators:  max allocators */
                  )
#undef BLOCK_BYTES


