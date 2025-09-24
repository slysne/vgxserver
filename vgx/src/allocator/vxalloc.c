/*######################################################################
 *#
 *# vxalloc.c
 *#
 *#
 *######################################################################
 */


#include "_cxmalloc.h"
#include "_vxalloc.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_ALLOC );



/*******************************************************************//**
 * Annotated Pointer Allocator
 ***********************************************************************
 */
CXMALLOC_TEMPLATE( AptrAllocator,             /*                                                                                */
                   aptr_t,                    /* sizeof(aptr_t) = 16, i.e. 4 slots per cache line                  */
                   ap_vector,                 /*                                                                                */
                   1ULL<<26,                  /* 64 MB per block                                                                */
                   1532,                      /* aidx=16 => size=1532 with S=1                                                  */
                   32,                        /* space for the class header and extras                                          */
                   1,                         /* S=1 =>    0:0,    1:4     2:8,    3:12,   4:20,   5:28,   6:44,    7:60,  8:92 */
                                              /*           9:124, 10:188, 11:252, 12:380, 13:508, 14:764, 15:1020, 16:1532      */
                   1,                         /* allow oversized                                                                */
                   17                         /* aidx 0 - 16                                                                    */
                 )



/*******************************************************************//**
 * Tagged Pointer Allocator
 ***********************************************************************
 */
CXMALLOC_TEMPLATE( TptrAllocator,             /*                                                                                */
                   tptr_t,                    /* sizeof(tptr_t) = 8, i.e. 8 slots per cache line                                */
                   tp_vector,                 /*                                                                                */
                   1ULL<<26,                  /* 64 MB per block                                                                */
                   3064,                      /* aidx=16 => size=3064 with S=1                                                  */
                   32,                        /* space for the class header and extras                                          */
                   1,                         /* S=1 =>    0:0,    1:8    2:16,   3:24,   4:40,   5:56,    6:88,    7:120,      */
                                              /*           8:184, 9:248, 10:376, 11:504, 12:760, 13:1016, 14:1528, 15:2040,     */
                                              /*          16:3064                                                               */
                   1,                         /* allow oversized                                                                */
                   17                         /* aidx 0 - 16                                                                    */
                 )



/*******************************************************************//**
 * Weighted Offset Allocator
 ***********************************************************************
 */
CXMALLOC_TEMPLATE( OffsetAllocator,           /*                                                                                */
                   weighted_offset_t,         /* sizeof(weighted_offset_t) = 4, i.e. 16 slots per cache line                    */
                   o_vector,                  /*                                                                                */
                   1ULL<<26,                  /* 64 MB per block                                                                */
                   8388592,                   /* aidx=19 => size=8388592 with S=1                                               */
                   32,                        /* space for the class header and extras                                          */
                   0,                         /* S=0 =>    0:0,        1:16       2:48,      3:112,     4:240,      5:496,      */
                                              /*           6:1008,     7:2032,    8:4080,    9:8176,   10:16368,   11:32752,    */
                                              /*          12:65520,   13:131056, 14:262128, 15:524272, 16:1048560, 17:2097136   */
                                              /*          18:4194288, 19:8388592                                                */
                   0,                         /* disallow oversized                                                             */
                   20                         /* aidx 0 - 19                                                                    */
                 )










void f(void) {

  const TptrAllocator_interface_t *TP_alloc = TptrAllocatorInterface();
  const AptrAllocator_interface_t *AP_alloc = AptrAllocatorInterface();
  const OffsetAllocator_interface_t *O_alloc = OffsetAllocatorInterface();


  tptr_t *tp;
  aptr_t *ap;
  weighted_offset_t *o;

  tp = TP_alloc->New( 18 );
  ap = AP_alloc->New( 123 );
  o = O_alloc->New( 222 );


  TPTR_SET_POINTER( tp, (void*)1000 );
  APTR_SET_POINTER( ap, (void*)2000 );
  o->offset = 17;
  o->weight = 42;




}
