/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxmalloc_family.c
 * Author:  Stian Lysne slysne.dev@gmail.com
 * 
 * Copyright © 2025 Rakuten, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *****************************************************************************/

#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );



static cxmalloc_family_t * __cxmalloc_family__new_family_OPEN(            const char *id, const cxmalloc_descriptor_t *descriptor );
static                void __cxmalloc_family__delete_family_OPEN(         cxmalloc_family_t **family );
static             int64_t __cxmalloc_family__restore_objects_FCS(        cxmalloc_family_t *family_CS );
static    CStringQueue_t * __cxmalloc_family__repr_FCS(                   cxmalloc_family_t *family_CS, CStringQueue_t *output );
static             int64_t __cxmalloc_family__validate_refcounts_FCS(     cxmalloc_family_t *family_CS, cxmalloc_linehead_t **bad_linehead );
static   comlib_object_t * __cxmalloc_family__get_object_at_address_FCS(  cxmalloc_family_t *family_CS, QWORD address );
static   comlib_object_t * __cxmalloc_family__find_object_by_obid_FCS(    cxmalloc_family_t *family_CS, objectid_t obid );
static   comlib_object_t * __cxmalloc_family__get_object_by_offset_FCS(   cxmalloc_family_t *family_CS, int64_t *poffset );
static             int64_t __cxmalloc_family__sweep_OPEN(                 cxmalloc_family_t *family, f_get_object_identifier get_object_identifier );
static             int64_t __cxmalloc_family__compute_active_OPEN(        cxmalloc_family_t *family );
static             int64_t __cxmalloc_family__active_OPEN(                cxmalloc_family_t *family );
static              double __cxmalloc_family__utilization_OPEN(           cxmalloc_family_t *family );



DLL_HIDDEN _icxmalloc_family_t _icxmalloc_family = {
  .NewFamily_OPEN         = __cxmalloc_family__new_family_OPEN,
  .DeleteFamily_OPEN      = __cxmalloc_family__delete_family_OPEN,
  .RestoreObjects_FCS     = __cxmalloc_family__restore_objects_FCS,
  .Repr_FCS               = __cxmalloc_family__repr_FCS,
  .ValidateRefcounts_FCS  = __cxmalloc_family__validate_refcounts_FCS,
  .GetObjectAtAddress_FCS = __cxmalloc_family__get_object_at_address_FCS,
  .FindObjectByObid_FCS   = __cxmalloc_family__find_object_by_obid_FCS,
  .GetObjectByOffset_FCS  = __cxmalloc_family__get_object_by_offset_FCS,
  .Sweep_OPEN             = __cxmalloc_family__sweep_OPEN,
  .ComputeActive_OPEN     = __cxmalloc_family__compute_active_OPEN,
  .Active_OPEN            = __cxmalloc_family__active_OPEN,
  .Utilization_OPEN       = __cxmalloc_family__utilization_OPEN
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_descriptor( cxmalloc_descriptor_t **descriptor ) {
  if( *descriptor ) {
    if( (*descriptor)->persist.CSTR__path ) {
      CStringDelete( (*descriptor)->persist.CSTR__path );
    }
    ALIGNED_FREE( *descriptor );
    *descriptor = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_descriptor_t * __clone_descriptor( const cxmalloc_descriptor_t *descriptor ) {
  cxmalloc_descriptor_t *clone = NULL;
  XTRY {
    TALIGNED_MALLOC_THROWS( clone, cxmalloc_descriptor_t, 0x701 );
    memset( clone, 0, sizeof( cxmalloc_descriptor_t ) );
    // [1] meta.initval
    clone->meta.initval.M = descriptor->meta.initval.M;
    // [2] meta.serialized_sz
    clone->meta.serialized_sz = descriptor->meta.serialized_sz;
    // [3+4] obj
    clone->obj = descriptor->obj;
    // [5+6]
    clone->unit = descriptor->unit;
    // [7]
    clone->serialize_line = descriptor->serialize_line;
    // [8]
    clone->deserialize_line = descriptor->deserialize_line;
    // [9+10+11+12+13]
    clone->parameter = descriptor->parameter;
    // [14]
    if( descriptor->persist.CSTR__path ) {
      if( (clone->persist.CSTR__path = CStringClone( descriptor->persist.CSTR__path )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x702 );
      }
    }
    // [15]
    clone->auxiliary = descriptor->auxiliary;

  }
  XCATCH( errcode ) {
    __delete_descriptor( &clone );
  }
  XFINALLY {
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __cxmalloc_family__new_family_OPEN( const char *id, const cxmalloc_descriptor_t *descriptor ) {
  cxmalloc_family_t *family = NULL;

  XTRY {

    DEBUG( 0x711, "Creating allocator family: %s", id );

    // -----------------------------------------
    // 1: Allocate object and initialize members
    // -----------------------------------------

    // [1] [2] [3] Allocate family object and hook up its vtable, set typeinfo and obid
    TALIGNED_MALLOC_THROWS( family, cxmalloc_family_t, 0x712 );
    COMLIB_OBJECT_INIT( cxmalloc_family_t, family, id );
    family->descriptor = NULL;
    family->allocators = NULL;

    // [Q1.5] Allocate and initialize descriptor objects - copy the whole thing to private area
    if( (family->descriptor = __clone_descriptor( descriptor )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x713 );
    }

    // [Q1.6.1] unit_sz_order
    family->unit_sz_order = ilog2( family->descriptor->unit.sz );

    // [Q1.6.2] object_bytes
    family->object_bytes = family->descriptor->obj.sz;

    // [Q1.7.1] header_bytes  (total header will always be a power of 2 to ensure cache line alignment, minimum header bytes = 32)
    family->header_bytes = 1U << (1 + ilog2( sizeof(cxmalloc_linehead_t) + family->object_bytes - 1 ));

    // [Q1.7.2] subdue
    family->subdue = family->descriptor->parameter.subdue;

    // [Q1.8.1] size (number of allocators, line_limit may require more allocators. TODO: i think the logic is backwards)
    family->size = family->descriptor->parameter.max_allocators;
    if( family->descriptor->parameter.line_limit < _icxmalloc_shape.GetLength_FRO( family, (uint16_t)family->size - 1 ) ) {
      family->size = _icxmalloc_shape.GetAIDX_FRO( family, family->descriptor->parameter.line_limit, NULL ) + 1; 
    }

    // [Q1.8.2] max_length
    family->max_length = _icxmalloc_shape.GetLength_FRO( family, (uint16_t)family->size-1 );

    // [Q2.7.1] min_length
    family->min_length = _icxmalloc_shape.GetLength_FRO( family, 0 );

    // [Q2.7.2.1] lazy_discards
    family->flag.lazy_discards = 0;

    // [Q2.6] allocators
    CALIGNED_INITIALIZED_ARRAY_THROWS( family->allocators, cxmalloc_allocator_t*, (size_t)family->size, NULL, 0x714 );
    
    // [Q2.1/2/3/4/5] lock
    INIT_SPINNING_CRITICAL_SECTION( &family->lock.lock, 4000 );

    // [Q2.8.1] readonly_cnt
    family->readonly_cnt = 0;

    // ---------------------
    // 2.
    // ---------------------
    int64_t n = 0;
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      n = _icxmalloc_serialization.RestoreFamily_FCS( family_CS );
    } RELEASE_CXMALLOC_FAMILY;
    if( n < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x715 );
    }
  }
  XCATCH( errcode ) {
    __cxmalloc_family__delete_family_OPEN( &family );
  }
  XFINALLY {
    if( family ) {
      // [12b] ready
      family->flag.ready = 1;
    }
  }

  return family;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __cxmalloc_family__delete_family_OPEN( cxmalloc_family_t **ppfamily ) {
  if( ppfamily && *ppfamily ) {
    cxmalloc_family_t *family = *ppfamily;
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {

      // [Q2.6] allocators
      if( family_CS->allocators ) {
        for( int i=0; i<family_CS->size; i++ ) {
          cxmalloc_allocator_t *allocator = family_CS->allocators[i];
          _icxmalloc_allocator.DestroyAllocator_FCS( family_CS, &allocator );
          family_CS->allocators[i] = NULL;
        }
        ALIGNED_FREE( family_CS->allocators );
        family_CS->allocators = NULL;
      }

      if( family_CS->flag.ready ) {
        // [Q2.7.2.2] ready
        family_CS->flag.ready = 0;
      }

      // [Q2.8.1] readonly_cnt
      family_CS->readonly_cnt = 0;

      // [Q1.7.2] subdue
      family_CS->subdue = 0;

      // [Q2.7.2.1] lazy_discards
      family_CS->flag.lazy_discards = 0;

      // [Q2.7.1] min_length
      family_CS->min_length = 0;

      // [Q1.8.2] max length
      family_CS->max_length = 0;

      // [Q1.8.1] size
      family_CS->size = 0;

      // [Q1.7.1] header_bytes
      family_CS->header_bytes = 0;

      // [Q1.6.2] object bytes
      family_CS->object_bytes = 0;

      // [Q1.6.1] unit_sz_order
      family_CS->unit_sz_order = 0;

      // [Q1.5] descriptor
      __delete_descriptor( &family_CS->descriptor );

      // [Q1.3/4] obid
      objectid_destroy_longstring( &family_CS->obid );
    } RELEASE_CXMALLOC_FAMILY;
      
    // [Q2.1/2/3/4/5] lock
    DEL_CRITICAL_SECTION( &family->lock.lock );

    // [Q1.2] typeinfo
    family->typeinfo.qword = 0;

    // [Q1.1] vtable
    family->vtable = NULL;

    // family
    ALIGNED_FREE( *ppfamily );

    // NULL
    *ppfamily = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __cxmalloc_family__restore_objects_FCS( cxmalloc_family_t *family_CS ) {
  int64_t nqwords = 0;
  int64_t n = -1;

  IGNORE_WARNING_UNBALANCED_LOCK_RELEASE
  XTRY {

    const char *name = family_CS->obid.longstring.string;;
    CXMALLOC_VERBOSE( 0x721, "Restoring: %s", name );

    for( int aidx = 0; aidx < family_CS->size; aidx++ ) {
      cxmalloc_allocator_t *allocator = family_CS->allocators[ aidx ];
      if( allocator ) {
        IF_WRITABLE_ALLOCATOR_THEN_SYNCHRONIZE( allocator ) {
          n = _icxmalloc_allocator.RestoreObjects_FCS_ACS( allocator_CS_W );
        } END_SYNCHRONIZED_WRITABLE_ALLOCATOR;
        if( n < 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x722, "Failed to restore allocator family: '%s'", name );
        }
        nqwords += n;
      }
    }

    if( nqwords > 0 ) {
      CXMALLOC_INFO( 0x722, "Restored: %s (%lld bytes)", name, nqwords * sizeof(QWORD) );
    }

  }
  XCATCH( errcode ) {
    nqwords = -1;
  }
  XFINALLY {
  }

  return nqwords;
  RESUME_WARNINGS
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static CStringQueue_t * __cxmalloc_family__repr_FCS( cxmalloc_family_t *family_CS, CStringQueue_t *output ) {
  typedef int64_t (*f_format)( CStringQueue_t *q, const char *fmt, ... );
  size_t alloc_lines;   // total lines in allocator
  size_t used_lines;    // used lines in allocator
  cxmalloc_allocator_t *allocator;
  cxmalloc_block_t **cursor;
  int empty = 0;
  f_format Format = CALLABLE(output)->Format;
# define PUT( FormatString, ... ) Format( output, FormatString, ##__VA_ARGS__ )

  COMLIB_DefaultRepresenter( (const comlib_object_t*)family_CS, output );
  PUT( "\n" );

  cxmalloc_descriptor_t *desc = family_CS->descriptor;

  IGNORE_WARNING_UNBALANCED_LOCK_RELEASE
  if( family_CS && family_CS->flag.ready ) {
    PUT( "=========================================================================================================\n" );
    PUT( "ALLOCATOR        : %s\n", _cxmalloc_id_string(family_CS) );
    PUT( "----------------------------------------\n" );
    PUT( "__ATTRIBUTES______\n" );
    PUT( "flag.lazy_discards        : %d\n",      (int)family_CS->flag.lazy_discards );
    PUT( "max_length                : %lu\n",     family_CS->max_length );
    PUT( "min_length                : %lu\n",     family_CS->min_length );
    PUT( "object_bytes              : %u\n",      family_CS->object_bytes );
    PUT( "header_bytes              : %u\n",      family_CS->header_bytes );
    PUT( "size                      : %d\n",      family_CS->size );
    PUT( "unit_sz_order             : %u\n",      family_CS->unit_sz_order );
    PUT( "__DESCRIPTOR______\n" );
    PUT( "meta.initval.M1           : 0x%llx\n",  desc->meta.initval.M1.bits );
    PUT( "meta.initval.M2           : 0x%llx\n",  desc->meta.initval.M2.bits );
    PUT( "meta.serialized_sz        : %lu\n",     desc->meta.serialized_sz );
    PUT( "unit.sz                   : %lu\n",     desc->unit.sz );
    PUT( "unit.serialized_sz        : %lu\n",     desc->unit.serialized_sz );
    PUT( "obj.sz                    : %u\n",      desc->obj.sz );
    PUT( "obj.serialized_sz         : %lu\n",     desc->obj.serialized_sz );
    PUT( "serialize_line            : %llp\n",    desc->serialize_line );
    PUT( "deserialize_line          : %llp\n",    desc->deserialize_line );
    PUT( "parameter.block_sz        : %llu\n",    desc->parameter.block_sz );
    PUT( "parameter.line_limit      : %lu\n",     desc->parameter.line_limit );
    PUT( "parameter.subdue          : %u\n",      desc->parameter.subdue );
    PUT( "parameter.allow_oversized : %d\n",      desc->parameter.allow_oversized );
    PUT( "parameter.max_allocators  : %d\n",      desc->parameter.max_allocators );
    PUT( "persist.path              : %s\n",      desc->persist.CSTR__path ? CStringValue(desc->persist.CSTR__path) : "(none)" );
    for( int i=0; i<8; i++ ) {
      PUT( "auxiliary.obj[%d]          : %llp\n", i, desc->auxiliary.obj[i] );
    }
    PUT( "----------------------------------------\n" );
    PUT( "family  bytes     : %lldMB\n",  _icxmalloc_shape.FamilyBytes_FCS( family_CS ) >> 20 );
    PUT( "----------------------------------------\n" );
    PUT( "aidx    width              lines  blks  lns/blk               used    ratio     block size     alloc size\n");
    PUT( "---------------------------------------------------------------------------------------------------------\n" );
    //                6:  [  63]  lines:[    278528   17*   16384]  usage:[    266240  95.59%]  block=   64MB  alloc=   64MB

    for( uint16_t aidx=0; aidx<family_CS->size; aidx++ ) {
      if( (allocator = family_CS->allocators[aidx]) == NULL ) {
        empty++;
        continue;
      }
      SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
        cxmalloc_block_t *block_RO = NULL;
        size_t quant = allocator_CS->shape.blockmem.quant;
        alloc_lines = 0;
        used_lines = 0;
        cursor = allocator_CS->blocks;
        while( cursor < allocator_CS->space ) {
          block_RO = *cursor++;
          alloc_lines += quant;
          used_lines += quant - _icxmalloc_block.ComputeAvailable_ARO( block_RO );
        }

        if( allocator_CS->space > allocator_CS->blocks ) {
          PUT( "%4u:  [%4lu]  lines:[%10llu %4llu*%8lu]  usage:[%10llu %6.2f%%]  block=%5lluMB  alloc=%5lluMB\n",
            /*    |      |              |      |     |              |      |               |              |                                */
                  aidx,/*|              |      |     |              |      |               |              |                                */
                         allocator_CS->shape.linemem.awidth,/*      |      |               |              |                                */
                                        alloc_lines,/*              |      |               |              |                                */
                                               allocator_CS->space - allocator_CS->blocks,/*|             |                                */
                                                     quant, /*      |      |               |              |                                */
                                                                    used_lines,/*          |              |                                */
                                                                           100.0*(float)used_lines/alloc_lines, /*                         */
                                                                                           _icxmalloc_shape.BlockBytes_ARO(block_RO) >> 20, /*                  */
                                                                                                          _icxmalloc_shape.AllocatorBytes_ACS(allocator_CS) >> 20
              );
        }
        else {
          empty++;
        }
        // chain info
        cxmalloc_block_t *block_in_chain_RO = *allocator_CS->head;
        PUT( "       chain: " );
        if( allocator_CS->last_reuse ) {
          PUT( "[LR=%u] ", allocator_CS->last_reuse->bidx );
        }
        if( allocator_CS->last_hole ) {
          PUT( "[LH=%u] ", allocator_CS->last_hole->bidx );
        }
        while( block_in_chain_RO ) {
          cxmalloc_bidx_t bidx = block_in_chain_RO->bidx;
          if( block_in_chain_RO->linedata ) {
            PUT( "%u(%llu/%llu) -> ", bidx, _icxmalloc_block.ComputeActive_ARO( block_in_chain_RO ), quant );
          }
          else {
            PUT( "%u(NULL) -> ", bidx );
          }
          block_in_chain_RO = block_in_chain_RO->next_block;
        }
        PUT( "NULL\n" );
      } RELEASE_CXMALLOC_ALLOCATOR;
    }
    if( empty > 0 ) {
      PUT( "empty allocators: %d\n", empty);
    }
    PUT( "=========================================================================================================\n" );
  }
#undef PUT
  return output;
  RESUME_WARNINGS
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_family__compute_active_OPEN( cxmalloc_family_t *family ) {
  int64_t n_active = 0;

  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    if( family_CS && family_CS->flag.ready ) {
      cxmalloc_allocator_t *allocator;
      for( uint16_t aidx=0; aidx<family_CS->size; aidx++ ) {
        if( (allocator = family_CS->allocators[aidx]) != NULL ) {
          SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
            cxmalloc_block_t **cursor_RO = allocator_CS->blocks;
            while( cursor_RO < allocator_CS->space ) {
              cxmalloc_block_t *block_RO = *cursor_RO++;
              if( block_RO->linedata ) {
                n_active += _icxmalloc_block.ComputeActive_ARO( block_RO );
              }
            }
          } RELEASE_CXMALLOC_ALLOCATOR;
        }
      }
    }
  } RELEASE_CXMALLOC_FAMILY;

  return n_active;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_family__active_OPEN( cxmalloc_family_t *family ) {
  int64_t n_active = 0;

  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    if( family_CS && family_CS->flag.ready ) {
      cxmalloc_allocator_t *allocator;
      for( uint16_t aidx=0; aidx<family_CS->size; aidx++ ) {
        if( (allocator = family_CS->allocators[aidx]) != NULL ) {
          SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
            cxmalloc_block_t **cursor_RO = allocator_CS->blocks;
            while( cursor_RO < allocator_CS->space ) {
              cxmalloc_block_t *block_RO = *cursor_RO++;
              n_active += block_RO->capacity - block_RO->available;
            }
          } RELEASE_CXMALLOC_ALLOCATOR;
        }
      }
    }
  } RELEASE_CXMALLOC_FAMILY;

  return n_active;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static double __cxmalloc_family__utilization_OPEN( cxmalloc_family_t *family ) {
  int64_t n_capacity = 0;
  int64_t n_available = 0;
  IGNORE_WARNING_UNBALANCED_LOCK_RELEASE
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    if( family_CS && family_CS->flag.ready ) {
      cxmalloc_allocator_t *allocator;
      for( uint16_t aidx=0; aidx<family_CS->size; aidx++ ) {
        if( (allocator = family_CS->allocators[aidx]) != NULL ) {
          SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
            cxmalloc_block_t **cursor_RO = allocator_CS->blocks;
            while( cursor_RO < allocator_CS->space ) {
              cxmalloc_block_t *block_RO = *cursor_RO++;
              n_capacity += block_RO->capacity;
              n_available += block_RO->available;
            }
          } RELEASE_CXMALLOC_ALLOCATOR;
        }
      }
    }
  } RELEASE_CXMALLOC_FAMILY;

  if( n_capacity > 0 ) {
    return (n_capacity - n_available) / (double)n_capacity;
  }
  else {
    return 0.0;
  }
  RESUME_WARNINGS
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_family__validate_refcounts_FCS( cxmalloc_family_t *family_CS, cxmalloc_linehead_t **bad_linehead ) {
  int64_t total_refcnt = 0;

  // Validate all allocators
  for( int aidx=0; aidx < family_CS->size; aidx++ ) {
    cxmalloc_allocator_t *allocator = family_CS->allocators[ aidx ];
    if( allocator ) {
      int64_t allocator_refcnt = 0;
      SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
        allocator_refcnt = _icxmalloc_allocator.ValidateRefcounts_ACS( allocator_CS, bad_linehead );
      } RELEASE_CXMALLOC_ALLOCATOR;
      if( allocator_refcnt < 0 ) {
        CXMALLOC_CRITICAL( 0x731, "Bad allocator in family '%s'", family_CS->obid.longstring.string );
        total_refcnt = -1;
        break;
      }
      total_refcnt += allocator_refcnt;
    }
  }

  // OK!
  return total_refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_family__get_object_at_address_FCS( cxmalloc_family_t *family_CS, QWORD address ) {
  comlib_object_t *obj = NULL;
  int aidx = 0;
  cxmalloc_allocator_t *allocator;
  while( obj == NULL && aidx < family_CS->size ) {
    if( (allocator = family_CS->allocators[ aidx++ ]) != NULL ) {
      SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
        obj = _icxmalloc_allocator.GetObjectAtAddress_ACS( allocator_CS, address );
      } RELEASE_CXMALLOC_ALLOCATOR;
    }
  }
  return obj;      
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_family__find_object_by_obid_FCS( cxmalloc_family_t *family_CS, objectid_t obid ) {
  comlib_object_t *obj = NULL;
  int aidx = 0;
  cxmalloc_allocator_t *allocator;
  while( obj == NULL && aidx < family_CS->size ) {
    if( (allocator = family_CS->allocators[ aidx++ ]) != NULL ) {
      SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
        obj = _icxmalloc_allocator.FindObjectByObid_ACS( allocator_CS, obid );
      } RELEASE_CXMALLOC_ALLOCATOR;
    }
  }
  return obj;     
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static comlib_object_t * __cxmalloc_family__get_object_by_offset_FCS( cxmalloc_family_t *family_CS, int64_t *poffset ) {
  static __THREAD bool rseed = false;
  comlib_object_t *obj = NULL;
  cxmalloc_allocator_t *allocator;
  int aidx;

  IGNORE_WARNING_UNBALANCED_LOCK_RELEASE
  // Get total objects in family
  int64_t n_active = 0;
  aidx = 0;
  while( aidx < family_CS->size ) {
    if( (allocator = family_CS->allocators[ aidx++ ]) != NULL ) {
      SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
        n_active += allocator_CS->n_active;
      } RELEASE_CXMALLOC_ALLOCATOR;
    }
  }

  if( n_active > 0 ) {
    int64_t offset;
    // Offset
    if( poffset ) {
      if( (offset = *poffset) < 0 ) {
        offset += n_active;
      }
    }
    // Pick a random object number
    else {
      if( !rseed ) {
        rseed = true;
        __lfsr63( (uint64_t)&rseed );
      }
      int64_t r = rand63();
      offset = r % n_active;
    }

    // Seek to the allocator containing the selected object number
    if( offset >= 0 && offset < n_active ) {
      aidx = 0;
      int64_t n_scan = 0;
      while( obj == NULL && aidx < family_CS->size ) {
        if( (allocator = family_CS->allocators[ aidx++ ]) != NULL ) {
          SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
            int64_t n_last = n_scan;
            n_scan += allocator_CS->n_active;
            // This allocator contains the selected object number
            if( offset < n_scan ) {
              int64_t i = offset - n_last;
              obj = _icxmalloc_allocator.GetObject_ACS( allocator_CS, i );
            }
          } RELEASE_CXMALLOC_ALLOCATOR;
        }
      }
    }
  }

  return obj;
  RESUME_WARNINGS
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __cxmalloc_family__sweep_OPEN( cxmalloc_family_t *family, f_get_object_identifier get_object_identifier ) {
  int64_t n_fix = 0;
  int64_t n = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    for( int aidx=0; aidx < family_CS->size; aidx++ ) {
      cxmalloc_allocator_t *allocator_FCS = family_CS->allocators[ aidx ];
      if( allocator_FCS ) {
        SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator_FCS ) {
          n = _icxmalloc_allocator.Sweep_FCS_ACS( allocator_FCS_CS, get_object_identifier );
        } RELEASE_CXMALLOC_ALLOCATOR;
        if( n < 0 ) {
          n_fix = -1;
          break;
        }
        n_fix += n;
      }
    }
  } RELEASE_CXMALLOC_FAMILY;
  return n_fix;
}
