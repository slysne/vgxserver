/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxoballoc_vertex.c
 * Author:  Stian Lysne <...>
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

#include "_vxoballoc_vertex.h"
#include "_vxoballoc_vector.h"
#include "_vxoballoc_cstring.h"

#include "_vgx.h"
#include "_vxevent.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static const int8_t vertex_index_order = 9;
static const vgx_mapping_spec_t vertex_index_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_128BIT | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;


static int __vxoballoc_vertex__cxmalloc_serialize_vertex( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_vertex__cxmalloc_deserialize_vertex( cxmalloc_line_deserialization_context_t *context );

const QWORD NO_HANDLE = 0xFFFFFFFFFFFFFFFFULL;


#define PARENT_GRAPH_AUX_IDX        0
#define MAIN_INDEX_AUX_IDX          1
#define TYPE_INDEX_AUX_IDX          2


static const size_t allocator_line_size = sizeof(cxmalloc_linehead_t) + sizeof(vgx_VertexHead_t) + sizeof(vgx_VertexData_t);


#define SZ_META_SERIALIZED    sizeof( objectid_t )
#define SZ_OBJ_SERIALIZED     2 * sizeof( QWORD )
#define SZ_ARRAY_SERIALIZED   14 * sizeof( QWORD )


/*******************************************************************//**
 *
 * Vertex Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t VertexAllocatorDescriptor( size_t block_items, const CString_t *CSTR__persist_path, vgx_Graph_t *parent_graph, framehash_t *index, framehash_t **typeindex  ) {

  size_t block_size = block_items * allocator_line_size;

  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},
      .serialized_sz    = SZ_META_SERIALIZED
    },
    .obj = {
      .sz               = sizeof(vgx_VertexHead_t),   /* space for the class header and extras */
      .serialized_sz    = SZ_OBJ_SERIALIZED           // operation + descriptor
    },
    .unit = {
      .sz               = sizeof( vgx_VertexData_t ), /* the only "data" part of the allocator, single unit. The allocator header holds vgx_VertexHead_t as "extras". */
      .serialized_sz    = SZ_ARRAY_SERIALIZED         // see serialization routine for details
    },
    .serialize_line     = __vxoballoc_vertex__cxmalloc_serialize_vertex,
    .deserialize_line   = __vxoballoc_vertex__cxmalloc_deserialize_vertex,
    .parameter = {
      .block_sz         = block_size,                 /*                      */
      .line_limit       = 2,                          /* aidx=2 => size=1     */
      .subdue           = 0,                          /* S=0 =>   0:0,  1:0,  2:1  (NOTE: since slot is larger than 1CL, it gets tricky) */
      .allow_oversized  = 0,                          /* disallow oversized   */
      .max_allocators   = 3                           /* aidx 0 - 2, only "2" will be used, has to do with the slot being > 1CL */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path          /*                      */
    },
    .auxiliary = {
      parent_graph,             /* 0: (vgx_Graph_t*)          */
      index,                    /* 1: (framehash_t*)          */
      typeindex,                /* 2: (framehash_t**)         */
      NULL                      /* -- END __                  */
    }
  };

  return descriptor;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vertex__vertex_block_order( cxmalloc_family_t * vertex_allocator );
static cxmalloc_family_t * __vxoballoc_vertex__new_allocator( vgx_Graph_t *graph, int vertex_block_order, framehash_t **pindex, framehash_t ***ptypeindices );
static void __vxoballoc_vertex__delete_allocator( vgx_Graph_t *graph );
static void __vxoballoc_vertex__set_for_current_thread( cxmalloc_family_t *vertex_allocator );
static cxmalloc_family_t * __vxoballoc_vertex__get_for_current_thread( void );
static int64_t __vxoballoc_vertex__verify( vgx_Graph_t *graph );

static vgx_Vertex_t * __vxoballoc_vertex__new_vertex( vgx_Graph_t *graph, objectid_t *obid );
static void __vxoballoc_vertex__delete_vertex( vgx_Graph_t *graph, vgx_Vertex_t *vertex, void (*deconstruct)( vgx_Vertex_t *vertex ) );

static cxmalloc_handle_t __vxoballoc_vertex__vertex_as_handle( const vgx_Vertex_t *vertex );
static vgx_Vertex_t * __vxoballoc_vertex__vertex_from_handle_current_thread_allocator( const cxmalloc_handle_t handle );
static vgx_AllocatedVertex_t * __vxoballoc_vertex__as_allocated_vertex( const vgx_Vertex_t *vertex );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IVertexAllocator_t ivertexalloc = {
  .BlockOrder = __vxoballoc_vertex__vertex_block_order,
  .New        = __vxoballoc_vertex__new_allocator,
  .Delete     = __vxoballoc_vertex__delete_allocator,
  .SetCurrent = __vxoballoc_vertex__set_for_current_thread,
  .GetCurrent = __vxoballoc_vertex__get_for_current_thread,
  .Verify     = __vxoballoc_vertex__verify
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IVertexObject_t ivertexobject = {
  .New                = __vxoballoc_vertex__new_vertex,
  .Delete             = __vxoballoc_vertex__delete_vertex,
  .AsHandle           = __vxoballoc_vertex__vertex_as_handle,
  .FromHandleNolock   = __vxoballoc_vertex__vertex_from_handle_current_thread_allocator,
  .AsAllocatedVertex  = __vxoballoc_vertex__as_allocated_vertex
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __THREAD cxmalloc_family_t *gt_CURRENT_THREAD_VERTEX_ALLOCATOR = NULL;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vertex__vertex_block_order( cxmalloc_family_t * vertex_allocator ) {
  return imag2( vertex_allocator->descriptor->parameter.block_sz / allocator_line_size );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vertex__new_allocator( vgx_Graph_t *graph, int vertex_block_order, framehash_t **pindex, framehash_t ***ptypeindices ) {
  const char *dirname = VGX_PATHDEF_VERTEX_DATA_DIRNAME;
  cxmalloc_family_t *alloc = NULL;
  CString_t *CSTR__home = NULL;

  // Maximum vertex objects per block is determined by the maximum cstring length.
  // We use cstring as the container of block bitvectors.
  const size_t max_vertex_per_block = (1ULL << ilog2( (_VXOBALLOC_CSTRING_MAX_LENGTH * 4ULL) / 1000ULL )) * 1000ULL;

  XTRY {

    char strbuf[OBJECTID_LONGSTRING_MAX+1]; 

    if( vertex_block_order < 10 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x141, "Vertex block order must be at least 10, got %d", vertex_block_order );
    }

    size_t block_items = (1ULL << vertex_block_order);
    if( block_items > max_vertex_per_block ) {
      block_items = max_vertex_per_block;
    }

    const char *graphname = CStringValue( CALLABLE(graph)->Name(graph) );
    if( (CSTR__home = CStringNewFormat( "%s/%s[%s]", CALLABLE(graph)->FullPath(graph), dirname, graphname )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x142 );
    }

    cxmalloc_descriptor_t desc = VertexAllocatorDescriptor( block_items, CSTR__home, graph, NULL, NULL );

    cxmalloc_family_constructor_args_t args = {
      .family_descriptor  = &desc  //
    };

    snprintf( strbuf, OBJECTID_LONGSTRING_MAX, "VertexAllocator(graph=%s)", CStringValue(graph->CSTR__name) );
    if( (alloc = COMLIB_OBJECT_NEW( cxmalloc_family_t, strbuf, &args )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x143 );
    }

    if( graph ) {
      // Build index from restored allocator objects
      if( pindex ) {
        CString_t *CSTR__prefix = NewEphemeralCString( graph, "vxtable" );
        if( CSTR__prefix ) {
          CString_t *CSTR__table_name = iString.Utility.NewGraphMapName( graph, CSTR__prefix );
          if( CSTR__table_name ) {
            *pindex = iMapping.NewMap( NULL, CSTR__table_name, MAPPING_SIZE_UNLIMITED, vertex_index_order, vertex_index_spec, CLASS_NONE );
            CStringDelete( CSTR__table_name );
          }
          CStringDelete( CSTR__prefix );
        }
        if( *pindex == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x144 );
        }
        alloc->descriptor->auxiliary.obj[ MAIN_INDEX_AUX_IDX ] = *pindex;
      }
      // Build type index from restored allocator objects
      if( ptypeindices ) {
        // Create the type index directory entries. Will create as needed during indexing scan later.
        if( (*ptypeindices = (framehash_t**)calloc( VERTEX_TYPE_ENUMERATION_MAX_ENTRIES, sizeof( framehash_t* ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x145 );
        }
        alloc->descriptor->auxiliary.obj[ TYPE_INDEX_AUX_IDX ] = *ptypeindices;
      }
    }

#ifndef NDEBUG
    COMLIB_OBJECT_PRINT( alloc );
#endif

  }
  XCATCH( errcode ) {
    if( alloc ) {
      //TODO: any clean up?
      COMLIB_OBJECT_DESTROY( alloc );
      alloc = NULL;
    }
  }
  XFINALLY {
    if( CSTR__home ) {
      CStringDelete( CSTR__home );
      CSTR__home = NULL;
    }
  }

  return alloc;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vertex__delete_allocator( vgx_Graph_t *graph ) {
  if( graph && graph->vertex_allocator ) {
    COMLIB_OBJECT_DESTROY( graph->vertex_allocator );
    graph->vertex_allocator = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vertex__set_for_current_thread( cxmalloc_family_t *vertex_allocator ) {
  gt_CURRENT_THREAD_VERTEX_ALLOCATOR = vertex_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vertex__get_for_current_thread( void ) {
  return gt_CURRENT_THREAD_VERTEX_ALLOCATOR;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vertex__verify( vgx_Graph_t *graph ) { 
  vgx_Vertex_vtable_t *iV = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );
  return CALLABLE( graph->vertex_allocator )->Sweep( graph->vertex_allocator, (f_get_object_identifier)iV->IDString );
}



/*******************************************************************//**
 * Do the internal work for vertex allocation and basic initialization
 * here to hide the details from the constructor. We do this because the
 * cxmalloc allocator stuff is a bit ugly to deal with. The reason is the
 * particular way the vertex allocator is configured, which is special.
 * The vgx_VertexHead_t structure is separated from the vgx_VertexData_t
 * structure since we use the allocator line header to store the former
 * and exactly 1 unit of allocator data to store the latter.
 *
 * vgx_Vertex_t is a combination of vgx_VertexHead_t and vgx_VertexData_t.
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __vxoballoc_vertex__new_vertex( vgx_Graph_t *graph, objectid_t *obid ) {
  cxmalloc_family_t *alloc = graph->vertex_allocator;
  cxmalloc_family_vtable_t *ialloc = CALLABLE(alloc);
  // The vertex
  vgx_Vertex_t *vertex = NULL;
  // Part of allocator object header
  vgx_VertexHead_t *vtxobj;
  // Single allocation unit 
  vgx_VertexData_t *vtxdata;
 
  // Perform allocation of one unit
  if( (vtxdata = (vgx_VertexData_t*)ialloc->New(alloc,1)) != NULL ) {
    // Initialize the header to make it a proper Vertex object whose pointer we will return
    if( (vtxobj = (vgx_VertexHead_t*)ialloc->ObjectFromArray(alloc, vtxdata)) != NULL ) {
      //
      // |                         CXMALLOC HEADER                             |    CXMALLOC ALLOCATION UNIT (1)
      // | ----------- linehead ---------- | ---------- extra bytes ---------- | 
      // |     metas      |    internals   |          vgx_VertexHead_t         |    vgx_VertexData_t
      // +----------------+----------------+--------+--------+--------+--------+---------------------------- .....
      // |   128-bit ID   |  (allocator)   | vtable | obtype |   op   | descr. |
      // +----------------+----------------+--------+--------+--------+--------+---------------------------- .....
      // ^                                 ^ 
      // |                                 | --------------------------- vgx_Vertex_t ---------------------- .....
      // |\                 vgx_Vertex_t *vertex
      // | \---<<<---getid()---<<<--------/
      // ^
      // | ------------------------------------ vgx_AllocatedVertex_t -------------------------------------- .....
      // |
      //  \<-- copy obid into here
      //

      // Align the vertex pointer to start at vertex head
      vertex = (vgx_Vertex_t*)vtxobj;

      // initialize comlib ID
      if( COMLIB_OBJECT_INIT( vgx_Vertex_t, vertex, obid ) == NULL ) {
        ialloc->DiscardObject( alloc, vertex );
        return NULL;
      }

      // Set all vertex data to zero
      memset( vtxdata, 0, sizeof(vgx_VertexData_t) );
      
      // Backreference parent graph
      vertex->graph = graph;
    }
  }

  // What is done at this point:
  // - One vertex has been allocated
  // - ID has been set to supplied obid
  // - parent graph backreference has been set
  // - all other data and properties are zero

  return vertex;
}



/*******************************************************************//**
 * Deleting a vertex object will call decref repeatedly until no more
 * references exist.
 *
 ***********************************************************************
 */
static void __vxoballoc_vertex__delete_vertex( vgx_Graph_t *graph, vgx_Vertex_t *vertex, void (*deconstruct)( vgx_Vertex_t *vertex ) ) {
  cxmalloc_family_t *alloc = graph->vertex_allocator;
  cxmalloc_family_vtable_t *ialloc = CALLABLE( alloc );
  int64_t refcnt = ialloc->RefCountObject( alloc, vertex );

  // Remove all but the last reference
  while( refcnt-- > 1 ) {
    ialloc->DiscardObject( alloc, vertex );
  }

  // Deconstruct and discard final reference
  if( refcnt == 1 ) {
    if( deconstruct ) {
      deconstruct( vertex );
    }
    ialloc->DiscardObject( alloc, vertex );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_handle_t __vxoballoc_vertex__vertex_as_handle( const vgx_Vertex_t *vertex ) {
  return _vxoballoc_vertex_as_handle( vertex );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __trap_invalid_handle_class( const cxmalloc_handle_t handle ) {
  FATAL( 0xFFF, "Invalid vgx_Vertex_t class code in handle 0x016X. Got class 0x%02X, expected 0x%02X.", handle.qword, handle.objclass, COMLIB_CLASS_CODE( vgx_Vertex_t ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vertex_t * __vxoballoc_vertex__vertex_from_handle_current_thread_allocator( const cxmalloc_handle_t handle ) {
  cxmalloc_family_vtable_t *iAlloc = gt_CURRENT_THREAD_VERTEX_ALLOCATOR ? CALLABLE( gt_CURRENT_THREAD_VERTEX_ALLOCATOR ) : NULL;
  if( handle.objclass != COMLIB_CLASS_CODE( vgx_Vertex_t ) ) {
    __trap_invalid_handle_class( handle );
  }
  vgx_Vertex_t *vertex = NULL;
  // NOTE: the vertex object may not be active yet if its allocator has not yet been restored. The vertex address is correct.
  if( iAlloc && (vertex = iAlloc->HandleAsObjectNolock( gt_CURRENT_THREAD_VERTEX_ALLOCATOR, handle )) != NULL ) {
    iAlloc->OwnObjectNolock( gt_CURRENT_THREAD_VERTEX_ALLOCATOR, vertex );
    return vertex;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_AllocatedVertex_t * __vxoballoc_vertex__as_allocated_vertex( const vgx_Vertex_t *vertex ) {
  return (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( vertex );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vertex__cxmalloc_serialize_vertex( cxmalloc_line_serialization_context_t *context ) {

  int64_t n;
  QWORD *cursor;

  vgx_Vertex_t *self = (vgx_Vertex_t*)_cxmalloc_object_from_linehead( context->linehead );

  // --------------
  // cxmalloc metas
  // [2 QW]
  // --------------
  // [1] ID -> (1 + 2)
  idcpy( (objectid_t*)context->tapout.line_meta, (objectid_t*)COMLIB_OBJECT_GETID(self) );

  // --------------------------------------
  // cxmalloc obj
  // [2 QW] (i.e. the vgx_VertexHead_t)
  // --------------------------------------
  cursor = context->tapout.line_obj;
  // [5] operation -> (3)
  *cursor++ = (QWORD)iOperation.GetId_LCK( &self->operation );
  // [6] descriptor -> (4)
  //     NOTE: Event schedule flag is NOT persisted
  *cursor++ = self->descriptor.bits & VERTEX_DESCRIPTOR_NON_EPHEMERAL_DATA_MASK;

  // -----------------------------------
  // cxmalloc array
  // [13 QW] (i.e. the vgx_VertexData_t
  // -----------------------------------
  cursor = context->tapout.line_array;

  // vector [1 QW]
  QWORD vector_handle_qword;
  if( self->vector ) {
    vector_handle_qword = ivectorobject.AsHandle( self->vector ).qword;
  }
  else {
    vector_handle_qword = NO_HANDLE; // no vector
  }
  // [8] vector -> (5)
  *cursor++ = vector_handle_qword;
  
  // rank [1 QW]
  // [9] rank -> (6)
  *cursor++ = self->rank.bits;  

  // inarcs [1 QW]
  // TODO: consider putting empty arc or simple arc directly in the main block (avoid using ext data when not necessary)
  // [10] inarcs -> (7)
  *cursor++ = context->ext_offset;
  if( (n = iarcvector.Serialize( &self->inarcs, context->out_ext )) < 0 ) {
    return -1;
  }
  context->ext_offset += n;

  // outarcs [1 QW]
  // TODO: consider putting empty arc or simple arc directly in the main block (avoid using ext data when not necessary)
  // [11] outarcs -> (8)
  *cursor++ = context->ext_offset;
  if( (n = iarcvector.Serialize( &self->outarcs, context->out_ext )) < 0 ) {
    return -1;
  }
  context->ext_offset += n;

  // properties [1 QW]
  // [12] properties -> (9)
  *cursor++ = context->ext_offset;
  if( (n = _vxvertex_property__serialize_RO_CS( self, context->out_ext )) < 0 ) {
    return -1;
  }
  context->ext_offset += n;

  // identifier [5 QW]
  // [13] idprefix -> (10 - 14)
  memcpy( cursor, self->identifier.idprefix.data, sizeof( vgx_VertexIdentifierPrefix_t ) );
  cursor += qwsizeof( vgx_VertexIdentifierPrefix_t );
  // [14] idstring -> (15)
  // long identifier CString reference
  QWORD cstring_handle_qword;
  if( self->identifier.CSTR__idstr ) {
    cstring_handle_qword = icstringobject.AsHandle( self->identifier.CSTR__idstr ).qword;
  }
  else {
    cstring_handle_qword = NO_HANDLE;
  }
  *cursor++ = cstring_handle_qword;

  // TMX
  // [15] TMX -> (16)
  *cursor++ = self->TMX.bits;

  // TMC
  // [16] TMC -> (17)
  *cursor++ = self->TMC;

  // TMM
  // [17] TMM -> (18)
  *cursor = self->TMM;

#ifdef VGX_CONSISTENCY_CHECK
#ifdef CXMALLOC_CONSISTENCY_CHECK
  char internalid[33];
  char prop_str[9];
  char state_str[9];
  objectid_t obid = CALLABLE( self )->InternalID( self );
  idtostr( internalid, &obid );
  uint8_to_bin( prop_str, self->descriptor.property.bits );
  uint8_to_bin( state_str, self->descriptor.state.bits );
  const char *name = CALLABLE( self )->IDString( self );
  fprintf( context->objdump, "<vgx_Vertex_t internalid=%s type=%02x property=%s state=%s sem=%-3d ideg=%-6lld odeg=%-6lld tmx_arc=%u tmx_vertex=%u> %s", internalid, self->descriptor.type.enumeration, prop_str, state_str,
    self->descriptor.semaphore.count, iarcvector.Degree( &self->inarcs ), iarcvector.Degree( &self->outarcs ), self->TMX.arc_ts, self->TMX.vertex_ts, name );
#endif
#endif

  // success
  return 0;

  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static framehash_t * __vxoballoc_vertex__new_typeindex( vgx_Graph_t *graph, vgx_Vertex_t *vertex ) {
  framehash_t *index = NULL;
  vgx_vertex_type_t vxtype = vertex->descriptor.type.enumeration;
  CString_t *CSTR__prefix = NULL;
  CString_t *CSTR__table_name = NULL;
  if( (CSTR__prefix = CStringNewFormat( "vxtable_type_%02X", vxtype )) != NULL ) {
    if( (CSTR__table_name = iString.Utility.NewGraphMapName( graph, CSTR__prefix )) != NULL ) {
      index = iMapping.NewMap( NULL, CSTR__table_name, MAPPING_SIZE_UNLIMITED, vertex_index_order, vertex_index_spec, CLASS_NONE );
      CStringDelete( CSTR__table_name );
    }
    CStringDelete( CSTR__prefix );
  }
  return index;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vertex__index( vgx_Vertex_t *vertex, framehash_t *mainidx, framehash_t *typeidx ) {
  objectid_t *pobid = __vertex_internalid( vertex );
  int *refc = &_cxmalloc_linehead_from_object( vertex )->data.refc;
  if( mainidx ) {
    if( CALLABLE( mainidx )->SetObj128Nolock( mainidx, pobid, COMLIB_OBJECT( vertex ) ) != CELL_VALUE_TYPE_OBJECT128 ) {
      return -1;
    }
    ++(*refc);
    __vertex_set_indexed_main( vertex );
  }
  if( typeidx ) {
    if( CALLABLE( typeidx )->SetObj128Nolock( typeidx, pobid, COMLIB_OBJECT( vertex ) ) != CELL_VALUE_TYPE_OBJECT128 ) {
      return -1;
    }
    ++(*refc);
    __vertex_set_indexed_type( vertex );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vertex__cxmalloc_deserialize_vertex( cxmalloc_line_deserialization_context_t *context ) {
  int ret = 0;
  vgx_Graph_t *graph = (vgx_Graph_t*)context->auxiliary[ PARENT_GRAPH_AUX_IDX ];
  framehash_t *mainindex = (framehash_t*)context->auxiliary[ MAIN_INDEX_AUX_IDX ];
  framehash_t **typeindices = (framehash_t**)context->auxiliary[ TYPE_INDEX_AUX_IDX ];
  framehash_t *typeindex = NULL;

  // The vertex's memory is already allocated internally in cxmalloc, find the object address and cast it to a vertex pointer.
  vgx_Vertex_t *self = (vgx_Vertex_t*)_cxmalloc_object_from_linehead( context->linehead );

  XTRY {
    QWORD *cursor;
    int64_t n;
    vgx_VertexDescriptor_t src_desc;

    // cxmalloc metas
    // (1+2) -> [1] obid
    objectid_t *pobid = (objectid_t*)context->tapin.line_meta;

    // cxmalloc obj
    COMLIB_OBJECT_INIT( vgx_Vertex_t, self, pobid ); 
    cursor = context->tapin.line_obj;
    // (3) -> [5] operation
    iOperation.InitId( &self->operation, (int64_t)*cursor++ );  // clean after init
    // (4) -> [6] descriptor
    src_desc.bits = *cursor++;
    self->descriptor.bits = 0;
    self->descriptor.property.bits = src_desc.property.bits;
    self->descriptor.state.context = src_desc.state.context;
    self->descriptor.type.enumeration = src_desc.type.enumeration;

    // [7] PARENT GRAPH
    self->graph = graph;

    // cxmalloc array
    cursor = context->tapin.line_array;
    // (5) -> [8] vector handle
    cxmalloc_handle_t vector_handle;
    if( (vector_handle.qword = *cursor++) != NO_HANDLE ) {
      // Convert handle to pointer and incref
      self->vector = ivectorobject.FromHandleNolock( vector_handle, graph->similarity->int_vector_allocator );
      // Increment global counter
      IncGraphVectorCount( self->graph );
    }
    else {
      self->vector = NULL;
    }

    // (6) -> [9] rank
    self->rank.bits = *cursor++;

    // (7) -> [10] inarcs ext offset (verify correct offset)
    if( context->ext_offset != (int64_t)*cursor++ ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x151 );
    }
    // rebuild inarcs from stored data
    if( (n = iarcvector.Deserialize( self, &graph->arcvector_fhdyn, context->family, &self->inarcs, context->in_ext )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x152 );
    }
    context->ext_offset += n;

    // (8) -> [11] outarcs ext offset (verify correct offset)
    if( context->ext_offset != (int64_t)*cursor++ ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x153 );
    }
    // rebuild outarcs from stored data
    if( (n = iarcvector.Deserialize( self, &graph->arcvector_fhdyn, context->family, &self->outarcs, context->in_ext )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x154 );
    }
    context->ext_offset += n;
    // increment graph size by the outdegree
    int64_t odeg = iarcvector.Degree( &self->outarcs );
    AddGraphSize( graph, odeg );
#ifndef NDEBUG
    AddGraphRevSize( graph, odeg );
#endif

    // (9) -> [12] properties ext offset
    if( context->ext_offset != (int64_t)*cursor++ ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x155 );
    }
    // rebuild properties from stored data
    if( (n = _vxvertex_property__deserialize_WL_CS( self, context->in_ext )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x156 );
    }
    context->ext_offset += n;

    // identifier [5 QW]
    // (10-14) -> [13]
    memcpy( self->identifier.idprefix.data, cursor, sizeof( vgx_VertexIdentifierPrefix_t ) );
    cursor += qwsizeof( vgx_VertexIdentifierPrefix_t );
    // (15) -> [14] long identifier CString reference
    cxmalloc_handle_t cstring_handle;
    if( (cstring_handle.qword = *cursor++) != NO_HANDLE ) {
      // Convert handle to pointer and incref
      self->identifier.CSTR__idstr = icstringobject.FromHandleNolock( cstring_handle, graph->property_allocator_context );
    }
    else {
      self->identifier.CSTR__idstr = NULL;
    }

    // TMX
    // (16) -> [15] TMX
    self->TMX.bits = *cursor++;

    // TMC
    // (17) -> [16] TMC
    self->TMC = (uint32_t)*cursor++;

    // TMM
    // (18) -> [17] TMM
    self->TMM = (uint32_t)*cursor++;

    // Feed into event processor input queue if vertex has TTL
    uint32_t tmx = __vertex_get_min_expiration_ts( self );
    if( tmx > TIMESTAMP_MIN && tmx < TIME_EXPIRES_NEVER ) {
      // Only enqueue events if the EVP daemon exists
      if( iGraphEvent.IsReady( graph ) ) {
        vgx_VertexStorableEvent_t ev = __get_vertex_expiration_event( self, tmx );
        if( APPEND_QUEUE_VERTEX_EVENT_NOLOCK( graph->EVP.PublicAPI.Queue, &ev ) ) {
          __vertex_set_event_scheduled( self );
          Vertex_INCREF_WL( self );
          if( LENGTH_QUEUE_VERTEX_EVENTS( graph->EVP.PublicAPI.Queue ) > 0xffff ) {
            iGraphEvent.Schedule( graph );
          }
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x157 );
        }
      }
    }
    else {
      __vertex_all_clear_expiration( self );
    }

    // Get the type index from set of type indices (create new if not yet created)
    if( typeindices ) {
      vgx_vertex_type_t vxtype = self->descriptor.type.enumeration;
      if( (typeindex = typeindices[ vxtype ]) == NULL ) {
        typeindices[ vxtype ] =__vxoballoc_vertex__new_typeindex( graph, self );
        if( (typeindex = typeindices[ vxtype ]) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x158 );
        }
      }
    }

    // Index vertex
    if( __vxoballoc_vertex__index( self, mainindex, typeindex ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x159 );
    }

    // One more vertex in graph
    IncGraphOrder( graph );

  }
  XCATCH( errcode ) {
    if( self->vector ) {
      cxmalloc_family_t *valloc = graph->similarity->int_vector_allocator;
      CALLABLE( valloc )->DiscardObject( valloc, self->vector );
      self->vector = NULL;
    }

    ret = -1;
    // TODO: I don't feel like implementing these things just yet.
    //       If we fail here the whole thing can't start anyway.
    //       But in the future when we do more than bulk restore, we should
    //       be able to fail gracefully here without leaking anything.
    // TODO: drop all inarcs if any
    // TODO: drop all outarcs if any
    // TODO: drop all properties if any

  }
  XFINALLY {
  }

  // success
  return ret;
}



DLL_EXPORT void __trap_invalid_vertex_CS_RO( const vgx_Vertex_t *vertex_CS_RO ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex_CS_RO );
  if( linehead->data.refc == 0 || __vertex_is_unindexed( vertex_CS_RO ) ) {
    PRINT_VERTEX( vertex_CS_RO );
    FATAL( 0xFFF, "Invalid vertex." );
  }
}



#ifdef VGX_CONSISTENCY_CHECK
DLL_EXPORT int64_t __check_vertex_consistency_WL( vgx_Vertex_t *vertex_WL ) {
  // Actual refcnt
  int64_t actual_refcnt = Vertex_REFCNT_WL( vertex_WL );

  // Check vertex unless defunct
  if( !__vertex_is_defunct( vertex_WL ) ) {
    // Compute expected refcnt based on vertex properties
    int64_t expected_refcnt = 0;

    // The main index owns 1 ref
    if( __vertex_is_indexed_main( vertex_WL ) ) {
      ++expected_refcnt;
    }

    // The type index owns 1 ref
    if( __vertex_is_indexed_type( vertex_WL ) ) {
      ++expected_refcnt;
    }

    // The event processor owns 1 ref
    if( __vertex_has_event_scheduled( vertex_WL ) ) {
      ++expected_refcnt;
      // Expect at least one of the TMX timestamps to be set
      if( !__vertex_has_any_expiration( vertex_WL ) ) {
        FATAL( 0xFFF, "vertex '%s' has event scheduled flag but no expiration timestamps", CALLABLE( vertex_WL )->IDString( vertex_WL ) );
      }
    }
    else {
      // Expect none of the TMX timestamps to be set if graph has event processor
      if( iGraphEvent.IsReady( vertex_WL->graph ) && __vertex_has_any_expiration( vertex_WL ) ) {
        FATAL( 0xFFF, "vertex '%s' has no event scheduled flag but has expiration timestamps(s): vertex_tmx=%u arc_tmx=%u", CALLABLE( vertex_WL )->IDString( vertex_WL ), vertex_WL->TMX.vertex_ts, vertex_WL->TMX.arc_ts );
      }
    }

    // Lock recursion
    int semcnt = __vertex_get_semaphore_count( vertex_WL );
    if( semcnt > 0 ) {
      // Each lock recursion owns 1 ref
      expected_refcnt += semcnt;
      // It should be locked
      if( !__vertex_is_locked( vertex_WL ) ) {
        FATAL( 0xFFF, "vertex lock flag not set" );
        return -1;
      }
    }

    // Busy inarcs
    if( __vertex_is_borrowed_inarcs_busy( vertex_WL ) ) {
      ++expected_refcnt;
    }

    // Each arc adds 1 ref
    if( !__vertex_is_isolated( vertex_WL ) ) {

      // PROBLEM: When we have forward-only arcs, how do we know what the expected refcount for a vertex is?
      //

      expected_refcnt += CALLABLE( vertex_WL )->Degree( vertex_WL );
    }

    if( actual_refcnt != expected_refcnt ) {

      // FORGET ABOUT THIS CHECK UNTIL WE FIGURE OUT HOW TO DEAL WITH THE REFCOUNT SITUATION
      // for forward-only arcs. The tail node doesn't get the incref from the reverse arc,
      // so the degree value can't be used to calculate the expected refcount. And if we
      // artificially incref the tail when creating a forward-only arc, then we have no way
      // of restoring that refcount when loading a saved graph, since the loading of outarcs
      // doesn't check what kind of arc is loaded and therefore can't know whether to
      // artificially incref the tail vertex. We have added the "XFWD" modifier bit to 
      // predicators which may be used by the deserializer to incref the tail one extra inc
      // for each deserialized forward-only arc. For now, disable the refcount check.

      bool refcount_check_disabled_because_of_fwdonly_arcs = true;

      if( !refcount_check_disabled_because_of_fwdonly_arcs ) {
        // BAD REFCOUNT!
        FATAL( 0xFFF, "bad refcount for vertex '%s'. Expected %lld, got %lld", CALLABLE( vertex_WL )->IDString( vertex_WL ), expected_refcnt, actual_refcnt );
        return -1;
      }
    }
  }

  return actual_refcnt;
}
#endif


#ifdef VGX_CONSISTENCY_CHECK
DLL_EXPORT const vgx_Vertex_t * __assert_vertex_lock( const vgx_Vertex_t *vertex_LCK ) {
  vgx_Graph_t *graph = vertex_LCK->graph;
  if( graph ) {
    GRAPH_LOCK( graph ) {
      if( !_vgx_is_readonly_CS( &graph->readonly ) ) {
        if( graph->__state_lock_count < 2 ) {
          if( !__vertex_is_locked_readonly( vertex_LCK ) && !__vertex_is_locked_writable_by_current_thread( vertex_LCK ) ) {
            FATAL( 0xFFF, "Illegal vertex access: vertex is not locked" );
          }
        }
      }
    } GRAPH_RELEASE;
  }
  return vertex_LCK;
}
#endif


#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_EXPORT const vgx_Graph_t * __assert_state_lock( const vgx_Graph_t *graph ) {
  if( graph->__state_lock_count < 1 ) {
    FATAL( 0xFFF, "Illegal operation: graph state lock is open" );
  }
#if defined CXPLAT_WINDOWS_X64
  else {
    uint64_t tid = (uint64_t)GET_CURRENT_THREAD_ID();
    HANDLE owner = graph->state_lock.lock.OwningThread;
    uint64_t sid = (uint64_t)owner;
    if( tid != sid ) {
      FATAL( 0xFFF, "Illegal operation: graph state lock owned by %u, thread %u attempted access", sid, tid );
    }
  }
#endif
  return graph;
}
#endif


#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_EXPORT int64_t __vertex_count_dec( int64_t *pcnt ) {
  if( *pcnt > 0 ) {
    --(*pcnt);
  }
  else {
    FATAL( 0xFFF, "Attempted decrement vertex lock count %lld", *pcnt );
  }
  return *pcnt;
}
#endif


#if defined (VGX_CONSISTENCY_CHECK) || defined (VGX_STATE_LOCK_CHECK)
DLL_EXPORT int64_t __vertex_count_dec_delta( int64_t *pcnt, int delta ) {
  if( delta > *pcnt ) {
    FATAL( 0xFFF, "Attempted decrement delta %d vertex lock count %lld", delta, *pcnt );
  }
  else {
    *pcnt -= delta;
  }
  return *pcnt;
}
#endif





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxoballoc_vertex.h"

test_descriptor_t _vgx_vxoballoc_vertex_tests[] = {
  { "VGX Vertex Object Allocation Tests", __utest_vxoballoc_vertex },

  {NULL}
};
#endif
