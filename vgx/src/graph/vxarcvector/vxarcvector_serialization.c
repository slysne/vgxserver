/*######################################################################
 *#
 *# vxarcvector_serialization.c
 *#
 *#
 *######################################################################
 */



#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


#define __QWORDS  nqwords
#define __OUTPUT  output
#define __INPUT   input


#define WRITE_OR_FAIL( qwarray )                                                                                    \
  do {                                                                                                              \
    int64_t nw;                                                                                                     \
    if( (nw = CALLABLE( __OUTPUT )->WriteNolock( __OUTPUT, (qwarray), qwsizeof(qwarray) )) != qwsizeof(qwarray) ) { \
      return -1;                                                                                                    \
    }                                                                                                               \
    __QWORDS += nw;                                                                                                 \
  } WHILE_ZERO




static const QWORD VGX_ARCVECTOR_END = 0xFFFFFFFFFFFFFFFFULL;
static const QWORD VGX_ARCVECTOR_TERM = 0;




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __serialize_predicator( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t __QWORDS = 0;
  CQwordQueue_t *__OUTPUT = (CQwordQueue_t*)processor->processor.output;
  vgx_predicator_t predicator = { .data = APTR_AS_UNSIGNED( fh_cell ) };
  QWORD __pred[1] = { predicator.data };
  WRITE_OR_FAIL( __pred );
  return __QWORDS;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __serialize_multiple_arc( framehash_cell_t *top, CQwordQueue_t *__OUTPUT ) {
  framehash_processing_context_t serialize_predicator = FRAMEHASH_PROCESSOR_NEW_CONTEXT( top, NULL, __serialize_predicator );
  FRAMEHASH_PROCESSOR_SET_IO( &serialize_predicator, NULL, __OUTPUT );
  return iFramehash.processing.ProcessNolock( &serialize_predicator );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __serialize_arc( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t __QWORDS = 0;
  CQwordQueue_t *__OUTPUT = (CQwordQueue_t*)processor->processor.output;

  vgx_Vertex_t *vertex = (vgx_Vertex_t*)fh_cell->annotation;

  switch( APTR_AS_DTYPE(fh_cell) ) {
  case TAGGED_DTYPE_PTR56:
    // GREEN    Multiple Arc: FRAME
    // [ MULTIPLE_ARC ] [ VERTEX_HANDLE ] [ ....... ] [ 0 ]
    {
      // header
      cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( vertex );
      QWORD __multiple_arc[2] = {
        VGX_ARCVECTOR_MULTIPLE_ARC,
        handle.qword
      };
      WRITE_OR_FAIL( __multiple_arc );

      // body
      vgx_ArcVector_cell_t arc_cell;
      framehash_cell_t eph_top;
      framehash_cell_t *frametop = (framehash_cell_t*)APTR_GET_PTR56(fh_cell);
      __arcvector_cell_set_multiple_arc( &arc_cell, vertex, frametop );
      __arcvector_set_ephemeral_top( &arc_cell, &eph_top );
      int64_t n;
      if( (n = __serialize_multiple_arc( &eph_top, __OUTPUT )) < 0 ) {
        return __ARCVECTOR_ERROR( NULL, vertex, &arc_cell, NULL );
      }
      __QWORDS += n;

      // terminator
      QWORD __term[1] = { VGX_ARCVECTOR_TERM };
      WRITE_OR_FAIL( __term );
      
      return __QWORDS;
    }
  case TAGGED_DTYPE_UINT56:
    // BLUE     Simple Arc
    // [ SIMPLE_ARC ] [ VERTEX_HANDLE ] [ PREDICATOR ]
    {
      cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( vertex );
      vgx_predicator_t predicator = { .data = APTR_AS_UNSIGNED( fh_cell ) };
      QWORD __simple_arc[3] = {
        VGX_ARCVECTOR_SIMPLE_ARC,
        handle.qword,
        predicator.data
      };
      WRITE_OR_FAIL( __simple_arc );

      return __QWORDS;
    }
  default:
    // unexpected cell value
    FRAMEHASH_PROCESSOR_SET_FAILED( processor );
    return __ARCVECTOR_ERROR( NULL, vertex, NULL, NULL );  // error!
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __serialize_array_of_arcs( framehash_cell_t *top, CQwordQueue_t *__OUTPUT ) {
  framehash_processing_context_t serialize_arc = FRAMEHASH_PROCESSOR_NEW_CONTEXT( top, NULL, __serialize_arc );
  FRAMEHASH_PROCESSOR_SET_IO( &serialize_arc, NULL, __OUTPUT );
  return iFramehash.processing.ProcessNolock( &serialize_arc );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static QWORD __next_qword( CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {
  QWORD qword;
  CALLABLE( __INPUT )->NextNolock( __INPUT, &qword );
  (*__QWORDS)++;
  return qword;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __deserialize_multiple_arc( vgx_Vertex_t *tail, int64_t *degree, vgx_ArcVector_cell_t *V, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {

  // The multiple arc predicator array is framehash based
  framehash_slot_t *frame_slots = NULL;
  framehash_cell_t top;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &top, dynamic );

  XTRY {
    // Read the vertex handle and convert to vertex object
    vgx_Vertex_t *vertex;;
    cxmalloc_handle_t vertex_handle;
    vertex_handle.qword = __next_qword( __INPUT, __QWORDS );
    if( vertex_handle.objclass == COMLIB_CLASS_CODE( vgx_Vertex_t ) ) {
      // NOTE: the vertex object may not be active yet if its allocator has not yet been restored. The vertex address is correct.
      if( (vertex = CALLABLE( vertex_allocator )->HandleAsObjectNolock( vertex_allocator, vertex_handle )) != NULL ) {
        // Create predicator map top frame as LEAF
        if( (frame_slots = iFramehash.memory.NewFrame( &context, 0, 1, FRAME_TYPE_LEAF )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x2D1 );
        }

        // Set the embedded arc cell to multiple arc predicator array
        framehash_cell_t *frametop = iFramehash.access.TopCell( frame_slots );
        vgx_ArcVector_cell_t MAV;
        __arcvector_cell_set_multiple_arc( &MAV, vertex, frametop );

        // Read all predicators and insert into the predicator map
        vgx_predicator_t predicator;
        while( (predicator.data = __next_qword( __INPUT, __QWORDS )) != VGX_ARCVECTOR_TERM ) {
          (*degree)++;
          _cxmalloc_object_incref_nolock( vertex );
          // Forward-only arc requires artificial incref of tail
          if( predicator.mod.stored.xfwd ) {
            _cxmalloc_object_incref_nolock( tail );
          }
          // Add predicator to predicator map
          if( _vxarcvector_fhash__append_to_multiple_arc( dynamic, &MAV, predicator ) != 1 ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x2D2 );
          }
        }

        // Insert the predicator map into the array of arcs
        frametop = __arcvector_get_frametop( &MAV );
        if( _vxarcvector_fhash__set_predicator_map( dynamic, V, vertex, frametop ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x2D3 );
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x2D4 );
      }
    }
    else {
      THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x2D5 );
    }
  }
  XCATCH( errcode ) {
    if( frame_slots ) {
      iFramehash.memory.DiscardFrame( &context );
    }
    (*__QWORDS) = -1;
  }
  XFINALLY {
  }

  return *__QWORDS;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __deserialize_no_arcs( vgx_ArcVector_cell_t *V, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {
  // Verify end of arcvector marker and set arcvector cell to no arc
  if( __next_qword( __INPUT, __QWORDS ) == VGX_ARCVECTOR_END ) {
    __arcvector_cell_set_no_arc( V );
    return *__QWORDS;
  }
  else {
    *__QWORDS = -1;
    return *__QWORDS;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_Vertex_t * __read_simple_arc( vgx_Arc_t *arc, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {
  cxmalloc_handle_t vertex_handle;
  QWORD __simple_arc[2], *p = __simple_arc;
  (*__QWORDS) += CALLABLE( __INPUT )->ReadNolock( __INPUT, (void**)&p, 2 );
  vertex_handle.qword = __simple_arc[0];
  if( vertex_handle.objclass == COMLIB_CLASS_CODE( vgx_Vertex_t ) ) {
    // NOTE: the vertex object may not be active yet if its allocator has not yet been restored. The vertex address is correct.
    if( (arc->head.vertex = (vgx_Vertex_t*)CALLABLE( vertex_allocator )->HandleAsObjectNolock( vertex_allocator, vertex_handle )) != NULL ) {
      arc->head.predicator.data = __simple_arc[1];
      return arc->head.vertex;
    }
  }
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __deserialize_simple_arc( vgx_Vertex_t *tail, vgx_ArcVector_cell_t *V, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {
  vgx_Arc_t arc;
  // Read the vertex and predicator into arc
  vgx_Vertex_t *head = __read_simple_arc( &arc, vertex_allocator, __INPUT, __QWORDS );
  // Verify end of arcvector marker, own another reference to the head vertex and set arcvector cell to array of arcs
  if( head && __next_qword( __INPUT, __QWORDS ) == VGX_ARCVECTOR_END ) {
    _cxmalloc_object_incref_nolock( head );
    __arcvector_cell_set_simple_arc( V, &arc );
    // Forward-only arc requires artificial incref of tail
    if( arc.head.predicator.mod.stored.xfwd ) {
      _cxmalloc_object_incref_nolock( tail );
    }
    return *__QWORDS;
  }
  else {
    *__QWORDS = -1;
    return *__QWORDS;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __deserialize_array_of_arcs( vgx_Vertex_t *tail, vgx_ArcVector_cell_t *V, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {

  // The array of arcs is framehash based
  framehash_slot_t *frame_slots = NULL;
  framehash_cell_t top;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &top, dynamic );

  XTRY {

    // Create top frame as LEAF
    if( (frame_slots = iFramehash.memory.NewFrame( &context, 0, 1, FRAME_TYPE_LEAF )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x2E1 );
    }

    // Set the top arcvector cell to array of arcs
    framehash_cell_t *frametop = iFramehash.access.TopCell( frame_slots );
    __arcvector_cell_set_array_of_arcs( V, 0, frametop ); // initialize degree=0

    // Parse the input stream and build up the framehash structure
    vgx_Arc_t arc;;
    vgx_Vertex_t *head;
    int64_t degree = 0;
    QWORD qword;
    while( (qword = __next_qword( __INPUT, __QWORDS )) != VGX_ARCVECTOR_END ) {
      switch( (int)qword ) {
      // Simple Arc
      case VGX_ARCVECTOR_SIMPLE_ARC:
        if( (head = __read_simple_arc( &arc, vertex_allocator, __INPUT, __QWORDS )) != NULL &&  _vxarcvector_fhash__set_simple_arc( dynamic, V, &arc ) == 1 ) {
          ++degree;
          _cxmalloc_object_incref_nolock( head );
          // Forward-only arc requires artificial incref of tail
          if( arc.head.predicator.mod.stored.xfwd ) {
            _cxmalloc_object_incref_nolock( tail );
          }
          continue;
        }
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x2E2 );
      // Multiple Arc
      case VGX_ARCVECTOR_MULTIPLE_ARC:
        if( __deserialize_multiple_arc( tail, &degree, V, dynamic, vertex_allocator, __INPUT, __QWORDS ) > 0 ) {
          continue;
        }
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x2E3 );
      // Bad data
      default:
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x2E4 );
      }
    }

    // Update the degree
    __arcvector_set_degree( V, degree );
  }
  XCATCH( errcode ) {
    if( frame_slots ) {
      iFramehash.memory.DiscardFrame( &context );
    }
    __arcvector_cell_set_no_arc( V );
    *__QWORDS = -1;
  }
  XFINALLY {
  }

  return *__QWORDS;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __deserialize_indegree_counter( vgx_ArcVector_cell_t *V, CQwordQueue_t *__INPUT, int64_t *__QWORDS ) {
  // Read indegree
  int64_t indegree = __next_qword( __INPUT, __QWORDS );
  // Verify end of arcvector marker and set arcvector cell to indegree counter
  if( __next_qword( __INPUT, __QWORDS ) == VGX_ARCVECTOR_END ) {
    __arcvector_cell_set_indegree_counter_only( V, indegree );
    return *__QWORDS;
  }
  else {
    *__QWORDS = -1;
    return *__QWORDS;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_serialization__serialize( const vgx_ArcVector_cell_t *V, CQwordQueue_t *__OUTPUT ) {
  int64_t __QWORDS = 0;

  framehash_cell_t eph_top;

  switch( __arcvector_cell_type( V ) ) {

  // WHITE: Empty
  case VGX_ARCVECTOR_NO_ARCS:
    // [ NO_ARCS ]
    {
      QWORD __no_arcs[2] = {
        VGX_ARCVECTOR_NO_ARCS,
        VGX_ARCVECTOR_END
      };
      WRITE_OR_FAIL( __no_arcs );
    }
    break;

  // BLUE: Simple arc
  case VGX_ARCVECTOR_SIMPLE_ARC:
    // [ SIMPLE_ARC ] [ VERTEX_HANDLE ] [ PREDICATOR ]
    {
      cxmalloc_handle_t handle = _vxoballoc_vertex_as_handle( __arcvector_get_vertex( V ) );
      vgx_predicator_t predicator = { .data = __arcvector_as_predicator_bits( V ) };

      QWORD __simple_arc[4] = {
        VGX_ARCVECTOR_SIMPLE_ARC,
        handle.qword,
        predicator.data,
        VGX_ARCVECTOR_END
      };
      WRITE_OR_FAIL( __simple_arc );
    }
    break;

  // GREEN: Array of arcs
  case VGX_ARCVECTOR_ARRAY_OF_ARCS:
    // [ ARRAY_OF_ARCS ]
    {
      QWORD __array_of_arcs[1] = {
        VGX_ARCVECTOR_ARRAY_OF_ARCS
      };
      WRITE_OR_FAIL( __array_of_arcs );

      __arcvector_set_ephemeral_top( V, &eph_top );
      int64_t n;
      if( (n = __serialize_array_of_arcs( &eph_top, __OUTPUT )) < 0 ) {
        return -1;
      }
      __QWORDS += n;

      QWORD __end[1] = {
        VGX_ARCVECTOR_END
      };
      WRITE_OR_FAIL( __end );
    }
    break;

  // GRAY: Indegre Counter
  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    // [ INDEGREE_COUNTER ]
    {
      int64_t indegree = __arcvector_get_degree( V );
      QWORD __indegree_count[3] = {
        VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY,
        indegree,
        VGX_ARCVECTOR_END
      };
      WRITE_OR_FAIL( __indegree_count );
    }
    break;

  default:
    break;
  }

  return __QWORDS;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_serialization__deserialize( vgx_Vertex_t *tail, vgx_ArcVector_cell_t *V, framehash_dynamic_t *dynamic, cxmalloc_family_t *vertex_allocator, CQwordQueue_t *__INPUT ) {
  // Get the arcvector mode
  int64_t __QWORDS = 0;
  switch( (int)__next_qword( __INPUT, &__QWORDS ) ) {
  // No arcs
  case VGX_ARCVECTOR_NO_ARCS:
    return __deserialize_no_arcs( V, __INPUT, &__QWORDS );
  // Simple Arc
  case VGX_ARCVECTOR_SIMPLE_ARC:
    return __deserialize_simple_arc( tail, V, vertex_allocator, __INPUT, &__QWORDS );
  // Array of Arcs
  case VGX_ARCVECTOR_ARRAY_OF_ARCS:
    return __deserialize_array_of_arcs( tail, V, dynamic, vertex_allocator, __INPUT, &__QWORDS );
  // Indegree Counter
  case VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY:
    return __deserialize_indegree_counter( V, __INPUT, &__QWORDS );
  default:
    return -1;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_serialization.h"


test_descriptor_t _vgx_vxarcvector_serialization_tests[] = {
  { "VGX Arcvector Serialization Test",   __utest_vxarcvector_serialization },
  {NULL}
};
#endif

