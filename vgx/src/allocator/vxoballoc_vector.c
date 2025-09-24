/*######################################################################
 *#
 *# vxoballoc_vector.c
 *#
 *#
 *######################################################################
 */


#include "_vxoballoc_vector.h"
#include "_vgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int __vxoballoc_vector__cxmalloc_serialize_internal_feature_vector( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_vector__cxmalloc_deserialize_internal_feature_vector( cxmalloc_line_deserialization_context_t *context );

static int __vxoballoc_vector__cxmalloc_serialize_external_feature_vector( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_vector__cxmalloc_deserialize_external_feature_vector( cxmalloc_line_deserialization_context_t *context );

static int __vxoballoc_vector__cxmalloc_serialize_internal_euclidean_vector( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_vector__cxmalloc_deserialize_internal_euclidean_vector( cxmalloc_line_deserialization_context_t *context );

static int __vxoballoc_vector__cxmalloc_serialize_external_euclidean_vector( cxmalloc_line_serialization_context_t *context );
static int __vxoballoc_vector__cxmalloc_deserialize_external_euclidean_vector( cxmalloc_line_deserialization_context_t *context );

static char * __serialize_feature_vector_elements( const vgx_Vector_t *vector, char *output );
static char * __serialize_euclidean_vector_elements( const vgx_Vector_t *vector, char *output );

static vgx_Vector_t * __deserialize_feature_vector( vgx_Similarity_t *sim, const char *input, vgx_VectorMetas_t *metas, CString_t **CSTR__error, uint32_t *dimerr );
static vgx_Vector_t * __deserialize_euclidean_vector( vgx_Similarity_t *sim, const char *input, vgx_VectorMetas_t *metas, CString_t **CSTR__error );




#define SIMILARITY_AUX_IDX 0


/*
  ==============
  FEATURE VECTOR
  ==============



  MEMORY LAYOUT:
                                vgx_Vector_t*
vgx_AllocatedVector_t*               |
  |                                  |
  V                                  V
  +-----------------+----------------+--------+--------+--------+--------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+---- ...
  |                 |                |        |        |        |        |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |     ...
  +--------+--------+----------------+--------+--------+--------+--------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+---- ...
  | *elems | *simobj|  (allocator)   | *vtable| typinfo|  metas |   fp   | e0 | e1 | e2 | e3 | e4 | e5 | e6 |    |    |    |    |    |    |    |    |    |     ...
  +--------+--------+----------------+--------+--------+--------+--------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+---- ...
      |                                                /        \        ^        /      \
      |                                             /            \       |       /        \______________________
      |                                          /                \      |      /                                \
      |                                        +--------+----+--+--+     |      +--------------------------+------+
      |                                        |  vmag  |vlen|tp|fl|     |      |        dim               | mag  |
      |                                        +--------+----+--+--+     |      +--------------------------+------+
       \                                                                /
        \______________________________________________________________/



  SERIALIZED LAYOUT:
  +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
  | metas  |   fp   | e0  e1 | e2  e3 | e4  e5 | e6  00 | 00  00 | 00  00 | 00  00 | 00  00 |
  +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
                                                   ^
                                                vlen

                     \______________________shape.linemem.qwords___________________________/
                    


For external vectors the format is similar, except the unit size of ext_vector_feature_t is sizeof(__m256i) while size of vector_feature_t is sizeof( uint32_t )

 */



/*******************************************************************//**
 * Internal Feature Vector Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t InternalFeatureVectorDescriptor( const CString_t *CSTR__persist_path, vgx_Similarity_t *simobj ) {

#if defined VGX_GRAPH_VECTOR_BLOCK_SIZE_MB
  const size_t block_size = ((size_t)VGX_GRAPH_VECTOR_BLOCK_SIZE_MB << 20);
#else
  const size_t block_size = (64ULL << 20); // 64 MB default
#endif

  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},

      // serialized: the allocator metas contain no serializable data
      .serialized_sz    = 0
    },
    .obj = {
      .sz               = sizeof( vgx_VectorHead_t ),     /* space for the class header and extras  */
      
      // serialized: metas + fp
      .serialized_sz    = sizeof( vgx_VectorMetas_t ) + sizeof( FP_t )
    },
    .unit = {
      .sz               = sizeof( vector_feature_t ),      /* 4 bytes                                */

      // serialized: same size as memory vector_feature_t
      .serialized_sz    = sizeof( vector_feature_t )
    },
    .serialize_line     = __vxoballoc_vector__cxmalloc_serialize_internal_feature_vector,
    .deserialize_line   = __vxoballoc_vector__cxmalloc_deserialize_internal_feature_vector,
    .parameter = {
      .block_sz         = block_size,               /* block size in bytes                    */
      .line_limit       = MAX_FEATURE_VECTOR_SIZE,  /* aidx=3 => size=48 with S=1             */
      .subdue           = 1,                        /* S=1 =>   0:0,  1:16,  2:32,  3:48      */
      .allow_oversized  = 0,                        /* disallow oversized                     */
      .max_allocators   = 4                         /* aidx 0 - 3                             */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path,       /*                                        */
    },
    .auxiliary = {
      simobj,          /* 0: (vgx_Similarity_t*)        */
      NULL             /* --END --                      */
    }
  };

  return descriptor;
}



/*******************************************************************//**
 * External Feature Vector Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t ExternalFeatureVectorDescriptor( const CString_t *CSTR__persist_path, vgx_Similarity_t *simobj ) {

#if defined VGX_GRAPH_EXT_VECTOR_BLOCK_SIZE_MB
  const size_t block_size = ((size_t)VGX_GRAPH_EXT_VECTOR_BLOCK_SIZE_MB << 20);
#else
  const size_t block_size = (2ULL << 20); // 2 MB default
#endif
  
  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},

      // serialized: the allocator metas contain no serializable data
      .serialized_sz    = 0
    },
    .obj = {
      .sz               = sizeof( vgx_VectorHead_t ),                       /* space for the class header and extras          */

      // serialized: metas + fp
      .serialized_sz    = sizeof( vgx_VectorMetas_t ) + sizeof( FP_t )
    },
    .unit = {
      .sz               = sizeof( ext_vector_feature_t ),  /* 32 bytes                                       */

      // serialized: same size as memory vector_feature_t
      .serialized_sz    = sizeof( ext_vector_feature_t )
    },
    .serialize_line     = __vxoballoc_vector__cxmalloc_serialize_external_feature_vector,
    .deserialize_line   = __vxoballoc_vector__cxmalloc_deserialize_external_feature_vector,
    .parameter = {
      .block_sz         = block_size,               /* block size in bytes                            */
      .line_limit       = 62,                       /* aidx=5 => size=62 with S=0                     */
      .subdue           = 0,                        /* S=0 =>   0:0,  1:2,  2:6,  3:14,  4:30,  5:62  */
      .allow_oversized  = 0,                        /* disallow oversized                             */
      .max_allocators   = 6                         /* aidx 0 - 5                                     */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path        /*                                                */
    },
    .auxiliary = {
      simobj,          /* 0: (vgx_Similarity_t*)        */
      NULL             /* --END --                      */
    }
  };
 
  return descriptor;
}



/*
  ================
  EUCLIDEAN VECTOR
  ================



  MEMORY LAYOUT:
                                vgx_Vector_t*
vgx_AllocatedVector_t*               |
  |                                  |
  V                                  V
  +-----------------+----------------+--------+--------+--------+--------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ ...
  |                 |                |        |        |        |        | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | |   ...
  +--------+--------+----------------+--------+--------+--------+--------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ ...
  | *elems | *simobj|  (allocator)   | *vtable| typinfo|  metas |   fp   |b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b|b| | | | | | |   ...
  +--------+--------+----------------+--------+--------+--------+--------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ ...
      |                                                /        \        ^
      |                                             /            \       |
      |                                          /                \      |
      |                                        +--------+----+--+--+     |
      |                                        |  vmag  |vlen|tp|fl|     |
      |                                        +--------+----+--+--+     |
       \                                                                /
        \______________________________________________________________/



  SERIALIZED LAYOUT:
  +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
  | metas  |   fp   |bbbbbbbb|bbbbbbbb|bbbbbbbb|bb000000|00000000|00000000|00000000|00000000|
  +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
                                                  ^
                                                vlen

                     \______________________shape.linemem.qwords___________________________/
                    


For external vectors the format is similar, except the unit size of the externalized dimension is sizeof(float) while size of internalized dimension is sizeof( BYTE )

 */



/*******************************************************************//**
 * Internal Euclidean Vector Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t InternalEuclideanVectorDescriptor( const CString_t *CSTR__persist_path, vgx_Similarity_t *simobj ) {

#if defined VGX_GRAPH_VECTOR_BLOCK_SIZE_MB
  const size_t block_size = ((size_t)VGX_GRAPH_VECTOR_BLOCK_SIZE_MB << 20);
#else
  const size_t block_size = (64ULL << 20); // 64 MB default
#endif

  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},

      // serialized: the allocator metas contain no serializable data
      .serialized_sz    = 0
    },
    .obj = {
      .sz               = sizeof( vgx_VectorHead_t ),     /* space for the class header and extras  */
      
      // serialized: metas + fp
      .serialized_sz    = sizeof( vgx_VectorMetas_t ) + sizeof( FP_t )
    },
    .unit = {
      .sz               = sizeof( BYTE ),      /* 1 byte                                */
      .serialized_sz    = sizeof( BYTE )
    },
    .serialize_line     = __vxoballoc_vector__cxmalloc_serialize_internal_euclidean_vector,
    .deserialize_line   = __vxoballoc_vector__cxmalloc_deserialize_internal_euclidean_vector,
    .parameter = {
      .block_sz         = block_size,               /* block size in bytes                    */
      .line_limit       = MAX_EUCLIDEAN_VECTOR_SIZE,    /* aidx=63 => size=65472 with S=3         */
      .subdue           = 3,                        /* S=3 =>   0:0,      1:64,     2:128,    3:192,    4:256,    5:320,    6:384,    7:448      */
                                                    /*          8:512,    9:576,   10:640,   11:704,   12:768,   13:832,   14:896,   15:906      */
                                                    /*         16:1088,  17:1216,  18:1344,  19:1472,  20:1600,  21:1728,  22:1856,  23:1984     */
                                                    /*         24:2240,  25:2496,  26:2752   27:3008,  28:3264,  29:3520,  30:3776,  31:4032     */
                                                    /*         32:4544,  33:5056,  34:5568,  35:6080,  36:6592,  37:7104,  38:7616,  39:8128     */
                                                    /*         40:9152,  41:10176, 42:11200, 43:12224, 44:13248, 45:14272, 46:15296, 47:16320    */
                                                    /*         48:18368, 49:20416, 50:22464, 51:24512, 52:26560, 53:28608, 54:30656, 55:32704    */
                                                    /*         56:36800, 57:40896, 58:44992, 59:49088, 60:53184, 61:57280, 62:61376, 63:65472    */
      .allow_oversized  = 0,                        /* disallow oversized                     */
      .max_allocators   = 64                        /* aidx 0 - 63                            */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path,       /*                                        */
    },
    .auxiliary = {
      simobj,          /* 0: (vgx_Similarity_t*)        */
      NULL             /* --END --                      */
    }
  };

  return descriptor;
}



/*******************************************************************//**
 * External Euclidean Vector Allocator Descriptor
 *
 ***********************************************************************
 */
static const cxmalloc_descriptor_t ExternalEuclideanVectorDescriptor( const CString_t *CSTR__persist_path, vgx_Similarity_t *simobj ) {

#if defined VGX_GRAPH_EXT_VECTOR_BLOCK_SIZE_MB
  const size_t block_size = ((size_t)VGX_GRAPH_EXT_VECTOR_BLOCK_SIZE_MB << 20);
#else
  const size_t block_size = (2ULL << 20); // 2 MB default
#endif
  
  cxmalloc_descriptor_t descriptor = {
    .meta = {
      .initval          = {0},

      // serialized: the allocator metas contain no serializable data
      .serialized_sz    = 0
    },
    .obj = {
      .sz               = sizeof( vgx_VectorHead_t ),                       /* space for the class header and extras          */

      // serialized: metas + fp
      .serialized_sz    = sizeof( vgx_VectorMetas_t ) + sizeof( FP_t )
    },
    .unit = {
      .sz               = sizeof( float ),  /* 4 bytes                                       */
      .serialized_sz    = sizeof( float )
    },
    .serialize_line     = __vxoballoc_vector__cxmalloc_serialize_external_euclidean_vector,
    .deserialize_line   = __vxoballoc_vector__cxmalloc_deserialize_external_euclidean_vector,
    .parameter = {
      .block_sz         = block_size,               /* block size in bytes                            */
      .line_limit       = MAX_EUCLIDEAN_VECTOR_SIZE,    /* aidx=12 => size=65520 with S=0                 */
      .subdue           = 0,                        /* S=0 =>   0:0,   1:16,  2:48,  3:112,  4:240,   5:496               */
                                                    /*          6:1008 7:2032 8:4080 9:8176 10:16368 11:32752 12:65520    */
      .allow_oversized  = 0,                        /* disallow oversized                             */
      .max_allocators   = 13                        /* aidx 0 - 12                                    */
    },
    .persist = {
      .CSTR__path       = CSTR__persist_path        /*                                                */
    },
    .auxiliary = {
      simobj,          /* 0: (vgx_Similarity_t*)        */
      NULL             /* --END --                      */
    }
  };
 
  return descriptor;
}






/*******************************************************************//**
*
*
***********************************************************************
*/
static cxmalloc_family_t *  __vxoballoc_vector__get( const vgx_Vector_t *self );
static cxmalloc_family_t *  __vxoballoc_vector__new_internal_map_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_internal_map_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_external_map_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_external_map_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_internal_euclidean_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_internal_euclidean_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_external_euclidean_allocator( vgx_Similarity_t *simobj, const char *name );
static cxmalloc_family_t *  __vxoballoc_vector__new_external_euclidean_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name );
static uint16_t             __vxoballoc_vector__count_internal_elements( const vector_feature_t *elements );
static uint16_t             __vxoballoc_vector__count_external_elements( const ext_vector_feature_t *elements );
static void                 __vxoballoc_vector__delete_allocator( cxmalloc_family_t **allocator );
static int64_t              __vxoballoc_vector__verify_allocator( cxmalloc_family_t *allocator );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IVectorAllocator_t ivectoralloc = {
  .Get                            = __vxoballoc_vector__get,
  .NewInternalMap                 = __vxoballoc_vector__new_internal_map_allocator,
  .NewInternalMapEphemeral        = __vxoballoc_vector__new_internal_map_ephemeral_allocator,
  .NewExternalMap                 = __vxoballoc_vector__new_external_map_allocator,
  .NewExternalMapEphemeral        = __vxoballoc_vector__new_external_map_ephemeral_allocator,
  .NewInternalEuclidean           = __vxoballoc_vector__new_internal_euclidean_allocator,
  .NewInternalEuclideanEphemeral  = __vxoballoc_vector__new_internal_euclidean_ephemeral_allocator,
  .NewExternalEuclidean           = __vxoballoc_vector__new_external_euclidean_allocator,
  .NewExternalEuclideanEphemeral  = __vxoballoc_vector__new_external_euclidean_ephemeral_allocator,
  .CountInternalElements          = __vxoballoc_vector__count_internal_elements,
  .CountExternalElements          = __vxoballoc_vector__count_external_elements,
  .Delete                         = __vxoballoc_vector__delete_allocator,
  .Verify                         = __vxoballoc_vector__verify_allocator
};



/*******************************************************************//**
*
*
***********************************************************************
*/
static vgx_Vector_t *         __vxoballoc_vector__new_vector( vgx_Similarity_t *simobj, vector_type_t type, uint16_t length, bool ephemeral );
static vgx_Vector_t *         __vxoballoc_vector__null_vector( vgx_Similarity_t *simobj );
static int                    __vxoballoc_vector__delete_vector( vgx_Vector_t *vector );
static void                   __vxoballoc_vector__incref_dimensions_nolock( vgx_Vector_t *vector );
static void                   __vxoballoc_vector__incref_dimensions( vgx_Vector_t *vector );
static int64_t                __vxoballoc_vector__incref_nolock( vgx_Vector_t *vector );
static int64_t                __vxoballoc_vector__incref( vgx_Vector_t *vector );
static void                   __vxoballoc_vector__decref_dimensions_nolock( vgx_Vector_t *vector );
static void                   __vxoballoc_vector__decref_dimensions( vgx_Vector_t *vector );
static int64_t                __vxoballoc_vector__decref_nolock( vgx_Vector_t *vector );
static int64_t                __vxoballoc_vector__decref( vgx_Vector_t *vector );
static int64_t                __vxoballoc_vector__refcnt( const vgx_Vector_t *vector );
static cxmalloc_handle_t      __vxoballoc_vector__vector_as_handle( const vgx_Vector_t *self );
static vgx_Vector_t *         __vxoballoc_vector__vector_from_handle_nolock( const cxmalloc_handle_t handle, cxmalloc_family_t *allocator );
static CString_t *            __vxoballoc_vector__serialize( const vgx_Vector_t *vector );
static vgx_Vector_t *         __vxoballoc_vector__deserialize( vgx_Similarity_t *sim, const CString_t *CSTR__data, CString_t **CSTR__error, uint32_t *dimerr );
static vgx_Similarity_t *     __vxoballoc_vector__set_simobj( vgx_Vector_t *self, vgx_Similarity_t *simobj );
static vgx_Similarity_t *     __vxoballoc_vector__get_simobj( const vgx_Vector_t *self );
static void *                 __vxoballoc_vector__set_elements( vgx_Vector_t *self, void *elements );
static void *                 __vxoballoc_vector__get_elements( vgx_Vector_t *self );
static vgx_VectorContext_t *  __vxoballoc_vector__set_context( const vgx_Vector_t *self, vgx_Similarity_t *simobj, void *elements );
static vgx_VectorContext_t *  __vxoballoc_vector__get_context( const vgx_Vector_t *self );





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IVectorObject_t ivectorobject = {
  .New                      = __vxoballoc_vector__new_vector,
  .Null                     = __vxoballoc_vector__null_vector,
  .Delete                   = __vxoballoc_vector__delete_vector,
  .IncrefDimensionsNolock   = __vxoballoc_vector__incref_dimensions_nolock,
  .IncrefDimensions         = __vxoballoc_vector__incref_dimensions,
  .IncrefNolock             = __vxoballoc_vector__incref_nolock,
  .Incref                   = __vxoballoc_vector__incref,
  .DecrefDimensionsNolock   = __vxoballoc_vector__decref_dimensions_nolock,
  .DecrefDimensions         = __vxoballoc_vector__decref_dimensions,
  .DecrefNolock             = __vxoballoc_vector__decref_nolock,
  .Decref                   = __vxoballoc_vector__decref,
  .Refcnt                   = __vxoballoc_vector__refcnt,
  .AsHandle                 = __vxoballoc_vector__vector_as_handle,
  .FromHandleNolock         = __vxoballoc_vector__vector_from_handle_nolock,
  .Serialize                = __vxoballoc_vector__serialize,
  .Deserialize              = __vxoballoc_vector__deserialize,
  .SetSimobj                = __vxoballoc_vector__set_simobj,
  .GetSimobj                = __vxoballoc_vector__get_simobj,
  .SetElements              = __vxoballoc_vector__set_elements,
  .GetElements              = __vxoballoc_vector__get_elements,
  .SetContext               = __vxoballoc_vector__set_context,
  .GetContext               = __vxoballoc_vector__get_context
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static cxmalloc_family_t * __vxoballoc_vector__get( const vgx_Vector_t *self ) {
  vgx_Similarity_t *simobj;
  if( self && (simobj = __vxoballoc_vector__get_simobj( self )) != NULL ) {
    if( self->metas.type & __VECTOR__MASK_EXTERNAL ) {
      if( self->metas.flags.eph ) {
        return simobj->ext_vector_ephemeral_allocator;
      }
      else {
        return simobj->ext_vector_allocator;
      }
    }
    else {
      if( self->metas.flags.eph ) {
        return simobj->int_vector_ephemeral_allocator;
      }
      else {
        return simobj->int_vector_allocator;
      }
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __new_vector_allocator( vgx_Graph_t *graph, cxmalloc_descriptor_t *descriptor, const char *name ) {
  cxmalloc_family_t *alloc = NULL;
  CString_t *CSTR__home = NULL;
  CString_t *CSTR__allocname = NULL;

  XTRY {

    cxmalloc_family_constructor_args_t args = {
      .family_descriptor  = descriptor    //
    };

    if( (CSTR__allocname = CStringNewFormat( "Vector Allocator '%s' (graph '%s')", name, graph ? CStringValue(graph->CSTR__name) : "ephemeral" )) != NULL ) {
      if( (alloc = COMLIB_OBJECT_NEW( cxmalloc_family_t, CStringValue(CSTR__allocname), &args )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x122 );
      }
    }

    // TODO: any post-construction initialization?

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
    if( CSTR__allocname ) {
      CStringDelete( CSTR__allocname );
      CSTR__allocname = NULL;
    }
  }

  return alloc;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint16_t __vxoballoc_vector__count_internal_elements( const vector_feature_t *elements ) {
  const vector_feature_t *e = elements;
  while( e->dim != FEATURE_VECTOR_DIMENSION_NONE ) {
    ++e;
  }
  size_t n = e - elements;
  return (uint16_t)n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static uint16_t __vxoballoc_vector__count_external_elements( const ext_vector_feature_t *elements ) {
  const ext_vector_feature_t *e = elements;
  while( *(e->term) ) {
    ++e;
  }
  size_t n = e - elements;
  return (uint16_t)n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vector__delete_allocator( cxmalloc_family_t **allocator ) {
  if( allocator && *allocator ) {
    COMLIB_OBJECT_DESTROY( *allocator );
    *allocator = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __simple_vector_id( vgx_Vector_t *vector ) {
  static char buffer[512];
  const char *simple = buffer;
  sprintf( buffer, "vgx_Vector_t (len=%u mag=%#g type=%02x)", vector->metas.vlen, CALLABLE( vector )->Magnitude( vector ), vector->metas.type );
  return simple;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__verify_allocator( cxmalloc_family_t *allocator ) {
  return CALLABLE( allocator )->Sweep( allocator, (f_get_object_identifier)__simple_vector_id );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_internal_map_allocator( vgx_Similarity_t *simobj, const char *name ) {
  const char *dirname = VGX_PATHDEF_INTERNAL_FEATURE_VECTOR_DIRNAME;
  cxmalloc_family_t *internal_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  CString_t *CSTR__home = NULL;
  if( graph ) {
    const char *graphname = CStringValue( CALLABLE(graph)->Name(graph) );
    if( (CSTR__home = CStringNewFormat( "%s/%s[%s]", CALLABLE(graph)->FullPath(graph), dirname, graphname )) == NULL ) {
      return NULL;
    }
  }
  cxmalloc_descriptor_t descriptor = InternalFeatureVectorDescriptor( CSTR__home, simobj );
  internal_allocator = __new_vector_allocator( graph, &descriptor, name );
  if( CSTR__home ) {
    CStringDelete( CSTR__home );
  }
  return internal_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_internal_map_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name ) {
  cxmalloc_family_t *internal_eph_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  cxmalloc_descriptor_t descriptor = InternalFeatureVectorDescriptor( NULL, simobj );
  internal_eph_allocator = __new_vector_allocator( graph, &descriptor, name );
  return internal_eph_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_external_map_allocator( vgx_Similarity_t *simobj, const char *name ) {
  const char *dirname = VGX_PATHDEF_EXTERNAL_FEATURE_VECTOR_DIRNAME;
  cxmalloc_family_t *external_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  CString_t *CSTR__home = NULL;
  if( graph ) {
    const char *graphname = CStringValue( CALLABLE(graph)->Name(graph) );
    if( (CSTR__home = CStringNewFormat( "%s/%s[%s]", CALLABLE(graph)->FullPath(graph), dirname, graphname )) == NULL ) {
      return NULL;
    }
  }
  cxmalloc_descriptor_t descriptor = ExternalFeatureVectorDescriptor( CSTR__home, simobj );
  external_allocator = __new_vector_allocator( graph, &descriptor, name );
  if( CSTR__home ) {
    CStringDelete( CSTR__home );
  }
  return external_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_external_map_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name ) {
  cxmalloc_family_t *external_eph_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  cxmalloc_descriptor_t descriptor = ExternalFeatureVectorDescriptor( NULL, simobj );
  external_eph_allocator = __new_vector_allocator( graph, &descriptor, name );
  return external_eph_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_internal_euclidean_allocator( vgx_Similarity_t *simobj, const char *name ) {
  const char *dirname = VGX_PATHDEF_INTERNAL_EUCLIDEAN_VECTOR_DIRNAME;
  cxmalloc_family_t *internal_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  CString_t *CSTR__home = NULL;
  if( graph ) {
    if( (CSTR__home = CStringNewFormat( "%s/%s", CALLABLE(graph)->FullPath(graph), dirname )) == NULL ) {
      return NULL;
    }
  }
  cxmalloc_descriptor_t descriptor = InternalEuclideanVectorDescriptor( CSTR__home, simobj );
  internal_allocator = __new_vector_allocator( graph, &descriptor, name );
  if( CSTR__home ) {
    CStringDelete( CSTR__home );
  }
  return internal_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_internal_euclidean_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name ) {
  cxmalloc_family_t *internal_eph_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  cxmalloc_descriptor_t descriptor = InternalEuclideanVectorDescriptor( NULL, simobj );
  internal_eph_allocator = __new_vector_allocator( graph, &descriptor, name );
  return internal_eph_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_external_euclidean_allocator( vgx_Similarity_t *simobj, const char *name ) {
  const char *dirname = VGX_PATHDEF_EXTERNAL_EUCLIDEAN_VECTOR_DIRNAME;
  cxmalloc_family_t *external_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  CString_t *CSTR__home = NULL;
  if( graph ) {
    if( (CSTR__home = CStringNewFormat( "%s/%s", CALLABLE(graph)->FullPath(graph), dirname )) == NULL ) {
      return NULL;
    }
  }
  cxmalloc_descriptor_t descriptor = ExternalEuclideanVectorDescriptor( CSTR__home, simobj );
  external_allocator = __new_vector_allocator( graph, &descriptor, name );
  if( CSTR__home ) {
    CStringDelete( CSTR__home );
  }
  return external_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_family_t * __vxoballoc_vector__new_external_euclidean_ephemeral_allocator( vgx_Similarity_t *simobj, const char *name ) {
  cxmalloc_family_t *external_eph_allocator = NULL;
  vgx_Graph_t *graph = simobj->parent;
  cxmalloc_descriptor_t descriptor = ExternalEuclideanVectorDescriptor( NULL, simobj );
  external_eph_allocator = __new_vector_allocator( graph, &descriptor, name );
  return external_eph_allocator;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __vxoballoc_vector__new_vector( vgx_Similarity_t *simobj, vector_type_t type, uint16_t length, bool ephemeral ) {
  vgx_Vector_t *vector = NULL;
  cxmalloc_family_t *valloc;

  bool ecl_mode = igraphfactory.EuclideanVectors();

  vgx_VectorFlags_t flags = {0};
  flags.eph = ephemeral;

  if( type == VECTOR_TYPE_NULL ) {
    length = 0;
    flags.nul = 1;
    valloc = flags.eph ? simobj->int_vector_ephemeral_allocator : simobj->int_vector_allocator;
  }
  else if( type & __VECTOR__MASK_INTERNAL ) {
    valloc = flags.eph ? simobj->int_vector_ephemeral_allocator : simobj->int_vector_allocator;
  }
  else if( type & __VECTOR__MASK_EXTERNAL ) {
    flags.ext = 1;
    valloc = flags.eph ? simobj->ext_vector_ephemeral_allocator : simobj->ext_vector_allocator;
  }
  else {
    REASON( 0x001, "bad vector type %02x", type );
    return NULL;
  }

  if( type != VECTOR_TYPE_NULL ) {
    if( ecl_mode ) {
      if( !(type & __VECTOR__MASK_EUCLIDEAN) ) {
        REASON( 0x002, "bad vector type %02x", type );
        return NULL; // misconfigured
      }
    }
    else {
      if( !(type & __VECTOR__MASK_FEATURE) ) {
        REASON( 0x003, "bad vector type %02x", type );
        return NULL; // misconfigured
      }
    }
  }

  if( ecl_mode ) {
    #if defined CXPLAT_ARCH_X64
    // Min SIMD width is 32 (AVX2 minimum)
    static int mask = 0x1f;
    #elif defined CXPLAT_ARCH_ARM64
    // NEON width is 16
    static int mask = 0x0f;
    #else
    #error "Unsupported architecture"
    #endif
    
    flags.ecl = 1;
    // Ensure length within limit and multiple of platform's minimum SIMD width
    if( length > MAX_EUCLIDEAN_VECTOR_SIZE || (length & mask) != 0 ) {
      REASON( 0x003, "bad vector length %u", length );
      return NULL;
    }
  }

  // Allocate new vector
  cxmalloc_family_vtable_t *ivalloc = CALLABLE( valloc );
  void *elements = ivalloc->New( valloc, length );
  if( elements ) {
    vector = (vgx_Vector_t*)ivalloc->ObjectFromArray( valloc, elements );
    __vxoballoc_vector__set_context( vector, simobj, elements );
    // Initialize the vector object
    if( COMLIB_OBJECT_INIT( vgx_Vector_t, vector, NULL) != NULL ) {
      vector->metas.flags.bits = flags.bits;
      vector->metas.type = type;
      vector->metas.vlen = length;
      vector->metas.scalar.bits = 0;
      vector->fp = 0;
    }
    else {
      ivalloc->DiscardObject( valloc, vector );
      vector = NULL;
    }
  }

  return vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __vxoballoc_vector__null_vector( vgx_Similarity_t *simobj ) {
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( simobj->nullvector );
  CALLABLE(valloc)->OwnObject( valloc, simobj->nullvector );
  return simobj->nullvector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__delete_vector( vgx_Vector_t *vector ) {
  int ret = 0;
  if( vector ) {
    cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );
    if( valloc ) {
      cxmalloc_family_vtable_t *ivalloc = CALLABLE( valloc );
      int64_t refcnt;
      while( (refcnt = ivalloc->DiscardObject( valloc, vector )) > 0 );
      if( refcnt < 0 ) {
        if( CALLABLE( valloc )->IsReadonly( valloc ) ) {
          REASON( 0xFFF, "Attempted vector deletion with readonly allocator!" );
        }
        ret = -1;
      }
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vector__incref_dimensions_nolock( vgx_Vector_t *vector ) {
  if( !vector->metas.flags.ecl && !vector->metas.flags.ext ) {
    void *elements = __vxoballoc_vector__get_elements( vector );
    vgx_Similarity_t *simobj = __vxoballoc_vector__get_simobj( vector );
    vector_feature_t *elem = (vector_feature_t*)elements;
    vector_feature_t *end = elem + vector->metas.vlen;
    while( elem < end ) {
      _vxenum_dim__own_CS( simobj, (elem++)->dim );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vector__incref_dimensions( vgx_Vector_t *vector ) {
  if( !vector->metas.flags.ecl && !vector->metas.flags.ext ) {
    void *elements = __vxoballoc_vector__get_elements( vector );
    vgx_Similarity_t *simobj = __vxoballoc_vector__get_simobj( vector );
    vector_feature_t *elem = (vector_feature_t*)elements;
    vector_feature_t *end = elem + vector->metas.vlen;
    if( simobj->parent ) {
      GRAPH_LOCK( simobj->parent ) {
        while( elem < end ) {
          _vxenum_dim__own_CS( simobj, (elem++)->dim );
        }
      } GRAPH_RELEASE;
    }
    else {
      while( elem < end ) {
        _vxenum_dim__own_CS( simobj, (elem++)->dim );
      }
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__incref_nolock( vgx_Vector_t *vector ) {
  // To own a vector means to own its dimensions, if it is a feature vector (not a euclidean vector)
  if( !vector->metas.flags.ecl ) {
    __vxoballoc_vector__incref_dimensions_nolock( vector );
  }

  // Own vector
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );
  if( valloc == NULL ) {
    return -1;
  }
  return CALLABLE(valloc)->OwnObjectNolock( valloc, vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__incref( vgx_Vector_t *vector ) {
  // To own a vector means to own its dimensions, if it is a feature vector (not a euclidean vector)
  if( !vector->metas.flags.ecl ) {
    __vxoballoc_vector__incref_dimensions( vector );
  }

  // Own vector
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );
  if( valloc == NULL ) {
    return -1;
  }
  return CALLABLE(valloc)->OwnObject( valloc, vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vector__decref_dimensions_nolock( vgx_Vector_t *vector ) {
  if( !vector->metas.flags.ecl && !vector->metas.flags.ext && vector->metas.flags.pop ) {
    vgx_Similarity_t *simobj = __vxoballoc_vector__get_simobj( vector );
    void *elements = __vxoballoc_vector__get_elements( vector );
    vector_feature_t *elem = (vector_feature_t*)elements;
    vector_feature_t *end = elem + vector->metas.vlen;
    while( elem < end ) {
      _vxenum_dim__discard_CS( simobj, (elem++)->dim );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __vxoballoc_vector__decref_dimensions( vgx_Vector_t *vector ) {
  if( !vector->metas.flags.ecl && !vector->metas.flags.ext && vector->metas.flags.pop ) {
    vgx_Similarity_t *simobj = __vxoballoc_vector__get_simobj( vector );
    void *elements = __vxoballoc_vector__get_elements( vector );
    vector_feature_t *elem = (vector_feature_t*)elements;
    vector_feature_t *end = elem + vector->metas.vlen;
    if( simobj->parent ) {
      GRAPH_LOCK( simobj->parent ) {
        while( elem < end ) {
          _vxenum_dim__discard_CS( simobj, (elem++)->dim );
        }
      } GRAPH_RELEASE;
    }
    else {
      while( elem < end ) {
        _vxenum_dim__discard_CS( simobj, (elem++)->dim );
      }

    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__decref_nolock( vgx_Vector_t *vector ) {
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );

  // Make sure call can proceed
  if( CALLABLE( valloc )->IsReadonly( valloc ) ) {
    if( CALLABLE( valloc )->RefCountObjectNolock( valloc, vector ) == 1 ) {
      REASON( 0xFFF, "Attempted vector decref of last reference with readonly allocator!" );
      return -1;
    }
  }

  // Discard all referenced dimensions if vector is map (not euclidean)
  if( !vector->metas.flags.ecl ) {
    __vxoballoc_vector__decref_dimensions_nolock( vector );
  }

  // Discard vector
  return CALLABLE(valloc)->DiscardObjectNolock( valloc, vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__decref( vgx_Vector_t *vector ) {
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );

  // Make sure call can proceed
  if( CALLABLE( valloc )->IsReadonly( valloc ) ) {
    if( CALLABLE( valloc )->RefCountObject( valloc, vector ) == 1 ) {
      REASON( 0xFFF, "Attempted vector decref of last reference with readonly allocator!" );
      return -1;
    }
  }

  // Discard all referenced dimensions if vector is map (not euclidean)
  if( !vector->metas.flags.ecl ) {
    __vxoballoc_vector__decref_dimensions( vector );
  }

  // Discard vector
  return CALLABLE(valloc)->DiscardObject( valloc, vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __vxoballoc_vector__refcnt( const vgx_Vector_t *vector ) {
  cxmalloc_family_t *valloc = __vxoballoc_vector__get( vector );
  return CALLABLE(valloc)->RefCountObject( valloc, vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static cxmalloc_handle_t __vxoballoc_vector__vector_as_handle( const vgx_Vector_t *vector ) {
  return _vxoballoc_vector_as_handle( vector );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __trap_invalid_handle_class( const cxmalloc_handle_t handle ) {
  FATAL( 0xFFF, "Invalid vgx_Vector_t class code in handle 0x016X. Got class 0x%02X, expected 0x%02X.", handle.qword, handle.objclass, COMLIB_CLASS_CODE( vgx_Vector_t ) );
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __vxoballoc_vector__vector_from_handle_nolock( const cxmalloc_handle_t handle, cxmalloc_family_t *allocator ) {
  if( handle.objclass != COMLIB_CLASS_CODE( vgx_Vector_t ) ) {
    __trap_invalid_handle_class( handle );
  }
  // NOTE: the vector object may not be active yet if its allocator has not yet been restored. The vector address is correct.
  vgx_Vector_t *vector = CALLABLE( allocator )->HandleAsObjectNolock( allocator, handle );
  if( vector ) {
    ivectorobject.IncrefNolock( vector );
    return vector;
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
static char * __serialize_feature_vector_elements( const vgx_Vector_t *vector, char *output ) {
  const vector_feature_t *elem = ivectorobject.GetElements( (vgx_Vector_t*)vector );
  char *c = output;
  for( int i=0; i<vector->metas.vlen; i++ ) {
    *c++ = ' ';
    // dddddddd
    c = write_HEX_dword( c, elem->data );
    elem++;
  }
  *c++ = '\0';
  return c;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __serialize_euclidean_vector_elements( const vgx_Vector_t *vector, char *output ) {
  const BYTE *elem = ivectorobject.GetElements( (vgx_Vector_t*)vector );
  char *c = output;
  *c++ = ' '; // sep
  // Raw bytes
  memcpy( c, elem, vector->metas.vlen );
  c += vector->metas.vlen;
  *c++ = '\0';
  return c;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __vxoballoc_vector__serialize( const vgx_Vector_t *vector ) {

  vgx_VectorContext_t *vctx = CALLABLE( vector )->Context( vector );
  vgx_Similarity_t *sim = vctx ? vctx->simobj : NULL;
  vgx_Graph_t *graph = sim ? sim->parent : NULL;
  object_allocator_context_t *alloc = graph ? graph->ephemeral_string_allocator_context : NULL;

  // FEATURE VECTOR RAW FORMAT:
  //                                         9         9      ...       9
  //                 36                  [_______][_______][_______][_______] 
  // ____________________________________111111111222222222333333333nnnnnnnnnz
  // FF TT LLLL MMMMMMMM PPPPPPPPPPPPPPPP dddddddd dddddddd ........ dddddddd0
  //
  //
  //
  // EUCLIDEAN VECTOR RAW FORMAT:
  //                                                    vlen
  //                 36                   [______________________________] 
  // ____________________________________
  // FF TT LLLL MMMMMMMM PPPPPPPPPPPPPPPP bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0
  //

  int32_t vlen = vector->metas.vlen;
  int32_t headbytes = 36;
  int32_t vectorbytes;
  if( vector->metas.flags.ecl ) {
    vectorbytes = 1 + vlen; // one byte per element plus leading space separator
  }
  else {
    vectorbytes = vlen * 9; // includes leading space per dim
  }
  int32_t remain = headbytes + vectorbytes;

  CString_constructor_args_t args = {
    .string       = NULL,
    .len          = remain,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = alloc
  };

  CString_t *CSTR__vector = COMLIB_OBJECT_NEW( CString_t, NULL, &args );
  if( CSTR__vector ) {

    char *data = (char*)CALLABLE( CSTR__vector )->ModifiableQwords( CSTR__vector );
    char *c = data;
    
    // ___________________
    // COMMON HEADER : 36 bytes

    // FF (flags)
    c = write_HEX_byte( c, vector->metas.flags.bits );
    *c++ = ' ';
    
    // TT (type)
    c = write_HEX_byte( c, vector->metas.type );
    *c++ = ' ';
    
    // LLLL (vlen)
    c = write_HEX_word( c, vector->metas.vlen );
    *c++ = ' ';

    // MMMMMMMM (mag)
    c = write_HEX_dword( c, vector->metas.scalar.bits );
    *c++ = ' ';

    // PPPPPPPPPPPPPPPP (fingerprint)
    c = write_HEX_qword( c, vector->fp );

    remain -= headbytes;

    //
    // ELEMENTS
    //

    if( !(vector->metas.type & __VECTOR__MASK_EXTERNAL) && vectorbytes <= remain ) {
      if( vector->metas.flags.ecl ) {
        c = __serialize_euclidean_vector_elements( vector, c );
      }
      else {
        c = __serialize_feature_vector_elements( vector, c );
      }
    }

  }

  return CSTR__vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __deserialize_feature_vector( vgx_Similarity_t *sim, const char *input, vgx_VectorMetas_t *metas, CString_t **CSTR__error, uint32_t *dimerr ) {
  vgx_Vector_t *vector = NULL;
  vector_feature_t *elements = NULL;
  const char *p = input;

  XTRY {
    // Dimensions
    if( (elements = calloc( metas->vlen + 1, sizeof( vector_feature_t ) )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    vector_feature_t *cursor = elements;
    const vector_feature_t *end = elements + metas->vlen;

    while( cursor < end ) {
      // ' '
      ++p;

      // dddddddd (dim)
      if( (p = hex_to_DWORD( p, (DWORD*)&cursor->data )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
      }

      ++cursor;
    }

    // Create unpopulated vector
    vgx_Vector_constructor_args_t vargs = {
      .vlen           = metas->vlen,
      .ephemeral      = false,
      .type           = metas->type,
      .elements       = NULL,
      .scale          = 1.0f,
      .simcontext     = sim
    };
    if( (vector = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Populate vector
    void *vector_elements = ivectorobject.GetElements( vector );
    vector_feature_t *dest = (vector_feature_t*)vector_elements;
    cursor = elements;
    int64_t refc = 0;
    GRAPH_LOCK( sim->parent ) {
      while( cursor < end && (refc = _vxenum_dim__own_CS( sim, cursor->dim )) > 0 ) {
        dest++->data = cursor++->data;
      }
    } GRAPH_RELEASE;
    if( elements != end && refc < 1 ) {
      if( dimerr ) {
        *dimerr = cursor ? cursor->dim : 0;
      }
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Magnitude
    vector->metas.scalar.norm = metas->scalar.norm;
    // Flags
    vector->metas.flags = metas->flags;

  }
  XCATCH( errcode ) {
    if( vector ) {
      COMLIB_OBJECT_DESTROY( vector );
      vector = NULL;
    }

    if( dimerr && *dimerr != 0 ) {
      __format_error_string( CSTR__error, "vector deserialize error %03X, undefined dimension %08X", errcode, *dimerr );
    }
    else {
      __format_error_string( CSTR__error, "vector deserialize error %03X", errcode );
    }
  }
  XFINALLY {
    if( elements ) {
      free( elements );
    }
  }

  return vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __deserialize_euclidean_vector( vgx_Similarity_t *sim, const char *input, vgx_VectorMetas_t *metas, CString_t **CSTR__error ) {
  vgx_Vector_t *vector = NULL;
  const char *p = input;

  XTRY {
    // Skip initial ' ' separator
    ++p;

    // Create unpopulated vector
    vgx_Vector_constructor_args_t vargs = {
      .vlen           = metas->vlen,
      .ephemeral      = false,
      .type           = metas->type,
      .elements       = NULL,
      .scale          = 1.0f,
      .simcontext     = sim
    };
    if( (vector = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Populate vector directly from raw input data
    BYTE *vector_elements = (BYTE*)ivectorobject.GetElements( vector );
    memcpy( vector_elements, p, metas->vlen );

    // Scaling factor or Magnitude
    vector->metas.scalar.bits = metas->scalar.bits;
    // Flags
    vector->metas.flags = metas->flags;

  }
  XCATCH( errcode ) {
    if( vector ) {
      COMLIB_OBJECT_DESTROY( vector );
      vector = NULL;
    }

    __format_error_string( CSTR__error, "vector deserialize error %03X", errcode );
  }
  XFINALLY {
  }

  return vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * __vxoballoc_vector__deserialize( vgx_Similarity_t *sim, const CString_t *CSTR__data, CString_t **CSTR__error, uint32_t *dimerr ) {

  vgx_Vector_t *vector = NULL;

  // FEATURE VECTOR RAW FORMAT:
  //                                         9         9      ...       9
  //                 36                  [_______][_______][_______][_______] 
  // ____________________________________111111111222222222333333333nnnnnnnnnz
  // FF TT LLLL MMMMMMMM PPPPPPPPPPPPPPPP dddddddd dddddddd ........ dddddddd0
  //
  //
  //
  // EUCLIDEAN VECTOR RAW FORMAT:
  //                                                    vlen
  //                 36                   [______________________________] 
  // ____________________________________
  // FF TT LLLL MMMMMMMM PPPPPPPPPPPPPPPP bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0
  //


  XTRY {
    const char *p = CStringValue( CSTR__data );
    vgx_VectorMetas_t metas = {0};
    QWORD fp = 0;

    // FF (flags)
    if( (p = hex_to_BYTE( p, &metas.flags.bits )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    // ' '
    ++p;

    // TT (type)
    if( (p = hex_to_BYTE( p, &metas.type )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x002 );
    }

    // External vectors not supported
    if( (metas.type & __VECTOR__MASK_EXTERNAL) ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    // ' '
    ++p;

    // LLLL (vlen)
    if( (p = hex_to_WORD( p, &metas.vlen )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x004 );
    }

    // ' '
    ++p;

    // MMMMMMMM (mag)
    if( (p = hex_to_DWORD( p, &metas.scalar.bits )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x005 );
    }

    // ' '
    ++p;

    // PPPPPPPPPPPPPPPP (fingerprint)
    if( (p = hex_to_QWORD( p, &fp )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
    }

    // Deserialize elements
    bool euclidean = igraphfactory.EuclideanVectors();
    bool is_null = metas.type == VECTOR_TYPE_NULL;
    bool is_euclidean = (metas.type & __VECTOR__MASK_EUCLIDEAN) != 0;
    bool is_feature = (metas.type & __VECTOR__MASK_FEATURE) != 0;

    if( euclidean && (is_null || is_euclidean) ) {
      vector = __deserialize_euclidean_vector( sim, p, &metas, CSTR__error );
    }
    else if( !euclidean && (is_null || is_feature) ) {
      vector = __deserialize_feature_vector( sim, p, &metas, CSTR__error, dimerr );
    }
    else {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x006 );
    }

    // Assign fingerprint
    if( vector ) {
      vector->fp = fp;
    }

  }
  XCATCH( errcode ) {
    if( vector ) {
      COMLIB_OBJECT_DESTROY( vector );
      vector = NULL;
    }

    __format_error_string( CSTR__error, "vector deserialize error %03X", errcode );
  }
  XFINALLY {
  }

  return vector;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Similarity_t * __vxoballoc_vector__set_simobj( vgx_Vector_t *self, vgx_Similarity_t *simobj ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  vector_context->simobj = simobj;
  return simobj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_Similarity_t * __vxoballoc_vector__get_simobj( const vgx_Vector_t *self ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  return vector_context->simobj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void * __vxoballoc_vector__set_elements( vgx_Vector_t *self, void *elements ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  vector_context->elements = elements;
  return elements;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void * __vxoballoc_vector__get_elements( vgx_Vector_t *self ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  return vector_context->elements;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VectorContext_t * __vxoballoc_vector__set_context( const vgx_Vector_t *self, vgx_Similarity_t *simobj, void *elements ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  vector_context->simobj = simobj;
  vector_context->elements = elements;
  return vector_context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VectorContext_t * __vxoballoc_vector__get_context( const vgx_Vector_t *self ) {
  vgx_VectorContext_t *vector_context = (vgx_VectorContext_t*)&(_cxmalloc_linehead_from_object( self )->metaflex);
  return vector_context;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_serialize_internal_feature_vector( cxmalloc_line_serialization_context_t *context ) {
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // --------------
  // cxmalloc metas
  // [0 QW]
  // --------------
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // ------------------------------------------
  // cxmalloc obj
  // [2 QW] (i.e. the vgx_VectorMetas_t + FP_t)
  // ------------------------------------------
  QWORD *cursor = context->tapout.line_obj;
  *cursor++ = self->metas.qword;
  *cursor++ = self->fp;

  // ----------------------------------------
  // cxmalloc array
  // [variable] (i.e. the array of vector_feature_t)
  // ----------------------------------------
  const vector_feature_t *src = __vxoballoc_vector__get_elements( self );
  const vector_feature_t * const end = src + self->metas.vlen;
  vector_feature_t *dest = (vector_feature_t*)context->tapout.line_array;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_deserialize_internal_feature_vector( cxmalloc_line_deserialization_context_t *context ) {
  vgx_Similarity_t *simobj = (vgx_Similarity_t*)context->auxiliary[ SIMILARITY_AUX_IDX ];
  

  // The vector's memory is already allocated internally in cxmalloc, find the object address and cast it to a vector pointer.
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // Compute the address of elements array manually and set the vector context
  void *elements = (char*)self + sizeof(vgx_VectorHead_t);
  __vxoballoc_vector__set_context( self, simobj, elements );

  // cxmalloc obj
  // Initialize the object and set the fields
  if( COMLIB_OBJECT_INIT( vgx_Vector_t, self, NULL) == NULL ) {
    return -1;
  }
  QWORD *cursor = context->tapin.line_obj;
  self->metas.qword = *cursor++;
  self->fp = *cursor++;

  // cxmalloc element array
  // Populate the vector array
  const vector_feature_t *src = (vector_feature_t*)context->tapin.line_array;
  const vector_feature_t * const end = src + self->metas.vlen;
  vector_feature_t *dest = (vector_feature_t*)elements;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;

  // At this point the vector object exists in the allocator with refcnt 0.
  // Restoration code elsewhere is responsible for setting the refcnt to an appropriate value.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_serialize_external_feature_vector( cxmalloc_line_serialization_context_t *context ) {
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // ------------------------------------------
  // cxmalloc obj
  // [2 QW] (i.e. the vgx_VectorMetas_t + FP_t)
  // ------------------------------------------
  QWORD *cursor = context->tapout.line_obj;
  *cursor++ = self->metas.qword;
  *cursor++ = self->fp;

  // --------------------------------------------
  // cxmalloc array
  // [variable] (i.e. the array of ext_vector_feature_t)
  // --------------------------------------------
  const ext_vector_feature_t *src = __vxoballoc_vector__get_elements( self );
  const ext_vector_feature_t * const end = src + self->metas.vlen;
  ext_vector_feature_t *dest = (ext_vector_feature_t*)context->tapout.line_array;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_deserialize_external_feature_vector( cxmalloc_line_deserialization_context_t *context ) {
  vgx_Similarity_t *simobj = (vgx_Similarity_t*)context->auxiliary[ SIMILARITY_AUX_IDX ];


  // The vector's memory is already allocated internally in cxmalloc, find the object address and cast it to a vector pointer.
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // Compute the address of elements array manually and set the vector context
  void *elements = (char*)self + sizeof(vgx_VectorHead_t);
  __vxoballoc_vector__set_context( self, simobj, elements );

  // cxmalloc obj
  // Initialize the object and set the fields
  if( COMLIB_OBJECT_INIT( vgx_Vector_t, self, NULL) == NULL ) {
    return -1;
  }
  QWORD *cursor = context->tapin.line_obj;
  self->metas.qword = *cursor++;
  self->fp = *cursor++;

  // cxmalloc element array
  // Populate the vector array
  const ext_vector_feature_t *src = (ext_vector_feature_t*)context->tapin.line_array;
  const ext_vector_feature_t * const end = src + self->metas.vlen;
  ext_vector_feature_t *dest = (ext_vector_feature_t*)elements;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;

  // At this point the vector object exists in the allocator with refcnt 0.
  // Restoration code elsewhere is responsible for setting the refcnt to an appropriate value.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_serialize_internal_euclidean_vector( cxmalloc_line_serialization_context_t *context ) {
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // --------------
  // cxmalloc metas
  // [0 QW]
  // --------------
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // ------------------------------------------
  // cxmalloc obj
  // [2 QW] (i.e. the vgx_VectorMetas_t + FP_t)
  // ------------------------------------------
  QWORD *cursor = context->tapout.line_obj;
  *cursor++ = self->metas.qword;
  *cursor++ = self->fp;

  // ----------------------------------------
  // cxmalloc array
  // [variable] (i.e. the array of bytes)
  // ----------------------------------------
  const BYTE *src = __vxoballoc_vector__get_elements( self );
  const BYTE * const end = src + self->metas.vlen;
  BYTE *dest = (BYTE*)context->tapout.line_array;
  while( src < end ) {
    *dest++ = *src++;
  }

#ifdef VGX_CONSISTENCY_CHECK
#ifdef CXMALLOC_CONSISTENCY_CHECK

  int vlen = self->metas.vlen;
  int bsz = 5 * vlen + 8;
  char *buffer = calloc( 1, bsz );
  if( buffer ) {
    char *p = buffer;
    p = CALLABLE( self )->ToBuffer( self, bsz, &p );
    size_t sz_data = 0;
    if( p > buffer ) {
      sz_data = (p - buffer) - 1; // disregard 0-term
    }

    char flags_str[9];

    uint8_to_bin( flags_str, self->metas.flags.bits );

    fprintf( context->objdump, "<vgx_Vector_t type=%02x flags=%s len=%d mag=%#g fp=%016llx> ", self->metas.type, flags_str, vlen, self->metas.scalar.norm, self->fp );
    fwrite( buffer, 1, sz_data, context->objdump );

    free( buffer );
  }

#endif
#endif

  // success
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_deserialize_internal_euclidean_vector( cxmalloc_line_deserialization_context_t *context ) {
  vgx_Similarity_t *simobj = (vgx_Similarity_t*)context->auxiliary[ SIMILARITY_AUX_IDX ];
  

  // The vector's memory is already allocated internally in cxmalloc, find the object address and cast it to a vector pointer.
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // Compute the address of elements array manually and set the vector context
  void *elements = (char*)self + sizeof(vgx_VectorHead_t);
  __vxoballoc_vector__set_context( self, simobj, elements );

  // cxmalloc obj
  // Initialize the object and set the fields
  if( COMLIB_OBJECT_INIT( vgx_Vector_t, self, NULL) == NULL ) {
    return -1;
  }
  QWORD *cursor = context->tapin.line_obj;
  self->metas.qword = *cursor++;
  self->fp = *cursor++;

  // cxmalloc element array
  // Populate the vector array
  const BYTE *src = (BYTE*)context->tapin.line_array;
  const BYTE * const end = src + self->metas.vlen;
  BYTE *dest = (BYTE*)elements;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;

  // At this point the vector object exists in the allocator with refcnt 0.
  // Restoration code elsewhere is responsible for setting the refcnt to an appropriate value.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_serialize_external_euclidean_vector( cxmalloc_line_serialization_context_t *context ) {
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // ------------------------------------------
  // cxmalloc obj
  // [2 QW] (i.e. the vgx_VectorMetas_t + FP_t)
  // ------------------------------------------
  QWORD *cursor = context->tapout.line_obj;
  *cursor++ = self->metas.qword;
  *cursor++ = self->fp;

  // --------------------------------------------
  // cxmalloc array
  // [variable] (i.e. the array of floats)
  // --------------------------------------------
  const float *src = __vxoballoc_vector__get_elements( self );
  const float * const end = src + self->metas.vlen;
  float *dest = (float*)context->tapout.line_array;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __vxoballoc_vector__cxmalloc_deserialize_external_euclidean_vector( cxmalloc_line_deserialization_context_t *context ) {
  vgx_Similarity_t *simobj = (vgx_Similarity_t*)context->auxiliary[ SIMILARITY_AUX_IDX ];


  // The vector's memory is already allocated internally in cxmalloc, find the object address and cast it to a vector pointer.
  vgx_Vector_t *self = (vgx_Vector_t*)_cxmalloc_object_from_linehead( context->linehead );

  // cxmalloc metas
  // vgx_VectorContext_t
  //  1.  void *elements
  //  2.  vgx_Similarity_t *context

  // Compute the address of elements array manually and set the vector context
  void *elements = (char*)self + sizeof(vgx_VectorHead_t);
  __vxoballoc_vector__set_context( self, simobj, elements );

  // cxmalloc obj
  // Initialize the object and set the fields
  if( COMLIB_OBJECT_INIT( vgx_Vector_t, self, NULL) == NULL ) {
    return -1;
  }
  QWORD *cursor = context->tapin.line_obj;
  self->metas.qword = *cursor++;
  self->fp = *cursor++;

  // cxmalloc element array
  // Populate the vector array
  const float *src = (float*)context->tapin.line_array;
  const float * const end = src + self->metas.vlen;
  float *dest = (float*)elements;
  while( src < end ) {
    *dest++ = *src++;
  }

  // success
  return 0;

  // At this point the vector object exists in the allocator with refcnt 0.
  // Restoration code elsewhere is responsible for setting the refcnt to an appropriate value.
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxoballoc_vector.h"

test_descriptor_t _vgx_vxoballoc_vector_tests[] = {
  { "VGX Vector Object Allocation Tests", __utest_vxoballoc_vector },

  {NULL}
};
#endif

