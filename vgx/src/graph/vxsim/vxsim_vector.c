/*######################################################################
 *#
 *# vxsim_vector.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxsim.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_VECTOR );

#if FEATURE_VECTOR_MAG_BITS != 6
#error "This implementation requires 6-bit feature vector element magnitude"
#endif


DLL_VISIBLE comlib_object_typeinfo_t _vgx_Vector_t_typeinfo = {0}; // Will be set when class is registered



static int __populate_feature_vector( vgx_Vector_t *self, vgx_Vector_constructor_args_t *args );
static int __populate_euclidean_vector( vgx_Vector_t *self, vgx_Vector_constructor_args_t *args );

static int __set_euclidean_vector( vgx_Vector_t *self, int nelem, float scale, const void *src_elements, void *dest_elements );
static int __set_feature_vector( vgx_Vector_t *self, int nelem, const void *src_elements, void *dest_elements );

static int __copy_map_elements( const vgx_Vector_t *self, vgx_Vector_t *copy );
static int __copy_euclidean_elements( const vgx_Vector_t *self, vgx_Vector_t *copy );

static char * __map_to_buffer( const vgx_Vector_t *self, int max_sz, char **cursor );
static char * __euclidean_to_buffer( const vgx_Vector_t *self, int max_sz, char **cursor );



/* common comlib_object_vtable_t interface */
static int64_t Vector_serialize( vgx_Vector_t *self, CQwordQueue_t *output );
static vgx_Vector_t * Vector_deserialize( CQwordQueue_t *input );
static vgx_Vector_t * Vector_constructor( const void *identifier, vgx_Vector_constructor_args_t *args );
static void Vector_destructor( vgx_Vector_t *self );
static CStringQueue_t * Vector_repr( const vgx_Vector_t *self, CStringQueue_t *output );
static comlib_object_t * Vector_allocator( vgx_Vector_t *self );
/* Vector interface */
// TODO
static int64_t            Vector_incref( vgx_Vector_t *self );
static int64_t            Vector_decref( vgx_Vector_t *self );
static int64_t            Vector_refcnt( const vgx_Vector_t *self );
static vgx_Vector_t *     Vector_set( vgx_Vector_t *self, int nelem, float scale, const void *elements );
static vgx_Vector_t *     Vector_clone( const vgx_Vector_t *self, bool ephemeral );
static vgx_Vector_t *     Vector_own_or_clone( const vgx_Vector_t *self, bool ephemeral );
static vgx_Vector_t *     Vector_copy( const vgx_Vector_t *self, vgx_Vector_t *other );
static void *             Vector_elements( const vgx_Vector_t *self );
static vgx_VectorContext_t * Vector_context( const vgx_Vector_t *self );
static int                Vector_length( const vgx_Vector_t *self );
static float              Vector_magnitude( const vgx_Vector_t *self );
static float              Vector_scaler( const vgx_Vector_t *self );
static bool               Vector_is_null( const vgx_Vector_t *self );
static bool               Vector_is_internal( const vgx_Vector_t *self );
static bool               Vector_is_centroid( const vgx_Vector_t *self );
static bool               Vector_is_external( const vgx_Vector_t *self );
static bool               Vector_is_ephemeral( const vgx_Vector_t *self );
static bool               Vector_is_euclidean( const vgx_Vector_t *self );
static FP_t               Vector_fingerprint( const vgx_Vector_t *self );
static objectid_t         Vector_identity( const vgx_Vector_t *self );
static char *             Vector_to_buffer( const vgx_Vector_t *self, int max_sz, char **buffer );
static CString_t *        Vector_short_repr( const vgx_Vector_t *self );



static vgx_Vector_vtable_t Vector_Methods = {
  /* common comlib_object_vtable_t interface */
  .vm_cmpid       = NULL,
  .vm_getid       = NULL,
  .vm_serialize   = (f_object_serializer_t)Vector_serialize,
  .vm_deserialize = (f_object_deserializer_t)Vector_deserialize,
  .vm_construct   = (f_object_constructor_t)Vector_constructor,
  .vm_destroy     = (f_object_destructor_t)Vector_destructor,
  .vm_represent   = (f_object_representer_t)Vector_repr,
  .vm_allocator   = (f_object_allocator_t)Vector_allocator,
  /* Vector interface */
  .Incref         = Vector_incref,
  .Decref         = Vector_decref,
  .Refcnt         = Vector_refcnt,
  .Set            = Vector_set,
  .Clone          = Vector_clone,
  .OwnOrClone     = Vector_own_or_clone,
  .Copy           = Vector_copy,
  .Elements       = Vector_elements,
  .Context        = Vector_context,
  .Length         = Vector_length,
  .Magnitude      = Vector_magnitude,
  .Scaler         = Vector_scaler,
  .IsNull         = Vector_is_null,
  .IsInternal     = Vector_is_internal,
  .IsCentroid     = Vector_is_centroid,
  .IsExternal     = Vector_is_external,
  .IsEphemeral    = Vector_is_ephemeral,
  .IsEuclidean    = Vector_is_euclidean,
  .Fingerprint    = Vector_fingerprint,
  .Identity       = Vector_identity,
  .ToBuffer       = Vector_to_buffer,
  .ShortRepr      = Vector_short_repr
  // TODO
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Vector_RegisterClass( void ) {
  size_t bytes;
  if( (bytes = sizeof( vgx_Vector_t     )) != 32 ) FATAL( 0xD01, "vgx_Vector_t must be 32 bytes, got %llu",     bytes );
  COMLIB_REGISTER_CLASS( vgx_Vector_t, CXLIB_OBTYPE_VECTOR, &Vector_Methods, OBJECT_IDENTIFIED_BY_NONE, -1 );
  _vgx_Vector_t_typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Vector_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Vector_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Vector_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t Vector_serialize( vgx_Vector_t *self, CQwordQueue_t *output ) {

  // TODO: Is this function really needed?

  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Vector_t * Vector_deserialize( CQwordQueue_t *input ) {
  return NULL; // not supported. TODO: support.
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __populate_feature_vector( vgx_Vector_t *self, vgx_Vector_constructor_args_t *args ) {
  // Pointer to the vector's array
  void *elements = ivectorobject.GetElements( self );

  // External vector from supplied external elements data
  if( (args->type & __VECTOR__MASK_EXTERNAL) ) {
    ext_vector_feature_t *src = (ext_vector_feature_t*)args->elements;
    ext_vector_feature_t *dest = (ext_vector_feature_t*)elements;
    ext_vector_feature_t *end = dest + args->vlen;
    double sum_sq_mag = 0.0;
    double weight;
    while( dest < end ) {
      weight = src->weight;
      sum_sq_mag += weight * weight;
      (dest++)->data = (src++)->data; // Copy
    }
    self->metas.scalar.norm = (float)sqrt( sum_sq_mag );
  }
  // Internal vector from supplied internal elements data
  else {
    vector_feature_t *src = (vector_feature_t*)args->elements;
    vector_feature_t *dest = (vector_feature_t*)elements;
    vector_feature_t *end = dest + args->vlen;
    unsigned sum_sq_imag = 0;
    while( dest < end ) {
      sum_sq_imag += _vxsim_square_magnitude_map[ src->mag ];
      (dest++)->data = (src++)->data;
    }
    self->metas.scalar.norm = (float)sqrt( _vxsim_square_magnitude_from_isqmag( sum_sq_imag ) );
  }
  self->metas.flags.pop = 1;

  // Compute vector's fingerprint if we supplied a fingerprinter
  vgx_Similarity_t *simobj = args->simcontext;
  vgx_Fingerprinter_t *fingerprinter = simobj->fingerprinter;
  if( fingerprinter ) {
    self->fp = CALLABLE(fingerprinter)->Compute( fingerprinter, self, 0, NULL );
    if( self->fp == 0 ) {
      return -1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __populate_euclidean_vector( vgx_Vector_t *self, vgx_Vector_constructor_args_t *args ) {
  // Pointer to the vector's array
  void *elements = ivectorobject.GetElements( self );

  // External vector from supplied external elements data
  if( (args->type & __VECTOR__MASK_EXTERNAL) ) {
    float *src = (float*)args->elements;
    float *dest = (float*)elements;
    float *end = dest + args->vlen;
    double sum_sq_mag = 0.0;
    while( dest < end ) {
      double x = *dest++ = *src++;
      sum_sq_mag += x * x;
    }
    self->metas.scalar.norm = (float)sqrt( sum_sq_mag );
  }
  // Internal vector from supplied internal elements data and scaling factor
  else {
    char *src = (char*)args->elements;
    char *dest = (char*)elements;
    char *end = dest + args->vlen;
    while( dest < end ) {
      *dest++ = *src++;
    }

    self->metas.scalar.factor = args->scale;
  }
  self->metas.flags.pop = 1;

  // Compute vector's fingerprint if we supplied a fingerprinter
  vgx_Similarity_t *simobj = args->simcontext;
  vgx_Fingerprinter_t *fingerprinter = simobj->fingerprinter;
  if( fingerprinter ) {
    self->fp = CALLABLE(fingerprinter)->Compute( fingerprinter, self, 0, NULL );
    if( self->fp == 0 ) {
      return -1;
    }
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Vector_constructor( const void *identifier, vgx_Vector_constructor_args_t *args ) {
  vgx_Vector_t *self = NULL;
  vgx_Similarity_t *simobj = args->simcontext;

  if( args->vlen == 0 ) {
    args->type = VECTOR_TYPE_NULL;
  }

  // null-vector 
  if( args->type == VECTOR_TYPE_NULL ) {
    self = ivectorobject.Null( simobj );
  }
  else {

    if( identifier ) {
      WARN( 0xD11, "identifier not supported" );
    }

    XTRY {
      // New, valid comlib object, but with no element data yet
      if( (self = ivectorobject.New( simobj, args->type, args->vlen, args->ephemeral )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xD12 );
      }

      // Populate vector if elements data provided
      if( args->elements ) {
        if( self->metas.flags.ecl ) {
          if( __populate_euclidean_vector( self, args ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0xD13 );
          }
        }
        else {
          if( __populate_feature_vector( self, args ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0xD14 );
          }
        }
      }
    }
    XCATCH( errcode ) {
      if( self ) {
        ivectorobject.Delete( self );
        self = NULL;
      }
    }
    XFINALLY {}
  }

  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void Vector_destructor( vgx_Vector_t *self ) {
  if( self ) {
    Vector_decref( self );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static comlib_object_t * Vector_allocator( vgx_Vector_t *self ) {
  return (comlib_object_t*)ivectoralloc.Get( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vector_incref( vgx_Vector_t *self ) {
  return ivectorobject.Incref( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vector_decref( vgx_Vector_t *self ) {
  return ivectorobject.Decref( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t Vector_refcnt( const vgx_Vector_t *self ) {
  return ivectorobject.Refcnt( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_feature_vector( vgx_Vector_t *self, int nelem, const void *src_elements, void *dest_elements ) {
  // Set external vector from elements interpreted as external elements
  if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    ext_vector_feature_t *src = (ext_vector_feature_t*)src_elements;
    ext_vector_feature_t *src_end = src + nelem;
    ext_vector_feature_t *dest = dest_elements;
    ext_vector_feature_t *dest_end = src + self->metas.vlen;
    double sum_sq_mag = 0.0;
    double weight;
    // Copy external source elements to destination, accumulate square sums for magnitude calculation
    while( dest < dest_end && src < src_end ) {
      weight = src->weight;
      sum_sq_mag += weight * weight;
      (dest++)->data = (src++)->data;
    }
    // Set trailing destination elements to 0 if source data was insufficient for vector length
    while( dest < dest_end ) {
      memset( dest++, 0, sizeof(ext_vector_feature_t) );
    }
    // Compute external vector magnitude
    self->metas.scalar.norm = (float)sqrt( sum_sq_mag );
  }
  // Set internal vector from elements interpreted as internal elements
  else {
    vector_feature_t *src = (vector_feature_t*)src_elements;
    vector_feature_t *src_end = src + nelem;
    vector_feature_t *dest = dest_elements;
    vector_feature_t *dest_end = src + self->metas.vlen;
    unsigned sum_sq_imag = 0;
    // Copy internal source elements to destination, accumulate square sums for magnitude calculation
    while( dest < dest_end && src < src_end ) {
      sum_sq_imag += _vxsim_square_magnitude_map[ src->mag ];
      (dest++)->data = (src++)->data;
    }
    // Set trailing destination elements to 0 if source data was insufficient for vector length
    while( dest < dest_end ) {
      (dest++)->data = 0;
    }
    // Compute internal vector magnitude
    self->metas.scalar.norm = (float)sqrt( _vxsim_square_magnitude_from_isqmag( sum_sq_imag ) );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __set_euclidean_vector( vgx_Vector_t *self, int nelem, float scale, const void *src_elements, void *dest_elements ) {
  // Set external vector from elements interpreted as external elements
  if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    float *src = (float*)src_elements;
    float *src_end = src + nelem;
    float *dest = dest_elements;
    float *dest_end = src + self->metas.vlen;
    double sum_sq_mag = 0.0;
    // Copy external source elements to destination, accumulate square sums for magnitude calculation
    while( dest < dest_end && src < src_end ) {
      double x = *dest++ = *src++;
      sum_sq_mag += x * x;
    }
    // Set trailing destination elements to 0 if source data was insufficient for vector length
    while( dest < dest_end ) {
      *dest++ = 0;
    }
    // Compute external vector magnitude
    self->metas.scalar.norm = (float)sqrt( sum_sq_mag );
  }
  // Set internal vector from elements interpreted as internal elements
  else {
    char *src = (char*)src_elements;
    char *src_end = src + nelem;
    char *dest = dest_elements;
    char *dest_end = src + self->metas.vlen;
    // Copy internal source elements to destination, accumulate square sums for magnitude calculation
    while( dest < dest_end && src < src_end ) {
      *dest++ = *src++;
    }
    // Set trailing destination elements to 0 if source data was insufficient for vector length
    while( dest < dest_end ) {
      *dest++ = 0;
    }

    self->metas.scalar.factor = scale;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Vector_set( vgx_Vector_t *self, int nelem, float scale, const void *elements ) {
  if( !self->metas.flags.nul ) {
    vgx_VectorContext_t *context = ivectorobject.GetContext( self );
    vgx_Fingerprinter_t *fingerprinter = context->simobj->fingerprinter;

    if( self->metas.flags.ecl ) {
      __set_euclidean_vector( self, nelem, scale, elements, context->elements );
    }
    else {
      __set_feature_vector( self, nelem, elements, context->elements );
    }
    
    if( nelem > 0 ) {
      self->metas.flags.pop = 1;
    }

    if( fingerprinter && self->metas.flags.pop ) {
      self->fp = CALLABLE(fingerprinter)->Compute( fingerprinter, self, 0, NULL );
    }

  }
  return self;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __copy_map_elements( const vgx_Vector_t *self, vgx_Vector_t *copy ) {
  // Copy vector elements
  if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    const ext_vector_feature_t *src = ivectorobject.GetElements( (vgx_Vector_t*)self );
    const ext_vector_feature_t *end = src + self->metas.vlen;
    ext_vector_feature_t *dest = ivectorobject.GetElements( copy );
    while( src < end ) {
      dest++->data = src++->data;
    }
  }
  else {
    const vector_feature_t *src = ivectorobject.GetElements( (vgx_Vector_t*)self );
    const vector_feature_t *end = src + self->metas.vlen;
    vector_feature_t *dest = ivectorobject.GetElements( copy );
    while( src < end ) {
      dest++->data = src++->data;
    }
    ivectorobject.IncrefDimensions( copy );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __copy_euclidean_elements( const vgx_Vector_t *self, vgx_Vector_t *copy ) {
  // Copy vector elements
  if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
    const float *src = ivectorobject.GetElements( (vgx_Vector_t*)self );
    const float *end = src + self->metas.vlen;
    float *dest = ivectorobject.GetElements( copy );
    while( src < end ) {
      *dest++ = *src++;
    }
  }
  else {
    const BYTE *src = ivectorobject.GetElements( (vgx_Vector_t*)self );
    const BYTE *end = src + self->metas.vlen;
    BYTE *dest = ivectorobject.GetElements( copy );
    while( src < end ) {
      *dest++ = *src++;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Vector_clone( const vgx_Vector_t *self, bool ephemeral ) {
  vgx_Vector_t *clone;

  //
  // !!!!! ?????
  // TODO: Use the COMLIB CLONE mechanism so that when we destroy the clone, the original is left intact
  //       [SL 20171031]: What does this mean? The COMLIB CLONE sets a clone flag and selects a special
  //                      clone destructor. Why is this needed?
  // !!!!! ?????
  //

  vgx_VectorContext_t *context = ivectorobject.GetContext( self );

  if( self->metas.flags.nul ) {
    clone = ivectorobject.Null( context->simobj );
  }
  else {
    // Create a new, unpopulated vector of same type and length as original
    vgx_Vector_constructor_args_t vargs = {
      .vlen           = self->metas.vlen,
      .ephemeral      = ephemeral,
      .type           = self->metas.type,
      .elements       = NULL,
      .scale          = 1.0f,
      .simcontext     = context->simobj
    };

    if( (clone = COMLIB_OBJECT_NEW( vgx_Vector_t, NULL, &vargs )) != NULL ) {
      // Copy metas and fingerprint
      clone->metas = self->metas;
      clone->fp = self->fp;
      // Override the ephemeral flag to desired mode
      clone->metas.flags.eph = ephemeral;

      if( self->metas.flags.ecl ) {
        __copy_euclidean_elements( self, clone );
      }
      else {
        __copy_map_elements( self, clone );
      }
    }
  }

  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Vector_t * Vector_own_or_clone( const vgx_Vector_t *self, bool ephemeral ) {
  if( ivectorobject.Incref((vgx_Vector_t*)self) > 0 ) {
    return (vgx_Vector_t*)self;
  }
  return CALLABLE(self)->Clone(self, ephemeral);
}



/*******************************************************************//**
 *
 * Copy contents into other vector, or return a new clone if other is NULL.
 *
 * Return: pointer to destination vector
 *
 ***********************************************************************
 */
static vgx_Vector_t * Vector_copy( const vgx_Vector_t *self, vgx_Vector_t *other ) {
  vgx_Vector_t *copy = NULL;

  // Don't copy to self
  if( self == other ) {
    copy = (vgx_Vector_t*)self;
    CALLABLE(copy)->Incref(copy);
  }
  // If destination is NULL create and return a clone of self with the same ephemeral/persistent mode
  else if( other == NULL ) {
    copy = Vector_clone( self, self->metas.flags.eph );
  }
  // Compatible?
  else if( self->metas.flags.compat.bits == other->metas.flags.compat.bits && other->metas.vlen >= self->metas.vlen ) {
    copy = other;
    ivectorobject.SetSimobj( copy, ivectorobject.GetSimobj( self ) );
    copy->metas = self->metas;
    copy->fp = self->fp;
    if( self->metas.flags.ecl ) {
      __copy_euclidean_elements( self, copy );
    }
    else {
      __copy_map_elements( self, copy );
    }
  }

  return copy;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void * Vector_elements( const vgx_Vector_t *self ) {
  return ivectorobject.GetElements( (vgx_Vector_t*)self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VectorContext_t * Vector_context( const vgx_Vector_t *self ) {
  return ivectorobject.GetContext( self );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Vector_length( const vgx_Vector_t *self ) {
  return self->metas.vlen;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float Vector_magnitude( const vgx_Vector_t *self ) {
  if( self->metas.flags.ecl && (self->metas.type & __VECTOR__MASK_INTERNAL) ) {
    // Internal euclidean vectors don't have magnitude stored.
    // We have to compute the magnitude
    const BYTE *elem = ivectorobject.GetElements( (vgx_Vector_t*)self );
    double magnitude = sqrt( vxeval_bytearray_sum_squares( elem, self->metas.vlen ) ) * self->metas.scalar.factor;
    return (float)magnitude;
  }
  return self->metas.scalar.norm;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static float Vector_scaler( const vgx_Vector_t *self ) {
  if( self->metas.flags.ecl ) { // && (self->metas.type & __VECTOR__MASK_INTERNAL) ) {
    return self->metas.scalar.factor;
  }
  return 1.0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_null( const vgx_Vector_t *self ) {
  return self->metas.type == VECTOR_TYPE_NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_internal( const vgx_Vector_t *self ) {
  return self->metas.type & (uint8_t)__VECTOR__MASK_INTERNAL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_centroid( const vgx_Vector_t *self ) {
  return self->metas.type & (uint8_t)__VECTOR__MASK_CENTROID;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_external( const vgx_Vector_t *self ) {
  return self->metas.type & (uint8_t)__VECTOR__MASK_EXTERNAL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_ephemeral( const vgx_Vector_t *self ) {
  return self->metas.flags.eph == 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool Vector_is_euclidean( const vgx_Vector_t *self ) {
  return self->metas.flags.ecl == 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static FP_t Vector_fingerprint( const vgx_Vector_t *self ) {
  return self->fp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static objectid_t Vector_identity( const vgx_Vector_t *self ) {
  int sz = self->metas.vlen;
  const char *data = ivectorobject.GetElements( (vgx_Vector_t*)self );
  // Feature vector
  if( self->metas.flags.ecl == 0 ) {
    sz *= sizeof( vector_feature_t );
  }
  return obid_from_string_len( data, sz );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __map_to_buffer( const vgx_Vector_t *self, int max_sz, char **cursor ) {
  int vlen = self->metas.vlen;
  char *c = *cursor;
  if( max_sz >= 3 ) { // account for at least "[]\0"
    *c++ = '[';
    --max_sz; // one less threshold
    if( !self->metas.flags.nul ) {
      if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
        const ext_vector_feature_t *elem = ivectorobject.GetElements( (vgx_Vector_t*)self );
        for( int i=0; i<vlen; i++ ) {
          if( max_sz > 64 ) { // <- more than worst-case for this element: ("something_up_27_characters",12345.6789),    plus the "]\0"
            const char *e = elem->term;
            char *start = c;
            *c++ = '[';
            *c++ = '"';
            while( (*c++ = *e++) != '\0' );
            --c;
            *c++ = '"';
            *c++ = ',';
            *c++ = ' ';
            c += sprintf( c, "%.4f", elem->weight );
            *c++ = ']';
            *c++ = ',';
            *c++ = ' ';
            max_sz -= (int)(c - start); // decrement by actual element size to get the new threshold,   plus the "[\0"
            elem++;
          }
          else {
            break; // no more room in buffer
          }
        }
      }
      else {
        const vector_feature_t *elem = ivectorobject.GetElements( (vgx_Vector_t*)self );
        for( int i=0; i<vlen; i++ ) {
          if( max_sz > 32 ) { // <- more than this element: (0x1234abcd,63),
            int sz = sprintf( c, "(0x%08x,%u), ", elem->dim, elem->mag );
            c += sz;
            max_sz -= sz; // decrement by actual element size to get the new threshold
            elem++;
          }
          else {
            break; // no more room in buffer
          }
        }
      }
      c -= 2; // erase the last comma and space
    }
    *c++ = ']';
    *c++ = '\0';
  }
  *cursor = c;
  return *cursor;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __euclidean_to_buffer( const vgx_Vector_t *self, int max_sz, char **cursor ) {
  int vlen = self->metas.vlen;
  char *c = *cursor;
  if( max_sz >= 3 ) { // account for at least "[]\0"
    *c++ = '[';
    --max_sz; // one less threshold
    if( !self->metas.flags.nul ) {
      if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
        const float *elem = ivectorobject.GetElements( (vgx_Vector_t*)self );
        for( int i=0; i<vlen; i++ ) {
          if( max_sz > 32 ) { // <- more than worst-case for this element: 123.456789,    plus the "]\0"
            int n = sprintf( c, "%.6f, ", *elem++ );
            c += n;
            max_sz -= n;
          }
          else {
            break; // no more room in buffer
          }
        }
      }
      else {
        const BYTE *elem = ivectorobject.GetElements( (vgx_Vector_t*)self );
        for( int i=0; i<vlen; i++ ) {
          if( max_sz > 8 ) { // <- more than this element: 123
            int n = sprintf( c, "%u, ", *elem++ );
            c += n;
            max_sz -= n;
          }
          else {
            break; // no more room in buffer
          }
        }
      }
      c -= 2; // erase the last comma and space
    }
    *c++ = ']';
    *c++ = '\0';
  }
  *cursor = c;
  return *cursor;

}



/*******************************************************************//**
 *
 *
 * NOTE: This method operates by streaming data into a buffer referenced
 * by a cursor that will be advanced by the method. DO NOT PASS IN THE
 * ACTUAL BUFFER, ITS ADDRESS WILL BE MODIFIED AND MEMORY LOST!
 ***********************************************************************
 */
static char * Vector_to_buffer( const vgx_Vector_t *self, int max_sz, char **cursor ) {
  if( self->metas.flags.ecl ) {
    return __euclidean_to_buffer( self, max_sz, cursor );
  }
  else {
    return __map_to_buffer( self, max_sz, cursor );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Vector_repr( const vgx_Vector_t *self, CStringQueue_t *output ) {
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )


  COMLIB_DefaultRepresenter( (const comlib_object_t*)self, output );
  PUT( "\n" );
  PUT( "<vgx_Vector_t at 0x%llx ", self );
  PUT( "type=%d ", self->metas.type );
  PUT( "nul=%d ", self->metas.flags.nul );
  PUT( "ext=%d ", self->metas.flags.ext );
  PUT( "pop=%d ", self->metas.flags.pop );
  PUT( "fp=0x%016llx ", self->fp );
  PUT( "mag=%#g ", Vector_magnitude( self ) );
  PUT( "scale=%#g ", self->metas.scalar.factor );
  PUT( "vlen=%d>\n", (int)self->metas.vlen );

  PUT( "vector=[" );
  const void *elements = ivectorobject.GetElements( (vgx_Vector_t*)self );
  if( !self->metas.flags.nul ) {
    if( self->metas.flags.ecl ) {
      if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
        const float *elem = elements;
        for( int i=0; i<self->metas.vlen; i++ ) {
          float x = *elem++;
          const char *t = i == self->metas.vlen-1 ? " " : ",";
          PUT( " %#g%s", x, t );
        }
      }
      else {
        const BYTE *elem = elements;
        for( int i=0; i<self->metas.vlen; i++ ) {
          BYTE x = *elem++;
          const char *t = i == self->metas.vlen-1 ? " " : ",";
          PUT( " %u%s", x, t );
        }
      }
    }
    else {
      if( (self->metas.type & __VECTOR__MASK_EXTERNAL) ) {
        const ext_vector_feature_t *elem = elements;
        char term[29] = {'\0'};
        for( int i=0; i<self->metas.vlen; i++ ) {
          const char *t = i == self->metas.vlen-1 ? " " : ",";
          strncpy( term, elem->term, 28 );
          PUT( " (%s, %#g)%s", term, elem->weight, t );
          elem++;
        }
      }
      else {
        const vector_feature_t *elem = elements;
        for( int i=0; i<self->metas.vlen; i++ ) {
          const char *t = i == self->metas.vlen-1 ? " " : ",";
          PUT( " (%08x, %u)%s", elem->dim, elem->mag, t );
          elem++;
        }
      }
    }
  }
  PUT( "]\n\n" );
  return output;
#undef PUT
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static CString_t * Vector_short_repr( const vgx_Vector_t *self ) {
  if( self ) {
    vgx_Vector_vtable_t *ivector = CALLABLE( self );
    return CStringNewFormat( "len=%d mag=%.4f fp=0x%016llX",
                                  ivector->Length( self ),
                                         ivector->Magnitude( self ),
                                                   ivector->Fingerprint( self )
                        );
  }
  else {
    return NULL;
  }
}


#include "tests/__utest_vxsim_vector.h"
  
test_descriptor_t _vgx_vxsim_vector_tests[] = {
  { "VGX Graph Vector Tests", __utest_vxsim_vector },
  {NULL}
};


