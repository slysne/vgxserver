/*######################################################################
 *#
 *# cxmalloc_api.c
 *#
 *######################################################################
 */

#include "_cxmalloc.h"
#include "versiongen.h"

#ifndef CXMALLOC_VERSION
#define CXMALLOC_VERSION ?.?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "cxmalloc", VERSIONGEN_XSTR( CXMALLOC_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "cxmalloc", VERSIONGEN_XSTR( CXMALLOC_VERSION ) );

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT const char * cxmalloc_version( bool ext ) {
  if( ext ) {
    return g_version_info_ext;
  }
  else {
    return g_version_info;
  }
}


/*******************************************************************//**
 * interface
 ***********************************************************************
 */
/* common interface */
static        longstring_t * cxmalloc_api__getid( cxmalloc_family_t *family );
static               int64_t cxmalloc_api__serialize( cxmalloc_family_t *family, CQwordQueue_t *out_queue );
static   cxmalloc_family_t * cxmalloc_api__deserialize( struct s_comlib_object_t *container, CQwordQueue_t *in_queue );
static   cxmalloc_family_t * cxmalloc_api__constructor( const void *identifier, const cxmalloc_family_constructor_args_t *args );
static                  void cxmalloc_api__destructor( cxmalloc_family_t *family );
static      CStringQueue_t * cxmalloc_api__repr( cxmalloc_family_t *family, CStringQueue_t *output );
/* allocator interface */
/* Allocate */
static                void * cxmalloc_api__new( cxmalloc_family_t *family, uint32_t sz );
/* Line array methods */
static                void * cxmalloc_api__renew( cxmalloc_family_t *family, void *line );
static               int64_t cxmalloc_api__own( cxmalloc_family_t *family, void *line );
static               int64_t cxmalloc_api__discard( cxmalloc_family_t *family, void *line );
static               int64_t cxmalloc_api__refcount( cxmalloc_family_t *family, const void *line );
static              uint32_t cxmalloc_api__prev_length( cxmalloc_family_t *family, const void *line );
static              uint32_t cxmalloc_api__next_length( cxmalloc_family_t *family, const void *line );
static              uint32_t cxmalloc_api__length_of( cxmalloc_family_t *family, const void *line );
static              uint16_t cxmalloc_api__index_of( cxmalloc_family_t *family, const void *line );
static                void * cxmalloc_api__object_from_array( cxmalloc_family_t *family, const void *line );
static     cxmalloc_handle_t cxmalloc_api__array_as_handle( cxmalloc_family_t *family, const void *line );
static                void * cxmalloc_api__handle_as_array( cxmalloc_family_t *family, cxmalloc_handle_t handle );
static cxmalloc_metaflex_t * cxmalloc_api__meta( cxmalloc_family_t *family, const void *line );
/* Object methods */
static                void * cxmalloc_api__renew_object( cxmalloc_family_t *family, void *obj );
static               int64_t cxmalloc_api__own_object_nolock( cxmalloc_family_t *family, void *obj );
static               int64_t cxmalloc_api__own_object( cxmalloc_family_t *family, void *obj );
static               int64_t cxmalloc_api__own_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle );
static               int64_t cxmalloc_api__discard_object_nolock( cxmalloc_family_t *family, void *obj );
static               int64_t cxmalloc_api__discard_object( cxmalloc_family_t *family, void *obj );
static               int64_t cxmalloc_api__discard_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle );
static               int64_t cxmalloc_api__refcount_object_nolock( cxmalloc_family_t *family, const void *obj );
static               int64_t cxmalloc_api__refcount_object( cxmalloc_family_t *family, const void *obj );
static               int64_t cxmalloc_api__refcount_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle );
static                void * cxmalloc_api__array_from_object( cxmalloc_family_t *family, const void *obj );
static     cxmalloc_handle_t cxmalloc_api__object_as_handle( cxmalloc_family_t *family, const void *obj );
static                void * cxmalloc_api__handle_as_object_nolock( cxmalloc_family_t *family, cxmalloc_handle_t handle );
static                void * cxmalloc_api__handle_as_object_safe( cxmalloc_family_t *family, cxmalloc_handle_t handle );

/* Family methods */
static              uint32_t cxmalloc_api__min_length( cxmalloc_family_t *family );
static              uint32_t cxmalloc_api__max_length( cxmalloc_family_t *family );
static              uint16_t cxmalloc_api__size_bounds( cxmalloc_family_t *family, uint32_t sz, uint32_t *low, uint32_t *high );
static                  void cxmalloc_api__lazy_discards( cxmalloc_family_t *family, int use_lazy_discards );
static                   int cxmalloc_api__check( cxmalloc_family_t *family );
static     comlib_object_t * cxmalloc_api__get_object_at_address( cxmalloc_family_t *family, QWORD address );
static     comlib_object_t * cxmalloc_api__find_object_by_obid( cxmalloc_family_t *family, objectid_t obid );
static     comlib_object_t * cxmalloc_api__get_object_by_offset( cxmalloc_family_t *family, int64_t *poffset );
static               int64_t cxmalloc_api__sweep( cxmalloc_family_t *family, f_get_object_identifier get_object_identifier );
static                   int cxmalloc_api__size( cxmalloc_family_t *family );
static               int64_t cxmalloc_api__bytes( cxmalloc_family_t *family );
static         histogram_t * cxmalloc_api__histogram( cxmalloc_family_t *family );
static               int64_t cxmalloc_api__active( cxmalloc_family_t *family );
static                double cxmalloc_api__utilization( cxmalloc_family_t *family );

/* Readonly */
static                   int cxmalloc_api__set_readonly( cxmalloc_family_t *family );
static                   int cxmalloc_api__is_readonly( cxmalloc_family_t *family );
static                   int cxmalloc_api__clear_readonly( cxmalloc_family_t *family );

/* Serialization */
static               int64_t cxmalloc_api__bulk_serialize( cxmalloc_family_t *family, bool force );
static               int64_t cxmalloc_api__restore_objects( cxmalloc_family_t *family );


static               int64_t cxmalloc_api__process_objects( cxmalloc_family_t *family, cxmalloc_object_processing_context_t *context );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN cxmalloc_family_vtable_t cxmalloc_family_Methods = {
  /* common interface */
  .vm_cmpid       = NULL,
  .vm_getid       = (f_object_identifier_t)cxmalloc_api__getid,
  .vm_serialize   = (f_object_serializer_t)cxmalloc_api__serialize,
  .vm_deserialize = (f_object_deserializer_t)cxmalloc_api__deserialize,
  .vm_construct   = (f_object_constructor_t)cxmalloc_api__constructor,
  .vm_destroy     = (f_object_destructor_t)cxmalloc_api__destructor,
  .vm_represent   = (f_object_representer_t)cxmalloc_api__repr,
  .vm_allocator   = NULL,
  /* allocator family interface */
  /* Allocate */
  .New                    = cxmalloc_api__new,
  /* Line array methods */
  .Renew                  = cxmalloc_api__renew,
  .Own                    = cxmalloc_api__own,
  .Discard                = cxmalloc_api__discard,
  .RefCount               = cxmalloc_api__refcount,
  .PrevLength             = cxmalloc_api__prev_length,
  .NextLength             = cxmalloc_api__next_length,
  .LengthOf               = cxmalloc_api__length_of,
  .IndexOf                = cxmalloc_api__index_of,
  .ObjectFromArray        = cxmalloc_api__object_from_array,
  .ArrayAsHandle          = cxmalloc_api__array_as_handle,
  .HandleAsArray          = cxmalloc_api__handle_as_array,
  .Meta                   = cxmalloc_api__meta,
  /* Object methods */
  .RenewObject            = cxmalloc_api__renew_object,
  .OwnObjectNolock        = cxmalloc_api__own_object_nolock,
  .OwnObject              = cxmalloc_api__own_object,
  .OwnObjectByHandle      = cxmalloc_api__own_object_by_handle,
  .DiscardObjectNolock    = cxmalloc_api__discard_object_nolock,
  .DiscardObject          = cxmalloc_api__discard_object,
  .DiscardObjectByHandle  = cxmalloc_api__discard_object_by_handle,
  .RefCountObjectNolock   = cxmalloc_api__refcount_object_nolock,
  .RefCountObject         = cxmalloc_api__refcount_object,
  .RefCountObjectByHandle = cxmalloc_api__refcount_object_by_handle,
  .ArrayFromObject        = cxmalloc_api__array_from_object,
  .ObjectAsHandle         = cxmalloc_api__object_as_handle,
  .HandleAsObjectNolock   = cxmalloc_api__handle_as_object_nolock,
  .HandleAsObjectSafe     = cxmalloc_api__handle_as_object_safe,
  /* Family methods */
  .MinLength              = cxmalloc_api__min_length,
  .MaxLength              = cxmalloc_api__max_length,
  .SizeBounds             = cxmalloc_api__size_bounds,
  .LazyDiscards           = cxmalloc_api__lazy_discards,
  .Check                  = cxmalloc_api__check,
  .GetObjectAtAddress     = cxmalloc_api__get_object_at_address,
  .FindObjectByObid       = cxmalloc_api__find_object_by_obid,
  .GetObjectByOffset      = cxmalloc_api__get_object_by_offset,
  .Sweep                  = cxmalloc_api__sweep,
  .Size                   = cxmalloc_api__size,
  .Bytes                  = cxmalloc_api__bytes,
  .Histogram              = cxmalloc_api__histogram,
  .Active                 = cxmalloc_api__active,
  .Utilization            = cxmalloc_api__utilization,
  /* Readonly */
  .SetReadonly            = cxmalloc_api__set_readonly,
  .IsReadonly             = cxmalloc_api__is_readonly,
  .ClearReadonly          = cxmalloc_api__clear_readonly,
  /* Serialization */
  .BulkSerialize          = cxmalloc_api__bulk_serialize,
  .RestoreObjects         = cxmalloc_api__restore_objects,
  .ProcessObjects         = cxmalloc_api__process_objects
};




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static longstring_t * cxmalloc_api__getid( cxmalloc_family_t *family ) {
  return &family->obid.longstring;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t cxmalloc_api__serialize( cxmalloc_family_t *family, CQwordQueue_t *out_queue ) {
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static cxmalloc_family_t * cxmalloc_api__deserialize( comlib_object_t *container, CQwordQueue_t *in_queue ) {
  return NULL;
}



/*******************************************************************//**
 * Create a new allocator family
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_family_t * cxmalloc_api__constructor( const void *identifier, const cxmalloc_family_constructor_args_t *args ) {
  uint32_t unit_sz_order = ilog2( args->family_descriptor->unit.sz );

  // Sanity check family descriptor
  if( args->family_descriptor->unit.sz != (1ULL << unit_sz_order) ) {
    CXMALLOC_FATAL( 0xA01, "Bad family descriptor: unit size must be power of 2" );
  }

#ifndef NDEBUG
  DEBUG( 0xDDD, "NEW CXMALLOC FAMILY: %s", identifier );
#endif

  return _icxmalloc_family.NewFamily_OPEN( identifier, args->family_descriptor );
}



/*******************************************************************//**
 * Delete allocator family 
 * 
 ***********************************************************************
 */
static void cxmalloc_api__destructor( cxmalloc_family_t *family ) {

  if( family ) {

#ifndef NDEBUG 
    if( family->obid.longstring.string ) {
      DEBUG( 0xDDD, "DELETE CXMALLOC FAMILY [@%llp]: %s", family, family->obid.longstring.string );
    }
    else {
      DEBUG( 0xA12, "DELETE CXMALLOC FAMILY [@%llp]: %016llx%016llx", family, family->obid.H, family->obid.L );
    }
#endif

    _icxmalloc_family.DeleteFamily_OPEN( &family );
  }

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static CStringQueue_t * cxmalloc_api__repr( cxmalloc_family_t *family, CStringQueue_t *output ) {
  CStringQueue_t *ret = NULL;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    ret = _icxmalloc_family.Repr_FCS( family_CS, output );
  } RELEASE_CXMALLOC_FAMILY;
  return ret;
}



/*******************************************************************//**
 * 
 * family : allocator family instance (not NULL)   
 * sz     : minimum number of allocation units to allocate
 *    
 *    call as:
 *      family->New( family, sz )
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__new( cxmalloc_family_t *family, uint32_t sz ) {
  void *arraydata = NULL;
  int64_t bytes = 0;

  XTRY {
    // Allocate normally
    if( sz <= family->max_length ) {
      if( (arraydata = _icxmalloc_line.New_OPEN( family, sz )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xA21 );
      }
    }
    // Handle large sz using one-off malloc() instead
    else {
      if( family->descriptor->parameter.allow_oversized ) {
        if( (arraydata = _icxmalloc_line.NewOversized_OPEN( family, sz )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0xA22 );
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_CAPACITY, 0xA23 ); // error, oversized allocation not allowed
      }
    }
  }
  XCATCH( errcode ) {
    if( sz > family->max_length ) {
      if( family->descriptor->parameter.allow_oversized ) {
        CXMALLOC_INFO( errcode, "Oversized allocation failed for allocator family \"%s\" (sz=%ld, bytes=%lld)", _cxmalloc_id_string(family), sz, bytes );
      }
      else {
        CXMALLOC_INFO( errcode, "Oversized allocation not allowed for allocator family \"%s\"", _cxmalloc_id_string(family) );
      }
    }
    else {
      CXMALLOC_INFO( errcode, "Allocation failed for allocator family \"%s\" (sz=%ld)", _cxmalloc_id_string(family), sz );
    }
    arraydata = NULL;
  }
  XFINALLY {}

  return arraydata;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__renew( cxmalloc_family_t *family, void *line ) {
  cxmalloc_linehead_t *linehead;

  // Not NULL
  if( line == NULL ) {
    return NULL;
  }

  linehead = _cxmalloc_linehead_from_array( family, line );

  if( linehead->data.flags.ovsz ) {
    /* no action for malloc memory */
    return line;
  }
  else {
    return _icxmalloc_line.Renew_OPEN( family, linehead );
  }

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__own( cxmalloc_family_t *family, void *line ) {
  int64_t refcnt = -1;

  // Not NULL
  if( line == NULL ) {
    return refcnt;
  }

  // We must synchronize the refcount inc (on some lock, might as well use the family lock)
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    // Back up to get linehead
    refcnt = ++(_cxmalloc_linehead_from_array( family_CS, line )->data.refc);
  } RELEASE_CXMALLOC_FAMILY;

#ifdef CXMALLOC_CONSISTENCY_CHECK
  if( refcnt < 0 ) {
    CXMALLOC_FATAL( 0xA31, "Refcount wrapped to negative" );
  }
#if defined (CXMALLOC_MAX_REFCOUNT) && (CXMALLOC_MAX_REFCOUNT > 0)
  static const int32_t max_refc = CXMALLOC_MAX_REFCOUNT;
  if( refcnt > max_refc ) {
    printf( "Line @ %p refcount=%lld", line, refcnt );
  }
#endif
#endif

  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__discard( cxmalloc_family_t *family, void *line ) {
  int64_t refcnt = -1;
  cxmalloc_linehead_t *linehead;

  // Not NULL
  if( !line ) {
    return -1;
  }

  // Back up to get linehead
  linehead = _cxmalloc_linehead_from_array( family, line );
  
  // Quick
  if( !family->flag.lazy_discards && linehead->data.refc > 1 ) {
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      // still > 1 after lock?
      if( linehead->data.refc > 1 ) {
        refcnt = --(linehead->data.refc);
      }
    } RELEASE_CXMALLOC_FAMILY;
  }
  
  // More complex discard with potential dealloc
  if( refcnt < 0 ) {
    // Discard
    refcnt = _icxmalloc_line.Discard_OPEN( family, linehead );
  }

  // Refcount after discard
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__refcount( cxmalloc_family_t *family, const void *line ) {
  int64_t refcnt = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    refcnt = line ? _cxmalloc_linehead_from_array( family_CS, line )->data.refc : 0;
  } RELEASE_CXMALLOC_FAMILY;
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint32_t cxmalloc_api__prev_length( cxmalloc_family_t *family, const void *line ) {
  uint16_t aidx = _cxmalloc_linehead_from_array( family, line )->data.aidx; // int cast ok by definition
  if( aidx > 0 ) {
    return _icxmalloc_shape.GetLength_FRO( family, aidx - 1 ); // <- also works for oversized, since we use virtual aidx
  }
  else {
    return family->min_length;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint32_t cxmalloc_api__next_length( cxmalloc_family_t *family, const void *line ) {
  uint16_t aidx = _cxmalloc_linehead_from_array( family, line )->data.aidx; // int cast ok by definition
  return _icxmalloc_shape.GetLength_FRO( family, aidx + 1 ); // <- also works for oversized, since we use virtual aidx
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint32_t cxmalloc_api__length_of( cxmalloc_family_t *family, const void *line ) {
  if( !line ) {
    return 0;
  }
  return _cxmalloc_linehead_from_array( family, line )->data.size;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint16_t cxmalloc_api__index_of( cxmalloc_family_t *family, const void *line ) {
  if( !line ) {
    return 0; // error - should we return something else?
  }
  return _cxmalloc_linehead_from_array( family, line )->data.aidx; // aidx should always be castable to int
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__object_from_array( cxmalloc_family_t *family, const void *line ) {
  return _cxmalloc_object_from_array( family, line );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static cxmalloc_handle_t cxmalloc_api__array_as_handle( cxmalloc_family_t *family, const void *line ) {
  cxmalloc_handle_t handle = {0};
  return handle; // TODO!
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void * cxmalloc_api__handle_as_array( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  return NULL; // TODO!
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static cxmalloc_metaflex_t * cxmalloc_api__meta( cxmalloc_family_t *family, const void *line ) {
  return &_cxmalloc_linehead_from_array( family, line )->metaflex;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__renew_object( cxmalloc_family_t *family, void *obj ) {
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );
  if( linehead->data.flags.ovsz ) {
    /* no action for malloc memory */
    return obj;
  }
  else {
    return _icxmalloc_line.Renew_OPEN( family, linehead );
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t cxmalloc_api__own_object_nolock( cxmalloc_family_t *family, void *obj ) {
  return _cxmalloc_object_incref_nolock( obj );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__own_object( cxmalloc_family_t *family, void *obj ) {
  int64_t refcnt = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    refcnt = _cxmalloc_object_incref_nolock( obj );
  } RELEASE_CXMALLOC_FAMILY;
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__own_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  int64_t refcnt = -1;
  cxmalloc_allocator_t *allocator;
  cxmalloc_block_t *block;
  if( (allocator = family->allocators[ handle.aidx ]) != NULL ) {
    if( (block = allocator->blocks[ handle.bidx ]) != NULL ) {
      cxmalloc_linehead_t *linehead = (cxmalloc_linehead_t*)(block->linedata + (allocator->shape.linemem.chunks * (uintptr_t)handle.offset));
      refcnt = cxmalloc_api__own_object( family, _cxmalloc_object_from_linehead( linehead ) );
    }
  }
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__discard_object_nolock( cxmalloc_family_t *family, void *obj ) {
  int64_t refcnt = -1;
  cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( obj );

  // Quick
  if( !family->flag.lazy_discards && linehead->data.refc > 1 ) {
    refcnt = --(linehead->data.refc);
    return refcnt;
  }

  // More complex discard with potential dealloc (will NOT LOCK anything on the inside)
  refcnt = _icxmalloc_line.Discard_NOLOCK( family, linehead );

  // Refcount after discard
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__discard_object( cxmalloc_family_t *family, void *obj ) {
  int64_t refcnt = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    refcnt = cxmalloc_api__discard_object_nolock( family_CS, obj );
  } RELEASE_CXMALLOC_FAMILY;
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * NOTE: This should only be called if the object referenced by the 
 * handle is guaranteed to have a refcount that cannot go to zero
 * simultaneously from another thread.
 ***********************************************************************
 */
static int64_t cxmalloc_api__discard_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  int64_t refcnt = -1;
  cxmalloc_allocator_t *allocator;
  cxmalloc_block_t *block;
  cxmalloc_linehead_t *linehead;

  if( (allocator  = family->allocators[ handle.aidx ]) != NULL   &&
      (block      = allocator->blocks[ handle.bidx ]) != NULL    &&
      (linehead   = (cxmalloc_linehead_t*)(block->linedata + (allocator->shape.linemem.chunks * (uintptr_t)handle.offset))) != NULL )
  {
    // Note at this point:
    // As long as the object referenced by the handle has no chance of
    // reaching a refcount of zero through a simultaneous operation from
    // another thread it is safe to assume that the linehead is valid at
    // this point.

    // The refcount could still go to zero after this discard.
    refcnt = _icxmalloc_line.Discard_NOLOCK( family, linehead );
  }

  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t cxmalloc_api__refcount_object_nolock( cxmalloc_family_t *family, const void *obj ) {
  return _cxmalloc_object_refcnt_nolock( obj );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__refcount_object( cxmalloc_family_t *family, const void *obj ) {
  int64_t refcnt;
  SYNCHRONIZE_ON( family->lock ) {
    refcnt = _cxmalloc_object_refcnt_nolock( obj );
  } RELEASE;
  return refcnt;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t cxmalloc_api__refcount_object_by_handle( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  return -1; // TODO
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__array_from_object( cxmalloc_family_t *family, const void *obj ) {
  return _cxmalloc_array_from_object( family, obj );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static cxmalloc_handle_t cxmalloc_api__object_as_handle( cxmalloc_family_t *family, const void *obj ) {
  return _cxmalloc_object_as_handle( obj );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__handle_as_object_nolock( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  // Look up the allocator pointer (maybe NULL). This lookup is always valid since the allocator size is static.
  cxmalloc_allocator_t *allocator = family->allocators[ handle.aidx ];
  // Allocator exists
  if( allocator ) {
    // Get the block pointer (maybe NULL). This lookup is always valid since the block register size is static.
    cxmalloc_block_t *block = allocator->blocks[ handle.bidx ];
    // A non-empty block exists (has linedata)
    if( block && block->linedata ) {
      // Get the line within the block's linedata. This memory is guaranteed to exist but may not be a valid object from callers point of view.
      cxmalloc_linehead_t *linehead = (cxmalloc_linehead_t*)(block->linedata + (allocator->shape.linemem.chunks * (uintptr_t)handle.offset));
      // The returned object may have refcount of zero and be defunct. It is up to caller to perform further validation of this pointer as necessary.
      // NOTE: This will return the object address even if it is not active! (CALLER MUST DECIDE WHAT TO DO WITH THIS OBJECT!)
      return _cxmalloc_object_from_linehead( linehead );
    }
  }

  // Could not convert handle to object
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static void * cxmalloc_api__handle_as_object_safe( cxmalloc_family_t *family, cxmalloc_handle_t handle ) {
  void *obj = NULL;
  cxmalloc_allocator_t *allocator;
  if( (allocator = family->allocators[ handle.aidx ]) != NULL ) {
    SYNCHRONIZE_CXMALLOC_ALLOCATOR( allocator ) {
      cxmalloc_block_t *block_CS = allocator_CS->blocks[ handle.bidx ];
      if( block_CS != NULL ) {
        cxmalloc_linehead_t *linehead = (cxmalloc_linehead_t*)(block_CS->linedata + (allocator_CS->shape.linemem.chunks * (uintptr_t)handle.offset));
        obj = _cxmalloc_object_from_linehead( linehead );
      }
    } RELEASE_CXMALLOC_ALLOCATOR;
  }
  return obj;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint32_t cxmalloc_api__min_length( cxmalloc_family_t *family ) {
  return family->min_length;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static uint32_t cxmalloc_api__max_length( cxmalloc_family_t *family ) {
  return family->max_length;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static uint16_t cxmalloc_api__size_bounds( cxmalloc_family_t *family, uint32_t sz, uint32_t *low, uint32_t *high ) {
  uint16_t aidx = _icxmalloc_shape.GetAIDX_FRO( family, sz, high );
  *low = aidx > 0 ? _icxmalloc_shape.GetLength_FRO( family, aidx-1 ) + 1 : 0;
  return aidx;
}



/*******************************************************************//**
* Set behavior of Discard() method. If argument is true, any subsequent 
* call to Discard() will set a flag instead of the more costly deallocation.
* The discarded memory will not be freed until a future call to Sweep().
* 
***********************************************************************
*/
static void cxmalloc_api__lazy_discards( cxmalloc_family_t *family, int use_lazy_discards ) {
IGNORE_WARNING_DEREFERENCING_NULL_POINTER
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    family_CS->flag.lazy_discards = use_lazy_discards > 0 ? 1 : 0;
  } RELEASE_CXMALLOC_FAMILY;
RESUME_WARNINGS
}



/*******************************************************************//**
 * Run consistency checks.
 * 
 * alloc  : the allocator family to check
 *
 * Returns  : 0 on success. (Core dump on error.)
 ***********************************************************************
 */
static int cxmalloc_api__check( cxmalloc_family_t *family ) {
  int retcode = 0;

  XTRY {

    if( !family ) {
      THROW_ERROR( CXLIB_ERR_ERROR, 0xA11 );
    }

    int64_t family_refcnt = 0;
    cxmalloc_linehead_t *bad_linehead = NULL;


    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      family_refcnt = _icxmalloc_family.ValidateRefcounts_FCS( family_CS, &bad_linehead ); 
    } RELEASE_CXMALLOC_FAMILY;

    if( family_refcnt < 0 || bad_linehead != NULL ) {
      CXMALLOC_INFO( 0xA51, "-------------------------------------------------------------" );
      CXMALLOC_INFO( 0xA52, "%s", family->obid.longstring.string );
      CXMALLOC_INFO( 0xA53, "-------------------------------------------------------------" );
      if( bad_linehead ) {
        CXMALLOC_REASON( 0xA54, "=== BAD LINE AT 0x%llx ===", (uintptr_t)bad_linehead );
        CXMALLOC_REASON( 0xA55, "meta.M1 = 0x%llx",  bad_linehead->metaflex.M1.bits );
        CXMALLOC_REASON( 0xA56, "meta.M2 = 0x%llx",  bad_linehead->metaflex.M2.bits );
        CXMALLOC_REASON( 0xA57, "aidx    = %u",      bad_linehead->data.aidx );
        CXMALLOC_REASON( 0xA58, "bidx    = %u",      bad_linehead->data.bidx );
        CXMALLOC_REASON( 0xA59, "offset  = %lu",     bad_linehead->data.offset );
        CXMALLOC_REASON( 0xA5A, "ovsz    = %u",      bad_linehead->data.flags.ovsz );
        CXMALLOC_REASON( 0xA5B, "invl    = %u",      bad_linehead->data.flags.invl );
        CXMALLOC_REASON( 0xA5C, "_chk    = %u",      bad_linehead->data.flags._chk );
        CXMALLOC_REASON( 0xA5C, "_mod    = %u",      bad_linehead->data.flags._mod );
        CXMALLOC_REASON( 0xA5D, "size    = %lu",     bad_linehead->data.size );
        CXMALLOC_REASON( 0xA5E, "refc    = %ld",     bad_linehead->data.refc );
      }
      CXMALLOC_INFO( 0xA61, "-------------------------------------------------------------" );
      CXMALLOC_INFO( 0xA62, "" );
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_CORRUPTION, 0xA12, "Corrupted allocator family." );
    }
  }
  XCATCH( errcode ) {
    retcode = -1;
  }
  XFINALLY {
  }
  return retcode;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static comlib_object_t * cxmalloc_api__get_object_at_address( cxmalloc_family_t *family, QWORD address ) {
  comlib_object_t *obj = NULL;
  if( address % sizeof( comlib_object_t ) == 0 ) {
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      obj = _icxmalloc_family.GetObjectAtAddress_FCS( family_CS, address );
    } RELEASE_CXMALLOC_FAMILY;
  }
  return obj;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static comlib_object_t * cxmalloc_api__find_object_by_obid( cxmalloc_family_t *family, objectid_t obid ) {
  comlib_object_t *obj = NULL;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    obj = _icxmalloc_family.FindObjectByObid_FCS( family_CS, obid );
  } RELEASE_CXMALLOC_FAMILY;
  return obj;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
static comlib_object_t * cxmalloc_api__get_object_by_offset( cxmalloc_family_t *family, int64_t *poffset ) {
  comlib_object_t *obj = NULL;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    obj = _icxmalloc_family.GetObjectByOffset_FCS( family_CS, poffset );
  } RELEASE_CXMALLOC_FAMILY;
  return obj;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__sweep( cxmalloc_family_t *family, f_get_object_identifier get_object_identifier ) {
  return _icxmalloc_family.Sweep_OPEN( family, get_object_identifier );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int cxmalloc_api__size( cxmalloc_family_t *family ) {
  return family->size;
}



/*******************************************************************//**
 * 
 * Return the number of bytes currently allocated internally for this family
 *
 ***********************************************************************
 */
static int64_t cxmalloc_api__bytes( cxmalloc_family_t *family ) {
  int64_t bytes = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    bytes = _icxmalloc_shape.FamilyBytes_FCS( family_CS );
  } RELEASE_CXMALLOC_FAMILY;
  return bytes;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static histogram_t * cxmalloc_api__histogram( cxmalloc_family_t *family ) {
  histogram_t *histogram = NULL;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    histogram = _icxmalloc_shape.Histogram_FCS( family_CS );
  } RELEASE_CXMALLOC_FAMILY;
  return histogram;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__active( cxmalloc_family_t *family ) {
  int64_t active = _icxmalloc_family.Active_OPEN( family );
  return active;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static double cxmalloc_api__utilization( cxmalloc_family_t *family ) {
  double util = _icxmalloc_family.Utilization_OPEN( family );
  return util;
}



/*******************************************************************//**
 * 
 * Returns: >= 1 : readonly recursion count
 *            -1 : error
 ***********************************************************************
 */
static int cxmalloc_api__set_readonly( cxmalloc_family_t *family ) {
  int readonly = 0;
IGNORE_WARNING_DEREFERENCING_NULL_POINTER
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    readonly = ++(family_CS->readonly_cnt);
#ifdef CXMALLOC_CONSISTENCY_CHECK
    cxmalloc_allocator_t **cursor = family_CS->allocators;
    cxmalloc_allocator_t *allocator_CS;
    for( int aidx=0; aidx<family_CS->size; aidx++ ) {
      if( (allocator_CS = *cursor++) != NULL ) {
        ATOMIC_INCREMENT_i32( &allocator_CS->readonly_atomic );
      }
    }
#endif
  } RELEASE_CXMALLOC_FAMILY;
RESUME_WARNINGS
  return readonly;
}



/*******************************************************************//**
 * 
 * Returns:   0 : writable
 *            1 : readonly
 * 
 ***********************************************************************
 */
static int cxmalloc_api__is_readonly( cxmalloc_family_t *family ) {
  int readonly = 0;
IGNORE_WARNING_DEREFERENCING_NULL_POINTER
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    readonly = family_CS->readonly_cnt > 0;
  } RELEASE_CXMALLOC_FAMILY;
RESUME_WARNINGS
  return readonly;
}



/*******************************************************************//**
 * 
 * Returns:    0 : Writable after clear
 *          >= 1 : Still readonly with this recursion count
 *            -1 : Error
 ***********************************************************************
 */
static int cxmalloc_api__clear_readonly( cxmalloc_family_t *family ) {
  int readonly = 0;
IGNORE_WARNING_DEREFERENCING_NULL_POINTER
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    if( family_CS->readonly_cnt > 0 ) {
      readonly = --(family_CS->readonly_cnt);
#ifdef CXMALLOC_CONSISTENCY_CHECK
      cxmalloc_allocator_t **cursor = family_CS->allocators;
      cxmalloc_allocator_t *allocator_CS;
      for( int aidx=0; aidx<family_CS->size; aidx++ ) {
        if( (allocator_CS = *cursor++) != NULL ) {
          ATOMIC_DECREMENT_i32( &allocator_CS->readonly_atomic );
        }
      }
#endif
    }
    else {
      readonly = -1;
    }
  } RELEASE_CXMALLOC_FAMILY;
RESUME_WARNINGS
  return readonly;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__bulk_serialize( cxmalloc_family_t *family, bool force ) {
  int64_t nqwords;
  nqwords = _icxmalloc_serialization.PersistFamily_OPEN( family, force );
  return nqwords;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__restore_objects( cxmalloc_family_t *family ) {
  int64_t nqwords = 0;
  SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
    nqwords = _icxmalloc_family.RestoreObjects_FCS( family_CS );
#ifndef NDEBUG
    COMLIB_OBJECT_PRINT( family_CS );
#endif
  } RELEASE_CXMALLOC_FAMILY;
  return nqwords;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t cxmalloc_api__process_objects( cxmalloc_family_t *family, cxmalloc_object_processing_context_t *context ) {
  int64_t n_lines = 0;
  if( family->readonly_cnt > 0 ) {
    n_lines = _icxmalloc_object_processor.ProcessFamily_FCS( family, context );
  }
  else {
    SYNCHRONIZE_CXMALLOC_FAMILY( family ) {
      n_lines = _icxmalloc_object_processor.ProcessFamily_FCS( family_CS, context );
    } RELEASE_CXMALLOC_FAMILY;
  }
  return n_lines;
}

