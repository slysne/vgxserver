/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    api_class.c
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

#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



static framehash_perfcounters_t * __get_perfcounters( framehash_t * const self, framehash_perfcounters_t *target );
static void __reset_perfcounters( framehash_t * const self );
static framehash_perfcounters_t * __new_perfcounters( void );
static void __delete_perfcounters( framehash_perfcounters_t *counters );



#ifdef FRAMEHASH_INSTRUMENTATION
/*******************************************************************//**
 * null_instrument
 *
 ***********************************************************************
 */
DLL_EXPORT framehash_instrument_t null_instrument = {0};


/*******************************************************************//**
 * __get_perfcounters
 *
 ***********************************************************************
 */
static framehash_perfcounters_t * __get_perfcounters( framehash_t * const self, framehash_perfcounters_t *target ) {
  _framehash_counters_t *read = &self->_counters->read;
  _framehash_counters_t *write = &self->_counters->write;
  SYNCHRONIZE_ON( read->lock ) {
    target->read.opcount = read->opcount;
    memcpy( &target->read.cache, &read->cache, sizeof(read->cache) );
    memcpy( &target->read.probe, &read->probe, sizeof(read->probe) );
  } RELEASE;
  SYNCHRONIZE_ON( write->lock ) {
    target->write.opcount = write->opcount;
    target->write.resize_up = write->resize_up;
    target->write.resize_down = write->resize_down;
    memcpy( &target->write.cache, &write->cache, sizeof(write->cache) );
    memcpy( &target->write.probe, &write->probe, sizeof(write->probe) );
  } RELEASE;
  return target;
}



/*******************************************************************//**
 * __reset_perfcounters
 *
 ***********************************************************************
 */
static void __reset_perfcounters( framehash_t * const self ) {
  _framehash_counters_t *read = &self->_counters->read;
  _framehash_counters_t *write = &self->_counters->write;
  SYNCHRONIZE_ON( read->lock ) {
    read->opcount = 0;
    memset( &read->cache, 0, sizeof(read->cache) );
    memset( &read->probe, 0, sizeof(read->probe) );
  } RELEASE;
  SYNCHRONIZE_ON( write->lock ) {
    write->opcount = 0;
    write->resize_up = 0;
    write->resize_down = 0;
    memset( &write->cache, 0, sizeof(write->cache) );
    memset( &write->probe, 0, sizeof(write->probe) );
  } RELEASE;
}



/*******************************************************************//**
 * __new_perfcounters
 *
 ***********************************************************************
 */
static framehash_perfcounters_t * __new_perfcounters( void ) {
  framehash_perfcounters_t *counters;
  if( CALIGNED_MALLOC( counters, framehash_perfcounters_t ) != NULL ) {
    memset( counters, 0, sizeof(framehash_perfcounters_t) );
    INIT_CRITICAL_SECTION( &counters->read.lock.lock );
    INIT_CRITICAL_SECTION( &counters->write.lock.lock );
  }
  return counters;
}



/*******************************************************************//**
 * __delete_perfcounters
 *
 ***********************************************************************
 */
static void __delete_perfcounters( framehash_perfcounters_t *counters ) {
  if( counters ) {
    DEL_CRITICAL_SECTION( &counters->read.lock.lock );
    DEL_CRITICAL_SECTION( &counters->write.lock.lock );
    ALIGNED_FREE( counters );
  }
}


#else


SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * __get_perfcounters
 *
 ******************************************************************************
 */
static framehash_perfcounters_t * __get_perfcounters( framehash_t * const self, framehash_perfcounters_t *target ) {
  return target;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * __reset_perfcounters
 *
 ******************************************************************************
 */
static void __reset_perfcounters( framehash_t * const self ) {
}


/**************************************************************************//**
 * __new_perfcounters
 *
 ******************************************************************************
 */
static framehash_perfcounters_t * __new_perfcounters( void ) {
  return NULL;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER

/**************************************************************************//**
 * __delete_perfcounters
 *
 ******************************************************************************
 */
static void __delete_perfcounters( framehash_perfcounters_t *counters ) {
}

#endif




/*******************************************************************//**
 * __print_allocators
 *
 ***********************************************************************
 */
static void __print_allocators( framehash_t *self) {
  iFramehash.dynamic.PrintAllocators( &self->_dynamic );
}



/*******************************************************************//**
 * __check_allocators
 *
 ***********************************************************************
 */
static int __check_allocators( framehash_t *self) {
  return iFramehash.dynamic.CheckAllocators( &self->_dynamic );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const framehash_constructor_args_t FRAMEHASH_DEFAULT_ARGS = {
  .param = {
    .order = FRAMEHASH_DEFAULT_ORDER,
    .synchronized       = false,
    .shortkeys          = false,
    .cache_depth        = FRAMEHASH_DEFAULT_MAX_CACHE_DOMAIN,
    .obclass_changelog  = CLASS_NONE,
    ._rsv               = {0}
  },
  .dirpath              = NULL,
  .name                 = NULL,
  .input_queue          = NULL,
  .fpath                = NULL,
  .frame_allocator      = NULL,
  .basement_allocator   = NULL,
  .shortid_hashfunction = ihash64
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
static              int __framehash_cmpid( const framehash_t *self, const void *identifier );
static     objectid_t * __framehash_getid( framehash_t *self );
static          int64_t __framehash_serialize( framehash_t *self, CQwordQueue_t *output_queue );
static    framehash_t * __framehash_deserialize( framehash_t *framehash, CQwordQueue_t *input_queue );
static    framehash_t * __framehash_constructor( const void *identifier, const framehash_constructor_args_t *args );
static             void __framehash_destructor( framehash_t *self );
static CStringQueue_t * __framehash_repr( const framehash_t *self, CStringQueue_t *output );





/*******************************************************************//**
 * __framehash_cmpid
 *
 ***********************************************************************
 */
static int __framehash_cmpid( const framehash_t *self, const void *identifier ) {
  return idcmp( &self->obid, (objectid_t*)identifier );
}



/*******************************************************************//**
 * __framehash_getid
 *
 ***********************************************************************
 */
static objectid_t * __framehash_getid( framehash_t *self ) {
  return &self->obid;
}



/*******************************************************************//**
 * __framehash_serialize
 *
 ***********************************************************************
 */
static int64_t __framehash_serialize( framehash_t *self, CQwordQueue_t *output_queue ) {
  return _framehash_serialization__serialize( self, output_queue, true );
}



/*******************************************************************//**
 * __framehash_deserialize
 *
 ***********************************************************************
 */
static framehash_t *__framehash_deserialize( framehash_t *framehash, CQwordQueue_t *input_queue ) {
  return _framehash_serialization__deserialize( framehash, input_queue );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static framehash_t * __framehash_constructor( const void *identifier, const framehash_constructor_args_t *args ) {
  const framehash_constructor_args_t default_args = FRAMEHASH_DEFAULT_ARGS;
  framehash_t *self = NULL;

  XTRY {
    int restore_from_file = false;

    if( args == NULL ) {
      args = &default_args;
    }

    // Create framehash instance
    PALIGNED_MALLOC_THROWS( self, framehash_t, 0x101 );
    memset( self, 0, sizeof(framehash_t) );

    if( COMLIB_OBJECT_INIT( framehash_t, self, identifier ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x102 );
    }

    self->_order = args->param.order;
    self->_flag.dontenter = 0;
    self->_flag.ready = 0; // not yet
    self->_CSTR__dirname = NULL;
    self->_CSTR__basename = NULL;
    self->_CSTR__masterpath = NULL;

    // Dynamics (will steal allocators from args)
    if( iFramehash.dynamic.InitDynamic( &self->_dynamic, args ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x103 );
    }

    // Determine file name and home directory if we are not reconstructing from an input stream
    if( args->input_queue == NULL && (args->fpath || args->dirpath || args->name) ) {
      if( args->fpath ) {
        char *dn, *fn;
        if( split_path( args->fpath, &dn, &fn ) == 0 ) {
          self->_CSTR__dirname = CStringNew( dn );
          self->_CSTR__basename = CStringNew( fn );
          free( dn );
          free( fn );
        }
      }
      else {
        self->_CSTR__dirname = CStringNew( args->dirpath ? args->dirpath : "." );
        self->_CSTR__basename = CStringNew( args->name ? args->name : "framehash" );
      }
      // Set masterpath
      if( self->_CSTR__dirname && self->_CSTR__basename ) {
        self->_CSTR__masterpath = CStringNewFormat( "%s/%s_%d.dat", CStringValue( self->_CSTR__dirname ), CStringValue( self->_CSTR__basename ), self->_order );
      }
      // Verify masterpath
      if( self->_CSTR__masterpath == NULL ) {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x104 );
      }
    }

#ifdef FRAMEHASH_INSTRUMENTATION
    // Instrumentation
    self->_counters = __new_perfcounters();
#endif

    _INITIALIZE_REFERENCE_CELL( &self->_topframe, 0xF, 0xF, FRAME_TYPE_NONE );

    self->_nobj = 0;
    self->_opcnt = CXLIB_OPERATION_NONE;
    self->_guard = NULL;

    // Enable caches by default
    self->_flag.cache.r_ena = 1;
    self->_flag.cache.w_ena = 1;

    if( args->input_queue ) {
      // Recreate everything from supplied input queue
      self->_order = FRAMEHASH_ARG_UNDEFINED;
      self->_flag.synchronized = FRAMEHASH_ARG_UNDEFINED;
      self->_dynamic.cache_depth = FRAMEHASH_ARG_UNDEFINED;
      if( DESERIALIZE( self, args->input_queue ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x103 );
      }
      if( CALLABLE( self )->Items( self ) != _framehash_processor__count_nactive( self ) ) {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x104 );
      }
    }
    else {
      // Create file-based instance
      self->_flag.synchronized = args->param.synchronized ? 1 : 0;
      self->_flag.shortkeys = args->param.shortkeys ? 11 : 0;
      self->_dynamic.cache_depth = args->param.cache_depth;

      if( _FRAMEHASH_HAS_FILE( self ) ) {
        // File exists, we will restore from disk
        if( file_exists( CStringValue( self->_CSTR__masterpath ) ) ) {
          restore_from_file = true;
        }
        // Directory does not exist, create new
        else if( !dir_exists( CStringValue( self->_CSTR__dirname ) ) ) {
          if( create_dirs( CStringValue( self->_CSTR__dirname ) ) < 0 ) {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x105 );
          }
        }
        if( _framehash_changelog__init( self, args->param.obclass_changelog ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x106 );
        }
      }

      // Create top-level cache frame
      if( restore_from_file == true ) {
        // Restore from disk
        if( DESERIALIZE( self, NULL ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x107 );
        }
        // Verify item count
        if( CALLABLE( self )->Items( self ) != _framehash_processor__count_nactive( self ) ) {
          THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x108 );
        }
      }
      else {
        // Create a new, empty top frame
        framehash_context_t top_context = CONTEXT_INIT_TOP_FRAME( &self->_topframe, &self->_dynamic );

        if( args->param.order == FRAMEHASH_ARG_UNDEFINED ) {
          self->_order = FRAMEHASH_DEFAULT_ORDER;
        }

        if( args->param.synchronized == 0 ) {
          self->_flag.synchronized = 0;
        }
        else {
          self->_flag.synchronized = 1;
        }
        
        if( args->param.cache_depth == FRAMEHASH_ARG_UNDEFINED ) {
          self->_dynamic.cache_depth = FRAMEHASH_DEFAULT_MAX_CACHE_DOMAIN;
        }

        self->_opcnt = CXLIB_OPERATION_START;
        
        if( _framehash_memory__new_frame( &top_context, self->_order, 0, FRAME_TYPE_CACHE ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x109 );
        }
        
        if( identifier == NULL ) {
          if( !_FRAMEHASH_HAS_FILE(self) ) {
            QWORD addr = (QWORD)self;
            self->obid = obid_from_string_len( (char*)&addr, 8 ); // anonymous ID gets the hash of its address
          }
          else {
            self->obid = *CStringObid( self->_CSTR__masterpath );
          }
        }

        if( SERIALIZE( self, NULL ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x10a );
        }
      }

#ifdef FRAMEHASH_CHANGELOG
      // Start changelog  
      if( self->changelog.enable ) {
        if( _framehash_changelog__start( self ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x106 );
        }
      }
#endif

    }
    
    // Create and initialize top-level locks if needed
    if( self->_flag.synchronized ) {
      int nslots = _CACHE_FRAME_NSLOTS(self->_order);
      TALIGNED_ARRAY_THROWS( self->_guard, framehash_access_guard_t, nslots, 0x10b ); 
      for( int i=0; i<nslots; i++ ) {
        self->_guard[i].busy = 0;
        // Initialize the ready condition variable
        INIT_CONDITION_VARIABLE( &self->_guard[i].ready.cond );
      }
      self->_plock = &self->_dynamic.lock;
    }

    // Set clean
    self->_flag.clean = 1;

    // Indicate data in synch with disk (if files are used)
    self->_flag.persisted = 1;

    // Ready
    self->_flag.ready = 1;

  }
  XCATCH( errcode ) {
    COMLIB_OBJECT_DESTROY( self );
    self = NULL;
  }
  XFINALLY {
  }

  return self;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void __framehash_destructor( framehash_t *self ) {
  if( self ) {
    framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &self->_topframe, &self->_dynamic );

    // Discard all objects and reset the entire structure to empty top frame
    _framehash_api_manage__discard( self );

    // Discard the empty top frame
    if( _CELL_REFERENCE_EXISTS( context.frame ) ) {
      _framehash_memory__discard_frame( &context );
    }

    // Remove the slot guards
    if( self->_flag.ready && self->_flag.synchronized ) {
      if( self->_guard ) {
        int nslots = _CACHE_FRAME_NSLOTS( self->_order );
        for( int i=0; i<nslots; i++ ) {
          // Delete the ready condition variable
          DEL_CONDITION_VARIABLE( &self->_guard[i].ready.cond );
        }
        ALIGNED_FREE( self->_guard );
        self->_guard = NULL;
      }
    }

#ifdef FRAMEHASH_INSTRUMENTATION
    __delete_perfcounters( self->_counters );
#endif

#ifdef FRAMEHASH_CHANGELOG
    // Delete changlog
    if( self->changelog.enable ) {
      _framehash_changelog__end( self );
      _framehash_changelog__destroy( self );
    }
#endif

    // Delete all the dynamics support objects
    iFramehash.dynamic.ClearDynamic( &self->_dynamic );

    // Delete paths
    if( self->_CSTR__masterpath ) {
      CStringDelete( self->_CSTR__masterpath );
    }
    if( self->_CSTR__basename ) {
      CStringDelete( self->_CSTR__basename );
    }
    if( self->_CSTR__dirname ) {
      CStringDelete( self->_CSTR__dirname );
    }

    // Free
    ALIGNED_FREE(self);
    self = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static CStringQueue_t * __framehash_repr( const framehash_t *self, CStringQueue_t *output ) {
  char buffer[33];
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )
  PUT( "<framehash_t at 0x%llx ID=%s order=%d nobj=%lld path=%s opcnt=%llu synchronized=%d>\n",
                        self, //  |        |       |         |        |                 |
                                  idtostr(buffer, COMLIB_OBJECT_GETID(self)), //        |
                                           self->_order, //  |        |                 |
                                                   self->_nobj, //    |                 |
                                                             CStringValue( self->_CSTR__masterpath ), //      |
                                                                      self->_opcnt, //  |
                                                                                        (int)self->_flag.synchronized );
  return output;
#undef PUT
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static IFramehashMathProtocol iFramehashMath = {
  .Mul       = _framehash_framemath__mul,
  .Add       = _framehash_framemath__add,
  .Sqrt      = _framehash_framemath__sqrt,
  .Pow       = _framehash_framemath__pow,
  .Log       = _framehash_framemath__log,
  .Exp       = _framehash_framemath__exp,
  .Decay     = _framehash_framemath__decay,
  .Set       = _framehash_framemath__set,
  .Randomize = _framehash_framemath__randomize,
  .Int       = _framehash_framemath__int,
  .Float     = _framehash_framemath__float,
  .Abs       = _framehash_framemath__abs,
  .Sum       = _framehash_framemath__sum,
  .Avg       = _framehash_framemath__avg,
  .Stdev     = _framehash_framemath__stdev
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
framehash_vtable_t FramehashMethods = {
  /* BASE COMLIB INTERFACE */
  .vm_cmpid           = (f_object_comparator_t)__framehash_cmpid,
  .vm_getid           = (f_object_identifier_t)__framehash_getid,
  .vm_serialize       = (f_object_serializer_t)__framehash_serialize,
  .vm_deserialize     = (f_object_deserializer_t)__framehash_deserialize,
  .vm_construct       = (f_object_constructor_t)__framehash_constructor,
  .vm_destroy         = (f_object_destructor_t)__framehash_destructor,
  .vm_represent       = (f_object_representer_t)__framehash_repr,
  .vm_allocator       = NULL,

  /* api generic */
  .Set                = _framehash_api_generic__set,
  .Delete             = _framehash_api_generic__del,
  .Has                = _framehash_api_generic__has,
  .Get                = _framehash_api_generic__get,
  .Inc                = _framehash_api_generic__inc,

  /* api object */
  .SetObj             = _framehash_api_object__set,
  .DelObj             = _framehash_api_object__del,
  .GetObj             = _framehash_api_object__get,
  .SetObj128Nolock    = _framehash_api_object__set_object128_nolock,
  .DelObj128Nolock    = _framehash_api_object__del_object128_nolock,
  .GetObj128Nolock    = _framehash_api_object__get_object128_nolock,
  .HasObj128Nolock    = _framehash_api_object__has_object128_nolock,

  /* api int56 */
  .SetInt56           = _framehash_api_int56__set,
  .DelKey64           = _framehash_api_int56__del_key64,
  .HasKey64           = _framehash_api_int56__has_key64,
  .GetInt56           = _framehash_api_int56__get,
  .IncInt56           = _framehash_api_int56__inc,

  /* api real56 */
  .SetReal56          = _framehash_api_real56__set,
  .GetReal56          = _framehash_api_real56__get,
  .IncReal56          = _framehash_api_real56__inc,

  /* api pointer */
  .SetPointer         = _framehash_api_pointer__set,
  .GetPointer         = _framehash_api_pointer__get,
  .IncPointer         = _framehash_api_pointer__inc,

  /* api iterator */
  .GetKeys            = _framehash_api_iterator__get_keys,
  .GetValues          = _framehash_api_iterator__get_values,
  .GetItems           = _framehash_api_iterator__get_items,

  /* api manage */
  .Flush              = _framehash_api_manage__flush,
  .Discard            = _framehash_api_manage__discard,
  .Compactify         = _framehash_api_manage__compactify,
  .CompactifyPartial  = _framehash_api_manage__compactify_partial,
  .SetReadonly        = _framehash_api_manage__set_readonly,
  .IsReadonly         = _framehash_api_manage__is_readonly,
  .ClearReadonly      = _framehash_api_manage__clear_readonly,
  .EnableReadCaches   = _framehash_api_manage__enable_read_caches,
  .DisableReadCaches  = _framehash_api_manage__disable_read_caches,
  .EnableWriteCaches  = _framehash_api_manage__enable_write_caches,
  .DisableWriteCaches = _framehash_api_manage__disable_write_caches,

  /* api info */
  .Items              = _framehash_api_info__items,
  .Masterpath         = _framehash_api_info__masterpath,
  .Hitrate            = _framehash_api_info__hitrate,

  /* processing */
  .CountActive        = _framehash_api_iterator__count_nactive,
  .Process            = _framehash_api_iterator__process,
  .ProcessPartial     = _framehash_api_iterator__process_partial,

  /* serialization */
  .BulkSerialize      = _framehash_serialization__bulk_serialize,
  .Erase              = _framehash_serialization__erase,

  /* framemath */
  .Math               = &iFramehashMath,

  /* instrumentation */
  .GetPerfCounters    = __get_perfcounters,
  .ResetPerfCounters  = __reset_perfcounters,

  /* debug */
  .PrintAllocators    = __print_allocators,
  .CheckAllocators    = __check_allocators
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_api_class__register_class( void ) {
  ASSERT_TYPE_SIZE( framehash_cell_t,                     2 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( CS_LOCK,                              sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( CS_COND,                              6 * sizeof( QWORD ) );
  ASSERT_TYPE_SIZE( framehash_access_guard_t,             1 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( framehash_dynamic_t,                  3 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( framehash_t,                          6 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( framehash_constructor_args_t,         1 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( framehash_processing_context_t,       1 * sizeof( cacheline_t ) );
  ASSERT_TYPE_SIZE( framehash_control_flags_t,            1 * sizeof( QWORD ) );

#ifdef FRAMEHASH_INSTRUMENTATION
  ASSERT_TYPE_SIZE( framehash_context_t,                  2 * sizeof( cacheline_t ) );
#else
  ASSERT_TYPE_SIZE( framehash_context_t,                  1 * sizeof( cacheline_t ) );
#endif

  COMLIB_REGISTER_CLASS( framehash_t, CXLIB_OBTYPE_MAP, &FramehashMethods, OBJECT_IDENTIFIED_BY_OBJECTID, 0 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_api_class__unregister_class( void ) {
  COMLIB_UNREGISTER_CLASS( framehash_t );
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_class.h"

DLL_HIDDEN test_descriptor_t _framehash_api_class_tests[] = {
  { "api_class",   __utest_framehash_api_class },
  {NULL}
};
#endif
