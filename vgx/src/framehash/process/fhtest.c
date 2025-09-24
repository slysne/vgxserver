/*######################################################################
 *#
 *# fhtest.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*******************************************************************//**
 *
 ***********************************************************************
 */
static int __test_cmpid( const FramehashTestObject_t *obj, const void *identifier ) {
  return idmatch( &obj->obid, (const objectid_t*)identifier ) ? 0 : -1;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static objectid_t * __test_getid( FramehashTestObject_t *obj ) {
  return &obj->obid;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __test_serialize( const FramehashTestObject_t *obj, CQwordQueue_t *out_queue ) {
  int64_t nbytes = 0;
  int64_t (*write)( CQwordQueue_t *Q, const QWORD *data, int64_t len ) = out_queue->vtable->WriteNolock;
  uint64_t sz = sizeof( sizeof(objectid_t) ); // this is the only object field
  nbytes += write( out_queue, &sz, sizeof(QWORD) );
  nbytes += write( out_queue, COMLIB_OBJECT_GETID(obj), sizeof(objectid_t) );
  return nbytes;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static comlib_object_t * __test_deserialize( comlib_object_t *container, CQwordQueue_t *in_queue ) {
  FramehashTestObject_t *obj = NULL;
  char *data = NULL;
  int64_t sz, *psz=&sz;
  objectid_t *pobid;
  int64_t (*read)( CQwordQueue_t *Q, void **dest, int64_t count ) = in_queue->vtable->ReadNolock;

  if( read( in_queue, (void**)&psz, sizeof(QWORD) ) != sizeof(QWORD) ) return NULL;
  if( read( in_queue, (void**)&data, sz ) != sz ) return NULL;

  // the only element is object id
  if( sz != sizeof(objectid_t) ) return NULL;
  pobid = (objectid_t*)data;  // the only element
  ALIGNED_FREE( data );

  // create the object
  obj = COMLIB_OBJECT_NEW( FramehashTestObject_t, pobid, NULL );

  return (comlib_object_t*)obj;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static FramehashTestObject_t * __test_constructor( const void *identifier, FramehashTestObject_constructor_args_t *args ) {
  FramehashTestObject_t *obj;
  if( (obj = (FramehashTestObject_t*)calloc( 1, sizeof(FramehashTestObject_t) )) == NULL ) return NULL;
  if( COMLIB_OBJECT_INIT( FramehashTestObject_t, obj, identifier ) == NULL ) {
    free( obj );
    return NULL;
  }
  return obj;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __test_destructor( FramehashTestObject_t *self ) {
  free( self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __test_DemoMethod( const FramehashTestObject_t *self ) {
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static FramehashTestObject_t_vtable_t __FramehashTestObjectMethods = {
  /* base interface */
  .vm_cmpid       = (f_object_comparator_t)__test_cmpid,            /* vm_cmpid       */
  .vm_getid       = (f_object_identifier_t)__test_getid,            /* vm_getid       */
  .vm_serialize   = (f_object_serializer_t)__test_serialize,        /* vm_serialize   */
  .vm_deserialize = (f_object_deserializer_t)__test_deserialize,    /* vm_deserialize */
  .vm_construct   = (f_object_constructor_t)__test_constructor,     /* vm_construct   */
  .vm_destroy     = (f_object_destructor_t)__test_destructor,       /* vm_destroy     */
  .vm_represent   = NULL,                                           /* vm_represent   */
  .vm_allocator   = NULL,                                           /* vm_allocator   */
  /* extended interface */
  .DemoMethod     = __test_DemoMethod
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_api_class__test_object_register_class( void ) {
  COMLIB_REGISTER_CLASS( FramehashTestObject_t, CXLIB_OBTYPE_GENERIC, &__FramehashTestObjectMethods, OBJECT_IDENTIFIED_BY_OBJECTID, 0 );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN void _framehash_api_class__test_object_unregister_class( void ) {
  COMLIB_UNREGISTER_CLASS( FramehashTestObject_t );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int show_sizeof(void) {
  pmesg( 4, "_FRAMEHASH_P_MIN                   : %d\n", _FRAMEHASH_P_MIN );
  pmesg( 4, "_FRAMEHASH_P_MAX                   : %d\n", _FRAMEHASH_P_MAX );
  pmesg( 4, "sizeof(framehash_cell_t)           : %d\n", sizeof(framehash_cell_t) );
  pmesg( 4, "sizeof(framehash_slot_t)           : %d\n", sizeof(framehash_slot_t) );
  pmesg( 4, "sizeof(framehash_metas_t)          : %d\n", sizeof(framehash_metas_t) );
  pmesg( 4, "sizeof(framehash_access_guard_t)   : %d\n", sizeof(framehash_access_guard_t) );
  pmesg( 4, "sizeof(framehash_retcode_t)       : %d\n", sizeof(framehash_retcode_t) );
  pmesg( 4, "\n" );
  return 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
test_descriptor_set_t framehash_utest_sets[] = {
  // Allocator
  { "memory.c",             _framehash_memory_tests },
  { "frameallocator.c",     _framehash_frameallocator_tests },
  { "basementallocator.c",  _framehash_basementallocator_tests },
  
  // Process
  { "processor.c",          _framehash_processor_tests },
  { "framemath.c",          _framehash_framemath_tests },
  { "delete.c",             _framehash_delete_tests },
  { "serialization.c",      _framehash_serialization_tests },
#ifdef FRAMEHASH_CHANGELOG
  { "changelog.c",          _framehash_changelog_tests },
#endif

  // Radix
  { "hashing.c",            _framehash_hashing_tests },
  { "leaf.c",               _framehash_leaf_tests },
  { "basement.c",           _framehash_basement_tests },
  { "cache.c",              _framehash_cache_tests },
  { "radix.c",              _framehash_radix_tests },
  { "fmacro.c",             _framehash_fmacro_tests },

  // API
  { "framehash.c",          _framehash_framehash_tests },
  { "api_class.c",          _framehash_api_class_tests },
  { "api_generic.c",        _framehash_api_generic_tests },
  { "api_object.c",         _framehash_api_object_tests },
  { "api_int56.c",          _framehash_api_int56_tests },
  { "api_real56.c",         _framehash_api_real56_tests },
  { "api_pointer.c",        _framehash_api_pointer_tests },
  { "api_info.c",           _framehash_api_info_tests },
  { "api_iterator.c",       _framehash_api_iterator_tests },
  { "api_manage.c",         _framehash_api_manage_tests },
  { "api_simple.c",         _framehash_api_simple_tests },

  {NULL}
};
#endif



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT char ** _framehash_fhtest__get_unit_test_names( void ) {
#ifdef INCLUDE_UNIT_TESTS
#define __TEST_COUNT( Set ) ((sizeof( Set ) / sizeof( test_descriptor_set_t ))-1)
  char **names = NULL;
  const int sz = __TEST_COUNT( framehash_utest_sets );
  if( (names = calloc( sz+1, sizeof( char* ) )) != NULL ) {
    test_descriptor_set_t *src = framehash_utest_sets;
    const char **dest = (const char**)names;
    while( src->name ) {
      *dest++ = src++->name;
    }
  }
  return names;
#else
  return calloc( 1, sizeof( char*) );
#endif
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int _framehash_fhtest__run_unit_tests( const char *runonly[], const char *testdir ) {
#ifdef INCLUDE_UNIT_TESTS
  int retcode = -1;

  // TODO: implement selective tests if runonly != NULL
  
  if( SetCurrentTestDirectory( testdir ) < 0 ) {
    return -1;
  }

  const _framehash_frameallocator___interface_t *F_ALLOC = _framehash_frameallocator__Interface();
  const _framehash_basementallocator___interface_t *B_ALLOC = _framehash_basementallocator__Interface();
  bool own_F_ALLOC = false;
  bool own_B_ALLOC = false;

  if( F_ALLOC->Bytes() == 0 ) {
    F_ALLOC->Init( );
    own_F_ALLOC = true;
  }

  if( B_ALLOC->Bytes() == 0 ) {
    B_ALLOC->Init( );
    own_B_ALLOC = true;
  }




  UnitTestSuite *framehash_utest_suite = NULL;

  XTRY {
    int result;
    const char *error = NULL;

    // Create the test suite
    if( (framehash_utest_suite = NewUnitTestSuite( "Framehash Test Suite" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xF01 );
    }

    // Make sure all selected tests are defined
    if( CALLABLE( framehash_utest_suite )->ValidateTestNames( framehash_utest_suite, framehash_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xF02, "Undefined test: %s", error );
    }

    // Add all (selected) test sets to test suite
    if( CALLABLE( framehash_utest_suite )->ExtendTestDescriptorSets( framehash_utest_suite, framehash_utest_sets, runonly, &error ) < 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xF03, "Failed to add test: %s", error );
    }

    // Run test suite
    result = framehash_utest_suite->vtable->Run( framehash_utest_suite ) ;

    // Check result
    if( result != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xF04, "Test suite failed" );
    }

    retcode = 0;
  }
  XCATCH( errcode ) {
    
  }
  XFINALLY {
    DeleteUnitTestSuite( &framehash_utest_suite );
    if( own_F_ALLOC ) F_ALLOC->Clear();
    if( own_B_ALLOC ) B_ALLOC->Clear();
  }

  return retcode;
#else
  return 0;
#endif
}







