/*######################################################################
 *#
 *# pyvgx_framehash.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"
#include <marshal.h>



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



typedef struct s_PyFramehashObjectWrapper_constructor_args_t {
  PyObject *py_obj;
} PyFramehashObjectWrapper_constructor_args_t;


/*******************************************************************//**
 * vtable: FramehashObjectMethods
 *
 ***********************************************************************
 */
typedef struct s_PyFramehashObjectWrapper_vtable_t {
  COMLIB_VTABLE_HEAD
} PyFramehashObjectWrapper_vtable_t;


/******************************************************************************
 * PyFramehashObjectWrapper_t
 *
 ******************************************************************************
 */
typedef struct s_PyFramehashObjectWrapper_t {
  COMLIB_OBJECT_HEAD( PyFramehashObjectWrapper_vtable_t )
  objectid_t obid;
  PyObject *py_object;
} PyFramehashObjectWrapper_t;


PyThreadState *_saved_thread_state = NULL;


static void __print_allocator_info( const char *name );
static PyObject * pyframehash_debuginfo( PyVGX_Framehash *self, PyObject *args, PyObject *kwds );
static PyObject * PyVGX_Framehash_unit_tests( PyVGX_Framehash *self );
static PyObject * PyVGX_Framehash_repr( PyVGX_Framehash *self );

/* */
static int _pywrapper_cmpid( const PyFramehashObjectWrapper_t *wobj, const objectid_t *pobid );
static objectid_t * _pywrapper_getid( PyFramehashObjectWrapper_t *wobj );
static int64_t _pywrapper_serialize( const comlib_object_t *obj, CQwordQueue_t *out_queue );
static comlib_object_t * _pywrapper_deserialize( comlib_object_t *null_container, CQwordQueue_t *in_queue );
static PyFramehashObjectWrapper_t * _pywrapper_constructor( const void *identifier, PyFramehashObjectWrapper_constructor_args_t *args );
static void _pywrapper_destructor( PyFramehashObjectWrapper_t *wobj );

static void PyVGX_Framehash_dealloc( PyVGX_Framehash *self );
static PyObject * PyVGX_Framehash_new( PyTypeObject *type, PyObject *args, PyObject *kwds );
static int PyVGX_Framehash_init( PyVGX_Framehash *self, PyObject *args, PyObject *kwds );

static framehash_keytype_t _key( PyVGX_Framehash *self, PyObject *py_key, void *pkey );

static PyObject * _set( PyVGX_Framehash *self, PyObject *args );
static PyObject * _get( PyVGX_Framehash *self, PyObject *args );
static PyObject * _delete( PyVGX_Framehash *self, PyObject *py_key );
static int _contains( PyVGX_Framehash *self, PyObject *py_key );
static PyObject * _inc( PyVGX_Framehash *self, PyObject *args );

static PyObject * _set_int( framehash_t *fhash, PyObject *args );
static PyObject * _get_int( framehash_t *fhash, PyObject *py_key );
static PyObject * _del_int( framehash_t *fhash, PyObject *py_key );
static PyObject * _contains_int( framehash_t *fhash, PyObject *py_key );
static PyObject * _inc_int( framehash_t *fhash, PyObject *args );

static PyObject * Framehash_set( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_get( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_delete( PyVGX_Framehash *self, PyObject *py_key );
static PyObject * Framehash_contains( PyVGX_Framehash *self, PyObject *py_key );
static PyObject * Framehash_inc( PyVGX_Framehash *self, PyObject *args );

static PyObject * Framehash_set_int( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_get_int( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_del_int( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_contains_int( PyVGX_Framehash *self, PyObject *py_key );
static PyObject * Framehash_inc_int( PyVGX_Framehash *self, PyObject *args );

static PyObject * Framehash_items( PyVGX_Framehash *self );
static PyObject * Framehash_keys( PyVGX_Framehash *self );
static PyObject * Framehash_values( PyVGX_Framehash *self );

static PyObject * Framehash_clear( PyVGX_Framehash *self );
static PyObject * Framehash_flush_caches( PyVGX_Framehash *self, PyObject *args );
static PyObject * Framehash_save( PyVGX_Framehash *self );

static PyObject * pyframehash_rand9( PyVGX_Framehash *self );
static PyObject * pyframehash_rand11( PyVGX_Framehash *self );
static PyObject * pyframehash_rand12( PyVGX_Framehash *self );
static PyObject * pyframehash_rand13( PyVGX_Framehash *self );
static PyObject * pyframehash_rand14( PyVGX_Framehash *self );
static PyObject * pyframehash_rand15( PyVGX_Framehash *self );
static PyObject * pyframehash_rand16( PyVGX_Framehash *self );
static PyObject * pyframehash_rand17( PyVGX_Framehash *self );
static PyObject * pyframehash_rand18( PyVGX_Framehash *self );
static PyObject * pyframehash_rand19( PyVGX_Framehash *self );
static PyObject * pyframehash_rand20( PyVGX_Framehash *self );
static PyObject * pyframehash_rand21( PyVGX_Framehash *self );
static PyObject * pyframehash_rand22( PyVGX_Framehash *self );
static PyObject * pyframehash_rand23( PyVGX_Framehash *self );
static PyObject * pyframehash_rand24( PyVGX_Framehash *self );
static PyObject * pyframehash_rand25( PyVGX_Framehash *self );
static PyObject * pyframehash_rand26( PyVGX_Framehash *self );
static PyObject * pyframehash_rand27( PyVGX_Framehash *self );
static PyObject * pyframehash_rand28( PyVGX_Framehash *self );
static PyObject * pyframehash_rand29( PyVGX_Framehash *self );
static PyObject * pyframehash_rand30( PyVGX_Framehash *self );
static PyObject * pyframehash_rand31( PyVGX_Framehash *self );
static PyObject * pyframehash_rand32( PyVGX_Framehash *self );
static PyObject * pyframehash_rand33( PyVGX_Framehash *self );
static PyObject * pyframehash_rand34( PyVGX_Framehash *self );
static PyObject * pyframehash_rand35( PyVGX_Framehash *self );
static PyObject * pyframehash_rand36( PyVGX_Framehash *self );
static PyObject * pyframehash_rand37( PyVGX_Framehash *self );
static PyObject * pyframehash_rand38( PyVGX_Framehash *self );
static PyObject * pyframehash_rand39( PyVGX_Framehash *self );
static PyObject * pyframehash_rand40( PyVGX_Framehash *self );
static PyObject * pyframehash_rand41( PyVGX_Framehash *self );
static PyObject * pyframehash_rand42( PyVGX_Framehash *self );
static PyObject * pyframehash_rand43( PyVGX_Framehash *self );
static PyObject * pyframehash_rand44( PyVGX_Framehash *self );
static PyObject * pyframehash_rand45( PyVGX_Framehash *self );
static PyObject * pyframehash_rand46( PyVGX_Framehash *self );
static PyObject * pyframehash_rand47( PyVGX_Framehash *self );
static PyObject * pyframehash_rand48( PyVGX_Framehash *self );
static PyObject * pyframehash_rand49( PyVGX_Framehash *self );
static PyObject * pyframehash_rand50( PyVGX_Framehash *self );
static PyObject * pyframehash_rand51( PyVGX_Framehash *self );
static PyObject * pyframehash_rand52( PyVGX_Framehash *self );
static PyObject * pyframehash_rand53( PyVGX_Framehash *self );
static PyObject * pyframehash_rand54( PyVGX_Framehash *self );
static PyObject * pyframehash_rand55( PyVGX_Framehash *self );
static PyObject * pyframehash_rand56( PyVGX_Framehash *self );
static PyObject * pyframehash_rand57( PyVGX_Framehash *self );
static PyObject * pyframehash_rand58( PyVGX_Framehash *self );
static PyObject * pyframehash_rand59( PyVGX_Framehash *self );
static PyObject * pyframehash_rand60( PyVGX_Framehash *self );
static PyObject * pyframehash_rand61( PyVGX_Framehash *self );
static PyObject * pyframehash_rand62( PyVGX_Framehash *self );
static PyObject * pyframehash_rand63( PyVGX_Framehash *self );
static PyObject * pyframehash_rand64( PyVGX_Framehash *self );


static PyObject * pyframehash_hash64( PyVGX_Framehash *self, PyObject *args );
static PyObject * pyframehash_hash32( PyVGX_Framehash *self, PyObject *args );

static PyObject * Framehash_count_active( PyVGX_Framehash *self );

static PyObject * Framehash_get_perfcounters( PyVGX_Framehash *self );
static PyObject * Framehash_reset_perfcounters( PyVGX_Framehash *self );

static PyObject * Framehash_math_mul( PyVGX_Framehash *self, PyObject *py_factor );
static PyObject * Framehash_math_div( PyVGX_Framehash *self, PyObject *py_divisor );
static PyObject * Framehash_math_add( PyVGX_Framehash *self, PyObject *py_val );
static PyObject * Framehash_math_sub( PyVGX_Framehash *self, PyObject *py_val );
static PyObject * Framehash_math_inc( PyVGX_Framehash *self );
static PyObject * Framehash_math_dec( PyVGX_Framehash *self );
static PyObject * Framehash_math_sqrt( PyVGX_Framehash *self );
static PyObject * Framehash_math_pow( PyVGX_Framehash *self, PyObject *py_exponent );
static PyObject * Framehash_math_log( PyVGX_Framehash *self, PyObject *py_base );
static PyObject * Framehash_math_exp( PyVGX_Framehash *self, PyObject *py_base );
static PyObject * Framehash_math_decay( PyVGX_Framehash *self, PyObject *py_exponent );
static PyObject * Framehash_math_set( PyVGX_Framehash *self, PyObject *py_value );
static PyObject * Framehash_math_randomize( PyVGX_Framehash *self );
static PyObject * Framehash_math_int( PyVGX_Framehash *self );
static PyObject * Framehash_math_float( PyVGX_Framehash *self );
static PyObject * Framehash_math_abs( PyVGX_Framehash *self );
static PyObject * Framehash_math_sum( PyVGX_Framehash *self );
static PyObject * Framehash_math_avg( PyVGX_Framehash *self );
static PyObject * Framehash_math_stdev( PyVGX_Framehash *self );

static Py_ssize_t PyVGX_Framehash_length( PyVGX_Framehash *self );
static PyObject * Framehash_subscript( PyVGX_Framehash *self, PyObject *py_key );
static int PyVGX_Framehash_ass_subscript( PyVGX_Framehash *self, PyObject *py_key, PyObject *py_value );




/******************************************************************************
 * __print_allocator_info
 *
 ******************************************************************************
 */
static void __print_allocator_info( const char *name ) {

  if( !strcmp( name, "frame" ) ) {
    COMLIB_OBJECT_PRINT( iFramehash.memory.DefaultFrameAllocator() );
  }
  else if( !strcmp( name, "basement" ) ) {
    COMLIB_OBJECT_PRINT( iFramehash.memory.DefaultBasementAllocator() );
  }
  else {
    DEBUG( 0, "Unknown allocator: %s", name );
  }
}



/******************************************************************************
 * pyframehash_debuginfo
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * pyframehash_debuginfo( PyVGX_Framehash *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"allocator",  NULL};
  
  char *alloc_name = NULL;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|s", kwlist, &alloc_name ) ) {
    return NULL;
  }

  if( alloc_name ) {
    __print_allocator_info( alloc_name );
  }


  Py_INCREF( Py_None );
  return Py_None;
}



/******************************************************************************
 * PyVGX_Framehash_selftest
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Framehash_selftest( PyObject *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { "testroot", "names", NULL };

  static const char *default_testroot = "./pyframehash_selftest";
  const char *testroot = default_testroot;
  int64_t sz_testroot = strlen( testroot );
  PyObject *py_testnames = NULL;

  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|s#O", kwlist, &testroot, &sz_testroot, &py_testnames ) ) {
    return NULL;
  }

  if( dir_exists( testroot ) ) {
    // Require manual deletion of testdir unless default dir is used (to avoid accidents)
    if( testroot != default_testroot || delete_dir( testroot ) < 0 ) {
      PyErr_Format( PyExc_Exception, "Remove directory '%s' before running unit tests", testroot );
      return NULL;
    }
  }

  const char **testnames = NULL;

  if( py_testnames != NULL ) {

    if( !PyList_Check( py_testnames ) ) {
      PyErr_SetString( PyExc_TypeError, "test names must be a list of strings" );
      return NULL;
    }


    int64_t sz = PyList_Size( py_testnames );
    for( int64_t i=0; i<sz; i++ ) {
      if( !PyUnicode_Check( PyList_GET_ITEM( py_testnames, i ) ) ) {
        PyErr_SetString( PyExc_TypeError, "test names must be a list of strings" );
        return NULL;
      }
    }

    testnames = calloc( sz+1, sizeof(char*) );

    for( int64_t i=0; i<sz; i++ ) {
      const char *name = PyUnicode_AsUTF8( PyList_GET_ITEM( py_testnames, i ) );
      testnames[i] = name;
    }
  }

  int ret;

  Py_BEGIN_ALLOW_THREADS
  ret = iFramehash.test.RunUnitTests( testnames, testroot );
  Py_END_ALLOW_THREADS

  if( testnames ) {
    free( (char**)testnames );
  }

  // Success
  if( ret == 0 ) {
    Py_RETURN_NONE;
  }
  else {
    PyErr_SetString( PyExc_Exception, "TESTS FAILED!" );
    return NULL;
  }

  SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
}




/******************************************************************************
 * PyVGX_Framehash_repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Framehash_repr( PyVGX_Framehash *self ) {
  char hex[33];
  const char *mp = self->fhash->_CSTR__masterpath ? CStringValue( self->fhash->_CSTR__masterpath ) : "null";

  return PyUnicode_FromFormat( "Framehash:  id=%s  file=%s  order=%d  nobj=%lld  opcnt=%lld  shortkeys=%d",
                                           idtostr(hex, &self->fhash->obid),
                                                  mp,
                                                           self->fhash->_order,
                                                                     self->fhash->_nobj,
                                                                                self->fhash->_opcnt,
                                                                                            (int)self->fhash->_flag.shortkeys);
}



/*******************************************************************//**
 * COMLIB OBJECT API IMPLEMENTATION 
 *
 ***********************************************************************
 */
/*******************************************************************//**
 * _pywrapper_cmpid
 * vm_cmpid
 *
 ***********************************************************************
 */
static int _pywrapper_cmpid( const PyFramehashObjectWrapper_t *wobj, const objectid_t *pobid ) {
  return idmatch( &wobj->obid, pobid ) ? 0 : -1;
}



/*******************************************************************//**
 * _pywrapper_getid
 * vm_getid
 *
 ***********************************************************************
 */
static objectid_t * _pywrapper_getid( PyFramehashObjectWrapper_t *wobj ) {
  return &wobj->obid;
}



typedef enum e_inner_type_t {
  INNER_PYOBJECT      = 0x0001,
  INNER_PYFRAMEHASH   = 0x0002  
} inner_type_t;



/*******************************************************************//**
 * marshal_error
 ***********************************************************************
 */
static void __marshal_error( const comlib_object_t *obj ) {
  char hexid[33];
  objectid_t *pobid = (objectid_t*)COMLIB_OBJECT_GETID(obj);
  PyObject *py_value = ((PyFramehashObjectWrapper_t*)obj)->py_object;
  PyErr_Format( PyExc_ValueError, "Marshal error: key=%s value=%s", idtostr(hexid, pobid), Py_TYPE(py_value)->tp_name );
}



typedef union {
  QWORD qword;
  char c[8];
} QBUF;



/*******************************************************************//**
 * _pywrapper_serialize
 * vm_serialize
 *
 ***********************************************************************
 */
static int64_t _pywrapper_serialize( const comlib_object_t *obj, CQwordQueue_t *output ) {
  PyObject *py_bytes;
  int64_t nqwords = 0;
  PyObject *py_value = ((PyFramehashObjectWrapper_t*)obj)->py_object;
  int64_t (*write)( CQwordQueue_t *Q, const QWORD *data, int64_t len ) = output->vtable->WriteNolock;
  QWORD inner_type; 


  if( Py_IS_TYPE(py_value, p_PyVGX_Framehash__FramehashType) ) {
    framehash_t *fhash = ((PyVGX_Framehash*)py_value)->fhash;
    // | INNER_TYPE | ID | FRAMEHASH | => OUTPUT
    // INNER_TYPE
    inner_type = INNER_PYFRAMEHASH;
    nqwords += write( output, &inner_type, 1 );
    // ID
    nqwords += write( output, COMLIB_OBJECT_GETID(obj), qwsizeof(objectid_t) );
    // FRAMEHASH
    nqwords += SERIALIZE( fhash, output );
  }
  else if( (py_bytes = PyMarshal_WriteObjectToString( py_value, Py_MARSHAL_VERSION )) != NULL ) {
    Py_ssize_t sz_marsh;
    char *str;
    if( PyBytes_AsStringAndSize( py_bytes, &str, &sz_marsh ) == 0 ) {
      // | INNER_TYPE | ID | MARSHAL_SZ | MARSHAL_DATA | => OUTPUT
      // INNER_TYPE
      inner_type = INNER_PYOBJECT;
      nqwords += write( output, &inner_type, 1 );
      // ID
      nqwords += write( output, COMLIB_OBJECT_GETID(obj), qwsizeof(objectid_t) );
      // MARSHAL bytes
      QWORD sz = sz_marsh;
      nqwords += write( output, &sz, 1 );       // number of marshalled bytes (need this to restore)
      // MARSHAL whole qwords
      int64_t qwsz_marsh = sz_marsh / sizeof(QWORD);    // number of whole QWORDs in str
      nqwords += write( output, (QWORD*)str, qwsz_marsh );  //
      // MARSHAL remainder (annoying as we can see)
      int64_t sz_remainder = sz_marsh - qwsz_marsh * sizeof(QWORD);
      if( sz_remainder > 0 ) {
        QBUF remainder = { .qword=0 };
        int64_t roffset = qwsz_marsh * sizeof(QWORD);
        for( int i=0; i<sz_remainder; i++ ) {
          remainder.c[i] = str[roffset++];
        }
        nqwords += write( output, &remainder.qword, 1 );      // last partial QWORD
      }
    }
    else {
      nqwords = -1;
    }
    Py_DECREF( py_bytes );
  }
  else {
    __marshal_error( obj );
    nqwords = -1;
  }

  return nqwords;
}



/*******************************************************************//**
 * _pywrapper_deserialize
 * vm_deserialize
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static comlib_object_t * _pywrapper_deserialize( comlib_object_t *null_container, CQwordQueue_t *input ) {
  PyFramehashObjectWrapper_constructor_args_t wargs = {.py_obj = NULL};
  PyFramehashObjectWrapper_t *wobj = NULL;
  comlib_object_t *obj = NULL;
  
  QWORD *qwdata = NULL;

  XTRY {
    int64_t sz_ser, *psz_ser=&sz_ser;
    objectid_t obid, *pobid=&obid;
    int64_t (*read)( CQwordQueue_t *Q, void **dest, int64_t count ) = input->vtable->ReadNolock;
    QWORD inner_type, *pinner_type=&inner_type;

    // INNER_TYPE
    if( read( input, (void**)&pinner_type, 1 ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_API, 0xA01 );
    }
    // ID
    if( read( input, (void**)&pobid, qwsizeof(objectid_t) ) != qwsizeof(objectid_t) ) {
      THROW_ERROR( CXLIB_ERR_API, 0xA02 );
    }

    if( inner_type == INNER_PYOBJECT ) {
      // MARSHAL bytes
      if( read( input, (void**)&psz_ser, 1 ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA03 );
      }
      // MARSHAL QWORD DATA
      int64_t qwsz_ser = sz_ser / sizeof(QWORD);
      if( sz_ser % sizeof(QWORD) > 0 ) {
        qwsz_ser++;
      }
      if( read( input, (void**)&qwdata, qwsz_ser ) != qwsz_ser ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA04 );
      }
      // Unmarshal python object
      if( (wargs.py_obj = PyMarshal_ReadObjectFromString( (char*)qwdata, sz_ser )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA05 );
      }
    }
    else if( inner_type == INNER_PYFRAMEHASH ) {
      comlib_object_typeinfo_t typeinfo = COMLIB_CLASS_TYPEINFO( framehash_t );
      if( (obj = COMLIB_DeserializeObject( NULL, &typeinfo, input )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA06 );
      }
      if( obj->typeinfo.tp_class != CLASS_framehash_t ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA07 );
      }
      if( (wargs.py_obj = PyVGX_Framehash_new( p_PyVGX_Framehash__FramehashType, NULL, NULL )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA08 );
      }
      ((PyVGX_Framehash*)wargs.py_obj)->fhash = (framehash_t*)obj;
    }
    else {
      THROW_ERROR( CXLIB_ERR_API, 0xA09 );
    }
    
    // Allocate wrapper object - becomes separate owner of python object
    if( (wobj = COMLIB_OBJECT_NEW( PyFramehashObjectWrapper_t, pobid, &wargs )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_API, 0xA0A );
    }
  }
  XCATCH(errcode) {
    if(wobj) {
      free(wobj);
      wobj = NULL;
    }
  }
  XFINALLY {
    Py_XDECREF( wargs.py_obj );
    ALIGNED_FREE( qwdata );
  }

  return (comlib_object_t*)wobj;
}



/*******************************************************************//**
 * _pywrapper_construct
 * vm_construct
 *
 ***********************************************************************
 */
static PyFramehashObjectWrapper_t * _pywrapper_constructor( const void *identifier, PyFramehashObjectWrapper_constructor_args_t *args ) {
  PyFramehashObjectWrapper_t *wobj;

  if( !args ) {
    return NULL;
  }
  if( (wobj = (PyFramehashObjectWrapper_t*)malloc( sizeof(PyFramehashObjectWrapper_t) )) == NULL ) {
    return NULL;
  }
  if( COMLIB_OBJECT_INIT( PyFramehashObjectWrapper_t, wobj, identifier ) == NULL ) {
    free( wobj );
    return NULL;
  }
  wobj->py_object = args->py_obj;
  Py_XINCREF( wobj->py_object );

  return wobj;
}



/*******************************************************************//**
 * _pywrapper_destroy
 * vm_destroy
 *
 ***********************************************************************
 */
static void _pywrapper_destructor( PyFramehashObjectWrapper_t *wobj ) {
  if( wobj ) {
    if( wobj->py_object ) {
      PyGILState_STATE state = PyGILState_Ensure();
      Py_DECREF( wobj->py_object );
      PyGILState_Release( state );
    }
    free( wobj );
  }
}



/*******************************************************************//**
 * vtable: PyFramehashObjectWrapperMethods
 *
 ***********************************************************************
 */
static PyFramehashObjectWrapper_vtable_t PyFramehashObjectWrapperMethods = {
  .vm_cmpid       = (f_object_comparator_t)_pywrapper_cmpid,          /* vm_cmpid       */
  .vm_getid       = (f_object_identifier_t)_pywrapper_getid,          /* vm_getid       */
  .vm_serialize   = (f_object_serializer_t)_pywrapper_serialize,      /* vm_serialize   */
  .vm_deserialize = (f_object_deserializer_t)_pywrapper_deserialize,  /* vm_deserialize */
  .vm_construct   = (f_object_constructor_t)_pywrapper_constructor,   /* vm_construct   */
  .vm_destroy     = (f_object_destructor_t)_pywrapper_destructor,     /* vm_destroy     */
  .vm_represent   = NULL,                                             /* vm_represent   */
  .vm_allocator   = NULL                                              /* vm_allocator   */
};



/******************************************************************************
 * PyVGX_Framehash_dealloc
 *
 ******************************************************************************
 */
static uint64_t nohash64( uint64_t x ) {
  return x;
}



/******************************************************************************
 * PyVGX_Framehash_dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Framehash_dealloc( PyVGX_Framehash *self ) {
  if( self->fhash ) {
    COMLIB_OBJECT_DESTROY( self->fhash );
    self->fhash = NULL;
  }
  Py_TYPE( self )->tp_free( self );
}



/******************************************************************************
 * PyVGX_Framehash_new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Framehash_new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_Framehash *self;

  self = (PyVGX_Framehash *)type->tp_alloc(type, 0);
  self->fhash = NULL;
  self->nointhash = 0;
  return (PyObject *)self;
}



/******************************************************************************
 * PyVGX_Framehash_init
 *
 ******************************************************************************
 */
static int PyVGX_Framehash_init( PyVGX_Framehash *self, PyObject *args, PyObject *kwds ) {
  int ret = 0;
  static char *kwlist[] = {"name", "path", "order", "threads", "nointhash", "shortkeys", "max_cache_domain", "allocator_order", NULL};
  char *name = NULL;
  Py_ssize_t sz_name = 0;
  char *path = NULL;
  Py_ssize_t sz_path = 0;
  int order = FRAMEHASH_ARG_UNDEFINED;
  int threads = 1;
  int nointhash = 0;
  int shortkeys = 0;
  int cache_depth = FRAMEHASH_ARG_UNDEFINED;
  int allocator_order = 0;
  cxmalloc_family_t *falloc = NULL;
  cxmalloc_family_t *balloc = NULL;
  framehash_constructor_args_t FH_args = {0};


  char buffer[100];
  snprintf( buffer, 99, "[PyVGX_Framehash @ %p]", self );

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|s#s#iiiiii", kwlist, &name, &sz_name, &path, &sz_path, &order, &threads, &nointhash, &shortkeys, &cache_depth, &allocator_order) ) {
    return -1; 
  }

  int use_default_allocators = allocator_order == 0;

  XTRY {

    if( use_default_allocators ) {
      falloc = iFramehash.memory.DefaultFrameAllocator();
      balloc = iFramehash.memory.DefaultBasementAllocator();
    }
    else {
      if( allocator_order > 31 ) {
        PyErr_SetString( PyExc_Exception, "allocator_order too large (max 31)" );
        THROW_SILENT( CXLIB_ERR_API, 0xA11 );
      }
      if( (falloc = iFramehash.memory.NewFrameAllocator( buffer, 1ULL<<allocator_order )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA12 );
      }
      if( (balloc = iFramehash.memory.NewBasementAllocator( buffer, 1ULL<<allocator_order )) == NULL ) { 
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xA13 );
      }
    }

    FH_args.param.order           = order == FRAMEHASH_ARG_UNDEFINED ? FRAMEHASH_DEFAULT_ORDER : (int8_t)order;
    FH_args.param.synchronized    = threads != 0; // if we allow threads, we must synchronize
    FH_args.param.shortkeys       = shortkeys != 0; // When nonzero, a collision-prone but more efficient 64-bit hash will be used for string keys rather than the safer 128-bit hash
    FH_args.param.cache_depth     = (int8_t)cache_depth;
    FH_args.dirpath               = path;
    FH_args.name                  = name;
    FH_args.input_queue           = NULL;
    FH_args.fpath                 = NULL;
    FH_args.frame_allocator       = &falloc;
    FH_args.basement_allocator    = &balloc;
    FH_args.shortid_hashfunction  = nointhash ? nohash64 : ihash64;

    if( (self->fhash = COMLIB_OBJECT_NEW( framehash_t, NULL, &FH_args )) == NULL ) {
      PyErr_SetString( PyExc_Exception, "Failed to initialize framehash" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xA14 );
    }

    self->nointhash = nointhash;
  }
  XCATCH( errcode ) {
    if( !use_default_allocators ) {
      if( falloc ) {
        COMLIB_OBJECT_DESTROY( falloc );
      }
      if( balloc ) {
        COMLIB_OBJECT_DESTROY( balloc );
      }
    }
    ret = -1;
  }
  XFINALLY {
  }
  return ret;
}



/******************************************************************************
 * _key
 *
 ******************************************************************************
 */
static framehash_keytype_t _key( PyVGX_Framehash *self, PyObject *py_key, void *pkey ) {
  Py_ssize_t sz_key = 0;
  framehash_keytype_t ktype = CELL_KEY_TYPE_NONE;
  const char *key = NULL;

  if( PyLong_CheckExact(py_key) ) {
    uint64_t x = PyLong_AsUnsignedLongLong( py_key );
    if( x != (uint64_t)-1 || !PyErr_Occurred() ) {
      if( self->fhash->_flag.shortkeys ) {
        *((shortid_t*)pkey) = self->fhash->_dynamic.hashf( x );
        ktype = CELL_KEY_TYPE_HASH64;
      }
      else {
        *((QWORD*)pkey) = x;
        ktype = CELL_KEY_TYPE_PLAIN64; // => OK 64-bit PLAY INTEGER KEY
      }
    }
    else {
      return CELL_KEY_TYPE_ERROR;
    }
  }
  else if( PyVGX_Framehash_PyObject_CheckString( py_key ) ) {
    if( (key = PyVGX_Framehash_PyObject_AsStringAndSize( py_key, &sz_key )) == NULL ) {
      PyErr_SetString( PyExc_TypeError, "Invalid key" );
      return CELL_KEY_TYPE_ERROR;
    }
  }
  else {
    PyErr_SetString( PyExc_TypeError, "Key must be string or integer" );
    return CELL_KEY_TYPE_ERROR;
  }
  
  if( key ) {
    // If using shortkeys we compute efficient 64-bit hash on key, but risk collisions
    if( self->fhash->_flag.shortkeys ) {
      *((QWORD*)pkey) = hash64( (unsigned char*)key, sz_key );
      ktype = CELL_KEY_TYPE_HASH64; // => OK 64-bit HASH
    }
    // Otherwise we use a safer 128-bit key
    else {
      objectid_t obid;
      if( sz_key == 32 && ((obid=strtoid(key)).H || obid.L) ) {
        // we interpret key as 128-bit hash, don't have to do the expensive computation
        *((objectid_t*)pkey) = obid;
        ktype = CELL_KEY_TYPE_HASH128; // => OK 128-bit OBID KEY
      }
      else if( sz_key > INT_MAX ) {
        PyErr_SetString( PyExc_KeyError, "Key is too large" );
        return CELL_KEY_TYPE_ERROR;
      }
      else { 
        /* we must compute 128-bit hash of the key string below */ 
        Py_BEGIN_ALLOW_THREADS
        *((objectid_t*)pkey) = obid_from_string_len( key, (int)sz_key );
        Py_END_ALLOW_THREADS
        ktype = CELL_KEY_TYPE_HASH128; // OK 128-bit OBID KEY
      }
    }
  }

  return ktype;
}




/******************************************************************************
 * _set
 *
 ******************************************************************************
 */
static PyObject * _set( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_key;
  PyObject *py_ret = NULL;
  PyFramehashObjectWrapper_constructor_args_t wargs = {.py_obj = NULL};

  if( !PyArg_UnpackTuple( args, "_set", 1, 2, &py_key, &wargs.py_obj ) ) {
    return NULL;
  }

  XTRY {
    framehash_t *fhash = self->fhash;
    PyFramehashObjectWrapper_t *wobj = NULL;
    framehash_valuetype_t vtype = CELL_VALUE_TYPE_NULL;
    framehash_keytype_t ktype = CELL_KEY_TYPE_NONE;
    union {
      int64_t integer;
      double real;
    } data;

    __m128i __KEYMEM;
    void *PKEY = &__KEYMEM;

    ktype = _key( self, py_key, PKEY );

    if( ktype == CELL_KEY_TYPE_ERROR || ktype == CELL_KEY_TYPE_NONE ) { 
      THROW_ERROR( CXLIB_ERR_API, 0xA21 );
    }

    if( ktype == CELL_KEY_TYPE_PLAIN64 || ktype == CELL_KEY_TYPE_HASH64 ) {
      // short keys require the value to be representable as a primitive C numeric type
      if( wargs.py_obj == NULL ) {
        vtype = CELL_VALUE_TYPE_MEMBER;
      }
      else if( PyBool_Check(wargs.py_obj) ) {
        data.integer = (wargs.py_obj == Py_True);
        vtype = CELL_VALUE_TYPE_BOOLEAN;
      }
      else if( wargs.py_obj == Py_None ) {
        vtype = CELL_VALUE_TYPE_MEMBER;
      }
      else if( PyLong_CheckExact(wargs.py_obj) ) {
        data.integer = PyLong_AsLongLong( wargs.py_obj );
        if( data.integer == -1 && PyErr_Occurred() ) {
          THROW_ERROR( CXLIB_ERR_API, 0xA22 );
        }
        vtype = CELL_VALUE_TYPE_INTEGER;
      }
      else if( PyFloat_CheckExact(wargs.py_obj) ) {
        data.real = PyFloat_AS_DOUBLE( wargs.py_obj );
        vtype = CELL_VALUE_TYPE_REAL;
      }
      // sorry, not a primitive numeric type...
      else {
        if( !self->fhash->_flag.shortkeys ) {
          PyErr_SetString( PyExc_LookupError, "String key or 32-character hex string key required for non-primitive values" );
        }
        else {
          PyErr_SetString( PyExc_LookupError, "Only primitive values supported in shortkeys mode" );
        }
        THROW_ERROR( CXLIB_ERR_API, 0xA25 );
      }

      // Proceed with inserting the primitive type
      if( fhash->_flag.synchronized ) {
        Py_BEGIN_ALLOW_THREADS
        vtype = CALLABLE(fhash)->Set( fhash, ktype, vtype, PKEY, &data );
        Py_END_ALLOW_THREADS
      }
      else {
        vtype = CALLABLE(fhash)->Set( fhash, ktype, vtype, PKEY, &data );
      }

      // Check for success
      if( vtype == CELL_VALUE_TYPE_ERROR ) {
        PyErr_SetString( PyExc_LookupError, "Insertion failed" );
        THROW_ERROR( CXLIB_ERR_API, 0xA26 );
      }
    }
    // Not a primitive value type, we require 128-bit key
    else if( ktype == CELL_KEY_TYPE_HASH128 ) {
      objectid_t obid = *((objectid_t*)PKEY);

      if( (wobj = COMLIB_OBJECT_NEW( PyFramehashObjectWrapper_t, &obid, &wargs) ) == NULL ) {
        PyErr_SetString( PyExc_MemoryError, "Out of memory" );
        THROW_ERROR( CXLIB_ERR_API, 0xA27 );
      }

      // special membership type for None
      if( wargs.py_obj == Py_None ) {
        vtype = CELL_VALUE_TYPE_MEMBER;
      }
      else {
        vtype = CELL_VALUE_TYPE_OBJECT128;
      }

      // Insert comlib object into framehash
      if( fhash->_flag.synchronized ) {
        Py_BEGIN_ALLOW_THREADS
        vtype = CALLABLE(fhash)->Set( fhash, ktype, vtype, PKEY, wobj );
        Py_END_ALLOW_THREADS
      }
      else {
        vtype = CALLABLE(fhash)->Set( fhash, ktype, vtype, PKEY, wobj );
      }

      // Check for success
      if( vtype == CELL_VALUE_TYPE_ERROR ) {
        PyErr_SetString( PyExc_LookupError, "Insertion failed" );
        THROW_ERROR( CXLIB_ERR_API, 0xA28 );
      }
    }
    // ???
    else {
      PyErr_SetString( PyExc_MemoryError, "Invalid key type" );
      THROW_ERROR( CXLIB_ERR_API, 0xA29 );
    }
    
    Py_INCREF( Py_None );
    py_ret = Py_None;
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
}



/******************************************************************************
 * _get
 *
 ******************************************************************************
 */
static PyObject * _get( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_ret = NULL;
  PyObject *py_key;
  PyObject *py_default = NULL;

  if( !PyArg_UnpackTuple( args, "_get", 1, 2, &py_key, &py_default ) ) {
    return NULL;
  }

  XTRY {
    framehash_t *fhash = self->fhash;
    QWORD retdata;
    framehash_valuetype_t vtype;
    framehash_keytype_t ktype;

    __m128i __KEYMEM;
    void *PKEY = &__KEYMEM;

    ktype = _key( self, py_key, PKEY );

    if( ktype == CELL_KEY_TYPE_ERROR ) {
      THROW_ERROR( CXLIB_ERR_API, 0xA31 );
    }

    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = CALLABLE(fhash)->Get( fhash, ktype, PKEY, &retdata );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = CALLABLE(fhash)->Get( fhash, ktype, PKEY, &retdata );
    }
    
    switch( vtype ) {
    case CELL_VALUE_TYPE_NULL:
      // not found, do we have a default?
      if( (py_ret = py_default) == NULL ) {
        // no default, raise KeyError
        PyObject *py_tup = PyTuple_Pack(1, py_key);
        if( py_tup ) {
          PyErr_SetObject( PyExc_KeyError, py_tup );
          Py_DECREF(py_tup);
        }
        XBREAK;
      }
      Py_INCREF( py_ret );
      break;
    case CELL_VALUE_TYPE_MEMBER:
      // key found
      py_ret = Py_None;
      Py_INCREF( py_ret );
      break;
    case CELL_VALUE_TYPE_BOOLEAN:
      // boolean found
      if( (py_ret = PyBool_FromLong( (int32_t)retdata )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA32 );
      }
      break;
    case CELL_VALUE_TYPE_UNSIGNED:
      // unsigned found
      if( (py_ret = PyLong_FromUnsignedLongLong( (uint64_t)retdata )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA33 );
      }
      break;
    case CELL_VALUE_TYPE_INTEGER:
      // integer found
      if( (py_ret = PyLong_FromLongLong( (int64_t)retdata )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA34 );
      }
      break;
    case CELL_VALUE_TYPE_REAL:
      // real found
      if( (py_ret = PyFloat_FromDouble( REINTERPRET_CAST_DOUBLE(retdata) )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA35 );
      }
      break;
    case CELL_VALUE_TYPE_OBJECT128:
      // object found
      py_ret = ((PyFramehashObjectWrapper_t*)retdata)->py_object;
      Py_INCREF( py_ret );
      break;
    case CELL_VALUE_TYPE_ERROR:
      // internal error
      PyErr_SetString( PyExc_LookupError, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA37 );
    default:
      PyErr_SetString( PyExc_LookupError, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA38 );
    }

  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * _delete
 *
 ******************************************************************************
 */
static PyObject * _delete( PyVGX_Framehash *self, PyObject *py_key ) {
  PyObject *py_ret = NULL;

  XTRY {
    framehash_t *fhash = self->fhash;
    framehash_vtable_t *ifhash = CALLABLE( fhash );
    framehash_valuetype_t vtype;
    framehash_keytype_t ktype;

    __m128i __KEYMEM;
    void *PKEY = &__KEYMEM;

    ktype = _key( self, py_key, PKEY );

    if( ktype == CELL_KEY_TYPE_ERROR ) {
      THROW_ERROR( CXLIB_ERR_API, 0xA41 );
    }

    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = ifhash->Delete( fhash, ktype, PKEY );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = ifhash->Delete( fhash, ktype, PKEY );
    }
    
    if( vtype == CELL_VALUE_TYPE_ERROR ) {
      PyErr_SetString( PyExc_LookupError, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA42 );
    }
    else if( vtype == CELL_VALUE_TYPE_NULL ) {
      // nothing was deleted
      PyObject *py_tup = PyTuple_Pack(1, py_key);
      if( py_tup ) {
        PyErr_SetObject( PyExc_KeyError, py_tup );
        Py_DECREF(py_tup);
      }
      py_ret = NULL;
    }
    else {
      Py_INCREF( Py_None );
      py_ret = Py_None;
    }
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * _contains
 *
 ******************************************************************************
 */
static int _contains( PyVGX_Framehash *self, PyObject *py_key ) {
  framehash_valuetype_t vtype;
  framehash_keytype_t ktype;

  __m128i __KEYMEM;
  void *PKEY = &__KEYMEM;

  ktype = _key( self, py_key, PKEY );

  if( ktype == CELL_KEY_TYPE_ERROR ) {
    return -1;
  }

  if( self->fhash->_flag.synchronized ) {
    Py_BEGIN_ALLOW_THREADS
    vtype = CALLABLE(self->fhash)->Has( self->fhash, ktype, PKEY );
    Py_END_ALLOW_THREADS
  }
  else {
    vtype = CALLABLE(self->fhash)->Has( self->fhash, ktype, PKEY );
  }
  
  switch( vtype ) {
  case CELL_VALUE_TYPE_NULL:
    // key not found
    return 0;
  case CELL_VALUE_TYPE_ERROR:
    // internal error
    PyErr_SetString( PyExc_LookupError, "Internal error" );
    return -1;
  case CELL_VALUE_TYPE_NOACCESS:
    // access error
    PyErr_SetString( PyExc_LookupError, "No access" );
    return -1;
  default:
    // key found
    return 1;
  }
}



/******************************************************************************
 * _inc
 *
 ******************************************************************************
 */
static PyObject * _inc( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_key;
  PyObject *py_ret = NULL;
  PyObject *py_amount = NULL;

  if( !PyArg_UnpackTuple( args, "_inc", 1, 2, &py_key, &py_amount ) ) {
    return NULL;
  }

  XTRY {
    framehash_t *fhash = self->fhash;
    framehash_vtable_t *iFH = CALLABLE( fhash );
    framehash_valuetype_t vtype = CELL_VALUE_TYPE_NULL;
    framehash_keytype_t ktype = CELL_KEY_TYPE_NONE;
    union {
      int64_t integer;
      double real;
    } inc_data;
    union {
      int64_t integer;
      double real;
    } ret_data;

    __m128i __KEYMEM;
    void *PKEY = &__KEYMEM;
    QWORD *key64 = (QWORD*)PKEY;

    // Check inc value
    if( py_amount == NULL ) {
      inc_data.integer = 1;
      vtype = CELL_VALUE_TYPE_INTEGER;
    }
    else if( PyLong_CheckExact(py_amount) ) {
      inc_data.integer = PyLong_AsLongLong( py_amount );
      if( inc_data.integer == -1 && PyErr_Occurred() )  {
        THROW_ERROR( CXLIB_ERR_API, 0xA51 );
      }
      vtype = CELL_VALUE_TYPE_INTEGER;
    }
    else if( PyFloat_CheckExact( py_amount ) ) {
      inc_data.real = PyFloat_AS_DOUBLE( py_amount );
      vtype = CELL_VALUE_TYPE_REAL;
    }
    else {
      PyErr_SetString( PyExc_LookupError, "Value is incompatible with increment operation" );
      THROW_ERROR( CXLIB_ERR_API, 0xA52 );
    }

    // Get key
    if( (ktype = _key( self, py_key, PKEY )) == CELL_KEY_TYPE_HASH128 ) {
      PyErr_SetString( PyExc_LookupError, "Key is incompatible with increment operation (shortkeys must be enabled to use string keys)" );
      THROW_ERROR( CXLIB_ERR_API, 0xA53 );
    }
    else if( ktype == CELL_KEY_TYPE_ERROR || ktype == CELL_KEY_TYPE_NONE ) { 
      THROW_ERROR( CXLIB_ERR_API, 0xA54 );
    }

    // Proceed with inserting the primitive type
    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      if( vtype == CELL_VALUE_TYPE_INTEGER ) {
        vtype = iFH->IncInt56( fhash, ktype, *key64, inc_data.integer, &ret_data.integer );
      }
      else {
        vtype = iFH->IncReal56( fhash, ktype, *key64, inc_data.real, &ret_data.real );
      }
      Py_END_ALLOW_THREADS
    }
    else {
      if( vtype == CELL_VALUE_TYPE_INTEGER ) {
        vtype = iFH->IncInt56( fhash, ktype, *key64, inc_data.integer, &ret_data.integer );
      }
      else {
        vtype = iFH->IncReal56( fhash, ktype, *key64, inc_data.real, &ret_data.real );
      }
    }

    // Interpret return data
    switch( vtype ) {
    case CELL_VALUE_TYPE_UNSIGNED:
      // unsigned found
      if( (py_ret = PyLong_FromUnsignedLongLong( (uint64_t)ret_data.integer )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA55 );
      }
      break;
    case CELL_VALUE_TYPE_INTEGER:
      // integer found
      if( (py_ret = PyLong_FromLongLong( (int64_t)ret_data.integer )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA56 );
      }
      break;
    case CELL_VALUE_TYPE_REAL:
      // real found
      if( (py_ret = PyFloat_FromDouble( ret_data.real )) == NULL ) {
        PyErr_SetString( PyExc_LookupError, "Internal error" );
        THROW_ERROR( CXLIB_ERR_API, 0xA57 );
      }
      break;
    case CELL_VALUE_TYPE_ERROR:
      PyErr_SetString( PyExc_LookupError, "Mapped value not increment-compatible" );
      THROW_ERROR( CXLIB_ERR_API, 0xA58 );
    default:
      PyErr_SetString( PyExc_LookupError, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA59 );
    }
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
}



/******************************************************************************
 * _set_int
 *
 ******************************************************************************
 */
static PyObject * _set_int( framehash_t *fhash, PyObject *args ) {
  PyObject *py_ret = NULL;
  PyObject *py_key;
  PyObject *py_value;

  if( !PyArg_UnpackTuple( args, "_set_int", 2, 2, &py_key, &py_value ) ) {
    return NULL;
  }

  XTRY {
    uint64_t key;
    int64_t value;
    framehash_valuetype_t vtype;

    // Convert Python key to C
    if( PyLong_CheckExact(py_key) ) {
      if( (key = PyLong_AsUnsignedLongLong( py_key )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA61 );
      }
    }
    else {
      PyErr_SetString( PyExc_KeyError, "Integer key expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA62 );
    }

    // Convert Python value to C
    if( PyLong_CheckExact(py_value) ) {
      if( (value = PyLong_AsLongLong( py_value )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA63 );
      }
    }
    else {
      PyErr_SetString( PyExc_ValueError, "Integer value expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA64 );
    }
        
    // Perform framehash insertion
    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = CALLABLE(fhash)->SetInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, value );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = CALLABLE(fhash)->SetInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, value );
    }

    // Check for abnormal return codes
    if( vtype == CELL_VALUE_TYPE_INTEGER ) {
      py_ret = Py_None; // <= Success!
      Py_INCREF( Py_None );
    }
    else {
      PyErr_Format( PyExc_Exception, "Internal error: vtype=%d", vtype );
      THROW_ERROR( CXLIB_ERR_API, 0xA65 );
    }
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * _get_int
 *
 ******************************************************************************
 */
static PyObject * _get_int( framehash_t *fhash, PyObject *py_key ) {
  PyObject *py_ret = NULL;

  XTRY {
    uint64_t key;
    int64_t retval;
    framehash_valuetype_t vtype;
    PyObject *py_tup;

    // Get the C integer of the Python integer key
    if( PyLong_CheckExact(py_key) ) {
      if( (key = PyLong_AsUnsignedLongLong( py_key )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA71 );
      }
    }
    else {
      PyErr_SetString( PyExc_KeyError, "Integer key expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA72 );
    }

    // Perform framehash lookup
    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = CALLABLE(fhash)->GetInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, &retval );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = CALLABLE(fhash)->GetInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, &retval );
    }

    // Check return code and create return Python object
    switch( vtype ) {
    case CELL_VALUE_TYPE_NULL:
      // raise KeyError
      if( (py_tup = PyTuple_Pack(1, py_key)) != NULL ) {
        PyErr_SetObject( PyExc_KeyError, py_tup );
        Py_DECREF(py_tup);
      }
      break;
    case CELL_VALUE_TYPE_INTEGER:
      if( retval > INT_MAX || retval < INT_MIN ) {
        py_ret = PyLong_FromLongLong( retval );
      }
      else {
        py_ret = PyLong_FromLong( (int32_t)retval );
      }
      break;
    case CELL_VALUE_TYPE_ERROR:
      PyErr_SetString( PyExc_LookupError, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA73 );
      break;
    default:
      PyErr_Format( PyExc_LookupError, "Value type %d found (%lld), unable to convert. Use generic get() method instead.", vtype, retval );
      THROW_ERROR( CXLIB_ERR_API, 0xA74 );
      break;
    }
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * _del_int
 *
 ******************************************************************************
 */
static PyObject * _del_int( framehash_t *fhash, PyObject *py_key ) {
  PyObject *py_ret = NULL;

  XTRY {
    uint64_t key;
    framehash_valuetype_t vtype;
    PyObject *py_tup;

    // Convert Python key to C
    if( PyLong_CheckExact(py_key) ) {
      if( (key = PyLong_AsUnsignedLongLong( py_key )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA81 );
      }
    }
    else {
      PyErr_SetString( PyExc_KeyError, "Integer key expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA82 );
    }

    // Perform framehash deletion
    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = CALLABLE(fhash)->DelKey64( fhash, CELL_KEY_TYPE_PLAIN64, key );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = CALLABLE(fhash)->DelKey64( fhash, CELL_KEY_TYPE_PLAIN64, key );
    }

    // Check for abnormal return codes
    switch( vtype ) {
    case CELL_VALUE_TYPE_NULL:
      // key not found
      if( (py_tup = PyTuple_Pack(1, py_key)) != NULL ) {
        PyErr_SetObject( PyExc_KeyError, py_tup );
        Py_DECREF(py_tup);
      }
      break;
    case CELL_VALUE_TYPE_ERROR:
      PyErr_Format( PyExc_Exception, "Internal error" );
      THROW_ERROR( CXLIB_ERR_API, 0xA83 );
      break;
    default:
      py_ret = Py_None; // <= Success!
      Py_INCREF( Py_None );
      break;
    }

  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * _contains_int
 *
 ******************************************************************************
 */
static PyObject * _contains_int( framehash_t *fhash, PyObject *py_key ) {
  framehash_valuetype_t vtype;
  uint64_t key;

  // Convert Python key to C
  if( PyLong_CheckExact(py_key) ) {
    if( (key = PyLong_AsUnsignedLongLong( py_key )) == (uint64_t)(-1) && PyErr_Occurred() ) {
      return NULL;
    }
  }
  else {
    PyErr_SetString( PyExc_LookupError, "Integer key expected" );
    return NULL;
  }

  // Perform framehash call
  if( fhash->_flag.synchronized ) {
    Py_BEGIN_ALLOW_THREADS
    vtype = CALLABLE(fhash)->HasKey64( fhash, CELL_KEY_TYPE_PLAIN64, key );
    Py_END_ALLOW_THREADS
  }
  else {
    vtype = CALLABLE(fhash)->HasKey64( fhash, CELL_KEY_TYPE_PLAIN64, key );
  }
  
  switch( vtype ) {
  case CELL_VALUE_TYPE_NULL:
    // key not found
    Py_RETURN_FALSE;
  case CELL_VALUE_TYPE_ERROR:
    // internal error
    PyErr_SetString( PyExc_LookupError, "Internal error" );
    return NULL;
  case CELL_VALUE_TYPE_NOACCESS:
    // access error
    PyErr_SetString( PyExc_LookupError, "No access" );
    return NULL;
  default:
    // key found
    Py_RETURN_TRUE;
  }
}



/******************************************************************************
 * _inc_int
 *
 ******************************************************************************
 */
static PyObject * _inc_int( framehash_t *fhash, PyObject *args ) {
  PyObject *py_ret = NULL;
  PyObject *py_key;
  PyObject *py_amount = NULL;

  if( !PyArg_UnpackTuple( args, "_inc_int", 1, 2, &py_key, &py_amount ) ) return NULL;

  XTRY {
    uint64_t key;
    int64_t amount;
    int64_t new_value;
    framehash_valuetype_t vtype;

    // Convert Python key to C
    if( PyLong_CheckExact(py_key) ) {
      if( (key = PyLong_AsUnsignedLongLong( py_key )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA91 );
      }
    }
    else {
      PyErr_SetString( PyExc_KeyError, "Integer key expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA92 );
    }

    // Convert Python value to C
    if( py_amount == NULL ) {
      amount = 1; // inc +1 by default
    }
    else if( PyLong_CheckExact(py_amount) ) {
      if( (amount = PyLong_AsLongLong( py_amount )) == (uint64_t)(-1) && PyErr_Occurred() ) {
        THROW_ERROR( CXLIB_ERR_API, 0xA93 );
      }
    }
    else {
      PyErr_SetString( PyExc_ValueError, "Integer value expected" );
      THROW_ERROR( CXLIB_ERR_API, 0xA94 );
    }
        
    // Perform framehash increment
    if( fhash->_flag.synchronized ) {
      Py_BEGIN_ALLOW_THREADS
      vtype = CALLABLE(fhash)->IncInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, amount, &new_value );
      Py_END_ALLOW_THREADS
    }
    else {
      vtype = CALLABLE(fhash)->IncInt56( fhash, CELL_KEY_TYPE_PLAIN64, key, amount, &new_value );
    }

    // Check for abnormal return codes
    if( vtype == CELL_VALUE_TYPE_INTEGER ) {
      if( new_value > INT_MAX || new_value < INT_MIN ) {
        py_ret = PyLong_FromLongLong( new_value );
      }
      else {
        py_ret = PyLong_FromLong( (int32_t)new_value );
      }
    }
    else {
      PyErr_Format( PyExc_Exception, "Internal error: vtype=%d", vtype );
      THROW_ERROR( CXLIB_ERR_API, 0xA95 );
    }
  }
  XCATCH( errcode ) {
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {}

  return py_ret;
   
}



/******************************************************************************
 * Framehash_set
 *
 ******************************************************************************
 */
static PyObject * Framehash_set( PyVGX_Framehash *self, PyObject *args ) {
  return _set( self, args );
}



/******************************************************************************
 * Framehash_get
 *
 ******************************************************************************
 */
static PyObject * Framehash_get( PyVGX_Framehash *self, PyObject *args ) {
  if( PyTuple_GET_SIZE( args ) == 1 ) {
    PyObject *args2 = PyTuple_New(2);
    PyObject *py_ret;
    if( args2 == NULL ) return NULL;
    PyTuple_SET_ITEM( args2, 0, PyTuple_GET_ITEM( args, 0 ) );
    PyTuple_SET_ITEM( args2, 1, Py_None );
    Py_INCREF( PyTuple_GET_ITEM( args, 0 ) );
    Py_INCREF( Py_None );
    py_ret = _get( self, args2 );
    Py_DECREF( args2 );
    return py_ret;
  }
  else {
    return _get( self, args );
  }
}



/******************************************************************************
 * Framehash_delete
 *
 ******************************************************************************
 */
static PyObject * Framehash_delete( PyVGX_Framehash *self, PyObject *py_key ) {
  return _delete( self, py_key );
}



/******************************************************************************
 * Framehash_contains
 *
 ******************************************************************************
 */
static PyObject * Framehash_contains( PyVGX_Framehash *self, PyObject *py_key ) {
  switch( _contains( self, py_key ) ) {
  case 0:
    Py_RETURN_FALSE;
  case 1:
    Py_RETURN_TRUE;
  default:
    return NULL;
  }
}



/******************************************************************************
 * Framehash_inc
 *
 ******************************************************************************
 */
static PyObject * Framehash_inc( PyVGX_Framehash *self, PyObject *args ) {
  return _inc( self, args );
}



/******************************************************************************
 * Framehash_set_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_set_int( PyVGX_Framehash *self, PyObject *args ) {
  return _set_int( self->fhash, args );
}



/******************************************************************************
 * Framehash_get_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_get_int( PyVGX_Framehash *self, PyObject *py_key ) {
  return _get_int( self->fhash, py_key );
}



/******************************************************************************
 * Framehash_del_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_del_int( PyVGX_Framehash *self, PyObject *py_key ) {
  return _del_int( self->fhash, py_key );
}



/******************************************************************************
 * Framehash_contains_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_contains_int( PyVGX_Framehash *self, PyObject *py_key ) {
  return _contains_int( self->fhash, py_key );
}



/******************************************************************************
 * Framehash_inc_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_inc_int( PyVGX_Framehash *self, PyObject *args ) {
  return _inc_int( self->fhash, args );
}



/******************************************************************************
 * Framehash_items
 *
 ******************************************************************************
 */
static PyObject * Framehash_items( PyVGX_Framehash *self ) {
  PyObject *py_ret = NULL;
  PyObject *py_tuple = NULL;
  PyObject *py_key = NULL;
  PyObject *py_value = NULL;

  CaptrList_t *output = self->fhash->_dynamic.cell_list;
  f_CaptrList_get get_cell = CALLABLE(output)->Get;

  XTRY {
    int64_t n_items;
    framehash_cell_t cell;

    // Assert that internal cell list is empty before we start collecting
    if( CALLABLE( output )->Length( output ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAA1 );
    }

    // Run item collection
    if( (n_items = CALLABLE(self->fhash)->GetItems(self->fhash)) < 0 ) {
      PyErr_SetString( PyExc_MemoryError, "Error during item collection" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAA2 );
    }

    // Create Python list
    py_ret = PyList_New( n_items );

    // Populate Python list with collected items
    for( int64_t n=0; n < n_items; n++ ) {
      if( get_cell( output, n, &cell ) == 1 ) {
        py_tuple = PyTuple_New(2);
        py_key = PyLong_FromUnsignedLongLong( APTR_AS_ANNOTATION(&cell) );
        py_value = PyLong_FromLongLong( APTR_AS_INTEGER(&cell) );

        if( py_tuple && py_key && py_value ) {
          PyTuple_SET_ITEM( py_tuple, 0, py_key );    // steal py_key
          PyTuple_SET_ITEM( py_tuple, 1, py_value );  // steal py_value
          PyList_SET_ITEM( py_ret, n, py_tuple );     // steal py_tuple
          py_tuple = py_key = py_value = NULL;
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xAA3 );
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xAA4 );
      }
    }
  }
  XCATCH( errcode ) {
    // TODO: cleanup
    Py_XDECREF( py_tuple );
    Py_XDECREF( py_key );
    Py_XDECREF( py_value );
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {
    CALLABLE( output )->Clear( output );
  }

  return py_ret;

}



/******************************************************************************
 * Framehash_keys
 *
 ******************************************************************************
 */
static PyObject * Framehash_keys( PyVGX_Framehash *self ) {
  PyObject *py_ret = NULL;
  PyObject *py_key = NULL;

  CQwordList_t *output = self->fhash->_dynamic.annotation_list;
  f_CQwordList_get get_key = CALLABLE( output )->Get;

  XTRY {
    int64_t n_keys;
    QWORD cell_key;

    // Assert that internal annotation list is empty before we start collecting
    if( CALLABLE( output )->Length( output ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAB1 );
    }

    // Run key collection
    if( (n_keys = CALLABLE(self->fhash)->GetKeys(self->fhash)) < 0 ) {
      PyErr_SetString( PyExc_MemoryError, "Error during key collection" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAB2 );
    }

    // Create Python list
    py_ret = PyList_New( n_keys );

    // Populate Python list with collected keys
    for( int64_t n=0; n < n_keys; n++ ) {
      if( get_key( output, n, &cell_key ) == 1 ) {
        py_key = PyLong_FromUnsignedLongLong( cell_key );
        if( py_key ) {
          PyList_SET_ITEM( py_ret, n, py_key );     // steal py_key
          py_key = NULL;
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xAB3 );
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xAB4 );
      }
    }
  }
  XCATCH( errcode ) {
    // TODO: cleanup
    Py_XDECREF( py_key );
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {
    CALLABLE( output )->Clear( output );
  }

  return py_ret;

}



/******************************************************************************
 * Framehash_values
 *
 ******************************************************************************
 */
static PyObject * Framehash_values( PyVGX_Framehash *self ) {
  PyObject *py_ret = NULL;
  PyObject *py_value = NULL;

  CtptrList_t *output = self->fhash->_dynamic.ref_list;
  f_CtptrList_get get_value = CALLABLE( output )->Get;
  
  XTRY {

    int64_t n_values;
    tptr_t value;

    // Assert that internal tptr list is empty before we start collecting
    if( CALLABLE( output )->Length( output ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAC1 );
    }
    
    // Run value collection
    if( (n_values = CALLABLE(self->fhash)->GetValues(self->fhash)) < 0 ) {
      PyErr_SetString( PyExc_MemoryError, "Error during value collection" );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xAC2 );
    }

    // Create Python list
    py_ret = PyList_New( n_values );

    // Populate Python list with collected keys
    for( int64_t n=0; n < n_values; n++ ) {
      if( get_value( output, n, &value ) == 1 ) {
        if( TPTR_IS_POINTER( &value ) ) {
          py_value = ((PyFramehashObjectWrapper_t*)TPTR_GET_POINTER( &value ))->py_object;
          Py_INCREF( py_value );
        }
        else {
          switch( TPTR_AS_DTYPE( &value ) ) {
          case TAGGED_DTYPE_ID56:
            py_value = PyLong_FromUnsignedLongLong( TPTR_GET_IDHIGH( &value ) );
            break;
          case TAGGED_DTYPE_BOOL:
            py_value = TPTR_AS_INTEGER( &value ) == 0 ? Py_False : Py_True;
            Py_INCREF( py_value );
            break;
          case TAGGED_DTYPE_UINT56:
            py_value = PyLong_FromUnsignedLongLong( TPTR_AS_UNSIGNED( &value ) );
            break;
          case TAGGED_DTYPE_INT56:
            py_value = PyLong_FromLongLong( TPTR_AS_INTEGER( &value ) );
            break;
          case TAGGED_DTYPE_REAL56:
            py_value = PyFloat_FromDouble( TPTR_GET_REAL( &value ) );
            break;
          default:
            py_value = NULL;
            /* invalid data */
            break;
          }
        }

        if( py_value ) {
          PyList_SET_ITEM( py_ret, n, py_value );     // steal py_value
          py_value = NULL;
        }
        else {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xAC3 );
        }
      }
      else {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xAC4 );
      }
    }
  }
  XCATCH( errcode ) {
    // TODO: cleanup
    Py_XDECREF( py_value );
    Py_XDECREF( py_ret );
    py_ret = NULL;
  }
  XFINALLY {
    CALLABLE( output )->Clear( output );
  }

  return py_ret;

}



/******************************************************************************
 * Framehash_clear
 *
 ******************************************************************************
 */
static PyObject * Framehash_clear( PyVGX_Framehash *self ) {
  PyObject *py_ret = NULL;
  if( CALL( self->fhash, Discard ) < 0 ) {
    PyErr_SetString( PyExc_LookupError, "Internal error" );
    py_ret = NULL;
  }
  else {
    py_ret = Py_None;
    Py_INCREF( Py_None );
  }
  return py_ret;
}



/******************************************************************************
 * Framehash_flush_caches
 *
 ******************************************************************************
 */
static PyObject * Framehash_flush_caches( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_ret = NULL;

  int invalidate = 0;
  if( !PyArg_ParseTuple( args, "|i", &invalidate ) ) {
    return NULL;
  }

  if( CALLABLE( self->fhash )->Flush( self->fhash, invalidate ) < 0 ) {
    PyErr_SetString( PyExc_LookupError, "Internal error" );
    py_ret = NULL;
  }
  else {
    py_ret = Py_None;
    Py_INCREF( Py_None );
  }
  return py_ret;
}



/******************************************************************************
 * Framehash_set_readonly
 *
 ******************************************************************************
 */
static PyObject * Framehash_set_readonly( PyVGX_Framehash *self ) {
  if( CALLABLE( self->fhash )->SetReadonly( self->fhash ) < 0 ) {
    PyErr_SetString( PyExc_Exception, "Unable to enter readonly mode" );
    return NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_is_readonly
 *
 ******************************************************************************
 */
static PyObject * Framehash_is_readonly( PyVGX_Framehash *self ) {
  if( CALLABLE( self->fhash )->IsReadonly( self->fhash ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * Framehash_clear_readonly
 *
 ******************************************************************************
 */
static PyObject * Framehash_clear_readonly( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->ClearReadonly( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_enable_read_caches
 *
 ******************************************************************************
 */
static PyObject * Framehash_enable_read_caches( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->EnableReadCaches( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_disable_read_caches
 *
 ******************************************************************************
 */
static PyObject * Framehash_disable_read_caches( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->DisableReadCaches( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_enable_write_caches
 *
 ******************************************************************************
 */
static PyObject * Framehash_enable_write_caches( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->EnableWriteCaches( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_disable_write_caches
 *
 ******************************************************************************
 */
static PyObject * Framehash_disable_write_caches( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->DisableWriteCaches( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_save
 *
 ******************************************************************************
 */
static PyObject * Framehash_save( PyVGX_Framehash *self ) {
  int64_t n = CALLABLE( self->fhash )->BulkSerialize( self->fhash, true );

  if( n < 0 ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_LookupError, "Internal error" );
    }
    return NULL;
  }
  else if( n == 0 ) {
    if( _FRAMEHASH_HAS_FILE( self->fhash ) ) {
      PyErr_SetString( PyExc_LookupError, "Serialization failed (check disk space?)" );
    }
    else {
      PyErr_SetString( PyExc_LookupError, "Serialization not supported - no path or name" );
    }
    return NULL;
  }
  else {
    return PyLong_FromLongLong( n );
  }
}



/******************************************************************************
 * Framehash_hitrate
 *
 ******************************************************************************
 */
static PyObject * Framehash_hitrate( PyVGX_Framehash *self ) {
  PyObject *py_result = PyTuple_New( 5 );
  if( py_result ) {
    framehash_cache_hitrate_t hitrate = CALLABLE( self->fhash )->Hitrate( self->fhash );
    for( int H=0; H<5; H++ ) {
      PyObject *py_entry = PyTuple_New( 2 );
      if( py_entry ) {
        PyTuple_SET_ITEM( py_entry, 0, PyFloat_FromDouble( hitrate.rate[ H ] ) );
        PyTuple_SET_ITEM( py_entry, 1, PyLong_FromLongLong( hitrate.count[ H ] ) );
        PyTuple_SET_ITEM( py_result, H, py_entry );
      }
    }
  }

  return py_result;
}



/******************************************************************************
 * Framehash_lfsrX
 *
 ******************************************************************************
 */
#define __pyframehash_lfsrX8_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER \
static PyObject * pyframehash_lfsr##X ( PyVGX_Framehash *self, PyObject *args ) {                                     \
  PyObject *py_seed = NULL;                                                                                           \
  if( !PyArg_UnpackTuple( args, "pyframehash_lfsr"#X, 0, 1, &py_seed ) ) { return NULL; }                             \
  uint8_t val;                                                                                                        \
  uint8_t seed = py_seed ? (uint8_t)PyLong_AsLong( py_seed ) : 0;                                                     \
  Py_BEGIN_ALLOW_THREADS                                                                                              \
  val = __lfsr##X ( seed );                                                                                           \
  Py_END_ALLOW_THREADS                                                                                                \
  return PyLong_FromLong( val );                                                                                      \
}

#define __pyframehash_lfsrX16_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER                                                                        \
static PyObject * pyframehash_lfsr##X ( PyVGX_Framehash *self, PyObject *args ) {                                     \
  PyObject *py_seed = NULL;                                                                                           \
  if( !PyArg_UnpackTuple( args, "pyframehash_lfsr"#X, 0, 1, &py_seed ) ) { return NULL; }                             \
  uint16_t val;                                                                                                       \
  uint16_t seed = py_seed ? (uint16_t)PyLong_AsLong( py_seed ) : 0;                                                   \
  Py_BEGIN_ALLOW_THREADS                                                                                              \
  val = __lfsr##X ( seed );                                                                                           \
  Py_END_ALLOW_THREADS                                                                                                \
  return PyLong_FromLong( val );                                                                                      \
}

#define __pyframehash_lfsrX32_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER                                                                        \
static PyObject * pyframehash_lfsr##X ( PyVGX_Framehash *self, PyObject *args ) {                                     \
  PyObject *py_seed = NULL;                                                                                           \
  if( !PyArg_UnpackTuple( args, "pyframehash_lfsr"#X, 0, 1, &py_seed ) ) { return NULL; }                             \
  uint32_t val;                                                                                                       \
  uint32_t seed = py_seed ? (uint32_t)PyLong_AsLong( py_seed ) : 0;                                                   \
  Py_BEGIN_ALLOW_THREADS                                                                                              \
  val = __lfsr##X ( seed );                                                                                           \
  Py_END_ALLOW_THREADS                                                                                                \
  return PyLong_FromLong( val );                                                                                      \
}

#define __pyframehash_lfsrX64_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER                                                                        \
static PyObject * pyframehash_lfsr##X ( PyVGX_Framehash *self, PyObject *args ) {                                     \
  PyObject *py_seed = NULL;                                                                                           \
  if( !PyArg_UnpackTuple( args, "pyframehash_lfsr"#X, 0, 1, &py_seed ) ) { return NULL; }                             \
  uint64_t val;                                                                                                       \
  uint64_t seed = py_seed ? ( (uint64_t)PyLong_AsUnsignedLongLong( py_seed ) ) : 0;                                   \
  Py_BEGIN_ALLOW_THREADS                                                                                              \
  val =__lfsr##X ( seed );                                                                                            \
  Py_END_ALLOW_THREADS                                                                                                \
  return PyLong_FromUnsignedLongLong( val );                                                                          \
}


__pyframehash_lfsrX8_func( 3 )
__pyframehash_lfsrX8_func( 4 )
__pyframehash_lfsrX8_func( 5 )
__pyframehash_lfsrX8_func( 6 )
__pyframehash_lfsrX8_func( 7 )
__pyframehash_lfsrX8_func( 8 )
__pyframehash_lfsrX16_func( 9 )
__pyframehash_lfsrX16_func( 10 )
__pyframehash_lfsrX16_func( 11 )
__pyframehash_lfsrX16_func( 12 )
__pyframehash_lfsrX16_func( 13 )
__pyframehash_lfsrX16_func( 14 )
__pyframehash_lfsrX16_func( 15 )
__pyframehash_lfsrX16_func( 16 )
__pyframehash_lfsrX32_func( 17 )
__pyframehash_lfsrX32_func( 18 )
__pyframehash_lfsrX32_func( 19 )
__pyframehash_lfsrX32_func( 20 )
__pyframehash_lfsrX32_func( 21 )
__pyframehash_lfsrX32_func( 22 )
__pyframehash_lfsrX32_func( 23 )
__pyframehash_lfsrX32_func( 24 )
__pyframehash_lfsrX32_func( 25 )
__pyframehash_lfsrX32_func( 26 )
__pyframehash_lfsrX32_func( 27 )
__pyframehash_lfsrX32_func( 28 )
__pyframehash_lfsrX32_func( 29 )
__pyframehash_lfsrX32_func( 30 )
__pyframehash_lfsrX32_func( 31 )
IGNORE_WARNING_CONVERSION_POSSIBLE_LOSS_OF_DATA
__pyframehash_lfsrX64_func( 32 )
RESUME_WARNINGS
__pyframehash_lfsrX64_func( 33 )
__pyframehash_lfsrX64_func( 34 )
__pyframehash_lfsrX64_func( 35 )
__pyframehash_lfsrX64_func( 36 )
__pyframehash_lfsrX64_func( 37 )
__pyframehash_lfsrX64_func( 38 )
__pyframehash_lfsrX64_func( 39 )
__pyframehash_lfsrX64_func( 40 )
__pyframehash_lfsrX64_func( 41 )
__pyframehash_lfsrX64_func( 42 )
__pyframehash_lfsrX64_func( 43 )
__pyframehash_lfsrX64_func( 44 )
__pyframehash_lfsrX64_func( 45 )
__pyframehash_lfsrX64_func( 46 )
__pyframehash_lfsrX64_func( 47 )
__pyframehash_lfsrX64_func( 48 )
__pyframehash_lfsrX64_func( 49 )
__pyframehash_lfsrX64_func( 50 )
__pyframehash_lfsrX64_func( 51 )
__pyframehash_lfsrX64_func( 52 )
__pyframehash_lfsrX64_func( 53 )
__pyframehash_lfsrX64_func( 54 )
__pyframehash_lfsrX64_func( 55 )
__pyframehash_lfsrX64_func( 56 )
__pyframehash_lfsrX64_func( 57 )
__pyframehash_lfsrX64_func( 58 )
__pyframehash_lfsrX64_func( 59 )
__pyframehash_lfsrX64_func( 60 )
__pyframehash_lfsrX64_func( 61 )
__pyframehash_lfsrX64_func( 62 )
__pyframehash_lfsrX64_func( 63 )
__pyframehash_lfsrX64_func( 64 )



/******************************************************************************
 * Framehash_randX
 *
 ******************************************************************************
 */
#define __pyframehash_randXInt_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER \
static PyObject * pyframehash_rand##X ( PyVGX_Framehash *self ) { \
  return PyLong_FromLong( rand##X () ); \
}

#define __pyframehash_randXLong_func( X ) \
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER \
static PyObject * pyframehash_rand##X ( PyVGX_Framehash *self ) { \
  return PyLong_FromUnsignedLongLong( rand##X () ); \
}

__pyframehash_randXInt_func( 9 )
__pyframehash_randXInt_func( 10 )
__pyframehash_randXInt_func( 11 )
__pyframehash_randXInt_func( 12 )
__pyframehash_randXInt_func( 13 )
__pyframehash_randXInt_func( 14 )
__pyframehash_randXInt_func( 15 )
__pyframehash_randXInt_func( 16 )
__pyframehash_randXInt_func( 17 )
__pyframehash_randXInt_func( 18 )
__pyframehash_randXInt_func( 19 )
__pyframehash_randXInt_func( 20 )
__pyframehash_randXInt_func( 21 )
__pyframehash_randXInt_func( 22 )
__pyframehash_randXInt_func( 23 )
__pyframehash_randXInt_func( 24 )
__pyframehash_randXInt_func( 25 )
__pyframehash_randXInt_func( 26 )
__pyframehash_randXInt_func( 27 )
__pyframehash_randXInt_func( 28 )
__pyframehash_randXInt_func( 29 )
__pyframehash_randXInt_func( 30 )
__pyframehash_randXInt_func( 31 )
__pyframehash_randXLong_func( 32 )
__pyframehash_randXLong_func( 33 )
__pyframehash_randXLong_func( 34 )
__pyframehash_randXLong_func( 35 )
__pyframehash_randXLong_func( 36 )
__pyframehash_randXLong_func( 37 )
__pyframehash_randXLong_func( 38 )
__pyframehash_randXLong_func( 39 )
__pyframehash_randXLong_func( 40 )
__pyframehash_randXLong_func( 41 )
__pyframehash_randXLong_func( 42 )
__pyframehash_randXLong_func( 43 )
__pyframehash_randXLong_func( 44 )
__pyframehash_randXLong_func( 45 )
__pyframehash_randXLong_func( 46 )
__pyframehash_randXLong_func( 47 )
__pyframehash_randXLong_func( 48 )
__pyframehash_randXLong_func( 49 )
__pyframehash_randXLong_func( 50 )
__pyframehash_randXLong_func( 51 )
__pyframehash_randXLong_func( 52 )
__pyframehash_randXLong_func( 53 )
__pyframehash_randXLong_func( 54 )
__pyframehash_randXLong_func( 55 )
__pyframehash_randXLong_func( 56 )
__pyframehash_randXLong_func( 57 )
__pyframehash_randXLong_func( 58 )
__pyframehash_randXLong_func( 59 )
__pyframehash_randXLong_func( 60 )
__pyframehash_randXLong_func( 61 )
__pyframehash_randXLong_func( 62 )
__pyframehash_randXLong_func( 63 )
__pyframehash_randXLong_func( 64 )



/******************************************************************************
 * pyframehash_hash64
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * pyframehash_hash64( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_key;
  uint64_t h64 = 0;

  if( !PyArg_UnpackTuple( args, "pyframehash_hash64", 1, 1, &py_key ) ) {
    return NULL;
  }

  if( PyLong_CheckExact( py_key ) ) {
    uint64_t x = PyLong_AsUnsignedLongLong( py_key );
    if( x != (uint64_t)-1 || !PyErr_Occurred() ) {
      h64 = ihash64( x );
    }
  }

  if( h64 == 0 ) {
    Py_ssize_t len;
    const char *data;
    if( PyVGX_Framehash_PyObject_CheckString( py_key ) ) {
      if( (data = PyVGX_Framehash_PyObject_AsStringAndSize( py_key, &len )) == NULL ) {
        return NULL;
      }
    }
    else {
      PyErr_SetString( PyExc_ValueError, "key must be string or 64-bit integer" );
      return NULL;
    }

    h64 = (uint64_t)hash64( (unsigned char*)data, len );
  }

  if( h64 != 0 ) {
    return PyLong_FromUnsignedLongLong( h64 );
  }
  else {
    return NULL;
  }

}



/******************************************************************************
 * pyframehash_hash32
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * pyframehash_hash32( PyVGX_Framehash *self, PyObject *args ) {
  PyObject *py_key;
  uint32_t h32 = 0;

  if( !PyArg_UnpackTuple( args, "pyframehash_hash32", 1, 1, &py_key ) ) {
    return NULL;
  }

  if( PyLong_CheckExact( py_key ) ) {
    int ovf;
    int32_t key = PyLong_AsLongAndOverflow( py_key, &ovf );
    if( ovf != 0 ) {
      return NULL;
    }
    h32 = framehash_hashing__tiny_hashkey( key );
  }

  if( h32 == 0 ) {
    Py_ssize_t len;
    const char *data;
    if( PyVGX_Framehash_PyObject_CheckString( py_key ) ) {
      if( (data = PyVGX_Framehash_PyObject_AsStringAndSize( py_key, &len )) == NULL ) {
        return NULL;
      }
      if( len > CXLIB_LONG_MAX ) {
        PyErr_SetString( PyExc_ValueError, "string is too long" );
        return NULL;
      }
    }
    else {
      PyErr_SetString( PyExc_ValueError, "key must be string or 32-bit integer" );
      return NULL;
    }

    h32 = (uint32_t)hash32( (unsigned char*)data, (int32_t)len );
  }

  if( h32 != 0 ) {
    return PyLong_FromUnsignedLong( h32 );
  }
  else {
    return NULL;
  }

}



/******************************************************************************
 * Framehash_count_active
 *
 ******************************************************************************
 */
static PyObject * Framehash_count_active( PyVGX_Framehash *self ) {
  int64_t n = CALL( self->fhash, CountActive );
  if( n < 0 ) {
    PyErr_SetString( PyExc_Exception, "Internal processing error" );
    return NULL;
  }
  else {
    return PyLong_FromLongLong( n );
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * __py_populate_perfcounter( _framehash_counters_t *src ) {
  PyObject *py_counter = NULL;
  if( src->opcount > 0 ) {
    double opcnt = (double)src->opcount;
    double hitrate = src->cache.hits == 0 ? 0.0 : src->cache.hits / (double)(src->cache.hits + src->cache.misses);
    double avg_depth          = src->probe.depth / opcnt; 
    double avg_nCL            = src->probe.nCL / opcnt; 
    double avg_ncachecells    = src->probe.ncachecells / opcnt; 
    double avg_nleafcells     = src->probe.nleafcells / opcnt; 
    double avg_nleafzones     = src->probe.nleafzones / opcnt; 
    double avg_nbasementcells = src->probe.nbasementcells / opcnt; 

    py_counter = Py_BuildValue( "{s:K s:{s:d} s:{s:d s:d s:d s:d s:d s:d}}", 
                                  "opcount", src->opcount,
                                      "cache", 
                                         "hitrate", hitrate,
                                              "probe",
                                                 "depth",          avg_depth,
                                                 "cpu_cachlines",  avg_nCL,
                                                 "cache_cells",    avg_ncachecells,
                                                 "leaf_cells",     avg_nleafcells,
                                                 "leaf_zones",     avg_nleafzones,
                                                 "basement_cells", avg_nbasementcells
                                  );
  }
  else {
    py_counter = Py_BuildValue( "{s:K}", "opcount", 0 );
  }
  return py_counter;
}



/******************************************************************************
 * Framehash_get_perfcounters
 *
 ******************************************************************************
 */
static PyObject * Framehash_get_perfcounters( PyVGX_Framehash *self ) {

  framehash_perfcounters_t counters = {0};

  CALLABLE(self->fhash)->GetPerfCounters( self->fhash, &counters );
  PyObject *py_read = __py_populate_perfcounter( &counters.read );
  PyObject *py_write = __py_populate_perfcounter( &counters.write );
  return Py_BuildValue( "{s:N,s:N}", "read", py_read, "write", py_write );

}



/******************************************************************************
 * Framehash_reset_perfcounters
 *
 ******************************************************************************
 */
static PyObject * Framehash_reset_perfcounters( PyVGX_Framehash *self ) {
  CALLABLE(self->fhash)->ResetPerfCounters( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash__test_ws_perf
 *
 ******************************************************************************
 */
static PyObject * Framehash__test_ws_perf( PyVGX_Framehash *self, PyObject *args, PyObject *kwds ) {
  framehash_t *fh = self->fhash;
  framehash_vtable_t *ifh = CALLABLE( fh );
#define __HAS_INT_HASH( x ) (ifh->HasKey64( fh, CELL_KEY_TYPE_HASH64, ihash64(x) ) == CELL_VALUE_TYPE_INTEGER)

  static char *kwlist[] = { "wsz", "n0", "n1", "duration", NULL };

  int wsz = 0;
  int n0 = 0;
  int n1 = 0;
  int duration = 0; // seconds




  if( !fh->_flag.synchronized ) {
    PyErr_SetString( PyExc_Exception, "framehash instance must support multiple threads" );
    return NULL;
  }

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "iiii", kwlist, &wsz, &n0, &n1, &duration ) ) {
    return NULL;
  }

  int range = n1 - n0;

  if( range <= 0 ) {
    PyErr_SetString( PyExc_ValueError, "invalid key range" );
    return NULL;
  }

  if( wsz <= 0 ) {
    PyErr_SetString( PyExc_ValueError, "invalid working set size" );
    return NULL;
  }


  uint64_t *working_set = NULL;

  int error = 0;
  double rate_us_per_q = 0.0;

  Py_BEGIN_ALLOW_THREADS

  // Ensure framehash is populated to support the key range being tested.
  // First a quick sample to be reasonably sure it's ok, that's good enough.
  if( !__HAS_INT_HASH(0) || !__HAS_INT_HASH((uint64_t)n1-1) || !__HAS_INT_HASH((uint64_t)n1/2) || ifh->Items( fh ) < n1 ) {
    int64_t n = 0;
    while( n < n1 ) {
      for( int i=0; i<100000 && n<n1; i++, n++ ) {
        ifh->SetInt56( fh, CELL_KEY_TYPE_HASH64, ihash64( n ), n );
      }
    }
  }

  // Allocate working set
  if( (working_set = malloc( wsz * sizeof( uint64_t ) )) != NULL ) {

    // Populate working set with hashed keys based on the specified integer key range
    for( int i=0; i<wsz; i++ ) {
      // Generate a random number in the interval [n0, n1>
      uint64_t rn = n0 + rand64() % range;
      // Hash the key and place in working set
      uint64_t rnh = ihash64( rn );
      working_set[i] = rnh;
    }

    ifh->Flush( fh, true );


    int64_t start_ms = __MILLISECONDS_SINCE_1970();
    int64_t end_time_ms = start_ms + duration * 1000ULL;
    int64_t now_ms = start_ms;
    
    uint64_t *kp = working_set;
    uint64_t *pend = working_set + wsz;

    int64_t val;

    int64_t count = 0;

    while( now_ms <= end_time_ms && !error ) {
      // Run a batch before checking time
      for( int i=0; i<1000000; i++ ) {
        // Run query
        if( ifh->GetInt56( fh, CELL_KEY_TYPE_HASH64, *kp++, &val ) != CELL_VALUE_TYPE_INTEGER ) {
          error = -101;
          break;
        }
        // wrap to start of working set
        if( kp == pend ) {
          kp = working_set;
        }
        // inc count
        ++count;
      }
      now_ms = __MILLISECONDS_SINCE_1970();
    }

    // Compute time per query
    if( now_ms > start_ms ) {
      rate_us_per_q = 1000 * (double)(now_ms - start_ms) / count;
    }

  }

  Py_END_ALLOW_THREADS

  if( working_set ) {
    free( working_set );
    if( error == 0 ) {
      return PyFloat_FromDouble( rate_us_per_q );
    }
    else {
      return PyLong_FromLong( error );
    }
  }
  else {
    PyErr_SetString( PyExc_MemoryError, "out of memory" );
    return NULL;
  }

}



/******************************************************************************
 * Framehash_math_mul
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_mul( PyVGX_Framehash *self, PyObject *py_factor ) {
  double factor = PyFloat_AsDouble( py_factor );
  if( factor == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Mul( self->fhash, factor );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_div
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_div( PyVGX_Framehash *self, PyObject *py_divisor ) {
  double divisor = PyFloat_AsDouble( py_divisor );
  if( divisor == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  if( divisor == 0.0 ) {
    PyErr_SetString( PyExc_ZeroDivisionError, "" );
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Mul( self->fhash, 1/divisor );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_add
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_add( PyVGX_Framehash *self, PyObject *py_val ) {
  double val = PyFloat_AsDouble( py_val );
  if( val == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Add( self->fhash, val );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_sub
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_sub( PyVGX_Framehash *self, PyObject *py_val ) {
  double val = PyFloat_AsDouble( py_val );
  if( val == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Add( self->fhash, -val );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_inc
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_inc( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Add( self->fhash, 1 );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_dec
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_dec( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Add( self->fhash, -1 );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_sqrt
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_sqrt( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Sqrt( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_pow
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_pow( PyVGX_Framehash *self, PyObject *py_exponent ) {
  double exponent = PyFloat_AsDouble( py_exponent );
  if( exponent == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Pow( self->fhash, exponent );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_log
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_log( PyVGX_Framehash *self, PyObject *py_base ) {
  double base = PyFloat_AsDouble( py_base );
  if( base == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Log( self->fhash, base );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_log
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_exp( PyVGX_Framehash *self, PyObject *py_base ) {
  double base = PyFloat_AsDouble( py_base );
  if( base == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Exp( self->fhash, base );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_decay
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_decay( PyVGX_Framehash *self, PyObject *py_exponent) {
  double exponent = PyFloat_AsDouble( py_exponent );
  if( exponent == -1.0 && PyErr_Occurred() ) {
    return NULL;
  }
  CALLABLE( self->fhash )->Math->Decay( self->fhash, exponent );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_set
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_set( PyVGX_Framehash *self, PyObject *py_value ) {
  double value = PyFloat_AsDouble( py_value );
  CALLABLE( self->fhash )->Math->Set( self->fhash, value );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_randomize
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_randomize( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Randomize( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_int
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_int( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Int( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_float
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_float( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Float( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_abs
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_abs( PyVGX_Framehash *self ) {
  CALLABLE( self->fhash )->Math->Abs( self->fhash );
  Py_RETURN_NONE;
}



/******************************************************************************
 * Framehash_math_sum
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_sum( PyVGX_Framehash *self ) {
  double sum = 0.0;
  CALLABLE( self->fhash )->Math->Sum( self->fhash, &sum );
  return PyFloat_FromDouble( sum );
}



/******************************************************************************
 * Framehash_math_avg
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_avg( PyVGX_Framehash *self ) {
  double avg = 0.0;
  CALLABLE( self->fhash )->Math->Avg( self->fhash, &avg );
  return PyFloat_FromDouble( avg );
}



/******************************************************************************
 * Framehash_math_stdev
 *
 ******************************************************************************
 */
static PyObject * Framehash_math_stdev( PyVGX_Framehash *self ) {
  double stdev = 0.0;
  CALLABLE( self->fhash )->Math->Stdev( self->fhash, &stdev );
  return PyFloat_FromDouble( stdev );
}



/******************************************************************************
 * PyVGX_Framehash_length
 *
 ******************************************************************************
 */
static Py_ssize_t PyVGX_Framehash_length( PyVGX_Framehash *self ) {
  return CALL( self->fhash, Items );
}



/******************************************************************************
 * Framehash_subscript
 *
 ******************************************************************************
 */
static PyObject * Framehash_subscript( PyVGX_Framehash *self, PyObject *py_key ) {
  PyObject *py_ret = NULL;
  PyObject *args;
  if( (args = PyTuple_New(1)) == NULL ) {
    return NULL;
  }
  PyTuple_SET_ITEM( args, 0, py_key );
  Py_INCREF(py_key); // got stolen above
  py_ret = _get( self, args );
  Py_DECREF(args);
  return py_ret;
}



/******************************************************************************
 * PyVGX_Framehash_ass_subscript
 *
 ******************************************************************************
 */
static int PyVGX_Framehash_ass_subscript( PyVGX_Framehash *self, PyObject *py_key, PyObject *py_value ) {
  int ret = 0;
  PyObject *py_ret = NULL;
  PyObject *args = NULL;

  if( py_value != NULL ) {
    if( (args = PyTuple_New(2)) == NULL ) {
      return -1;
    }
    PyTuple_SET_ITEM( args, 0, py_key );
    PyTuple_SET_ITEM( args, 1, py_value );
    Py_INCREF( py_key );
    Py_INCREF( py_value );
    py_ret = _set( self, args );
  }
  else {
    py_ret = _delete( self, py_key );
  }

  if( py_ret ) {
    Py_DECREF( py_ret );
  }
  else {
    ret = -1;
  }

  Py_XDECREF( args );

  return ret;
}



/******************************************************************************
 * PyVGX_Framehash_as_sequence
 *
 ******************************************************************************
 */
/* Hack to implement "key in table" - stolen from dictobject.c */
static PySequenceMethods PyVGX_Framehash_as_sequence = {
    .sq_length          = 0,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)_contains,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0 
};



/******************************************************************************
 * PyVGX_Framehash_members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Framehash_members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Framehash_as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_Framehash_as_mapping = {
  (lenfunc)PyVGX_Framehash_length,              /* mp_length    */
  (binaryfunc)Framehash_subscript,          /* mp_subscript */
  (objobjargproc)PyVGX_Framehash_ass_subscript  /* mp_ass_subscript */
};


#define __method_pyframehash_lfsrX( X )  {"lfsr"#X,        (PyCFunction)pyframehash_lfsr##X ,        METH_VARARGS | METH_STATIC, "lfsr"#X"( [seed] ) -> "#X"-bit integer" }
#define __method_pyframehash_randX( X )  {"rand"#X,        (PyCFunction)pyframehash_rand##X ,        METH_NOARGS | METH_STATIC, "rand"#X"() -> "#X"-bit random integer" }

/******************************************************************************
 * Framehash_methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef Framehash_methods[] = {
    {"__contains__",        (PyCFunction)Framehash_contains,            METH_O | METH_COEXIST, "True if key exists, else False\n" },
    {"__getitem__",         (PyCFunction)Framehash_subscript,           METH_O | METH_COEXIST, "__getitem__\n" },

    {"set",                 (PyCFunction)Framehash_set,                 METH_VARARGS, "set(key, value) -> None\n" },
    {"get",                 (PyCFunction)Framehash_get,                 METH_VARARGS, "get(key [,default]) -> value\n" },
    {"delete",              (PyCFunction)Framehash_delete,              METH_O, "delete(key) -> None\n" },
    {"has",                 (PyCFunction)Framehash_contains,            METH_O | METH_COEXIST, "True if key exists, else False\n" },
    {"inc",                 (PyCFunction)Framehash_inc,                 METH_VARARGS, "inc(key, amount) -> new_value\n" },

    {"setint",              (PyCFunction)Framehash_set_int,             METH_VARARGS, "setint(key, value) -> None\n" },
    {"getint",              (PyCFunction)Framehash_get_int,             METH_O, "getint(key) -> value\n" },
    {"delint",              (PyCFunction)Framehash_del_int,             METH_O, "delint(key) -> None\n" },
    {"hasint",              (PyCFunction)Framehash_contains_int,        METH_O, "True if raw key exists, else False\n" },
    {"incint",              (PyCFunction)Framehash_inc_int,             METH_VARARGS, "incint(key, amount) -> new_value\n" },

    {"items",               (PyCFunction)Framehash_items,               METH_NOARGS, "items() -> [(key,value), ...]\n" },
    {"keys",                (PyCFunction)Framehash_keys,                METH_NOARGS, "keys() -> [key, ...]\n" },
    {"values",              (PyCFunction)Framehash_values,              METH_NOARGS, "values() -> [values, ...]\n" },

    {"clear",               (PyCFunction)Framehash_clear,               METH_NOARGS,  "clear() -> None\n" },
    {"flush_caches",        (PyCFunction)Framehash_flush_caches,        METH_VARARGS, "flush_caches( [invalidate] ) -> None\n" },
    {"set_readonly",        (PyCFunction)Framehash_set_readonly,        METH_NOARGS,  "set_readonly() -> None\n" },
    {"is_readonly",         (PyCFunction)Framehash_is_readonly,         METH_NOARGS,  "is_readonly() -> True/False\n" },
    {"clear_readonly",      (PyCFunction)Framehash_clear_readonly,      METH_NOARGS,  "clear_readonly() -> None\n" },
    {"enable_read_caches",  (PyCFunction)Framehash_enable_read_caches,  METH_NOARGS,  "enable_read_caches() -> None\n" },
    {"disable_read_caches", (PyCFunction)Framehash_disable_read_caches, METH_NOARGS,  "disable_read_caches() -> None\n" },
    {"enable_write_caches", (PyCFunction)Framehash_enable_write_caches, METH_NOARGS,  "enable_write_caches() -> None\n" },
    {"disable_write_caches",(PyCFunction)Framehash_disable_write_caches,METH_NOARGS,  "disable_write_caches() -> None\n" },

    {"count_active",        (PyCFunction)Framehash_count_active,        METH_NOARGS, "count_active() -> n" },

    {"math_mul",            (PyCFunction)Framehash_math_mul,            METH_O,       "math_mul( factor ) -> None\n"
                                                                                      "Multiply all numeric values in map by factor\n"
                                                                                      "x = x * factor\n"
                                                                                      },
    {"math_div",            (PyCFunction)Framehash_math_div,            METH_O,       "math_div( divisor ) -> None\n"
                                                                                      "Divide all numeric values in map by divisor\n"
                                                                                      "x = x / divisor\n"
                                                                                      },
    {"math_add",            (PyCFunction)Framehash_math_add,            METH_O,       "math_add( v ) -> None\n"
                                                                                      "Add v to all numeric values in map\n"
                                                                                      "x = x + v\n"
                                                                                      },
    {"math_sub",            (PyCFunction)Framehash_math_sub,            METH_O,       "math_sub( v ) -> None\n"
                                                                                      "Subtract v from all numeric values in map\n"
                                                                                      "x = x - v\n"
                                                                                      },
    {"math_inc",            (PyCFunction)Framehash_math_inc,            METH_NOARGS,  "math_inc() -> None\n"
                                                                                      "Increment all numeric values in map by 1\n"
                                                                                      "x = x + 1\n"
                                                                                      },
    {"math_dec",            (PyCFunction)Framehash_math_dec,            METH_NOARGS,  "math_dec() -> None\n"
                                                                                      "Decrement all numeric values in map by 1\n"
                                                                                      "x = x - 1\n"
                                                                                      },
    {"math_sqrt",           (PyCFunction)Framehash_math_sqrt,           METH_NOARGS,  "math_sqrt() -> None\n"
                                                                                      "Apply the square root to all numeric values in map\n"
                                                                                      "x = sqrt(x)\n"
                                                                                      },
    {"math_pow",            (PyCFunction)Framehash_math_pow,            METH_O,       "math_pow( exp ) -> None\n"
                                                                                      "Raise all numeric values in map to power\n"
                                                                                      "x = x ** exp\n"
                                                                                      },
    {"math_log",            (PyCFunction)Framehash_math_log,            METH_O,       "math_log( base ) -> None\n"
                                                                                      "Apply log to all numeric values in map\n"
                                                                                      "x = log_base(x)\n"
                                                                                      },
    {"math_exp",            (PyCFunction)Framehash_math_exp,            METH_O,       "math_exp( base ) -> None\n"
                                                                                      "Exponentiate all numeric values in map\n"
                                                                                      "x = base ** x\n"
                                                                                      },
    {"math_decay",          (PyCFunction)Framehash_math_decay,          METH_O,       "math_decay( exp ) -> None\n"
                                                                                      "Apply exponential decay function to all numeric values in map\n"
                                                                                      "x = x * (e ** exp)\n"
                                                                                      "(NOTE: exp should be negative to apply exponential decay)\n"
                                                                                      },
    {"math_set",            (PyCFunction)Framehash_math_set,            METH_O,       "math_set( value ) -> None\n"
                                                                                      "Set all numeric values in map to this value\n"
                                                                                      "x = value\n"
                                                                                      },
    {"math_randomize",      (PyCFunction)Framehash_math_randomize,      METH_NOARGS,  "math_randomize() -> None\n"
                                                                                      "Set all numeric values in map to random numbers\n"
                                                                                      },
    {"math_int",            (PyCFunction)Framehash_math_int,            METH_NOARGS,  "math_int() -> None\n"
                                                                                      "Cast all numeric values in map to integers\n"
                                                                                      },
    {"math_float",          (PyCFunction)Framehash_math_float,          METH_NOARGS,  "math_float() -> None\n"
                                                                                      "Cast all numeric values in map to floating point\n"
                                                                                      },
    {"math_abs",            (PyCFunction)Framehash_math_abs,            METH_NOARGS,  "math_abs() -> None\n"
                                                                                      "Set all numeric values in map to their absolute values\n"
                                                                                      },
    {"math_sum",            (PyCFunction)Framehash_math_sum,            METH_NOARGS,  "math_sum() -> float\n"
                                                                                      "Add all numeric values in map and return the sum\n"
                                                                                      },
    {"math_avg",            (PyCFunction)Framehash_math_avg,            METH_NOARGS,  "math_avg() -> float\n"
                                                                                      "Compute and return the average of all numeric values in map\n"
                                                                                      },
    {"math_stdev",          (PyCFunction)Framehash_math_stdev,          METH_NOARGS,  "math_stdev() -> float\n"
                                                                                      "Compute and return the standard deviation of all numeric values in map\n" 
                                                                                      },

    {"save",                (PyCFunction)Framehash_save,                METH_NOARGS, "save() -> bytes\n" },

    {"hitrate",             (PyCFunction)Framehash_hitrate,             METH_NOARGS, "hitrate() -> (h0, h1, h2, h3, h4)" },
    {"counters",            (PyCFunction)Framehash_get_perfcounters,    METH_NOARGS, "counters() -> dict" },
    {"reset_counters",      (PyCFunction)Framehash_reset_perfcounters,  METH_NOARGS, "reset_counters() -> None" },

    {"_test_ws_perf",       (PyCFunction)Framehash__test_ws_perf,       METH_VARARGS | METH_KEYWORDS, "_test_ws_perf( wsz, n0, n1 ) -> result\n" },

  {"hash64",        (PyCFunction)pyframehash_hash64,        METH_VARARGS | METH_STATIC, "hash64( key ) -> 64-bit hash" },
  {"hash32",        (PyCFunction)pyframehash_hash32,        METH_VARARGS | METH_STATIC, "hash32( key ) -> 32-bit hash" },

  // LFSR
  __method_pyframehash_lfsrX( 3 ),
  __method_pyframehash_lfsrX( 4 ),
  __method_pyframehash_lfsrX( 5 ),
  __method_pyframehash_lfsrX( 6 ),
  __method_pyframehash_lfsrX( 7 ),
  __method_pyframehash_lfsrX( 8 ),
  __method_pyframehash_lfsrX( 9 ),
  __method_pyframehash_lfsrX( 10 ),
  __method_pyframehash_lfsrX( 11 ),
  __method_pyframehash_lfsrX( 12 ),
  __method_pyframehash_lfsrX( 13 ),
  __method_pyframehash_lfsrX( 14 ),
  __method_pyframehash_lfsrX( 15 ),
  __method_pyframehash_lfsrX( 16 ),
  __method_pyframehash_lfsrX( 17 ),
  __method_pyframehash_lfsrX( 18 ),
  __method_pyframehash_lfsrX( 19 ),
  __method_pyframehash_lfsrX( 20 ),
  __method_pyframehash_lfsrX( 21 ),
  __method_pyframehash_lfsrX( 22 ),
  __method_pyframehash_lfsrX( 23 ),
  __method_pyframehash_lfsrX( 24 ),
  __method_pyframehash_lfsrX( 25 ),
  __method_pyframehash_lfsrX( 26 ),
  __method_pyframehash_lfsrX( 27 ),
  __method_pyframehash_lfsrX( 28 ),
  __method_pyframehash_lfsrX( 29 ),
  __method_pyframehash_lfsrX( 30 ),
  __method_pyframehash_lfsrX( 31 ),
  __method_pyframehash_lfsrX( 32 ),
  __method_pyframehash_lfsrX( 33 ),
  __method_pyframehash_lfsrX( 34 ),
  __method_pyframehash_lfsrX( 35 ),
  __method_pyframehash_lfsrX( 36 ),
  __method_pyframehash_lfsrX( 37 ),
  __method_pyframehash_lfsrX( 38 ),
  __method_pyframehash_lfsrX( 39 ),
  __method_pyframehash_lfsrX( 40 ),
  __method_pyframehash_lfsrX( 41 ),
  __method_pyframehash_lfsrX( 42 ),
  __method_pyframehash_lfsrX( 43 ),
  __method_pyframehash_lfsrX( 44 ),
  __method_pyframehash_lfsrX( 45 ),
  __method_pyframehash_lfsrX( 46 ),
  __method_pyframehash_lfsrX( 47 ),
  __method_pyframehash_lfsrX( 48 ),
  __method_pyframehash_lfsrX( 49 ),
  __method_pyframehash_lfsrX( 50 ),
  __method_pyframehash_lfsrX( 51 ),
  __method_pyframehash_lfsrX( 52 ),
  __method_pyframehash_lfsrX( 53 ),
  __method_pyframehash_lfsrX( 54 ),
  __method_pyframehash_lfsrX( 55 ),
  __method_pyframehash_lfsrX( 56 ),
  __method_pyframehash_lfsrX( 57 ),
  __method_pyframehash_lfsrX( 58 ),
  __method_pyframehash_lfsrX( 59 ),
  __method_pyframehash_lfsrX( 60 ),
  __method_pyframehash_lfsrX( 61 ),
  __method_pyframehash_lfsrX( 62 ),
  __method_pyframehash_lfsrX( 63 ),
  __method_pyframehash_lfsrX( 64 ),
  
  // Random integers
  __method_pyframehash_randX( 9 ),
  __method_pyframehash_randX( 10 ),
  __method_pyframehash_randX( 11 ),
  __method_pyframehash_randX( 12 ),
  __method_pyframehash_randX( 13 ),
  __method_pyframehash_randX( 14 ),
  __method_pyframehash_randX( 15 ),
  __method_pyframehash_randX( 16 ),
  __method_pyframehash_randX( 17 ),
  __method_pyframehash_randX( 18 ),
  __method_pyframehash_randX( 19 ),
  __method_pyframehash_randX( 20 ),
  __method_pyframehash_randX( 21 ),
  __method_pyframehash_randX( 22 ),
  __method_pyframehash_randX( 23 ),
  __method_pyframehash_randX( 24 ),
  __method_pyframehash_randX( 25 ),
  __method_pyframehash_randX( 26 ),
  __method_pyframehash_randX( 27 ),
  __method_pyframehash_randX( 28 ),
  __method_pyframehash_randX( 29 ),
  __method_pyframehash_randX( 30 ),
  __method_pyframehash_randX( 31 ),
  __method_pyframehash_randX( 32 ),
  __method_pyframehash_randX( 33 ),
  __method_pyframehash_randX( 34 ),
  __method_pyframehash_randX( 35 ),
  __method_pyframehash_randX( 36 ),
  __method_pyframehash_randX( 37 ),
  __method_pyframehash_randX( 38 ),
  __method_pyframehash_randX( 39 ),
  __method_pyframehash_randX( 40 ),
  __method_pyframehash_randX( 41 ),
  __method_pyframehash_randX( 42 ),
  __method_pyframehash_randX( 43 ),
  __method_pyframehash_randX( 44 ),
  __method_pyframehash_randX( 45 ),
  __method_pyframehash_randX( 46 ),
  __method_pyframehash_randX( 47 ),
  __method_pyframehash_randX( 48 ),
  __method_pyframehash_randX( 49 ),
  __method_pyframehash_randX( 50 ),
  __method_pyframehash_randX( 51 ),
  __method_pyframehash_randX( 52 ),
  __method_pyframehash_randX( 53 ),
  __method_pyframehash_randX( 54 ),
  __method_pyframehash_randX( 55 ),
  __method_pyframehash_randX( 56 ),
  __method_pyframehash_randX( 57 ),
  __method_pyframehash_randX( 58 ),
  __method_pyframehash_randX( 59 ),
  __method_pyframehash_randX( 60 ),
  __method_pyframehash_randX( 61 ),
  __method_pyframehash_randX( 62 ),
  __method_pyframehash_randX( 63 ),
  __method_pyframehash_randX( 64 ),

  {"debuginfo",     (PyCFunction)pyframehash_debuginfo,     METH_VARARGS | METH_KEYWORDS, "debuginfo(...)\n" },
  {"selftest" ,     (PyCFunction)PyVGX_Framehash_selftest,      METH_VARARGS | METH_KEYWORDS, "selftest( testroot[, names  ] ) -> None" },


    {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef pyframehash_methods[] = {

  {"hash64",        (PyCFunction)pyframehash_hash64,        METH_VARARGS | METH_STATIC, "hash64( key ) -> 64-bit hash" },
  {"hash32",        (PyCFunction)pyframehash_hash32,        METH_VARARGS | METH_STATIC, "hash32( key ) -> 32-bit hash" },

  // LFSR
  __method_pyframehash_lfsrX( 3 ),
  __method_pyframehash_lfsrX( 4 ),
  __method_pyframehash_lfsrX( 5 ),
  __method_pyframehash_lfsrX( 6 ),
  __method_pyframehash_lfsrX( 7 ),
  __method_pyframehash_lfsrX( 8 ),
  __method_pyframehash_lfsrX( 9 ),
  __method_pyframehash_lfsrX( 10 ),
  __method_pyframehash_lfsrX( 11 ),
  __method_pyframehash_lfsrX( 12 ),
  __method_pyframehash_lfsrX( 13 ),
  __method_pyframehash_lfsrX( 14 ),
  __method_pyframehash_lfsrX( 15 ),
  __method_pyframehash_lfsrX( 16 ),
  __method_pyframehash_lfsrX( 17 ),
  __method_pyframehash_lfsrX( 18 ),
  __method_pyframehash_lfsrX( 19 ),
  __method_pyframehash_lfsrX( 20 ),
  __method_pyframehash_lfsrX( 21 ),
  __method_pyframehash_lfsrX( 22 ),
  __method_pyframehash_lfsrX( 23 ),
  __method_pyframehash_lfsrX( 24 ),
  __method_pyframehash_lfsrX( 25 ),
  __method_pyframehash_lfsrX( 26 ),
  __method_pyframehash_lfsrX( 27 ),
  __method_pyframehash_lfsrX( 28 ),
  __method_pyframehash_lfsrX( 29 ),
  __method_pyframehash_lfsrX( 30 ),
  __method_pyframehash_lfsrX( 31 ),
  __method_pyframehash_lfsrX( 32 ),
  __method_pyframehash_lfsrX( 33 ),
  __method_pyframehash_lfsrX( 34 ),
  __method_pyframehash_lfsrX( 35 ),
  __method_pyframehash_lfsrX( 36 ),
  __method_pyframehash_lfsrX( 37 ),
  __method_pyframehash_lfsrX( 38 ),
  __method_pyframehash_lfsrX( 39 ),
  __method_pyframehash_lfsrX( 40 ),
  __method_pyframehash_lfsrX( 41 ),
  __method_pyframehash_lfsrX( 42 ),
  __method_pyframehash_lfsrX( 43 ),
  __method_pyframehash_lfsrX( 44 ),
  __method_pyframehash_lfsrX( 45 ),
  __method_pyframehash_lfsrX( 46 ),
  __method_pyframehash_lfsrX( 47 ),
  __method_pyframehash_lfsrX( 48 ),
  __method_pyframehash_lfsrX( 49 ),
  __method_pyframehash_lfsrX( 50 ),
  __method_pyframehash_lfsrX( 51 ),
  __method_pyframehash_lfsrX( 52 ),
  __method_pyframehash_lfsrX( 53 ),
  __method_pyframehash_lfsrX( 54 ),
  __method_pyframehash_lfsrX( 55 ),
  __method_pyframehash_lfsrX( 56 ),
  __method_pyframehash_lfsrX( 57 ),
  __method_pyframehash_lfsrX( 58 ),
  __method_pyframehash_lfsrX( 59 ),
  __method_pyframehash_lfsrX( 60 ),
  __method_pyframehash_lfsrX( 61 ),
  __method_pyframehash_lfsrX( 62 ),
  __method_pyframehash_lfsrX( 63 ),
  __method_pyframehash_lfsrX( 64 ),
  
  // Random integers
  __method_pyframehash_randX( 9 ),
  __method_pyframehash_randX( 10 ),
  __method_pyframehash_randX( 11 ),
  __method_pyframehash_randX( 12 ),
  __method_pyframehash_randX( 13 ),
  __method_pyframehash_randX( 14 ),
  __method_pyframehash_randX( 15 ),
  __method_pyframehash_randX( 16 ),
  __method_pyframehash_randX( 17 ),
  __method_pyframehash_randX( 18 ),
  __method_pyframehash_randX( 19 ),
  __method_pyframehash_randX( 20 ),
  __method_pyframehash_randX( 21 ),
  __method_pyframehash_randX( 22 ),
  __method_pyframehash_randX( 23 ),
  __method_pyframehash_randX( 24 ),
  __method_pyframehash_randX( 25 ),
  __method_pyframehash_randX( 26 ),
  __method_pyframehash_randX( 27 ),
  __method_pyframehash_randX( 28 ),
  __method_pyframehash_randX( 29 ),
  __method_pyframehash_randX( 30 ),
  __method_pyframehash_randX( 31 ),
  __method_pyframehash_randX( 32 ),
  __method_pyframehash_randX( 33 ),
  __method_pyframehash_randX( 34 ),
  __method_pyframehash_randX( 35 ),
  __method_pyframehash_randX( 36 ),
  __method_pyframehash_randX( 37 ),
  __method_pyframehash_randX( 38 ),
  __method_pyframehash_randX( 39 ),
  __method_pyframehash_randX( 40 ),
  __method_pyframehash_randX( 41 ),
  __method_pyframehash_randX( 42 ),
  __method_pyframehash_randX( 43 ),
  __method_pyframehash_randX( 44 ),
  __method_pyframehash_randX( 45 ),
  __method_pyframehash_randX( 46 ),
  __method_pyframehash_randX( 47 ),
  __method_pyframehash_randX( 48 ),
  __method_pyframehash_randX( 49 ),
  __method_pyframehash_randX( 50 ),
  __method_pyframehash_randX( 51 ),
  __method_pyframehash_randX( 52 ),
  __method_pyframehash_randX( 53 ),
  __method_pyframehash_randX( 54 ),
  __method_pyframehash_randX( 55 ),
  __method_pyframehash_randX( 56 ),
  __method_pyframehash_randX( 57 ),
  __method_pyframehash_randX( 58 ),
  __method_pyframehash_randX( 59 ),
  __method_pyframehash_randX( 60 ),
  __method_pyframehash_randX( 61 ),
  __method_pyframehash_randX( 62 ),
  __method_pyframehash_randX( 63 ),
  __method_pyframehash_randX( 64 ),

  {"debuginfo",     (PyCFunction)pyframehash_debuginfo,     METH_VARARGS | METH_KEYWORDS, "debuginfo(...)\n" },
  {"selftest" ,     (PyCFunction)PyVGX_Framehash_selftest,      METH_VARARGS | METH_KEYWORDS, "selftest( testroot[, names  ] ) -> None" },

  {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_FramehashType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Framehash__FramehashType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Framehash",
    .tp_basicsize       = sizeof(PyVGX_Framehash),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Framehash_dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Framehash_repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &PyVGX_Framehash_as_sequence,
    .tp_as_mapping      = &PyVGX_Framehash_as_mapping,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX_Framehash objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = Framehash_methods,
    .tp_members         = PyVGX_Framehash_members,
    .tp_getset          = 0,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Framehash_init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Framehash_new,
    .tp_free            = (freefunc)0,
    .tp_is_gc           = (inquiry)0,
    .tp_bases           = NULL,
    .tp_mro             = NULL,
    .tp_cache           = NULL,
    .tp_subclasses      = NULL,
    .tp_weaklist        = NULL,
    .tp_del             = (destructor)0,
    .tp_version_tag     = 0,
    .tp_finalize        = (destructor)0,
    .tp_vectorcall      = (vectorcallfunc)0

};




/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int __pyvgx_framehash__init( void ) {
  COMLIB_REGISTER_CLASS( PyFramehashObjectWrapper_t, CXLIB_OBTYPE_PY_WRAPPER, &PyFramehashObjectWrapperMethods, OBJECT_IDENTIFIED_BY_OBJECTID, 0 );
  return 0;
}



DLL_HIDDEN PyTypeObject * p_PyVGX_Framehash__FramehashType = &PyVGX_Framehash__FramehashType;
