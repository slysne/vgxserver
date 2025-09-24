/*######################################################################
 *#
 *# pyvgx_vector.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


static PyObject *g_default_similarity = NULL;



/******************************************************************************
 * __PyVGX_Vector_as_dict
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vector_as_dict( PyVGX_Vector *pyvector ) {
  PyObject *py_dict = PyDict_New();
  if( py_dict ) {
    PyObject *py_ext, *py_int;
    vgx_Vector_t *vext = pyvector->vext;
    vgx_Vector_t *vint = pyvector->vint;
    
    py_ext = iPyVGXBuilder.ExternalVector( vext );
    py_int = iPyVGXBuilder.InternalVector( vint );

    if( py_ext && py_int ) {
      float mag = CALLABLE( vext )->Magnitude( vext );
      float scale = CALLABLE( vint )->Scaler( vint );
      int sz = vext->metas.vlen;
      FP_t fp = vext->fp;
      int ctr = (vint->metas.type & __VECTOR__MASK_CENTROID) != 0;
      int ecl = (vint->metas.type & __VECTOR__MASK_EUCLIDEAN) != 0;
      if( PyVGX_DictStealItemString( py_dict, "external",    py_ext ) < 0 || 
          PyVGX_DictStealItemString( py_dict, "internal",    py_int ) < 0 || 
          PyVGX_DictStealItemString( py_dict, "centroid",    PyBool_FromLong( ctr ) ) < 0 || 
          PyVGX_DictStealItemString( py_dict, "type",        ecl ? PyUnicode_FromString("euclidean") : PyUnicode_FromString("feature") ) < 0 || 
          PyVGX_DictStealItemString( py_dict, "length",      PyLong_FromLong( sz) ) < 0 ||
          PyVGX_DictStealItemString( py_dict, "fingerprint", PyLong_FromUnsignedLongLong( fp ) ) < 0 ||
          PyVGX_DictStealItemString( py_dict, "magnitude",   PyFloat_FromDouble( mag ) ) < 0 ||
          PyVGX_DictStealItemString( py_dict, "scale",       PyFloat_FromDouble( scale ) ) < 0 )
      {
        // error
        PyVGX_DECREF( py_dict );
        py_dict = NULL;
      }
    }
    else {
      PyVGX_XDECREF( py_ext );
      PyVGX_XDECREF( py_int );
    }
  }
  return py_dict;
}



/******************************************************************************
 * __PyVGX_Vector__length
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__length( PyVGX_Vector *pyvector, void *closure ) {
  return PyLong_FromLong( pyvector->vext->metas.vlen );
}



/******************************************************************************
 * __PyVGX_Vector__magnitude
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__magnitude( PyVGX_Vector *pyvector, void *closure ) {
  return PyFloat_FromDouble( pyvector->vext->metas.scalar.norm );
}



/******************************************************************************
 * __PyVGX_Vector__fingerprint
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__fingerprint( PyVGX_Vector *pyvector, void *closure ) {
  return PyLong_FromUnsignedLongLong( pyvector->vext->fp );
}



/******************************************************************************
 * __PyVGX_Vector__ident
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__ident( PyVGX_Vector *pyvector, void *closure ) {
  vgx_Vector_t *vector = pyvector->vint;
  if( vector == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  char buf[33];
  BEGIN_PYVGX_THREADS {
    objectid_t obid = CALLABLE( vector )->Identity( vector );
    idtostr( buf, &obid );
  } END_PYVGX_THREADS;

  return PyUnicode_FromStringAndSize( buf, 32 );
}



/******************************************************************************
 * __PyVGX_Vector__external
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__external( PyVGX_Vector *pyvector, void *closure ) {
  PyObject *py_external = iPyVGXBuilder.ExternalVector( pyvector->vext );
  return py_external;
}



/******************************************************************************
 * __PyVGX_Vector__internal
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__internal( PyVGX_Vector *pyvector, void *closure ) {
  PyObject *py_internal = iPyVGXBuilder.InternalVector( pyvector->vint );
  return py_internal;
}



/******************************************************************************
 * __PyVGX_Vector__array
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__array( PyVGX_Vector *pyvector, void *closure ) {
  PyObject *py_internal_array = iPyVGXBuilder.InternalVectorArray( pyvector->vint );
  return py_internal_array;
}



/******************************************************************************
 * __PyVGX_Vector__alpha
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Vector__alpha( PyVGX_Vector *pyvector, void *closure ) {
  if( pyvector->vint == NULL ) {
    return PyFloat_FromDouble( 0.0 );
  }
  return PyFloat_FromDouble( pyvector->vint->metas.scalar.factor );
}



/******************************************************************************
 * PyVGX_Vector__AsDict
 *
 ******************************************************************************
 */
PyDoc_STRVAR( AsDict__doc__,
  "AsDict() -> dict\n"
);
static PyObject * PyVGX_Vector__AsDict( PyVGX_Vector *pyvector ) {
  return __PyVGX_Vector_as_dict( pyvector );
}



/******************************************************************************
 * PyVGX_Vector__Fingerprint
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Fingerprint__doc__,
  "Fingerprint( [seed] ) -> int\n"
);
static PyObject * PyVGX_Vector__Fingerprint( PyVGX_Vector *pyvector, PyObject *args ) {
  vgx_Vector_t *vector = pyvector->vint;
  if( vector == NULL ) {
    return PyLong_FromLong( 0 );
  }

  // Parse args
  int64_t seed = 0;
  if( !PyArg_ParseTuple( args, "|L", &seed ) ) {
    return NULL;
  }

  FP_t fp = vector->fp;
  if( seed ) {
    BEGIN_PYVGX_THREADS {
      // Get fingerprinter
      vgx_Similarity_t *sim = CALLABLE( vector )->Context( vector )->simobj;
      vgx_Fingerprinter_t *fingerprinter = sim ? sim->fingerprinter : NULL;
      // Compute fingerprint
      if( fingerprinter ) {
        fp = CALLABLE( fingerprinter )->Compute( fingerprinter, vector, seed, NULL );
      }
    } END_PYVGX_THREADS;
  }

  return PyLong_FromUnsignedLongLong( fp );
}



/******************************************************************************
 * PyVGX_Vector__Projections
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Projections__doc__,
  "Projections( seed, lsh=0, lcm=0, reduce=0, expand=0 ) -> list\n"
);
static PyObject * PyVGX_Vector__Projections( PyVGX_Vector *pyvector, PyObject *args, PyObject *kwdict ) {
  static char *kwlist[] = {"seed", "lsh", "lcm", "reduce", "expand", NULL}; 

  vgx_Vector_t *vector = pyvector->vint;
  if( vector == NULL ) {
    PyErr_SetString( PyExc_Exception, "NULL-vector" );
    return NULL;
  }

  uint64_t seed = 0;
  PyObject *py_lsh = NULL;
  PyObject *py_lcm = NULL;
  int reduce = 0;
  int expand = 0;

  if( !PyArg_ParseTupleAndKeywords( args, kwdict, "K|O!O!ii", kwlist, &seed, &PyLong_Type, &py_lsh, &PyLong_Type, &py_lcm, &reduce, &expand ) ) {
    return NULL;
  }

  if( reduce && expand ) {
    PyErr_SetString( PyExc_ValueError, "cannot both reduce and expand" );
    return NULL;
  }

  if( seed > 0xFFF ) {
    PyErr_SetString( PyExc_ValueError, "seed must be <= 0xFFF" );
    return NULL;
  }

  vgx_Similarity_t *sim = CALLABLE( vector )->Context( vector )->simobj;
  if( sim == NULL || sim->fingerprinter == NULL ) {
    PyErr_SetString( PyExc_ValueError, "vector has no similarity context" );
    return NULL;
  }

  if( sim->params.fingerprint.ann.nprojections == 0 ) {
    PyErr_SetString( PyExc_ValueError, "no projection sets (forgot to call CreateProjectionSets()?)" );
    return NULL;
  }

  FP_t lsh = 0;
  FP_t lcm = 0;

  if( py_lsh ) {
    lsh = PyLong_AsUnsignedLongLong( py_lsh );
  }

  if( py_lcm ) {
    lcm = PyLong_AsUnsignedLongLong( py_lcm );
  }

  char buffer321[321];
  buffer321[320] = '\0';
  char *p;
  BEGIN_PYVGX_THREADS {
    // Compute partition names
    vgx_Fingerprinter_t *F = sim->fingerprinter;
    if( lsh == 0 ) {
      lsh = CALLABLE( F )->Compute( F, vector, seed, &lcm );
    }
    p = CALLABLE( F )->Projections( F, buffer321, vector, lsh, lcm, (WORD)seed, sim->params.fingerprint.ann.ksize, reduce, expand );
  } END_PYVGX_THREADS;

  if( p == NULL ) {
    PyErr_SetString( PyExc_Exception, "failed to generate projections (internal error)" );
    return NULL;
  }

  int nproj = 0;
  const char *c = p;
  const char *e = p + sizeof( buffer321 );
  while( c < e && *c ) {
    ++nproj;
    c += 10;
  }

  PyObject *py_ret = NULL;
  PyObject *py_projections = NULL;
  py_lsh = NULL;
  py_lcm = NULL;
  XTRY {

    if( (py_lsh = PyLong_FromUnsignedLongLong( lsh )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( (py_lcm = PyLong_FromUnsignedLongLong( lcm )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    if( (py_projections = PyList_New( nproj )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    c = p;
    for( int i=0; i<nproj; ++i ) {
      PyObject *py_part = PyBytes_FromStringAndSize( c, 9 );
      if( py_part == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
      c += 10;
      PyList_SET_ITEM( py_projections, i, py_part );
    }

    if( (py_ret = PyTuple_New(3)) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x005 );
    }

    PyTuple_SET_ITEM( py_ret, 0, py_lsh );
    PyTuple_SET_ITEM( py_ret, 1, py_lcm );
    PyTuple_SET_ITEM( py_ret, 2, py_projections );

  }
  XCATCH( errcode ) {
    Py_XDECREF( py_lsh );
    Py_XDECREF( py_lcm );
    Py_XDECREF( py_projections );
    Py_XDECREF( py_ret );
  }
  XFINALLY {
  }

  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_Similarity_t * __vector_simcontext( PyObject *pyvec1, PyObject *pyvec2, vgx_Vector_t **rv1, vgx_Vector_t **rv2 ) {
  if( !PyVGX_Vector_CheckExact( pyvec1 ) || !PyVGX_Vector_CheckExact( pyvec2 ) ) {
    PyErr_SetString( PyExc_TypeError, "vector object required" );
    return NULL;
  }

  vgx_Vector_t *v1 = ((PyVGX_Vector*)pyvec1)->vint;
  vgx_Vector_t *v2 = ((PyVGX_Vector*)pyvec2)->vint;
  if( v1 == NULL || v2 == NULL ) {
    PyErr_SetString( PyExc_TypeError, "vector(s) not initialized" );
    return NULL;
  }

  vgx_Similarity_t *sim1 = CALLABLE( v1 )->Context( v1 )->simobj;
  vgx_Similarity_t *sim2 = CALLABLE( v2 )->Context( v2 )->simobj;
  if( sim1 != sim2 ) {
    PyErr_SetString( PyExc_TypeError, "incompatible vectors" );
    return NULL;
  }

  *rv1 = v1;
  *rv2 = v2;
  return sim1;
}



/******************************************************************************
 * PyVGX_Vector__Cosine
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Cosine__doc__,
  "Cosine() -> float\n"
);
static PyObject * PyVGX_Vector__Cosine( PyObject *py_self, PyObject *py_other ) {
  vgx_Vector_t *v1, *v2;
  vgx_Similarity_t *sim = __vector_simcontext( py_self, py_other, &v1, &v2 );
  if( sim == NULL ) {
    return NULL;
  }

  return PyFloat_FromDouble( CALLABLE( sim )->Cosine( sim, v1, v2 ) );
}



/******************************************************************************
 * PyVGX_Vector__EuclideanDistance
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EuclideanDistance__doc__,
  "EuclideanDistance() -> float\n"
);
static PyObject * PyVGX_Vector__EuclideanDistance( PyObject *py_self, PyObject *py_other ) {
  vgx_Vector_t *v1, *v2;
  vgx_Similarity_t *sim = __vector_simcontext( py_self, py_other, &v1, &v2 );
  if( sim == NULL ) {
    return NULL;
  }

  return PyFloat_FromDouble( CALLABLE( sim )->EuclideanDistance( sim, v1, v2 ) );
}



/******************************************************************************
 * PyVGX_Vector__Debug
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Debug__doc__,
  "Debug() -> None\n"
);
static PyObject * PyVGX_Vector__Debug( PyVGX_Vector *pyvector ) {
  if( pyvector->vint ) {
    iPyVGXDebug.PrintVectorAllocator( pyvector->vint );
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Vector__repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vector__repr( PyVGX_Vector *pyvector ) {
  PyObject *py_repr = NULL;
  vgx_Vector_t *vint = pyvector->vint;
  vgx_Vector_t *vext = pyvector->vext;
  if( vint && vext ) {
    vgx_Vector_vtable_t *ivector = CALLABLE(vext);
    
    PyObject *py_external = iPyVGXBuilder.ExternalVector( vext );
    if( py_external ) {
      PyObject *py_str = PyObject_Str( py_external );
      if( py_str ) {
        const char *elem_str = PyVGX_PyObject_AsString( py_str );
        CString_t *CSTR__repr = NULL;
        BEGIN_PYVGX_THREADS {
          const char *type_str;
          if( igraphfactory.EuclideanVectors() ) {
            type_str = "euclidean";
          }
          else {
            type_str = "feature";
          }
          float alpha = ivector->Scaler( vint );
          CSTR__repr = CStringNewFormat( "<PyVGX_Vector: typ=%s len=%d mag=%#g alpha=%#g fp=0x%016llX elem=%s>",
                                                             type_str,
                                                                    ivector->Length(vext),
                                                                           ivector->Magnitude(vext),
                                                                                     alpha,
                                                                                              ivector->Fingerprint(vext),
                                                                                                           elem_str
                          );
        } END_PYVGX_THREADS;
        if( CSTR__repr ) {
          py_repr = PyUnicode_FromString( CStringValue( CSTR__repr ) );
          CStringDelete( CSTR__repr );
        }
        Py_DECREF( py_str );
      }
    }
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vector cannot be accessed" );
  }

  return py_repr;
}



/******************************************************************************
 * PyVGX_Vector__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Vector__dealloc( PyVGX_Vector *pyvector ) {
  if( _registry_loaded ) {
    vgx_Vector_t *vector = NULL;
    if( (vector = pyvector->vext) != NULL ) {
      if( COMLIB_OBJECT_CLASSMATCH( vector, COMLIB_CLASS_CODE( vgx_Vector_t ) ) ) {
        BEGIN_PYVGX_THREADS {
          CALLABLE( vector )->Decref( vector );
        } END_PYVGX_THREADS;
      }
    }
    if( (vector = pyvector->vint) != NULL ) {
      if( COMLIB_OBJECT_CLASSMATCH( vector, COMLIB_CLASS_CODE( vgx_Vector_t ) ) ) {
        BEGIN_PYVGX_THREADS {
          CALLABLE( vector )->Decref( vector );
        } END_PYVGX_THREADS;
      }
    }
  }
  Py_TYPE( pyvector )->tp_free( pyvector );
}



/******************************************************************************
 * __new_PyVGX_Vector
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __new_PyVGX_Vector( void ) {
  PyVGX_Vector *pyvector;

  if( (pyvector = (PyVGX_Vector*)p_PyVGX_Vector__VectorType->tp_alloc(p_PyVGX_Vector__VectorType, 0)) == NULL ) {
    return NULL;
  }

  pyvector->vext = NULL;
  pyvector->vint = NULL;
  pyvector->origin_is_ext = 0;

  return (PyObject *)pyvector;
}




/******************************************************************************
 * PyVGX_Vector__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Vector__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  return __new_PyVGX_Vector();
}


/******************************************************************************
 * __init_PyVGX_Vector_internal
 *
 ******************************************************************************
 */
static int __init_PyVGX_Vector_internal( PyVGX_Vector *pyvector, vgx_Vector_t *vector ) {
  int ret = 0;

  vgx_Vector_t *vint = NULL;
  vgx_Vector_t *vext = NULL;
  vgx_Vector_t *translated = NULL;

  BEGIN_PYVGX_THREADS {
    XTRY {
      if( vector->metas.flags.eph ) {
        CALLABLE( vector )->Incref( vector );
      }
      else {
        if( (vector = CALLABLE( vector )->Clone( vector, true )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }
      }

      if( CALLABLE( vector )->IsExternal( vector ) ) {
        vext = vector;
      }
      else {
        vint = vector;
      }

      vgx_Similarity_t *simcontext = CALLABLE( vector )->Context( vector )->simobj;

      // Use the same ephemeral/persistent mode as the source
      if( (translated = CALLABLE( simcontext )->TranslateVector( simcontext, vector, vector->metas.flags.eph, NULL )) == NULL ) {
        CALLABLE(vector)->Decref(vector);
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    } 
    XCATCH(errcode) {
      ret = -1;
    }
    XFINALLY {
    }

  } END_PYVGX_THREADS;

  if( vext ) {
    pyvector->origin_is_ext = 1;
    pyvector->vext = vext;
    pyvector->vint = translated;
  }
  else {
    pyvector->origin_is_ext = 0;
    pyvector->vext = translated;
    pyvector->vint = vint;
  }

  if( ret < 0 ) {
    PyErr_SetString( PyExc_Exception, "unable to initialize vector object" );
  }

  return ret;
}



/******************************************************************************
 * __init_PyVGX_Vector
 *
 ******************************************************************************
 */
static int __init_PyVGX_Vector( PyVGX_Vector *pyvector, PyVGX_Similarity *pysim, PyObject *py_data, PyObject *py_alpha ) {
  vgx_Similarity_t *simcontext = pysim ? pysim->sim : _global_default_similarity->sim;
  int retcode = 0;
  vgx_Vector_t *vector = NULL;
  vgx_Vector_t *translated = NULL;
  CString_t *CSTR__error = NULL;

  // ------------------------------------------
  // 1. Extract or generate vgx_Vector_t object
  // ------------------------------------------

  // NULL vector
  if( py_data == NULL ) {
    if( (vector = CALLABLE( simcontext )->NewInternalVector( simcontext, NULL, 1.0f, 0, true )) == NULL ) {
      PyErr_SetString( PyExc_MemoryError, "Failed to allocate internal null-vector" );
      return -1;
    }
    pyvector->vint = vector;
    pyvector->origin_is_ext = 0;
  }
  // Vector data supplied
  else {
    // Vector data supplied as vgx_Vector_t in a capsule and may be used directly or cloned
    if( PyCapsule_CheckExact( py_data ) ) {
      vector = PyCapsule_GetPointer( py_data, NULL );
      if( vector == NULL || !vgx_Vector_t_CheckExact( vector ) ) {
        PyVGXError_SetString( PyExc_ValueError, "Data capsule does not contain a vector object" );
        return -1;
      }
      BEGIN_PYVGX_THREADS {
        if( vector->metas.flags.eph ) {
          CALLABLE( vector )->Incref( vector );
        }
        else {
          vector = CALLABLE( vector )->Clone( vector, true );
        }
      } END_PYVGX_THREADS;
      if( vector == NULL ) {
        PyErr_SetString( PyExc_MemoryError, "Vector ownership error" );
        return -1;
      }
      if( CALLABLE( vector )->IsExternal( vector ) ) {
        pyvector->origin_is_ext = 1;
        pyvector->vext = vector;
      }
      else {
        pyvector->origin_is_ext = 0;
        pyvector->vint = vector;
      }
    }
    // Vector data supplied in the form of another PyVGX_Vector
    else if( PyVGX_Vector_CheckExact( py_data ) ) {
      const PyVGX_Vector *py_source = (PyVGX_Vector*)py_data;
      vector = py_source->vint;
      BEGIN_PYVGX_THREADS {
        CALLABLE( vector )->Incref( vector );
      } END_PYVGX_THREADS;
      pyvector->origin_is_ext = 0;
      pyvector->vint = vector;
    }
    // Data is (presumably) a python list or bytes
    else {
      if( (vector = iPyVGXParser.InternalVectorFromPyObject( simcontext, py_data, py_alpha, true )) == NULL ) {
        return -1;
      }
      pyvector->origin_is_ext = 1;
      pyvector->vint = vector;
    }
  }

  // ---------------------------------------------------
  // 2. Translate the provided vector to its counterpart
  // ---------------------------------------------------
  BEGIN_PYVGX_THREADS {
    // Use the same ephemeral/persistent mode as the source
    if( (translated = CALLABLE( simcontext )->TranslateVector( simcontext, vector, vector->metas.flags.eph, &CSTR__error )) == NULL ) {
      CALLABLE(vector)->Decref(vector);
    }
  } END_PYVGX_THREADS;

  if( translated == NULL ) {
    if( CSTR__error ) {
      PyVGXError_SetString( PyVGX_EnumerationError, CStringValue( CSTR__error ) );
      CStringDelete( CSTR__error );
    }
    else if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyExc_Exception, "Internal vector translation error" );
    }
    return -1;
  }

  if( pyvector->vext == NULL ) {
    pyvector->vext = translated;
  }
  else {
    pyvector->vint = translated;
  }

  return retcode;

}



/******************************************************************************
 * PyVGX_Vector__init
 *
 ******************************************************************************
 */
static int PyVGX_Vector__init( PyVGX_Vector *pyvector, PyObject *args, PyObject *kwds ) {

  static char *kwlist[] = { "data", "alpha", NULL };

  PyObject *py_data = NULL;
  PyObject *py_alpha = NULL;
  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist, &py_data, &py_alpha ) ) {
    return -1;
  }

  return __init_PyVGX_Vector( pyvector, NULL, py_data, py_alpha );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Vector__vectorcall( PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "data",
    "alpha",
    NULL
  };

  typedef union u_vector_args {
    PyObject *_args[2];
    struct {
      PyObject *py_data;
      PyObject *py_alpha;
    };
  } vector_args;

  vector_args vcargs = {0};

  int64_t nargs = PyVectorcall_NARGS( nargsf );

  PyVGX_Similarity *pysim = NULL;
  if( (nargsf & PY_VECTORCALL_ARGUMENTS_OFFSET) ) {
    PyObject *self = *(args-1);
    if( self && PyVGX_Similarity_CheckExact( self ) ) {
      pysim = (PyVGX_Similarity*)self;
    }
  }
  
  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs._args ) < 0 ) {
    return NULL;
  }

  PyObject *pyobj = __new_PyVGX_Vector();
  if( pyobj ) {
    if( __init_PyVGX_Vector( (PyVGX_Vector*)pyobj, pysim, vcargs.py_data, vcargs.py_alpha ) < 0 ) {
      PyVGX_Vector__dealloc( (PyVGX_Vector*)pyobj );
      return NULL;
    }
  }
  return pyobj;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
DLL_HIDDEN extern PyObject * PyVGX_Vector__FromVector( vgx_Vector_t *vector ) {
  PyObject *pyobj = __new_PyVGX_Vector();
  if( !pyobj ) {
    return NULL;
  }

  if( __init_PyVGX_Vector_internal( (PyVGX_Vector*)pyobj, vector ) < 0 ) {
    return NULL;
  }

  return pyobj;
}



/******************************************************************************
 * nb_add_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_add_PyVGX_Vector( PyObject *py_op1, PyObject *py_op2 ) {
  if( !igraphfactory.EuclideanVectors() ) {
    PyErr_SetString( PyExc_Exception, "Vector operation not supported" );
    return NULL;
  }
  vgx_Vector_t *v1, *v2;
  vgx_Similarity_t *sim = __vector_simcontext( py_op1, py_op2, &v1, &v2 );
  if( sim == NULL ) {
    return NULL;
  }
  CString_t *CSTR__error = NULL;
  vgx_Vector_t *v3;

  BEGIN_PYVGX_THREADS {
    v3 = CALLABLE( sim )->VectorArithmetic( sim, v1, v2, false, &CSTR__error );
  } END_PYVGX_THREADS;

  if( v3 == NULL ) {
    PyErr_Format( PyExc_Exception, "%s", CSTR__error ? CStringValue(CSTR__error) : "?" );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  PyObject *py_ret = PyVGX_Vector__FromVector( v3 );
  CALLABLE( v3 )->Decref( v3 );
  return py_ret;
}



/******************************************************************************
 * nb_subtract_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_subtract_PyVGX_Vector( PyObject *py_op1, PyObject *py_op2 ) {
  if( !igraphfactory.EuclideanVectors() ) {
    PyErr_SetString( PyExc_Exception, "Vector operation not supported" );
    return NULL;
  }
  vgx_Vector_t *v1, *v2;
  vgx_Similarity_t *sim = __vector_simcontext( py_op1, py_op2, &v1, &v2 );
  if( sim == NULL ) {
    return NULL;
  }

  CString_t *CSTR__error = NULL;
  vgx_Vector_t *v3;

  BEGIN_PYVGX_THREADS {
    v3 = CALLABLE( sim )->VectorArithmetic( sim, v1, v2, true, &CSTR__error );
  } END_PYVGX_THREADS;

  if( v3 == NULL ) {
    PyErr_Format( PyExc_Exception, "%s", CSTR__error ? CStringValue(CSTR__error) : "?" );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  PyObject *py_ret = PyVGX_Vector__FromVector( v3 );
  CALLABLE( v3 )->Decref( v3 );
  return py_ret;

}



/******************************************************************************
 * nb_multiply_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_multiply_PyVGX_Vector( PyObject *py_op1, PyObject *py_op2 ) {
  bool isvec1 = PyVGX_Vector_CheckExact( py_op1 );
  bool isvec2 = PyVGX_Vector_CheckExact( py_op2 );

  vgx_Vector_t *v1 = NULL;
  vgx_Vector_t *v2 = NULL;
  PyObject *py_factor = NULL;
  // We'll be doing dot product
  if( isvec1 && isvec2 ) {
    v1 = ((PyVGX_Vector*)py_op1)->vint;
    v2 = ((PyVGX_Vector*)py_op2)->vint;
  }
  // Expect 2nd to be number
  else if( isvec1 ) {
    v1 = ((PyVGX_Vector*)py_op1)->vint;
    py_factor = py_op2;
  }
  // Expect 1st to be number
  else if( isvec2 ) {
    v1 = ((PyVGX_Vector*)py_op2)->vint;
    py_factor = py_op1;
  }
  // Error
  else {
    PyErr_SetString( PyExc_TypeError, "vectors or scalar required" );
    return NULL;
  }

  if( v1 == NULL || !igraphfactory.EuclideanVectors() ) {
    PyErr_SetString( PyExc_Exception, "vector operation not supported" );
    return NULL;
  }

  vgx_Similarity_t *sim = CALLABLE( v1 )->Context( v1 )->simobj;
  if( sim == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  // Scalar multiplication
  double factor = 0.0;
  if( py_factor ) {
    if( PyFloat_Check( py_factor ) ) {
      factor = PyFloat_AsDouble( py_factor );
    }
    else if( PyLong_Check( py_factor ) ) {
      factor = (double)PyLong_AsLongLong( py_factor );
    }
    else {
      PyErr_SetString( PyExc_TypeError, "a numeric factor is required" );
      return NULL;
    }
    CString_t *CSTR__error = NULL;
    vgx_Vector_t *vret;

    BEGIN_PYVGX_THREADS {
      vret = CALLABLE( sim )->VectorScalarMultiply( sim, v1, factor, &CSTR__error );
    } END_PYVGX_THREADS;

    if( vret == NULL || CSTR__error ) {
      PyErr_Format( PyExc_ValueError, "%s", CSTR__error ? CStringValue( CSTR__error ) : "?" );
      iString.Discard( &CSTR__error );
      return NULL;
    }
    PyObject *py_ret = PyVGX_Vector__FromVector( vret );
    CALLABLE( vret )->Decref( vret );
    return py_ret;
  }
  // Dot product
  else if( v2 && CALLABLE( v2 )->Context( v2 )->simobj == sim ) {
    CString_t *CSTR__error = NULL;
    double dp;

    BEGIN_PYVGX_THREADS {
      dp = CALLABLE( sim )->VectorDotProduct( sim, v1, v2, &CSTR__error );
    } END_PYVGX_THREADS;

    if( CSTR__error ) {
      PyErr_Format( PyExc_ValueError, "%s", CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
      return NULL;
    }
    return PyFloat_FromDouble( dp );
  }
  // Error
  else {
    PyErr_SetString( PyExc_TypeError, "incompatible similarity contexts" );
    return NULL;
  }
}



/******************************************************************************
 * nb_negative_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_negative_PyVGX_Vector( PyObject *py_vector ) {
  return nb_multiply_PyVGX_Vector( py_vector, g_py_minus_one );
}



/******************************************************************************
 * nb_positive_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_positive_PyVGX_Vector( PyObject *py_vector ) {
  Py_INCREF( py_vector );
  return py_vector;
}



/******************************************************************************
 * nb_absolute_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * nb_absolute_PyVGX_Vector( PyVGX_Vector *py_vector ) {
  return PyFloat_FromDouble( py_vector->vext->metas.scalar.norm );
}



/******************************************************************************
 * sq_length_PyVGX_Vector
 *
 ******************************************************************************
 */
static Py_ssize_t sq_length_PyVGX_Vector( PyVGX_Vector *pyvector ) {
  vgx_Vector_t *vector = pyvector->vext;
  return CALLABLE(vector)->Length(vector);
}



/******************************************************************************
 * sq_item_PyVGX_Vector
 *
 ******************************************************************************
 */
static PyObject * sq_item_PyVGX_Vector( PyVGX_Vector *pyvector, Py_ssize_t i ) {
  vgx_Vector_t *vector = pyvector->vext;

  ext_vector_feature_t * elem = CALLABLE(vector)->Elements(vector);
  int sz = vector->metas.vlen;
  if( i >= 0 && i < sz ) {
    PyObject *py_term = PyUnicode_FromString( (elem+i)->term );
    PyObject *py_weight = PyFloat_FromDouble( (elem+i)->weight ); 
    PyObject *py_tuple = PyTuple_New(2);
    if( py_tuple && py_weight && py_term ) {
      PyTuple_SET_ITEM( py_tuple, 0, py_term );
      PyTuple_SET_ITEM( py_tuple, 1, py_weight );
      return py_tuple; // ok
    }
    else {
      PyVGX_XDECREF( py_term );
      PyVGX_XDECREF( py_weight );
      PyVGX_XDECREF( py_tuple );
    }
  }
  else {
    PyErr_SetNone( PyExc_IndexError );
  }

  return NULL; // error
}



/******************************************************************************
 * PyVGX_Vector__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Vector__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Vector__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_Vector__getset[] = {
  {"length",       (getter)__PyVGX_Vector__length,       (setter)NULL,   "vector length",      NULL },
  {"magnitude",    (getter)__PyVGX_Vector__magnitude,    (setter)NULL,   "vector magnitude",   NULL },
  {"fingerprint",  (getter)__PyVGX_Vector__fingerprint,  (setter)NULL,   "vector fingerprint", NULL },
  {"ident",        (getter)__PyVGX_Vector__ident,        (setter)NULL,   "vector identity",    NULL },
  {"external",     (getter)__PyVGX_Vector__external,     (setter)NULL,   "external vector",    NULL },
  {"internal",     (getter)__PyVGX_Vector__internal,     (setter)NULL,   "internal vector",    NULL },
  {"array",        (getter)__PyVGX_Vector__array,        (setter)NULL,   "internal vector array",    NULL },
  {"alpha",        (getter)__PyVGX_Vector__alpha,        (setter)NULL,   "internal vector scaling factor",    NULL },
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PyNumberMethods tp_as_number_PyVGX_Vector = {
    .nb_add             = (binaryfunc)nb_add_PyVGX_Vector,
    .nb_subtract        = (binaryfunc)nb_subtract_PyVGX_Vector,
    .nb_multiply        = (binaryfunc)nb_multiply_PyVGX_Vector,
    .nb_negative        = (unaryfunc)nb_negative_PyVGX_Vector,
    .nb_positive        = (unaryfunc)nb_positive_PyVGX_Vector,
    .nb_absolute        = (unaryfunc)nb_absolute_PyVGX_Vector
};



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static PySequenceMethods tp_as_sequence_PyVGX_Vector = {
    .sq_length          = (lenfunc)sq_length_PyVGX_Vector,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = (ssizeargfunc)sq_item_PyVGX_Vector,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = 0,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0
};



/******************************************************************************
 * PyVGX_Vector__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Vector__methods[] = {

    {"AsDict",            (PyCFunction)PyVGX_Vector__AsDict,            METH_NOARGS,                  AsDict__doc__  },
    {"Fingerprint",       (PyCFunction)PyVGX_Vector__Fingerprint,       METH_VARARGS,                 Fingerprint__doc__  },

    {"Projections",       (PyCFunction)PyVGX_Vector__Projections,       METH_VARARGS | METH_KEYWORDS, Projections__doc__  },

    {"Cosine",            (PyCFunction)PyVGX_Vector__Cosine,            METH_O,                       Cosine__doc__  },
    {"EuclideanDistance", (PyCFunction)PyVGX_Vector__EuclideanDistance, METH_O,                       EuclideanDistance__doc__  },

    {"Debug",             (PyCFunction)PyVGX_Vector__Debug,             METH_NOARGS,                  Debug__doc__  },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS


/******************************************************************************
 * PyVGX_Vector__VectorType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Vector__VectorType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Vector",
    .tp_basicsize       = sizeof(PyVGX_Vector),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Vector__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Vector__repr,
    .tp_as_number       = &tp_as_number_PyVGX_Vector,
    .tp_as_sequence     = &tp_as_sequence_PyVGX_Vector,
    .tp_as_mapping      = 0,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Vector objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Vector__methods,
    .tp_members         = PyVGX_Vector__members,
    .tp_getset          = PyVGX_Vector__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Vector__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Vector__new,
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
    .tp_vectorcall      = (vectorcallfunc)PyVGX_Vector__vectorcall

};


DLL_HIDDEN PyTypeObject * p_PyVGX_Vector__VectorType = &PyVGX_Vector__VectorType;


