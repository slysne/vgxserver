/*######################################################################
 *#
 *# pyvgx_sim.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );




static unsigned short LSH_SEEDS[] = {
    0,
    1087,
    1381,
    1663,
    1993,
    2293,
    2621,
    2909
};






/******************************************************************************
 * __PyVGX_Similarity__set_int_member
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_int_member( PyVGX_Similarity *pysim, PyObject *pyval, int *dest, int min, int max ) {
  if( pyval == NULL ) {
    PyVGXError_SetString( PyExc_TypeError, "cannot delete parameter" );
    return -1;
  }
  int val = PyLong_AsLong( pyval );
  if( val == -1 && PyErr_Occurred() ) {
    return -1;
  }
  if( val < min || val > max ) {
    PyErr_Format( PyExc_ValueError, "parameter out of range, valid range is %d - %d", min, max );
    return -1;
  }
  *dest = val;
  return 0;
}



/******************************************************************************
 * __PyVGX_Similarity__set_float_member
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_float_member( PyVGX_Similarity *pysim, PyObject *pyval, float *dest, float min, float max ) {
  if( pyval == NULL ) {
    PyVGXError_SetString( PyExc_TypeError, "cannot delete parameter" );
    return -1;
  }
  double val = PyFloat_AsDouble( pyval );
  if( val == -1.0 && PyErr_Occurred() ) {
    return -1;
  }
  if( val < min || val > max ) {
    CString_t *CSTR__msg = CStringNewFormat( "parameter out of range, value range is %#g - %#g", min, max );
    PyErr_SetString( PyExc_ValueError, CSTR__msg ? CStringValue( CSTR__msg ) : "parameter out of range" );
    iString.Discard( &CSTR__msg );
    return -1;
  }
  *dest = (float)val;
  return 0;
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_hamming_threshold
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_hamming_threshold( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  return __PyVGX_Similarity__set_int_member( pysim, pyval, &pysim->sim->params.threshold.hamming, 0, 64 );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_hamming_threshold( PyVGX_Similarity *pysim, void *closure ) {
  return PyLong_FromLong( pysim->sim->params.threshold.hamming );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_sim_threshold
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_sim_threshold( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  return __PyVGX_Similarity__set_float_member( pysim, pyval, &pysim->sim->params.threshold.similarity, 0.0f, 1.0f );
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_sim_threshold( PyVGX_Similarity *pysim, void *closure ) {
  return PyFloat_FromDouble( pysim->sim->params.threshold.similarity );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_min_isect
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_min_isect( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  int retcode = __PyVGX_Similarity__set_int_member( pysim, pyval, &pysim->sim->params.vector.min_intersect, 0, pysim->sim->params.vector.max_size );
  return retcode;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_min_isect( PyVGX_Similarity *pysim, void *closure ) {
  return PyLong_FromLong( pysim->sim->params.vector.min_intersect );
}



/******************************************************************************
 * __PyVGX_Similarity__get_max_size
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_max_size( PyVGX_Similarity *pysim, void *closure ) {
  return PyLong_FromLong( pysim->sim->params.vector.max_size );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_cosine_exp
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_cosine_exp( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  int retcode = __PyVGX_Similarity__set_float_member( pysim, pyval, &pysim->sim->params.vector.cosine_exponent, 0.0f, 1.0f );
  pysim->sim->params.vector.jaccard_exponent = 1.0f - pysim->sim->params.vector.cosine_exponent;
  return retcode;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_cosine_exp( PyVGX_Similarity *pysim, void *closure ) {
  return PyFloat_FromDouble( pysim->sim->params.vector.cosine_exponent );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_jaccard_exp
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_jaccard_exp( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  int retcode = __PyVGX_Similarity__set_float_member( pysim, pyval, &pysim->sim->params.vector.jaccard_exponent, 0.0f, 1.0f );
  pysim->sim->params.vector.cosine_exponent = 1.0f - pysim->sim->params.vector.jaccard_exponent;
  return retcode;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_jaccard_exp( PyVGX_Similarity *pysim, void *closure ) {
  return PyFloat_FromDouble( pysim->sim->params.vector.jaccard_exponent );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_min_cosine
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_min_cosine( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  int retcode = __PyVGX_Similarity__set_float_member( pysim, pyval, &pysim->sim->params.vector.min_cosine, 0.0f, 1.0f );
  return retcode;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_min_cosine( PyVGX_Similarity *pysim, void *closure ) {
  return PyFloat_FromDouble( pysim->sim->params.vector.min_cosine );
}



/******************************************************************************
 * __PyVGX_Similarity__GET/SET_min_jaccard
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __PyVGX_Similarity__set_min_jaccard( PyVGX_Similarity *pysim, PyObject *pyval, void *closure ) {
  int retcode = __PyVGX_Similarity__set_float_member( pysim, pyval, &pysim->sim->params.vector.min_jaccard, 0.0f, 1.0f );
  return retcode;
}

SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_min_jaccard( PyVGX_Similarity *pysim, void *closure ) {
  return PyFloat_FromDouble( pysim->sim->params.vector.min_jaccard );
}



/******************************************************************************
 * __PyVGX_Similarity__get_fp_nsegm
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_fp_nsegm( PyVGX_Similarity *pysim, void *closure ) {
  return PyLong_FromLong( pysim->sim->params.fingerprint.nsegm );
}



/******************************************************************************
 * __PyVGX_Similarity__get_fp_nsign
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_fp_nsign( PyVGX_Similarity *pysim, void *closure ) {
  return PyLong_FromLong( pysim->sim->params.fingerprint.nsign );
}



/******************************************************************************
 * __PyVGX_Similarity__get_fp_seeds
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __PyVGX_Similarity__get_fp_seeds( PyVGX_Similarity *pysim, void *closure ) {
  if( pysim->py_lsh_seeds == NULL ) {
    PyErr_SetString( PyExc_Exception, "similarity object has no fingerprint seeds" );
  }
  else {
    Py_INCREF( pysim->py_lsh_seeds );
  }
  return pysim->py_lsh_seeds;
}



/******************************************************************************
 * __PyVGX_Similarity_as_dict
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Similarity_as_dict( PyVGX_Similarity *pysim ) {
  PyObject *py_dict = PyDict_New();
  if( py_dict ) {
    vgx_Similarity_config_t *p = &pysim->sim->params;

    if( PyVGX_DictStealItemString( py_dict, "nsegm",               PyLong_FromLong( p->fingerprint.nsegm ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "nsign",               PyLong_FromLong( p->fingerprint.nsign ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "max_vector_size",     PyLong_FromLong( p->vector.max_size ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "min_isect",           PyLong_FromLong( p->vector.min_intersect ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "min_cosine",          PyFloat_FromDouble( p->vector.min_cosine ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "min_jaccard",         PyFloat_FromDouble( p->vector.min_jaccard ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "cosine_exp",          PyFloat_FromDouble( p->vector.cosine_exponent ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "jaccard_exp",         PyFloat_FromDouble( p->vector.jaccard_exponent ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "hamming_threshold",   PyLong_FromLong( p->threshold.hamming ) ) < 0 ||
        PyVGX_DictStealItemString( py_dict, "sim_threshold",       PyFloat_FromDouble( p->threshold.similarity ) ) < 0 )
    {
      // error
      PyVGX_DECREF( py_dict );
      py_dict = NULL;
    }
  }
  return py_dict;
}



/******************************************************************************
 * PyVGX_Similarity__rvec
 *
 ******************************************************************************
 */
PyDoc_STRVAR( rvec__doc__,
  "rvec( n ) -> random vector with n elements\n"
  "\n"
  "\n"
);
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Similarity__rvec( PyVGX_Similarity *py_sim, PyObject *py_n ) {
  if( !igraphfactory.EuclideanVectors() ) {
    PyErr_SetString( PyExc_TypeError, "Euclidean mode required" );
    return NULL;
  }

  Py_ssize_t n;
  if( !PyLong_Check( py_n ) || (n =PyLong_AsLongLong( py_n )) < 1 ) {
    PyErr_SetString( PyExc_TypeError, "a positive integer is required" );
    return NULL;
  }

  vgx_Similarity_t *sim = py_sim->sim;
  CString_t *CSTR__error = NULL;
  uint16_t sz = (uint16_t)n;
  vgx_Vector_t *vector = NULL;

  BEGIN_PYVGX_THREADS {
    float *rval = malloc( sizeof(float) * sz );
    if( rval ) {
      float *p = rval;
      float *end = rval + sz;
      while( p < end ) {
        *p++ = (float)(2 * randfloat() - 1);
      }
      vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, rval, sz, true, &CSTR__error );
      free( rval ); 
    }
  } END_PYVGX_THREADS;

  if( vector == NULL ) {
    PyErr_Format( PyExc_Exception, "%s", CSTR__error ? CStringValue( CSTR__error ) : "internal error" );
    iString.Discard( &CSTR__error );
    return NULL;
  }

  PyObject *py_vector = PyVGX_Vector__FromVector( vector );
  CALLABLE( vector )->Decref( vector );
  return py_vector;
  
}



/******************************************************************************
 * PyVGX_Similarity__NewVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( NewVector__doc__,
  "NewVector( elements_list ) -> Vector\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__NewVector( PyVGX_Similarity *py_sim, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "data",
    "alpha",
    NULL
  };

  typedef union u_vector_args {
    PyObject *args[3];
    struct {
      PyObject *py_sim;
      PyObject *py_data;
      PyObject *py_alpha;
    };
  } vector_args;

  vector_args vcargs = {0};

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs.args+1 ) < 0 ) {
    return NULL;
  }

  vcargs.py_sim = (PyObject*)py_sim;

  PyObject *py_vector = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vector__VectorType, vcargs.args+1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );

  return py_vector;

}



/******************************************************************************
 * PyVGX_Similarity__NewCentroid
 *
 ******************************************************************************
 */
PyDoc_STRVAR( NewCentroid__doc__,
  "NewCentroid( [vector_list] ) -> Centroid\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__NewCentroid( PyVGX_Similarity *py_sim, PyObject *py_vectors ) {

  PyObject *py_centroid = NULL;

  if( !PySequence_Check( py_vectors ) ) {
    PyErr_SetString( PyExc_TypeError, "a sequence of vectors is required" );
    return NULL;
  }

  vgx_Similarity_t *simcontext = py_sim->sim;
  int64_t sz = PySequence_Size( py_vectors );
  vgx_Vector_t **vectors = NULL;
  vgx_Vector_t *centroid = NULL;

  XTRY {
    // Allocate vector list
    if( (vectors = calloc( sz+1, sizeof( vgx_Vector_t* ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x601 );
    }
    // Parse and populate vector list
    for( int64_t i=0; i<sz; i++ ) {
      PyObject *py_vector = PySequence_GetItem( py_vectors, i );
      if( !py_vector ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x602 );
      }
      vectors[ i ] = iPyVGXParser.InternalVectorFromPyObject( simcontext, py_vector, NULL, true );
      Py_DECREF( py_vector );
      if( vectors[ i ] == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x603 );
      }
    }
    // Generate centroid
    BEGIN_PYVGX_THREADS {
      centroid = CALLABLE( simcontext )->NewCentroid( simcontext, (const vgx_Vector_t**)vectors, true );
    } END_PYVGX_THREADS;
    // Create Python centroid
    if( centroid == NULL ) {
      PyVGXError_SetString( PyExc_Exception, "Centroid computation failed" );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x604 );
    }

    typedef union u_vector_args {
      PyObject *args[3];
      struct {
        PyObject *py_sim;
        PyObject *py_data;
        PyObject *py_alpha;
      };
    } vector_args;

    PyObject *py_datacapsule = PyVGX_PyCapsule_NewNoErr( centroid, NULL, NULL );
    vector_args vcargs = {
      .py_sim = (PyObject*)py_sim,
      .py_data = py_datacapsule,
      .py_alpha = NULL
    };

    py_centroid = PyObject_Vectorcall( (PyObject*)p_PyVGX_Vector__VectorType, vcargs.args+1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL );
    Py_XDECREF( py_datacapsule );

  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    BEGIN_PYVGX_THREADS {
      if( vectors ) {
        vgx_Vector_t *vector;
        for( int64_t i=0; i<sz; i++ ) {
          if( (vector = vectors[i]) != NULL ) {
            CALLABLE( vector )->Decref( vector );
          }
        }
        free( vectors );
      }
    } END_PYVGX_THREADS;

  }

  return py_centroid;
}



/******************************************************************************
 * PyVGX_Similarity__Fingerprint
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Fingerprint__doc__,
  "Fingerprint( vector[, seed] ) -> int\n"
  "\n"
  "\n"
);
DLL_HIDDEN PyObject * PyVGX_Similarity__Fingerprint( PyObject *pysim, PyObject *args ) {
  // Ensure fingerprinter
  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  vgx_Fingerprinter_t *fingerprinter = sim ? sim->fingerprinter : NULL;
  if( fingerprinter == NULL ) {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }

  // Parse args
  PyObject *py_vector = NULL;
  int64_t seed = 0;
  if( !PyArg_ParseTuple( args, "O|L", &py_vector, &seed ) ) {
    return NULL;
  }

  // Get vector (own ref)
  vgx_Vector_t *vector = iPyVGXParser.InternalVectorFromPyObject( sim, py_vector, NULL, true );
  if( vector == NULL ) {
    return NULL;
  }

  // Compute fingerprint
  FP_t fp = CALLABLE( fingerprinter )->Compute( fingerprinter, vector, seed, NULL );

  // Release ref
  CALLABLE( vector )->Decref( vector );

  return PyLong_FromUnsignedLongLong( fp );
}



/******************************************************************************
 * PyVGX_Similarity__Projections
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Projections__doc__,
  "Projections( seed ) -> list\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__Projections( PyObject *pysim, PyObject *py_seed ) {
  if( !PyLong_CheckExact( py_seed ) ) {
    PyErr_SetString( PyExc_TypeError, "seed must be int" );
    return NULL;
  }

  uint64_t seed = PyLong_AsUnsignedLongLong( py_seed );
  if( seed > 0xFFF ) {
    PyErr_SetString( PyExc_ValueError, "seed must be <= 0xFFF" );
    return NULL;
  }

  PyVGX_Similarity *pyvgx_sim = (PyVGX_Similarity*)pysim;


  unsigned nproj = pyvgx_sim->sim->params.fingerprint.ann.nprojections;
  unsigned kmax = 1U << pyvgx_sim->sim->params.fingerprint.ann.ksize;
  unsigned nkeys = nproj * kmax;

  PyObject *py_projections = PyList_New( nkeys );
  if( py_projections == NULL ) {
    return NULL;
  }

  PyObject *py_part;
  char buffer[16];
  for( unsigned i=0; i<nkeys; ++i ) {
    snprintf( buffer, 15, "_%04X|%03X", i, (unsigned)(seed & 0xFFF) );
    if( (py_part = PyBytes_FromString( buffer )) == NULL ) {
      Py_DECREF( py_projections );
      return NULL;
    }
    PyList_SET_ITEM( py_projections, i, py_part );
  }

  return py_projections;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __delete_index_projection_subset( vgx_Similarity_t *sim, int sn, int ux, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Graph_t *graph = sim->parent;
  int ret = 0;

  vgx_IGraphSimple_t *Simple = CALLABLE( graph )->simple;

  CString_t *CSTR__name = NULL;

  XTRY {
    int ksize = sim->params.fingerprint.ann.ksize;
    int step = 0;
    switch( sim->params.fingerprint.ann.ksize ) {
    case 8:
      step = 4;
      break;
    case 10:
      step = 5;
      break;
    case 12:
      step = 4;
      break;
    default:
      THROW_ERROR( CXLIB_ERR_API, 0x000 );
    }

    unsigned short offset = (unsigned short)ux << ksize;
    unsigned short kmax = 1U << ksize;
    for( unsigned short kx=0; kx<kmax; ++kx ) {
      unsigned short z = offset + kx;
      unsigned short seed = LSH_SEEDS[sn];

      if( (CSTR__name = CStringNewFormat( "_%04X|%03X", (unsigned)z, (unsigned)seed )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x000 );
      }

      // Delete projection if it exists
      if( Simple->HasVertex( graph, CSTR__name ) && Simple->DeleteVertex( graph, CSTR__name, 0, reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x000 );
      }

      iString.Discard( &CSTR__name );

    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    iString.Discard( &CSTR__name );
  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __delete_index_projection_set( vgx_Similarity_t *sim, int sn, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_Graph_t *graph = sim->parent;
  unsigned short seed = LSH_SEEDS[sn]; 

  vgx_IGraphSimple_t *Simple = CALLABLE( graph )->simple;

  CString_t *CSTR__root = NULL;
  CString_t *CSTR__setname = NULL;

  XTRY {

    // Projection root name
    if( (CSTR__root = NewEphemeralCString( graph, "_|" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    // Delete projection root if it exists
    if( Simple->HasVertex( graph, CSTR__root ) && Simple->DeleteVertex( graph, CSTR__root, 0, reason, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Projection set root name
    char buffer[16] = {0};
    snprintf( buffer, 16, "_|%03X", (unsigned)seed );
    if( (CSTR__setname = NewEphemeralCString( graph, buffer )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Delete projection set if it exists
    if( Simple->HasVertex( graph, CSTR__setname ) && Simple->DeleteVertex( graph, CSTR__setname, 0, reason, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x000 );
    }

    // Delete subsets
    int nproj = sim->params.fingerprint.ann.nprojections;
    for( int ux=0; ux<nproj; ++ux ) {
      if( __delete_index_projection_subset( sim, sn, ux, reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
      }
    }

  }
  XCATCH( errcode ) {
    if( CSTR__error && *CSTR__error == NULL ) {
      *CSTR__error = CStringNewFormat( "error %x", errcode );
    }
    ret = -1;
  }
  XFINALLY {

    iString.Discard( &CSTR__root );
    iString.Discard( &CSTR__setname );

  }

  return ret;
}



/******************************************************************************
 * PyVGX_Similarity__DeleteProjectionSets
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DeleteProjectionSets__doc__,
  "DeleteProjectionSets() -> None\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__DeleteProjectionSets( PyObject *pysim ) {
  PyVGX_Similarity *pyvgx_sim = (PyVGX_Similarity*)pysim;
  vgx_Graph_t *graph = pyvgx_sim->sim->parent;
  if( graph == NULL ) {
    PyErr_SetString( PyExc_ValueError, "no graph associated with this similarity object" );
    return NULL;
  }


  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;

  for( int sn=0; sn < (int)sizeof(LSH_SEEDS) / (int)sizeof(LSH_SEEDS[0]); sn ++ ) {
    int err;
    BEGIN_PYVGX_THREADS {
      err = __delete_index_projection_set( pyvgx_sim->sim, sn, &reason, &CSTR__error );
    } END_PYVGX_THREADS;

    if( err < 0 ) {
      iPyVGXBuilder.SetPyErrorFromAccessReason( NULL, reason, &CSTR__error );
      iString.Discard( &CSTR__error );
      return NULL;
    }
  }

  pyvgx_sim->sim->params.fingerprint.ann.nprojections = 0;
  pyvgx_sim->sim->params.fingerprint.ann.ksize = 0;

  Py_RETURN_NONE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __create_index_projection_subset( vgx_Similarity_t *sim, int sn, vgx_Vertex_t *projection_set_WL, int ux, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  vgx_Graph_t *graph = sim->parent;
  int ret = 0;

  // Seed number
  vgx_RankSetANNSeedNumber( &projection_set_WL->rank, sn );

  vgx_IGraphSimple_t *Simple = CALLABLE( graph )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( graph )->advanced;

  char type[] = "projection";
  char name[16] = {0};

  vgx_Vertex_t *projection_WL = NULL;

  XTRY {
    int ksize = sim->params.fingerprint.ann.ksize;
    int step = 0;
    switch( sim->params.fingerprint.ann.ksize ) {
    case 8:
      step = 4;
      break;
    case 10:
      step = 5;
      break;
    case 12:
      step = 4;
      break;
    default:
      THROW_ERROR( CXLIB_ERR_API, 0x000 );
    }


    unsigned short offset = (unsigned short)ux << ksize;
    unsigned short kmax = 1U << ksize;
    for( unsigned short kx=0; kx<kmax; ++kx ) {
      unsigned short z = offset + kx;
      unsigned short seed = LSH_SEEDS[sn];
      snprintf( name, 15, "_%04X|%03X", (unsigned)z, (unsigned)seed );

      if( (projection_WL = Simple->NewVertex( graph, name, type, -1, 5000, reason, CSTR__error )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }

      // Seed number
      vgx_RankSetANNSeedNumber( &projection_WL->rank, sn );

      // Right-rotation amount needed for arc-lsh
      int rr = (ux*step + ksize) % 64;
      vgx_RankSetANNArcLSHRotate( &projection_WL->rank, rr );

      if( Advanced->Connect_M_UINT_WL( graph, projection_set_WL, projection_WL, "projection", z, reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }

      // Capture rank sn/rr
      iOperation.Vertex_WL.SetRank( projection_WL, &projection_WL->rank );

      Simple->CloseVertex( graph, &projection_WL );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {

    if( projection_WL ) {
      Simple->CloseVertex( graph, &projection_WL );
    }

  }

  return ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __create_index_projection_set( vgx_Similarity_t *sim, int sn, vgx_AccessReason_t *reason, CString_t **CSTR__error ) {
  int ret = 0;
  vgx_Graph_t *graph = sim->parent;
  unsigned short seed = LSH_SEEDS[sn]; 

  vgx_IGraphSimple_t *Simple = CALLABLE( graph )->simple;
  vgx_IGraphAdvanced_t *Advanced = CALLABLE( graph )->advanced;

  CString_t *CSTR__root_type = NULL;
  CString_t *CSTR__root = NULL;
  vgx_Vertex_t *projection_root_WL = NULL;
  CString_t *CSTR__setname = NULL;
  vgx_Vertex_t *projection_set_WL = NULL;

  XTRY {
    // Projection root type name
    if( (CSTR__root_type = NewEphemeralCString( graph, "projection_root" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Projection root name
    if( (CSTR__root = NewEphemeralCString( graph, "_|" )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
    
    // Projection set root name
    char buffer[16] = {0};
    snprintf( buffer, 16, "_|%03X", (unsigned)seed );
    if( (CSTR__setname = NewEphemeralCString( graph, buffer )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Projection set already exists
    if( Simple->HasVertex( graph, CSTR__setname ) ) {
      XBREAK;
    }

    // Make sure projection root exists
    if( Simple->CreateVertex( graph, CSTR__root, CSTR__root_type, reason, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    // Create projection set
    if( (projection_set_WL = Advanced->NewVertex( graph, CSTR__setname, CSTR__root_type, 5000, reason, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    // Subsets
    int nproj = sim->params.fingerprint.ann.nprojections;
    for( int ux=0; ux<nproj; ++ux ) {
      if( __create_index_projection_subset( sim, sn, projection_set_WL, ux, reason, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
      }
    }

    // Connect ann root to projection set
    if( (projection_root_WL = Simple->OpenVertex( graph, CSTR__root, VGX_VERTEX_ACCESS_WRITABLE_NOCREATE, 5000, reason, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x009 );
    }
    if( Advanced->Connect_M_UINT_WL( graph, projection_root_WL, projection_set_WL, "subset", (unsigned)seed, reason, CSTR__error ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x00A );
    }

  }
  XCATCH( errcode ) {
    if( CSTR__error && *CSTR__error == NULL ) {
      *CSTR__error = CStringNewFormat( "error %x", errcode );
    }
    ret = -1;
  }
  XFINALLY {

    iString.Discard( &CSTR__root_type );
    iString.Discard( &CSTR__root );
    iString.Discard( &CSTR__setname );

    if( projection_set_WL ) {
      Simple->CloseVertex( graph, &projection_set_WL );
    }

    if( projection_root_WL ) {
      Simple->CloseVertex( graph, &projection_root_WL );
    }

  }

  return ret;
}



/******************************************************************************
 * PyVGX_Similarity__CreateProjectionSets
 *
 ******************************************************************************
 */
PyDoc_STRVAR( CreateProjectionSets__doc__,
  "CreateProjectionSets( nseeds, ksize ) -> list\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__CreateProjectionSets( PyObject *pysim, PyObject *args ) {

  PyVGX_Similarity *pyvgx_sim = (PyVGX_Similarity*)pysim;
  vgx_Graph_t *graph = pyvgx_sim->sim->parent;
  if( graph == NULL ) {
    PyErr_SetString( PyExc_ValueError, "no graph associated with this similarity object" );
    return NULL;
  }

  // Parse args
  int nseeds = 0;
  int ksize = 0;
  if( !PyArg_ParseTuple( args, "ii", &nseeds, &ksize ) ) {
    return NULL;
  }

  int max_seeds = sizeof( LSH_SEEDS ) / sizeof( unsigned short );

  if( nseeds < 1 || nseeds > max_seeds ) {
    PyErr_Format( PyExc_ValueError, "nseeds must be 1 - %d", max_seeds );
    return NULL;
  }

  pyvgx_sim->sim->params.fingerprint.ann.ksize = ksize;
  switch( ksize ) {
  case 8:
    pyvgx_sim->sim->params.fingerprint.ann.nprojections = 16; // 2 times 8, offset 4 (8+8)
    break;
  case 10:
    pyvgx_sim->sim->params.fingerprint.ann.nprojections = 12; // 2 times 10, offset 5 (6+6)
    break;
  case 12:
    pyvgx_sim->sim->params.fingerprint.ann.nprojections = 15; // 3 times 12, offset 4 (5+5+5)
    break;
  default:
    PyErr_SetString( PyExc_ValueError, "ksize must be 8, 10 or 12" );
    return NULL;
  }

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  CString_t *CSTR__error = NULL;

  for( int sn=0; sn<nseeds; sn++ ) {
    int err;
    BEGIN_PYVGX_THREADS {
      err = __create_index_projection_set( pyvgx_sim->sim, sn, &reason, &CSTR__error );
    } END_PYVGX_THREADS;

    if( err < 0 ) {
      iPyVGXBuilder.SetPyErrorFromAccessReason( NULL, reason, &CSTR__error );
      PyVGX_Similarity__DeleteProjectionSets( pysim );
      iString.Discard( &CSTR__error );
      return NULL;
    }

  }

  return PyList_GetSlice( pyvgx_sim->py_lsh_seeds, 0, nseeds );

}



/******************************************************************************
 * __PyVGX_Similarity__compare_vectors
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Similarity__compare_vectors( PyVGX_Similarity *pysim, PyObject *args, float (*method)( vgx_Similarity_t *sim, const vgx_Comparable_t A, const vgx_Comparable_t B ) ) {
  PyObject *py_A, *py_B;
  if( !PyArg_ParseTuple( args, "OO", &py_A, &py_B ) ) {
    return NULL;
  }
  vgx_Comparable_t A = PyVGX_PyObject_AsComparable( py_A );
  vgx_Comparable_t B = PyVGX_PyObject_AsComparable( py_B );
  if( A && B ) {
    float sim;
    BEGIN_PYVGX_THREADS {
      sim = method( pysim->sim, A, B );
    } END_PYVGX_THREADS;
    return PyFloat_FromDouble( sim );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Objects are not comparable" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Similarity__Similarity
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Similarity__doc__,
  "Similarity( v1, v2 ) -> float\n"
  "\n"
  "\n"
);
DLL_HIDDEN PyObject * PyVGX_Similarity__Similarity( PyObject *pysim, PyObject *args ) {
  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  return __PyVGX__compare_vectors( args, sim, (f_similarity_method)CALLABLE(sim)->Similarity );
}



/******************************************************************************
 * PyVGX_Similarity__EuclideanDistance
 *
 ******************************************************************************
 */
PyDoc_STRVAR( EuclideanDistance__doc__,
  "EuclideanDistance( v1, v2 ) -> float\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__EuclideanDistance( PyObject *pysim, PyObject *args ) {
  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  return __PyVGX__compare_vectors( args, sim, (f_similarity_method)CALLABLE(sim)->EuclideanDistance );
}



/******************************************************************************
 * PyVGX_Similarity__Cosine
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Cosine__doc__,
  "Cosine( v1, v2 ) -> float\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__Cosine( PyObject *pysim, PyObject *args ) {
  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  return __PyVGX__compare_vectors( args, sim, (f_similarity_method)CALLABLE(sim)->Cosine );
}



/******************************************************************************
 * PyVGX_Similarity__Jaccard
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Jaccard__doc__,
  "Jaccard( v1, v2 ) -> float\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__Jaccard( PyObject *pysim, PyObject *args ) {
  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  return __PyVGX__compare_vectors( args, sim, (f_similarity_method)CALLABLE(sim)->Jaccard );
}



/******************************************************************************
 * PyVGX_Similarity__HammingDistance
 *
 ******************************************************************************
 */
PyDoc_STRVAR( HammingDistance__doc__,
  "HammingDistance( v1, v2 ) -> int\n"
  "\n"
  "\n"
);
static PyObject * PyVGX_Similarity__HammingDistance( PyObject *pysim, PyObject *args ) {
  PyObject *py_ham = NULL;
  PyObject *py_A, *py_B;
  vgx_Comparable_t A = NULL, B = NULL;

  if( !PyArg_ParseTuple( args, "OO", &py_A, &py_B ) ) {
    return NULL;
  }

  vgx_Similarity_t *sim = ((PyVGX_Similarity*)pysim)->sim;
  PyObject *py_parent_A = __PyVGX__comparable_from_pyobject( py_A, sim, &A );
  PyObject *py_parent_B = __PyVGX__comparable_from_pyobject( py_B, sim, &B );

  if( A && B ) {
    int ham;
    BEGIN_PYVGX_THREADS {
      ham = CALLABLE(sim)->HammingDistance( sim, A, B );
    } END_PYVGX_THREADS;
    py_ham = PyLong_FromLong( ham );
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Objects are not comparable" );
  }

  PyVGX_XDECREF( py_parent_A );
  PyVGX_XDECREF( py_parent_B );

  return py_ham;
}



/******************************************************************************
 * PyVGX_Similarity__AsDict
 *
 ******************************************************************************
 */
PyDoc_STRVAR( AsDict__doc__,
  "AsDict() -> dict\n"
);
static PyObject * PyVGX_Similarity__AsDict( PyVGX_Similarity *pysim ) {
  return __PyVGX_Similarity_as_dict( pysim );
}



/******************************************************************************
 * PyVGX_Similarity__repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Similarity__repr( PyVGX_Similarity *pysim ) {
  return PyObject_Repr( __PyVGX_Similarity_as_dict( pysim ) );
}



/******************************************************************************
 * PyVGX_Similarity__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Similarity__dealloc( PyVGX_Similarity *pysim ) {
  vgx_Similarity_t *sim = pysim->sim;
  if( sim && pysim->standalone == true ) {
    BEGIN_PYVGX_THREADS {
      COMLIB_OBJECT_DESTROY( sim );
    } END_PYVGX_THREADS;
    pysim->sim = NULL;
  }
  Py_XDECREF( pysim->py_lsh_seeds );
  Py_TYPE( pysim )->tp_free( pysim );
}



/******************************************************************************
 * PyVGX_Similarity__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Similarity__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_Similarity *pysim;

  pysim = (PyVGX_Similarity*)type->tp_alloc(type, 0);
  pysim->sim = NULL;
  pysim->standalone = true;

  return (PyObject *)pysim;
}



/******************************************************************************
 * PyVGX_Similarity__init
 *
 ******************************************************************************
 */
static int PyVGX_Similarity__init( PyVGX_Similarity *pysim, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = { 
    "graph", 
    "fp_nsegm", "fp_nsign",
    "max_vector_size", "min_isect", "min_cosine", "min_jaccard", "cosine_exp", "jaccard_exp",
    "hamming_threshold", "sim_threshold",
    NULL
  };

  PyVGX_Graph *py_graph = NULL;
  vgx_Similarity_config_t simconfig = DEFAULT_EUCLIDEAN_SIMCONFIG;
  vgx_Similarity_t *simcontext = NULL;
  PyObject *py_capsule;
  if( PyTuple_GET_SIZE(args) == 1 && PyCapsule_CheckExact( (py_capsule = PyTuple_GET_ITEM(args,0)) ) ) {
    simcontext = (vgx_Similarity_t*)PyCapsule_GetPointer( py_capsule, NULL );
    if( !COMLIB_OBJECT_ISINSTANCE( simcontext, vgx_Similarity_t ) ) {
      PyVGXError_SetString( PyExc_ValueError, "Similarity capsule does not contain a Similarity object" );
      return -1;
    }
  }
  else {
    int nsegm = -1;
    int nsign = -1;
    int max_size = -1;
    int min_intersect = -1;
    float min_cosine = -1.0f;
    float min_jaccard = -1.0f;
    float cosine_exponent = -1.0f;
    float jaccard_exponent = -1.0f;
    int hamming = -1;
    float similarity = -1.0f;

    if( !PyArg_ParseTupleAndKeywords(args, kwds, "|Oiiiiffffif", kwlist,
      &py_graph,
      &nsegm,
      &nsign,
      &max_size,
      &min_intersect,
      &min_cosine,
      &min_jaccard,
      &cosine_exponent,
      &jaccard_exponent,
      &hamming,
      &similarity )
      )
    {
      return -1;
    }

    if( (simconfig.vector.euclidean = igraphfactory.EuclideanVectors()) != 0 ) {
      simconfig.vector.max_size = MAX_EUCLIDEAN_VECTOR_SIZE;
    }
    else if( igraphfactory.FeatureVectors() ) {
      simconfig.vector.max_size = MAX_FEATURE_VECTOR_SIZE;
    }

    if( nsegm != -1 ) {
      simconfig.fingerprint.nsegm = nsegm;
    }
    if( nsign != -1 ) {
      simconfig.fingerprint.nsign = nsign;
    }
    if( max_size != -1 ) {
      simconfig.vector.max_size = (uint16_t)max_size;
    }
    if( min_intersect != -1 ) {
      simconfig.vector.min_intersect = min_intersect;
    }
    if( min_cosine != -1.0f ) {
      simconfig.vector.min_cosine = min_cosine;
    }
    if( min_jaccard != -1.0f ) {
      simconfig.vector.min_jaccard = min_jaccard;
    }
    if( cosine_exponent != -1.0f ) {
      simconfig.vector.cosine_exponent = cosine_exponent;
    }
    if( jaccard_exponent != -1.0f ) {
      simconfig.vector.jaccard_exponent = jaccard_exponent;
    }
    if( hamming != -1 ) {
      simconfig.threshold.hamming = hamming;
    }
    if( similarity != -1.0f ) {
      simconfig.threshold.similarity = similarity;
    }
  }

  // Similarity context provided, just wrap in the python object
  if( simcontext ) {
    pysim->sim = simcontext;
    pysim->standalone = false;
  }
  // No args provided: New Standalone Similarity object
  else if( py_graph == NULL ) {
    vgx_Similarity_t *sim;
    BEGIN_PYVGX_THREADS {
      vgx_Similarity_constructor_args_t sargs = {
        .parent = NULL,
        .params = &simconfig
      };

      sim = COMLIB_OBJECT_NEW( vgx_Similarity_t, NULL, &sargs );
    } END_PYVGX_THREADS;

    if( (pysim->sim = sim) == NULL ) {
      PyVGXError_SetString( PyExc_Exception, "Similarity error" );
      return -1;
    }
    pysim->standalone = true;
  }
  // Bad graph object
  else if( !PyVGX_Graph_Check( py_graph ) ) {
    PyVGXError_SetString( PyExc_TypeError, "Invalid graph object" );
    return -1;
  }
  // Graph object provided: Update Graph Similarity object (replace in graph)
  else {
    pysim->sim = py_graph->graph->similarity;
    pysim->standalone = false;
    // !!! NOTE !!!
    // This is only for testing now, because changing some of these parameters
    // will require fundamental changes to graph structures. I.e., changing fingerprinting nsegm/nsign
    // in an existing graph is a major change that requires new permutation tables and redoing the entire thing basically.
    // So it will not be supported to change these things on a real graph and expect the graph structures to change.

    // !!! FOR TESTING ONLY !!!
    update_simconfig( &pysim->sim->params, &simconfig );
    // ^^^^^^^^^^^^^^^^^^^^^^^^
  }

  if( pysim->standalone == false ) {
    int max_size = sizeof( LSH_SEEDS ) / sizeof( unsigned short );
    if( (pysim->py_lsh_seeds = PyList_New( max_size )) == NULL ) {
      return -1;
    }
    for( int i=0; i<max_size; i++ ) {
      if( PyList_SetItem( pysim->py_lsh_seeds, i, PyLong_FromUnsignedLong( LSH_SEEDS[i] ) ) < 0 ) {
        Py_DECREF( pysim->py_lsh_seeds );
        pysim->py_lsh_seeds = NULL;
        return -1;
      }
    }
  }

  return 0;

}



/******************************************************************************
 * PyVGX_Similarity__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Similarity__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Similarity__getset
 *
 ******************************************************************************
 */
static PyGetSetDef PyVGX_Similarity__getset[] = {
  {"hamming_threshold",   (getter)__PyVGX_Similarity__get_hamming_threshold,  (setter)__PyVGX_Similarity__set_hamming_threshold,  "hamming threshold", NULL },
  {"sim_threshold",       (getter)__PyVGX_Similarity__get_sim_threshold,      (setter)__PyVGX_Similarity__set_sim_threshold,      "similarity threshold", NULL },
  {"cosine_exp",          (getter)__PyVGX_Similarity__get_cosine_exp,         (setter)__PyVGX_Similarity__set_cosine_exp,         "cosine exponent", NULL },
  {"jaccard_exp",         (getter)__PyVGX_Similarity__get_jaccard_exp,        (setter)__PyVGX_Similarity__set_jaccard_exp,        "jaccard exponent", NULL },
  {"min_cosine",          (getter)__PyVGX_Similarity__get_min_cosine,         (setter)__PyVGX_Similarity__set_min_cosine,         "min cosine similarity", NULL },
  {"min_jaccard",         (getter)__PyVGX_Similarity__get_min_jaccard,        (setter)__PyVGX_Similarity__set_min_jaccard,        "min jaccard similarity", NULL },
  {"min_isect",           (getter)__PyVGX_Similarity__get_min_isect,          (setter)__PyVGX_Similarity__set_min_isect,          "min vector intersection", NULL },
  {"max_vector_size",     (getter)__PyVGX_Similarity__get_max_size,           (setter)NULL,                                       "max vector size", NULL },
  {"nsegm",               (getter)__PyVGX_Similarity__get_fp_nsegm,           (setter)NULL,                                       "fingerprint segments", NULL },
  {"nsign",               (getter)__PyVGX_Similarity__get_fp_nsign,           (setter)NULL,                                       "fingerprint significant segments", NULL },
  {"seeds",               (getter)__PyVGX_Similarity__get_fp_seeds,           (setter)NULL,                                       "fingerprint seeds", NULL },

  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Similarity__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Similarity__methods[] = {
    {"rvec",                  (PyCFunction)PyVGX_Similarity__rvec,                  METH_O,                   rvec__doc__  },

    {"NewVector",             (PyCFunction)PyVGX_Similarity__NewVector,             METH_FASTCALL | METH_KEYWORDS,  NewVector__doc__  },
    {"NewCentroid",           (PyCFunction)PyVGX_Similarity__NewCentroid,           METH_O,                   NewCentroid__doc__  },
    
    {"Fingerprint",           (PyCFunction)PyVGX_Similarity__Fingerprint,           METH_VARARGS,             Fingerprint__doc__  },
    {"Projections",           (PyCFunction)PyVGX_Similarity__Projections,           METH_O,                   Projections__doc__  },
    {"DeleteProjectionSets",  (PyCFunction)PyVGX_Similarity__DeleteProjectionSets,  METH_NOARGS,              DeleteProjectionSets__doc__  },
    {"CreateProjectionSets",  (PyCFunction)PyVGX_Similarity__CreateProjectionSets,  METH_VARARGS,             CreateProjectionSets__doc__  },


    {"Similarity",            (PyCFunction)PyVGX_Similarity__Similarity,            METH_VARARGS,             Similarity__doc__  },
    {"EuclideanDistance",     (PyCFunction)PyVGX_Similarity__EuclideanDistance,     METH_VARARGS,             EuclideanDistance__doc__  },
    {"Cosine",                (PyCFunction)PyVGX_Similarity__Cosine,                METH_VARARGS,             Cosine__doc__  },
    {"Jaccard",               (PyCFunction)PyVGX_Similarity__Jaccard,               METH_VARARGS,             Jaccard__doc__  },
    {"HammingDistance",       (PyCFunction)PyVGX_Similarity__HammingDistance,       METH_VARARGS,             HammingDistance__doc__  },

    {"AsDict",                (PyCFunction)PyVGX_Similarity__AsDict,                METH_NOARGS,              AsDict__doc__  },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS



/******************************************************************************
 * PyVGX_Sim__SimilarityType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Similarity__SimilarityType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Similarity",
    .tp_basicsize       = sizeof(PyVGX_Similarity),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Similarity__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Similarity__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = 0,
    .tp_as_mapping      = 0,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Similarity objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Similarity__methods,
    .tp_members         = PyVGX_Similarity__members,
    .tp_getset          = PyVGX_Similarity__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Similarity__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Similarity__new,
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


DLL_HIDDEN PyTypeObject * p_PyVGX_Similarity__SimilarityType = &PyVGX_Similarity__SimilarityType;


