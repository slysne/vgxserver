/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_vertex.c
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

#include "pyvgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


typedef PyObject * (*__getfunc)( const vgx_Vertex_t *vertex_LCK, void *closure );
typedef vgx_RankElement_t * (*__rank_cXf)( vgx_Rank_t *pR );

static vgx_Vertex_t * __ensure_writable_nocreate( vgx_Vertex_t *vertex, vgx_Vertex_t **vertex_LOCALLY_LOCKED );
static vgx_Vertex_t * __ensure_readable( vgx_Vertex_t *vertex, vgx_Vertex_t **vertex_LOCALLY_LOCKED );
static vgx_Vertex_t * __return_readable( PyVGX_Vertex *pyvertex );
static PyObject * __py_get_if_readable( const vgx_Vertex_t *vertex, __getfunc getfunc, void *closure );
static PyObject * __py_get_identifier( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_internalid( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_outdegree( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_indegree( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_degree( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_isolated( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_descriptor( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_type( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_manifestation( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_vector( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_properties( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_TMC( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_TMM( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_TMX( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_rank_cX( const vgx_Vertex_t *vertex_RO, void *__rank_cX );
static PyObject * __py_get_virtual( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_address( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_index( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_bitindex( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_bitvector( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_opcnt( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_refc( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_aidx( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_bidx( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_oidx( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_handle( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_enum( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_readers( const vgx_Vertex_t *vertex_RO, void *closure );
static PyObject * __py_get_xrecursion( const vgx_Vertex_t *vertex_LCK, void *closure );
static PyObject * __py_get_owner( const vgx_Vertex_t *vertex_LCK, void *closure );



/******************************************************************************
 * init_args
 ******************************************************************************
 */
typedef union u_init_args {
  PyObject *arg[6];
  struct {
    PyObject *pygraph;
    PyObject *py_id;
    PyObject *py_type;
    PyObject *py_mode;
    PyObject *py_lifespan;
    PyObject *py_timeout;
  };
} init_args;



/******************************************************************************
 * __rank_c1
 ******************************************************************************
 */
static vgx_RankElement_t * __rank_c1( vgx_Rank_t *pR ) {
  return &pR->slope;
}



/******************************************************************************
 * __rank_c0
 ******************************************************************************
 */
static vgx_RankElement_t * __rank_c0( vgx_Rank_t *pR ) {
  return &pR->offset;
}



/******************************************************************************
 * __ensure_writable_nocreate
 ******************************************************************************
 */
static vgx_Vertex_t * __ensure_writable_nocreate( vgx_Vertex_t *vertex, vgx_Vertex_t **vertex_LOCALLY_LOCKED ) {
  vgx_Vertex_t *writable = NULL;
  BEGIN_PYVGX_THREADS {
    vgx_Graph_t *graph = vertex->graph;

    // Not writable, acquire the vertex
    if( !CALLABLE(vertex)->Writable(vertex) ) {
      // Get the internalid (assumed to be accessible without any locks)
      objectid_t obid = CALLABLE(vertex)->InternalID(vertex);
      vgx_AccessReason_t reason;
      CString_t *CSTR__error = NULL;
      // Lock vertex writable without creating
      if( (writable = CALLABLE(graph)->advanced->AcquireVertexWritableNocreate( graph, &obid, 0, &reason, &CSTR__error )) != NULL ) {
        *vertex_LOCALLY_LOCKED = writable;
      }
      else {
        BEGIN_PYTHON_INTERPRETER {
          PyErr_Format( PyVGX_AccessError, "Vertex not writable: 0x%04x", reason );
        } END_PYTHON_INTERPRETER;
      }
    }
    // Already writable, ok
    else {
      writable = vertex;
    }
  } END_PYVGX_THREADS;
  return writable;
}



/******************************************************************************
 * __ensure_readable
 ******************************************************************************
 */
static vgx_Vertex_t * __ensure_readable( vgx_Vertex_t *vertex, vgx_Vertex_t **vertex_LOCALLY_LOCKED ) {
  vgx_Vertex_t *readable = NULL;
  BEGIN_PYVGX_THREADS {
    vgx_Graph_t *graph = vertex->graph;

    // Not readable, acquire the vertex
    if( !CALLABLE(vertex)->Readable(vertex) ) {
      // Get the internalid (assumed to be accessible without any locks)
      objectid_t obid = CALLABLE(vertex)->InternalID(vertex);
      vgx_AccessReason_t reason;
      // Lock vertex readable
      if( (readable = CALLABLE(graph)->advanced->AcquireVertexReadonly( graph, &obid, 0, &reason )) != NULL ) {
        *vertex_LOCALLY_LOCKED = readable;
      }
      else {
        BEGIN_PYTHON_INTERPRETER {
          PyErr_Format( PyVGX_AccessError, "Vertex not readable: %d", reason );
        } END_PYTHON_INTERPRETER;
      }
    }
    // Already readable, ok
    else {
      readable = vertex;
    }
  } END_PYVGX_THREADS;
  return readable;
}



/******************************************************************************
 * __py_get_if_readable
 ******************************************************************************
 */
static PyObject * __py_get_if_readable( const vgx_Vertex_t *vertex, __getfunc getfunc, void *closure ) {
  PyObject *py_ret = NULL;
  if( vertex && __vertex_is_locked( vertex ) ) {
    if( CALLABLE( vertex )->Readable( vertex ) ) {
      py_ret = getfunc( vertex, closure );
    }
    else {
      vgx_Graph_t *graph = vertex->graph;
      vgx_IGraphAdvanced_t *igraph = CALLABLE( vertex->graph )->advanced;
      objectid_t obid = CALLABLE( vertex )->InternalID( vertex );
      vgx_AccessReason_t reason;
      vgx_Vertex_t *vertex_RO;
      BEGIN_PYVGX_THREADS {
        vertex_RO = igraph->AcquireVertexReadonly( graph, &obid, 0, &reason );
      } END_PYVGX_THREADS;
      if( vertex_RO ) {
        py_ret = getfunc( vertex, closure );
        igraph->ReleaseVertex( graph, &vertex_RO );
      }
      else {
        PyVGXError_SetString( PyVGX_AccessError, "Vertex not readable" );
      }
    }
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex is not accessible" );
  }
  return py_ret;
}



/******************************************************************************
 * __PyVGX_Vertex__get_identifier
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * __py_get_identifier( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_identifier = NULL;
  size_t sz = CALLABLE( vertex_RO )->IDLength( vertex_RO );
  const char *identifier = CALLABLE( vertex_RO )->IDString( vertex_RO );
  if( identifier && sz ) {
    py_identifier = PyUnicode_FromStringAndSize( identifier, sz );
  }
  else {
    PyVGXError_SetString( PyExc_AttributeError, "no identifier!" );
  }
  return py_identifier;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_identifier
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_identifier( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_identifier, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_internalid
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_internalid( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_internalid = NULL;
  char buffer[ 33 ];
  objectid_t obid = CALLABLE( vertex_RO )->InternalID( vertex_RO );
  const char *str = idtostr( buffer, &obid ); 
  py_internalid = PyUnicode_FromStringAndSize( str, 32 ); 
  return py_internalid;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_internalid
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_internalid( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_internalid, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_outdegree
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_outdegree( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_outdegree = NULL;
  int64_t odeg = CALLABLE( vertex_RO )->OutDegree( vertex_RO );
  py_outdegree = PyLong_FromLongLong( odeg );
  return py_outdegree;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_outdegree
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_outdegree( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_outdegree, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_indegree
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_indegree( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_indegree = NULL;
  int64_t ideg = CALLABLE( vertex_RO )->InDegree( vertex_RO );
  py_indegree = PyLong_FromLongLong( ideg );
  return py_indegree;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_indegree
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_indegree( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_indegree, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_degree
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_degree( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_degree = NULL;
  int64_t deg = CALLABLE( vertex_RO )->Degree( vertex_RO );
  py_degree = PyLong_FromLongLong( deg );
  return py_degree;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_degree
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_degree( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_degree, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_isolated
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_isolated( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_isolated = __vertex_is_isolated( vertex_RO ) ? Py_True : Py_False;
  Py_INCREF( py_isolated );
  return py_isolated;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_isolated
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_isolated( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_isolated, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_descriptor
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_descriptor( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_desc = NULL;
  py_desc = PyLong_FromUnsignedLongLong( vertex_RO->descriptor.bits );
  return py_desc;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_descriptor
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_descriptor( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_descriptor, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_type
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_type( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_type = NULL;
  const char *name;
  BEGIN_PYVGX_THREADS {
    const CString_t *CSTR__type = CALLABLE( vertex_RO )->TypeName( vertex_RO );
    name = CStringValue( CSTR__type );
  } END_PYVGX_THREADS;
  py_type = PyUnicode_FromString( name );
  return py_type;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_type
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_type( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_type, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_manifestation
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_manifestation( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_man = NULL;
  const char *man = __vertex_is_manifestation_real( vertex_RO ) ? "REAL" : "VIRTUAL";
  py_man = PyUnicode_FromString( man );
  return py_man;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_manifestation
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_manifestation( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_manifestation, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_vector
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_vector( const vgx_Vertex_t *vertex_RO, void *closure ) {
  vgx_Vector_t *vector = CALLABLE( vertex_RO )->GetVector( (vgx_Vertex_t*)vertex_RO );
  PyObject *py_vector = iPyVGXBuilder.ExternalVector( vector );
  return py_vector;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_vector
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_vector( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_vector, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_properties
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_properties( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return iPyVGXSearchResult.PyDict_FromVertexProperties( (vgx_Vertex_t*)vertex_RO );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_properties
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_properties( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_properties, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_TMC
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_TMC( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_TMC = NULL;
  int64_t tmc = CALLABLE( vertex_RO )->CreationTime( vertex_RO );
  py_TMC = PyLong_FromLongLong( tmc );
  return py_TMC;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_TMC
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_TMC( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_TMC, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_TMM
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_TMM( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_TMM = NULL;
  int64_t tmm = CALLABLE( vertex_RO )->ModificationTime( vertex_RO );
  py_TMM = PyLong_FromLongLong( tmm );
  return py_TMM;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_TMM
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_TMM( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_TMM, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_TMX
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_TMX( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_TMX = NULL;
  int64_t tmx = CALLABLE( vertex_RO )->GetExpirationTime( vertex_RO );
  py_TMX = PyLong_FromLongLong( tmx );
  return py_TMX;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_TMX
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_TMX( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_TMX, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_rtx
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_rtx( const vgx_Vertex_t *vertex_RO, void *closure ) {
  int64_t tmx = CALLABLE( vertex_RO )->GetExpirationTime( vertex_RO );
  int64_t rtx = UINT32_MAX; // 32 yes
  if( tmx < TIME_EXPIRES_NEVER ) {
    int64_t now = _vgx_graph_seconds( vertex_RO->graph );
    rtx = tmx > now ? tmx - now : 0;
  }
  return PyLong_FromUnsignedLong( (unsigned long)rtx );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_rtx
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_rtx( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_rtx, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_rank_cX
 ******************************************************************************
 */
__inline static PyObject * __py_get_rank_cX( const vgx_Vertex_t *vertex_RO, void *__rank_cX ) {
  PyObject *py_cX;
  __rank_cXf cXf = (__rank_cXf)__rank_cX;
  vgx_Rank_t *pR = (vgx_Rank_t*)&vertex_RO->rank;
  vgx_RankElement_t *reX = cXf( pR );
  float cX = REINTERPRET_CAST_FLOAT_24( reX->c24 );
  py_cX = PyFloat_FromDouble( cX );
  return py_cX;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_rank_cX
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_rank_cX( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_rank_cX, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__set_rank_cX
 ******************************************************************************
 */
static int __PyVGX_Vertex__set_rank_cX( PyVGX_Vertex *py_vertex, PyObject *py_cX, void *__rank_cX ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( py_vertex );
  if( vertex == NULL ) {
    return -1;
  }

  double cX = 0.0;
  if( py_cX == NULL ) {
    if( __rank_cX == (void*)__rank_c1 ) {
      cX = 1.0;
    }
    else if( __rank_cX == (void*)__rank_c0 ) {
      cX = 0.0;
    }
    else {
      PyErr_SetString( PyExc_Exception, "internal error" );
      return -1;
    }
  }
  else if( PyFloat_CheckExact( py_cX ) ) {
    cX = PyFloat_AS_DOUBLE( py_cX );
  }
  else if( PyLong_CheckExact( py_cX ) ) {
    cX = (double)PyLong_AsLongLong( py_cX );
  }
  else {
    PyErr_SetString( PyExc_TypeError, "a numeric coefficient is required" );
    return -1;
  }

  vgx_Vertex_t *vertex_WL;
  vgx_Vertex_t *LOCAL_WL = NULL;
  if( (vertex_WL = __ensure_writable_nocreate( vertex, &LOCAL_WL )) != NULL ) {
    // get existing coefficients
    double c1, c0;
    if( __rank_cX == (void*)__rank_c1 ) {
      c1 = cX;
      c0 = vgx_RankGetC0( &vertex_WL->rank );
    }
    else {
      c1 = vgx_RankGetC1( &vertex_WL->rank );
      c0 = cX;
    }

    CALLABLE( vertex_WL )->SetRank( vertex_WL, (float)c1, (float)c0 );
    if( LOCAL_WL ) {
      vgx_Graph_t *graph = LOCAL_WL->graph;
      CALLABLE( graph )->advanced->ReleaseVertex( graph, &LOCAL_WL );
    }
    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * __PyVGX_Vertex__get_rank_b1
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_rank_b1( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLong( vertex_RO->rank.slope.b8 );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_rank_b1
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_rank_b1( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_rank_b1, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_rank_b0
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_rank_b0( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLong( vertex_RO->rank.offset.b8 );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_rank_b0
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_rank_b0( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_rank_b0, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_virtual
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_virtual( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_virtual;
  py_virtual = vertex_RO->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_VIRTUAL ? Py_True : Py_False;
  Py_INCREF( py_virtual );
  return py_virtual;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_virtual
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_virtual( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_virtual, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_address
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_address( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLongLong( (uintptr_t)vertex_RO );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_address
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_address( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_address( py_vertex->vertex, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_index
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_index( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLongLong( __vertex_get_index( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( vertex_RO ) ) );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_index
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_index( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_index( py_vertex->vertex, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_bitindex
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_bitindex( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLongLong( __vertex_get_bitindex( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( vertex_RO ) ) );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_bitindex
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_bitindex( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_bitindex( py_vertex->vertex, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_bitvector
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_bitvector( const vgx_Vertex_t *vertex_RO, void *closure ) {
  return PyLong_FromUnsignedLongLong( __vertex_get_bitvector( (vgx_AllocatedVertex_t*)_cxmalloc_linehead_from_object( vertex_RO ) ) );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_bitvector
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_bitvector( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_bitvector( py_vertex->vertex, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_opcnt
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_opcnt( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_opcnt = NULL;
  int64_t op = iOperation.GetId_LCK( &vertex_RO->operation );
  py_opcnt = PyLong_FromLongLong( op );
  return py_opcnt;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_opcnt
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_opcnt( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_opcnt, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_refc
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_refc( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_refc;
  py_refc = PyLong_FromLongLong( _cxmalloc_object_refcnt_nolock( vertex_RO ) );
  return py_refc;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_refc
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_refc( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_refc, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_aidx
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_aidx( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_aidx;
  py_aidx = PyLong_FromLong( _cxmalloc_linehead_from_object( vertex_RO )->data.aidx );
  return py_aidx;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_aidx
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_aidx( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_aidx, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_bidx
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_bidx( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_bidx;
  py_bidx = PyLong_FromUnsignedLong( _cxmalloc_linehead_from_object( vertex_RO )->data.bidx );
  return py_bidx;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_bidx
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_bidx( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_bidx, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_oidx
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_oidx( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_oidx;
  py_oidx = PyLong_FromUnsignedLong( _cxmalloc_linehead_from_object( vertex_RO )->data.offset );
  return py_oidx;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_oidx
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_oidx( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_oidx, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_handle
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_handle( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_handle;
  py_handle = PyLong_FromUnsignedLongLong( _cxmalloc_linehead_from_object( vertex_RO )->data.handle );
  return py_handle;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_handle
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_handle( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_handle, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_enum
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_enum( const vgx_Vertex_t *vertex_RO, void *closure ) {

  int32_t e32 = CALLABLE( vertex_RO )->VertexEnum( (vgx_Vertex_t*)vertex_RO );
  if( e32 < 0 ) {
    PyErr_SetString( PyVGX_AccessError, "undefined enum for readonly vertex" );
    return NULL;
  }

  return PyLong_FromLong( e32 );
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_enum
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_enum( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_enum, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_readers
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_readers( const vgx_Vertex_t *vertex_RO, void *closure ) {
  PyObject *py_readers = NULL;
  int readers = 0;
  BEGIN_PYVGX_THREADS { 
    GRAPH_LOCK( vertex_RO->graph ) {
      if( __vertex_is_readonly( vertex_RO ) ) {
        readers = __vertex_get_readers( vertex_RO );
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;
  py_readers = PyLong_FromLong( readers );
  return py_readers;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_readers
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_readers( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_readers, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_xrecursion
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_xrecursion( const vgx_Vertex_t *vertex_LCK, void *closure ) {
  PyObject *py_xrecursion = NULL;
  int xrecursion = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( vertex_LCK->graph ) {
      if( __vertex_is_writable( vertex_LCK ) ) {
        xrecursion = __vertex_get_writer_recursion( vertex_LCK );
      }
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;
  py_xrecursion = PyLong_FromLong( xrecursion );
  return py_xrecursion;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_xrecursion
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_xrecursion( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_xrecursion, closure );
}



/******************************************************************************
 * __PyVGX_Vertex__get_owner
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static PyObject * __py_get_owner( const vgx_Vertex_t *vertex_LCK, void *closure ) {
  PyObject *py_owner = NULL;
  DWORD owner = 0;
  BEGIN_PYVGX_THREADS {
    GRAPH_LOCK( vertex_LCK->graph ) {
      owner = vertex_LCK->descriptor.writer.threadid;
    } GRAPH_RELEASE;
  } END_PYVGX_THREADS;
  py_owner = PyLong_FromLong( owner );
  return py_owner;
}


/**************************************************************************//**
 * __PyVGX_Vertex__get_owner
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__get_owner( PyVGX_Vertex *py_vertex, void *closure ) {
  return __py_get_if_readable( py_vertex->vertex, __py_get_owner, closure );
}



/******************************************************************************
 * PyVGX_Vertex__Writable
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Writable__doc__,
  "Writable() -> boolean\n"
  "\n"
  "Return True if vertex is writable, False otherwise"
);

/**************************************************************************//**
 * PyVGX_Vertex__Writable
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Writable( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = pyvertex->vertex;
  if( vertex && CALLABLE( vertex )->Writable( vertex ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__Readable
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Readable__doc__,
  "Readable() -> boolean\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Readable
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Readable( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = pyvertex->vertex;
  if( vertex &&  CALLABLE( vertex )->Readable( vertex ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__Readonly
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Readonly__doc__,
  "Readonly() -> boolean\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Readonly
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Readonly( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = pyvertex->vertex;
  if( vertex && CALLABLE( vertex )->Readonly( vertex ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__Close
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Close__doc__,
  "Close() -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Close
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Close( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex == NULL ) {
    return NULL;
  }
  bool released = false;
  vgx_Graph_t *graph = vertex->graph;
  BEGIN_PYVGX_THREADS {
    released = CALLABLE( graph )->simple->CloseVertex( graph, &vertex );
  } END_PYVGX_THREADS;
  if( released ) {
    pyvertex->vertex = NULL;
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Vertex__Escalate
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Escalate__doc__,
  "Escalate( timeout=0 ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Escalate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Escalate( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"timeout", NULL};
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex == NULL ) {
    return NULL;
  }
  vgx_Graph_t *graph = vertex->graph;

  int timeout_ms = 0;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|i", kwlist, &timeout_ms ) ) {
    return NULL;
  }

  if( timeout_ms < 0 ) {
    PyVGXError_SetString( PyExc_ValueError, "Infinite timeout not allowed for this operation" );
    return NULL;
  }

  CString_t *CSTR__error = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  vgx_Vertex_t *vertex_WL;
  BEGIN_PYVGX_THREADS {
    vertex_WL = CALLABLE( graph )->advanced->EscalateReadonlyToWritable( graph, vertex, timeout_ms, &reason, &CSTR__error );
  } END_PYVGX_THREADS;

  if( vertex_WL == NULL ) {
    iPyVGXBuilder.SetPyErrorFromAccessReason( CALLABLE( vertex )->IDString( vertex ), reason, &CSTR__error );
    return NULL;
  }

  // Success. Vertex is now writable.
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Vertex__Relax
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Relax__doc__,
  "Relax() -> bool\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Relax
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Relax( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex == NULL ) {
    return NULL;
  }
  vgx_Graph_t *graph = vertex->graph;
  int relaxed;
  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *vertex_RO;
    if( (vertex_RO = CALLABLE( graph )->advanced->RelaxWritableToReadonly( graph, vertex )) == NULL ) {
      relaxed = -1;
    }
    else {
      relaxed = __vertex_is_readonly( vertex_RO );
    }
  } END_PYVGX_THREADS;

  if( relaxed < 0 ) {
    PyVGXError_SetString( PyExc_Exception, "Vertex could not be released. Check error logs." );
    return NULL;
  }
  else if( relaxed > 0 ) {
    Py_INCREF( Py_True );
    return Py_True;
  }
  else {
    Py_INCREF( Py_False );
    return Py_False;
  }
}



/******************************************************************************
 * PyVGX_Vertex__SetRank
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetRank__doc__,
  "SetRank( [c1[, c0]] ) -> ( c1, c0 )\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetRank
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetRank( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"c1", "c0", NULL};

  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  float c1 = NAN;
  float c0 = NAN;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "|ff", kwlist, &c1, &c0 ) ) {
    return NULL;
  }

  vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
  if( iV->Writable(vertex) ) {
    PyObject *py_rank = PyTuple_New( 2 );
    if( py_rank ) {
      vgx_Rank_t rank = iV->GetRank( vertex );
      BEGIN_PYVGX_THREADS {
        if( !(c1 < INFINITY) ) {
          // use existing c1
          c1 = (float)vgx_RankGetC1( &rank );
        }
        if( !(c0 < INFINITY) ) {
          // use existing c0
          c0 = (float)vgx_RankGetC0( &rank );
        }
        iV->SetRank( vertex, c1, c0 );
      } END_PYVGX_THREADS;
      PyTuple_SET_ITEM( py_rank, 0, PyVGX_PyFloat_FromDoubleNoErr( c1 ) );
      PyTuple_SET_ITEM( py_rank, 1, PyVGX_PyFloat_FromDoubleNoErr( c0 ) );
    }
    return py_rank;
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex not writable" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__GetRank
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetRank__doc__,
  "GetRank() -> ( c1, offset )\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetRank
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetRank( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  PyObject *py_rank = PyTuple_New( 2 );
  if( py_rank ) {
    vgx_Rank_t rank = CALLABLE( vertex )->GetRank( vertex );
    double c1 = vgx_RankGetC1( &rank );
    double c0 = vgx_RankGetC0( &rank );
    PyTuple_SET_ITEM( py_rank, 0, PyVGX_PyFloat_FromDoubleNoErr( c1 ) );
    PyTuple_SET_ITEM( py_rank, 1, PyVGX_PyFloat_FromDoubleNoErr( c0 ) );
  }
  return py_rank;
}



/******************************************************************************
 * PyVGX_Vertex__ArcLSH
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ArcLSH__doc__,
  "ArcLSH( lsh64 ) -> lsh32\n"
  "\n"
  "Extract a 32-bit region from lsh64 as defined by a right-rotate\n"
  "amount stored in the vertex.\n"
  "\n"
  "NOTE: The right-rotate amount is stored in an area shared with the\n"
  "rank attribute, making this attribute unavailable for ranking purposes.\n" 
  "\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__ArcLSH
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__ArcLSH( PyVGX_Vertex *pyvertex, PyObject *py_lsh ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  uint64_t lsh64 = PyLong_AsUnsignedLongLong( py_lsh );
  if( lsh64 == (uint64_t)-1 && PyErr_Occurred() ) {
    return NULL;
  }

  // Special internal overload of rank.
  int rr = vgx_RankGetANNArcLSHRotate( &vertex->rank );

  // Rotate and cast to 32 bits
  uint32_t lsh32 = (uint32_t)( (lsh64 >> rr) | (lsh64 << (64-rr)) );

  return PyLong_FromUnsignedLong( lsh32 );
}



/******************************************************************************
 * PyVGX_Vertex__SetVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetVector__doc__,
  "SetVector( [data [,alpha]] ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetVector
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetVector( PyVGX_Vertex *pyvertex, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

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

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs._args ) < 0 ) {
    return NULL;
  }

  PyObject *py_ret = Py_None;

  vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
  if( iV->Writable(vertex) ) {
    vgx_Graph_t *graph = iV->Parent(vertex);
    // Create a persistent vector
    vgx_Vector_t *vector = iPyVGXParser.InternalVectorFromPyObject( graph->similarity, vcargs.py_data, vcargs.py_alpha, false );
    // Set the vector on the vertex
    if( vector != NULL ) {
      BEGIN_PYVGX_THREADS {
        // Vector may be stolen
        if( iV->SetVector( vertex, &vector ) < 0 ) {
          // error
          py_ret = NULL;
        }
        // Discard if not stolen
        if( vector ) {
          CALLABLE( vector )->Decref( vector );
        }
      } END_PYVGX_THREADS;
    }
    else {
      py_ret = NULL;
    }
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex not writable" );
    py_ret = NULL;
  }

  if( py_ret ) {
    Py_INCREF( py_ret );
    return py_ret;
  }
  else {
    if( !PyErr_Occurred() ) {
      PyVGXError_SetString( PyVGX_InternalError, "Internal error" );
    }
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__HasVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( HasVector__doc__,
  "HasVector() -> bool\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__HasVector
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__HasVector( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  if( CALLABLE( vertex )->GetVector( vertex ) != NULL ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__GetVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetVector__doc__,
  "GetVector() -> Vector\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetVector
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetVector( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  vgx_Vector_t *vector = CALLABLE(vertex)->GetVector(vertex);

  if( vector == NULL ) {
    BEGIN_PYVGX_THREADS {
      vgx_Similarity_t *sim = vertex->graph->similarity;
      vector = CALLABLE( sim )->NewInternalVector( sim, NULL, 1.0f, 0, true );
    } END_PYVGX_THREADS;
    if( vector == NULL ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
      return NULL;
    }
  }

  return PyVGX_Vector__FromVector( vector );
}



/******************************************************************************
 * PyVGX_Vertex__RemoveVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( RemoveVector__doc__,
  "RemoveVector() -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__RemoveVector
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__RemoveVector( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
  if( iV->Writable(vertex) ) {
    BEGIN_PYVGX_THREADS {
      iV->RemoveVector( vertex );
    } END_PYVGX_THREADS;
    Py_RETURN_NONE;
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex not writable" );
    return NULL;
  }

}



/******************************************************************************
 * __py_set_property
 *
 ******************************************************************************
 */
static int __py_set_property( vgx_Vertex_t *vertex_WL, const char *name, PyObject *py_value, PyObject *py_virtual ) {
  int ret = 0;
  PyObject *py_repr = NULL;
  CString_t *CSTR__value = NULL;

  // vprop hint (hidden feature: '*' prefix means virtual prop if supported)
  bool vprop = false;
  if( *name == '*' ) {
    ++name;
    vprop = true;
  }

  // The property to insert
  vgx_VertexProperty_t vertex_property = {0};
  vertex_property.key = iEnumerator_OPEN.Property.Key.New( vertex_WL->graph, name );

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      // Key
      if( vertex_property.key == NULL ) {
        PyErr_Format( PyExc_ValueError, "invalid property key: '%s'", name );
        THROW_SILENT( CXLIB_ERR_API, 0x411 );
      }
      // We have a value
      if( py_value && py_value != Py_None ) {
        // INTEGER
        if( PyLong_Check( py_value ) ) {
          int ovf;
          int64_t x = PyLong_AsLongLongAndOverflow( py_value, &ovf );
          if( ovf != 0 || x > (1LL<<55)-1 || x < -(1LL<<56) ) {
            PyErr_SetString( PyExc_ValueError, "integer value out of range" );
            THROW_ERROR( CXLIB_ERR_API, 0x412 );
          }
          else {
            vertex_property.val.type = VGX_VALUE_TYPE_INTEGER;
            vertex_property.val.data.simple.integer = x;
          }
        }
        // REAL
        else if( PyFloat_Check( py_value ) ) {
          vertex_property.val.type = VGX_VALUE_TYPE_REAL;
          vertex_property.val.data.simple.real = PyFloat_AS_DOUBLE( py_value );
        }
        // STRING or OBJECT
        else {
          // Virtual (disk) property?
          if( !vprop ) { // no * hint in name, check virtual parameter
            vprop = py_virtual && ((Py_IsTrue( py_virtual ) || (PyLong_Check( py_virtual ) && PyLong_AsLong( py_virtual ) > 0 )));
          }
          if( vprop ) {
            vertex_property.val.type = VGX_VALUE_TYPE_CSTRING;
          }
          else {
            vertex_property.val.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING;
          }
          // Encode
          if( (CSTR__value = iPyVGXCodec.NewEncodedObjectFromPyObject( NULL, py_value, vertex_WL->graph->property_allocator_context, vprop )) == NULL ) {
            THROW_SILENT( CXLIB_ERR_GENERAL, 0x413 );
          }
          vertex_property.val.data.simple.CSTR__string = CSTR__value;
        }
      }
      else {
        // EXISTS / BOOLEAN
        vertex_property.val.type = VGX_VALUE_TYPE_BOOLEAN;
        vertex_property.val.data.simple.integer = 0;
      }

      // Set the vertex property
      BEGIN_PYVGX_THREADS {
        ret = CALLABLE( vertex_WL )->SetProperty( vertex_WL, &vertex_property );
      } END_PYVGX_THREADS;

      if( ret < 0 ) {
        PyErr_SetString( PyExc_Exception, "Failed to set vertex property" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x414 );
      }

    }
    XCATCH( errcode ) {
      if( !PyErr_Occurred() ) {
        PyErr_Format( PyExc_Exception, "internal error %03x", errcode );
      }
      ret = -1;
    }
    XFINALLY {
      PyVGX_XDECREF( py_repr );
    }
  } END_PYTHON_INTERPRETER;

  if( CSTR__value ) {
    CStringDelete( CSTR__value );
  }
  if( vertex_property.key ) {
    CStringDelete( vertex_property.key );
  }

  return ret;
}



/******************************************************************************
 * __set_vertex_as_first_arg
 *
 ******************************************************************************
 */
static PyObject * __set_vertex_as_first_arg( PyVGX_Vertex *pyvertex, PyObject **args ) {
  Py_ssize_t sz = PyTuple_GET_SIZE( *args );
  PyObject *local_args = PyTuple_New( 1 + sz );
  if( local_args == NULL ) {
    return NULL;
  }
  // Set vertex as first arg
  Py_INCREF( pyvertex );
  PyTuple_SET_ITEM( local_args, 0, (PyObject*)pyvertex );
  // Copy rest of args
  for( int64_t i=0; i<sz; i++ ) {
    PyObject *arg = PyTuple_GET_ITEM( *args, i );
    Py_INCREF( arg );
    PyTuple_SET_ITEM( local_args, i+1, arg );
  }
  // Use local args
  return *args = local_args;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
#define BEGIN_ARG_INSERT( PyVertex, Args )          \
  do {                                              \
    PyObject *__local__ = NULL;                     \
    if( (__local__ = __set_vertex_as_first_arg( PyVertex, &(Args) )) != NULL ) {  \
      Args = __local__;                             \
      do
      
#define END_ARG_INSERT                              \
      WHILE_ZERO;                                   \
      PyVGX_DECREF( __local__ );                    \
    }                                               \
  } WHILE_ZERO



/******************************************************************************
 * PyVGX_Vertex__Neighborhood
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Neighborhood( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  PyVGX_Graph *pygraph = pyvertex->pygraph;
  BEGIN_ARG_INSERT( pyvertex, args ) {
    py_ret = pyvgx_Neighborhood( pygraph, args, kwds );
  } END_ARG_INSERT;
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__Adjacent
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Adjacent( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  PyVGX_Graph *pygraph = pyvertex->pygraph;
  BEGIN_ARG_INSERT( pyvertex, args ) {
    py_ret = pyvgx_Adjacent( pygraph, args, kwds );
  } END_ARG_INSERT;
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__Aggregate
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Aggregate( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  PyVGX_Graph *pygraph = pyvertex->pygraph;
  BEGIN_ARG_INSERT( pyvertex, args ) {
    py_ret = pyvgx_Aggregate( pygraph, args, kwds );
  } END_ARG_INSERT;
  return py_ret;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static vgx_Vertex_t * __return_readable( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex && CALLABLE( vertex )->Readable( vertex ) ) {
    return vertex;
  }
  else {
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__Inarcs
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Inarcs( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  PyVGX_Graph *pygraph = pyvertex->pygraph;
  BEGIN_ARG_INSERT( pyvertex, args ) {
    py_ret = pyvgx_Inarcs( pygraph, args, kwds );
  } END_ARG_INSERT;
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__Outarcs
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Outarcs( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  PyVGX_Graph *pygraph = pyvertex->pygraph;
  BEGIN_ARG_INSERT( pyvertex, args ) {
    py_ret = pyvgx_Outarcs( pygraph, args, kwds );
  } END_ARG_INSERT;
  return py_ret;
}



/******************************************************************************
 * __PyVGX_Vertex__Neighbors
 *
 ******************************************************************************
 */
static PyObject * __PyVGX_Vertex__Neighbors( PyVGX_Vertex *pyvertex, vgx_BaseCollector_context_t * (*collect_neighbors)( vgx_Vertex_t *vertex ) ) {
  PyObject *py_output = NULL;
  vgx_BaseCollector_context_t *output = NULL;
  vgx_Vertex_t *vertex = NULL;
  BEGIN_PYVGX_THREADS {
    if( (vertex = __return_readable( pyvertex )) != NULL ) {
      output = collect_neighbors( vertex );
    }
  } END_PYVGX_THREADS;

  if( output ) {
    vgx_Graph_t *graph = vertex->graph;
    vgx_Vertex_vtable_t *iV = CALLABLE( vertex );
    Cm256iList_vtable_t *iList = CALLABLE( output->container.sequence.list );
    int64_t sz = iList->Length( output->container.sequence.list );
    if( (py_output = PyList_New( sz )) != NULL ) {
      vgx_CollectorItem_t *cursor = (vgx_CollectorItem_t*)iList->Cursor( output->container.sequence.list, 0 );
      vgx_CollectorItem_t *end = cursor + sz;
      vgx_CollectorItem_t *item;
      int n_yield = 100;
      int64_t i = 0;
      while( (item=cursor++) < end ) {
#ifdef VGX_CONSISTENCY_CHECK
        if( item->headref == NULL || item->headref->vertex == NULL || item->headref->slot.locked == 0 ) {
          PYVGX_API_FATAL( "vertex", 0x001, "Deref vertex NULL or not locked" );
        }
#endif
        vgx_Vertex_t *terminal_RO = item->headref->vertex;
        PyList_SET_ITEM( py_output, i, PyVGX_PyUnicode_FromStringNoErr( iV->IDString( terminal_RO ) ) );
        ++i;
        if( --n_yield < 0 ) {
          n_yield = 100;
          BEGIN_PYVGX_THREADS {
            GRAPH_LOCK( graph ) {
              GRAPH_YIELD_AND_SIGNAL( graph );
            } GRAPH_RELEASE;
          } END_PYVGX_THREADS;
        }
      }
    }
    CALLABLE( graph )->advanced->DeleteCollector( &output );
  }

  if( PyErr_Occurred() ) {
    Py_XDECREF( py_output );
    py_output = NULL;
  }
  else if( py_output == NULL ) {
    PyErr_SetString( PyExc_Exception, "unknown error" );
  }
  return py_output;
}



/******************************************************************************
 * PyVGX_Vertex__Initials
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Initials( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex ) {
    // No args, faster version
    if( ( kwds == NULL || PyDict_Size( kwds ) == 0 ) && PyTuple_GET_SIZE( args ) == 0 ) {
      py_ret = __PyVGX_Vertex__Neighbors( pyvertex, CALLABLE( vertex )->Initials );
    }
    else {
      BEGIN_ARG_INSERT( pyvertex, args ) {
        py_ret = pyvgx_Initials( pyvertex->pygraph, args, kwds );
      } END_ARG_INSERT;
    }
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__Terminals
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Terminals( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex ) {
    // No args, faster version
    if( ( kwds == NULL || PyDict_Size( kwds ) == 0 ) && PyTuple_GET_SIZE( args ) == 0 ) {
      py_ret = __PyVGX_Vertex__Neighbors( pyvertex, CALLABLE( vertex )->Terminals );
    }
    else {
      BEGIN_ARG_INSERT( pyvertex, args ) {
        py_ret = pyvgx_Terminals( pyvertex->pygraph, args, kwds );
      } END_ARG_INSERT;
    }
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__Neighbors
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Neighbors( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( vertex ) {
    // No args, faster version
    if( ( kwds == NULL || PyDict_Size( kwds ) == 0 ) && PyTuple_GET_SIZE( args ) == 0 ) {
      py_ret = __PyVGX_Vertex__Neighbors( pyvertex, CALLABLE( vertex )->Neighbors );
    }
    else {
      BEGIN_ARG_INSERT( pyvertex, args ) {
        py_ret = pyvgx_Neighborhood( pyvertex->pygraph, args, kwds );
      } END_ARG_INSERT;
    }
  }
  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__SetProperty
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetProperty__doc__,
  "SetProperty( name[, value[, virtual]] ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetProperty
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetProperty( PyVGX_Vertex *pyvertex, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  static const char *kwlist[] = {
    "name",
    "value",
    "virtual",
    NULL
  };

  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  union {
    PyObject *args[3];
    struct {
      PyObject *py_name;
      PyObject *py_value;
      PyObject *py_virtual;
    };
  } vcargs = {0};
  
  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 3, vcargs.args ) < 0 ) {
    return NULL;
  }

  // name
  if( vcargs.py_name == NULL ) {
    PyErr_SetString( PyExc_TypeError, "name required" );
    return NULL;
  }
  const char *name = PyVGX_PyObject_AsString( vcargs.py_name );
  if( name == NULL ) {
    return NULL;
  }

  // Assume ok
  py_ret = Py_None;
  Py_INCREF( Py_None );

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      // Ensure vertex is writable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_WL;
      if( (vertex_WL = __ensure_writable_nocreate( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x421 );
      }

      if( __py_set_property( vertex_WL, name, vcargs.py_value, vcargs.py_virtual ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x422 );
      }

    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_ret );
      } END_PYTHON_INTERPRETER;
      py_ret = NULL;
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  return py_ret;
}



/******************************************************************************
 * pyvgx_SetVertexProperties
 *
 ******************************************************************************
 */
DLL_HIDDEN int64_t pyvgx_SetVertexProperties( vgx_Vertex_t *vertex_WL, PyObject *py_properties ) {
  int64_t n = 0;

  BEGIN_PYTHON_INTERPRETER {

    if( PyDict_Check( py_properties ) ) {
      Py_ssize_t pos = 0;
      PyObject *py_name;
      PyObject *py_value;
      int notstr = 0;
      int properr = 0;
      // Process all items in dict
      while( PyDict_Next( py_properties, &pos, &py_name, &py_value ) ) {
        if( PyVGX_PyObject_CheckString( py_name ) ) {
          const char *name = PyVGX_PyObject_AsString( py_name );
          if( __py_set_property( vertex_WL, name, py_value, NULL ) < 0 ) {
            ++properr;
          }
          else {
            ++n;
          }
        }
        else {
          ++notstr;
        }
      }
      // Check for errors
      if( notstr > 0 || properr > 0 ) {
        if( !PyErr_Occurred() ) {
          PyVGXError_SetString( PyExc_TypeError, "Property names must be strings or bytes-like objects" );
        }
        n = -1;
      }
    }
    else {
      PyVGXError_SetString( PyExc_ValueError, "Properties must be a dict: {key:value, ...}" );
      n = -1;
    }

  } END_PYTHON_INTERPRETER;

  return n;
}



/******************************************************************************
 * PyVGX_Vertex__SetProperties
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetProperties__doc__,
  "SetProperties( name ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetProperties
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetProperties( PyVGX_Vertex *pyvertex, PyObject *py_propdict ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    // Ensure vertex is writable (set local locked to non-NULL if needed)
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      vgx_Vertex_t *vertex_WL;
      if( (vertex_WL = __ensure_writable_nocreate( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x431 );
      }

      int64_t n = pyvgx_SetVertexProperties( vertex_WL, py_propdict );

      if( n < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x435 );
      }
      else {
        py_ret = Py_None;
        Py_INCREF( Py_None );
      }
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_ret );
      } END_PYTHON_INTERPRETER;
      py_ret = NULL;
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  return py_ret;
}



/******************************************************************************
 * __py_inc_property
 *
 ******************************************************************************
 */
static PyObject * __py_inc_property( vgx_Vertex_t *vertex_WL, const char *name, PyObject *py_inc ) {
  PyObject *py_ret = NULL;

  // The property to insert
  vgx_VertexProperty_t vertex_property = {0};
  vertex_property.key = iEnumerator_OPEN.Property.Key.New( vertex_WL->graph, name );

  BEGIN_PYTHON_INTERPRETER {
    XTRY {
      if( vertex_property.key == NULL ) {
        PyErr_Format( PyExc_ValueError, "invalid property key: '%s'", name );
        THROW_SILENT( CXLIB_ERR_API, 0x441 );
      }
      // We have an increment value
      if( py_inc ) {
        // INTEGER
        if( PyLong_Check( py_inc ) ) {
          int ovf;
          int64_t x = PyLong_AsLongLongAndOverflow( py_inc, &ovf );
          if( ovf != 0 || x > (1LL<<55)-1 || x < -(1LL<<56) ) {
            PyErr_SetString( PyExc_ValueError, "integer value out of range" );
            THROW_SILENT( CXLIB_ERR_API, 0x442 );
          }
          else {
            vertex_property.val.type = VGX_VALUE_TYPE_INTEGER;
            vertex_property.val.data.simple.integer = x;
          }
        }
        // REAL
        else if( PyFloat_Check( py_inc ) ) {
          vertex_property.val.type = VGX_VALUE_TYPE_REAL;
          vertex_property.val.data.simple.real = PyFloat_AS_DOUBLE( py_inc );
        }
        // STRING
        else {
          PyVGXError_SetString( PyExc_ValueError, "Increment value must be numeric" );
        }
      }
      // Default to inc=1
      else {
        vertex_property.val.type = VGX_VALUE_TYPE_INTEGER;
        vertex_property.val.data.simple.integer = 1;
      }

      // Increment the vertex property
      vgx_VertexProperty_t *ret;
      BEGIN_PYVGX_THREADS {
        ret = CALLABLE( vertex_WL )->IncProperty( vertex_WL, &vertex_property );
      } END_PYVGX_THREADS;
      if( ret == NULL ) {
        PyVGXError_SetString( PyExc_ValueError, "Vertex property cannot be incremented" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x443 );
      }

      // Set the return value to the incremented value
      if( vertex_property.val.type == VGX_VALUE_TYPE_INTEGER ) {
        py_ret = PyLong_FromLongLong( vertex_property.val.data.simple.integer );
      }
      else if( vertex_property.val.type == VGX_VALUE_TYPE_REAL ) {
        py_ret = PyFloat_FromDouble( vertex_property.val.data.simple.real );
      }

    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      PyVGX_XDECREF( py_ret );
      py_ret = NULL;
    }
    XFINALLY {
    }
  } END_PYTHON_INTERPRETER;

  if( vertex_property.key ) {
    CStringDelete( vertex_property.key );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__IncProperty
 *
 ******************************************************************************
 */
PyDoc_STRVAR( IncProperty__doc__,
  "IncProperty( name [, increment] ) -> incremented value\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__IncProperty
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__IncProperty( PyVGX_Vertex *pyvertex, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  static const char *kwlist[] = {
    "name",
    "increment",
    NULL
  };

  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  union {
    PyObject *args[2];
    struct {
      PyObject *py_name;
      PyObject *py_inc;
    };
  } vcargs = {0};

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs.args ) < 0 ) {
    return NULL;
  }

  if( vcargs.py_name == NULL ) {
    PyErr_SetString( PyExc_TypeError, "name required" );
    return NULL;
  }

  // name
  const char *name = PyVGX_PyObject_AsString( vcargs.py_name );
  if( name == NULL ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      // Ensure vertex is writable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_WL;
      if( (vertex_WL = __ensure_writable_nocreate( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x451 );
      }

      if( (py_ret = __py_inc_property( vertex_WL, name, vcargs.py_inc )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x452 );
      }

    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_ret );
      } END_PYTHON_INTERPRETER;
      py_ret = NULL;
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  return py_ret;
}



/******************************************************************************
 * __py_get_property_default
 *
 ******************************************************************************
 */
static PyObject * __py_get_property_default( PyVGX_Vertex *pyvertex, PyObject *py_name, PyObject *py_default ) {
  PyObject *py_value = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  // Property name
  const char *name = PyVGX_PyObject_AsString( py_name );
  if( name == NULL ) {
    return NULL;
  }
  

  BEGIN_PYVGX_THREADS {

    CString_t *CSTR__value = NULL; // If property value is a string we will own it and must discard at the end
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    vgx_Vertex_t *vertex_RO = NULL;

    vgx_VertexProperty_t vertex_property = {0};
    // Special case: Force returned value to be an integer derived from the actual value (when supported for the actual value type found)
    if( *name == '*' ) {
      ++name;
      vertex_property.val.type = VGX_VALUE_TYPE_INTEGER;
    }

    XTRY {
      vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);
      // Ensure vertex is readable (set local locked to non-NULL if needed)
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x461 );
      }

      // Compute lookup key
      iGraphResponse.SelectProperty( vertex_RO->graph, name, &vertex_property );
      if( !vertex_property.keyhash ) {
        PyVGXError_SetString( PyExc_ValueError, "Invalid key" );
        THROW_SILENT( CXLIB_ERR_API, 0x462 );
      }

      // Internal attribute lookup
      if( *name == '_' ) {
        // Look up the internal attribute
        if( iV->GetInternalAttribute( vertex_RO, &vertex_property ) == NULL ) {
          PyVGXError_SetString( PyExc_Exception, "Error during internal attribute lookup" );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x463 );
        }
      }
      // Key-Val property
      else {
        if( iV->GetProperty( vertex_RO, &vertex_property ) == NULL ) {
          PyVGXError_SetString( PyExc_Exception, "Error during property lookup" );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x464 );
        }
      }

      int err = 0;
      BEGIN_PYTHON_INTERPRETER {
        // Handle lookup result
        switch( vertex_property.val.type ) {
        case VGX_VALUE_TYPE_NULL:
          // Nothing found
          if( py_default == NULL ) {
            PyErr_Format( PyExc_LookupError, "Vertex has no property '%s'", name );
            err = -1;
          }
          else {
            py_value = py_default;
            Py_INCREF( py_value );
          }
          break;
        case VGX_VALUE_TYPE_BOOLEAN:
          // The property name exists, but has no value
          py_value = Py_None;
          Py_INCREF( Py_None );
          break;
        case VGX_VALUE_TYPE_INTEGER:
          py_value = PyLong_FromLongLong( vertex_property.val.data.simple.integer );
          break;
        case VGX_VALUE_TYPE_REAL:
          py_value = PyFloat_FromDouble( vertex_property.val.data.simple.real );
          break;
        case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
          /* FALLTHRU */
        case VGX_VALUE_TYPE_CSTRING:
          CSTR__value = (CString_t*)vertex_property.val.data.simple.CSTR__string;
          py_value = iPyVGXCodec.NewPyObjectFromEncodedObject( CSTR__value, NULL );
          break;
        case VGX_VALUE_TYPE_STRING:
          /* FALLTHRU */
        case VGX_VALUE_TYPE_BORROWED_STRING:
          py_value = PyUnicode_FromString( vertex_property.val.data.simple.string );
          break;
        default:
          // Unknown property!
          PyErr_Format( PyExc_LookupError, "An unknown property type was found: %d", vertex_property.val.type );
          err = -1;
        }
      } END_PYTHON_INTERPRETER;

      if( err < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x465 );
      }
    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_value );
      } END_PYTHON_INTERPRETER;
      py_value = NULL;
    }
    XFINALLY {
      // Clean up
      iVertexProperty.ClearSelectProperty( vertex_RO->graph, &vertex_property );
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  if( py_value == NULL && !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "unknown internal error" );
  }

  return py_value;
}



/******************************************************************************
 * __py_vertex_property
 *
 ******************************************************************************
 */
static PyObject * __py_get_property( PyVGX_Vertex *pyvertex, PyObject *py_name ) {
  return __py_get_property_default( pyvertex, py_name, NULL );
}



/******************************************************************************
 * PyVGX_Vertex__GetProperty
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetProperty__doc__,
  "GetProperty( name, default=None ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetProperty
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetProperty( PyVGX_Vertex *pyvertex, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  static const char *kwlist[] = {
    "name",
    "default",
    NULL
  };

  union {
    PyObject *args[2];
    struct {
      PyObject *py_name;
      PyObject *py_default;
    };
  } vcargs = {
      NULL,
      Py_None // NOTE: We don't own a reference (yet). If it is to be returned then INCREF will be performed.
  };

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs.args ) < 0 ) {
    return NULL;
  }

  if( vcargs.py_name == NULL ) {
    PyErr_SetString( PyExc_TypeError, "name required" );
    return NULL;
  }

  // Note: Will own a new reference to the returned value, this applies to default Py_None as well if applicable
  return __py_get_property_default( pyvertex, vcargs.py_name, vcargs.py_default );
}



/******************************************************************************
 * PyVGX_Vertex__GetProperties
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetProperties__doc__,
  "GetProperties() -> dict\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetProperties
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetProperties( PyVGX_Vertex *pyvertex ) {
  PyObject *py_dict = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    XTRY {
      // Ensure vertex is readable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_RO;
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x471 );
      }

      // Get the vertex properties as a new dict
      if( (py_dict = iPyVGXBuilder.VertexPropertiesAsDict( vertex_RO )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x472 );
      }

    }
    XCATCH( errcode ) {
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }

  } END_PYVGX_THREADS;

  return py_dict;
}



/******************************************************************************
 * __vertex_keys_and_values
 *
 ******************************************************************************
 */
DLL_HIDDEN PyObject * pyvgx__vertex_keys_and_values( PyVGX_Vertex *pyvertex, bool _keys, bool _values ) {
  PyObject *py_list = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    vgx_SelectProperties_t *selected_properties = NULL;
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);
    vgx_Graph_t *graph = __vertex->graph;

    XTRY {

      // Ensure vertex is readable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_RO;
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x481 );
      }

      // Get all properties
      if( (selected_properties = iV->GetProperties( vertex_RO )) == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Error during property lookup" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x482 );
      }
      int64_t n_items = selected_properties->len;
      vgx_VertexProperty_t *cursor = selected_properties->properties;

      // Create and populate return list
      bool failed = false;
      BEGIN_PYTHON_INTERPRETER {
        if( (py_list = PyList_New( n_items )) != NULL ) {
          PyObject *py_key = NULL;
          PyObject *py_val = NULL;
          for( int64_t px=0; px<n_items; px++ ) {
            PyObject *py_entry = NULL;
            // Keys should be included
            if( _keys ) {
              CString_t *CSTR__key = (CString_t*)cursor->key;
              if( CSTR__key ) {
                if( (py_key = PyUnicode_FromStringAndSize( CStringValue( CSTR__key ), CStringLength( CSTR__key ) )) == NULL ) {
                  failed = true;
                  break;
                }
              }
              else {
                Py_INCREF( Py_None );
                py_key = Py_None;
              }
              py_entry = py_key;
            }
            // Values should be included
            if( _values ) {
              switch( cursor->val.type ) {
              case VGX_VALUE_TYPE_BOOLEAN:
                Py_INCREF( Py_True );
                py_val = Py_True;
                break;
              case VGX_VALUE_TYPE_INTEGER:
                py_val = PyLong_FromLongLong( cursor->val.data.simple.integer );
                break;
              case VGX_VALUE_TYPE_REAL:
                py_val = PyFloat_FromDouble( cursor->val.data.simple.real );
                break;
              case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
                /* FALLTHRU */
              case VGX_VALUE_TYPE_CSTRING:
                py_val = iPyVGXCodec.NewPyObjectFromEncodedObject( cursor->val.data.simple.CSTR__string, NULL );
                break;
              case VGX_VALUE_TYPE_STRING:
                /* FALLTHRU */
              case VGX_VALUE_TYPE_BORROWED_STRING:
                py_val = PyUnicode_FromString( cursor->val.data.simple.string );
                break;
              default:
                Py_INCREF( Py_None );
                py_val = Py_None;
              }
              if( py_val == NULL ) {
                failed = true;
                break;
              }
              // Key:Value requested, we replace the entry with a (key,value) tuple
              if( _keys ) {
                if( (py_entry = PyTuple_New( 2 )) != NULL ) {
                  PyTuple_SET_ITEM( py_entry, 0, py_key );
                  PyTuple_SET_ITEM( py_entry, 1, py_val );
                }
                else {
                  failed = true;
                  break;
                }
              }
              // Values only
              else {
                py_entry = py_val;
              }
            }
            // Set entry in output list
            PyList_SET_ITEM( py_list, px, py_entry );
            // Next item
            ++cursor;
          }
        }
      } END_PYTHON_INTERPRETER;

      if( py_list == NULL || failed ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x483 );
      }

    }
    XCATCH( errcode ) {
      BEGIN_PYTHON_INTERPRETER {
        PyVGX_SetPyErr( errcode );
        PyVGX_XDECREF( py_list );
      } END_PYTHON_INTERPRETER;
      py_list = NULL;
    }
    XFINALLY {
      iVertexProperty.FreeSelectProperties( graph, &selected_properties );
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *g = __vertex_LOCAL_LOCK->graph;
        CALLABLE( g )->advanced->ReleaseVertex( g, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  if( py_list == NULL && !PyErr_Occurred() ) {
    PyErr_SetString( PyExc_Exception, "unknown internal error" );
  }

  return py_list;
}



/******************************************************************************
 * PyVGX_Vertex__items
 *
 ******************************************************************************
 */
PyDoc_STRVAR( items__doc__,
  "items() -> [(key,val), ...]\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__items
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__items( PyVGX_Vertex *pyvertex ) {
  return pyvgx__vertex_keys_and_values( pyvertex, true, true );
}



/******************************************************************************
 * PyVGX_Vertex__keys
 *
 ******************************************************************************
 */
PyDoc_STRVAR( keys__doc__,
  "keys() -> [key1, key2, ...]\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__keys
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__keys( PyVGX_Vertex *pyvertex ) {
  return pyvgx__vertex_keys_and_values( pyvertex, true, false );
}



/******************************************************************************
 * PyVGX_Vertex__values
 *
 ******************************************************************************
 */
PyDoc_STRVAR( values__doc__,
  "values() -> [val1, val2, ...]\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__values
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__values( PyVGX_Vertex *pyvertex ) {
  return pyvgx__vertex_keys_and_values( pyvertex, false, true );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int pyvgx__vertex_contains( PyVGX_Vertex *pyvertex, PyObject *py_key ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return -1;
  }

  const char *key = PyVGX_PyObject_AsString( py_key );
  if( key == NULL ) {
    return -1;
  }

  vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);

  int ret = 0;
  BEGIN_PYVGX_THREADS {
    // Ensure vertex is readable (set local locked to non-NULL if needed)
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    vgx_Vertex_t *vertex_RO = NULL;
    vgx_VertexProperty_t *vertex_property = NULL;

    XTRY {
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x491 );
      }

      // New property with key
      if( (vertex_property = iVertexProperty.NewDefault( vertex_RO->graph, key )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x492 );
      }

      // Look up the property
      if( iV->HasProperty( vertex_RO, vertex_property ) == true ) {
        ret = 1;
      }

    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
      ret = -1;
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
      iVertexProperty.Delete( &vertex_property );
    }
  } END_PYVGX_THREADS;

  return ret;
}



/******************************************************************************
 * PyVGX_Vertex__HasProperty
 *
 ******************************************************************************
 */
PyDoc_STRVAR( HasProperty__doc__,
  "HasProperty( name [, value_condition] ) -> boolean\n"
  "\n"
);

//static PyObject * PyVGX_Vertex__HasProperty( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {

/**************************************************************************//**
 * PyVGX_Vertex__HasProperty
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__HasProperty( PyVGX_Vertex *pyvertex, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames ) {
  static const char *kwlist[] = {
    "name",
    "value",
    NULL
  };
  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }
  vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);

  union {
    PyObject *args[2];
    struct {
      PyObject *py_name;
      PyObject *py_value;
    };
  } vcargs = {0};

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 2, vcargs.args ) < 0 ) {
    return NULL;
  }

  // name
  if( vcargs.py_name == NULL ) {
    PyErr_SetString( PyExc_TypeError, "name required" );
    return NULL;
  }
  const char *name = PyVGX_PyObject_AsString( vcargs.py_name );
  if( name == NULL ) {
    return NULL;
  }

  // Validate key
  if( !iString.Validate.SelectKey( name ) ) {
    PyErr_Format( PyExc_ValueError, "invalid property key: '%s'", name );
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    // Ensure vertex is readable (set local locked to non-NULL if needed)
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    vgx_Vertex_t *vertex_RO = NULL;
    vgx_VertexProperty_t *vertex_property = NULL;
    vgx_value_condition_t *value_condition = NULL;

    XTRY {
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4A1 );
      }

      if( (value_condition = iVertexProperty.NewValueCondition()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4A2 );
      }

      // The optional property value condition
      if( vcargs.py_value ) {
        vgx_value_constraint_t property_constraint = _vgx_no_value_constraint();
        if( iPyVGXParser.ParseValueCondition( vcargs.py_value, value_condition, property_constraint, VGX_VALUE_EQU ) == VGX_VALUE_NONE ) {
          THROW_ERROR( CXLIB_ERR_API, 0x4A3 );
        }
      }

      // Steal the condition and create the property
      if( (vertex_property = iVertexProperty.NewFromValueCondition( NULL, name, &value_condition )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x4A4 );
      }

      // Look up the property
      if( iV->HasProperty( vertex_RO, vertex_property ) == true ) {
        py_ret = Py_True;
      }
      else {
        py_ret = Py_False;
      }

    }
    XCATCH( errcode ) {
      iVertexProperty.DeleteValueCondition( &value_condition );
      PyVGX_SetPyErr( errcode );
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
      iVertexProperty.Delete( &vertex_property );
    }
  } END_PYVGX_THREADS;

  if( py_ret ) {
    Py_INCREF( py_ret );
  }

  return py_ret;
}




/******************************************************************************
 * PyVGX_Vertex__HasProperties
 *
 ******************************************************************************
 */
PyDoc_STRVAR( HasProperties__doc__,
  "HasProperties() -> boolean\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__HasProperties
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__HasProperties( PyVGX_Vertex *pyvertex ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);

      // Ensure vertex is readable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_RO;
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x4B1 );
      }

      // Look up the property
      if( iV->HasProperties( vertex_RO ) == true ) {
        py_ret = Py_True;
      }
      else {
        py_ret = Py_False;
      }
    }
    XCATCH( errcode ) {
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      } 
    }
  } END_PYVGX_THREADS;

  if( py_ret ) {
    Py_INCREF( py_ret );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__NumProperties
 * 
 ******************************************************************************
 */
PyDoc_STRVAR( NumProperties__doc__,
  "NumProperties() -> integer\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__NumProperties
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__NumProperties( PyVGX_Vertex *pyvertex ) {
  // TODO: FIND A WAY TO GET THE LENGTH DIRECTLY
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  int64_t count = 0;

  BEGIN_PYVGX_THREADS {
    // Ensure vertex is readable (set local locked to non-NULL if needed)
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;
    XTRY {
      vgx_Vertex_t *vertex_RO;
      if( (vertex_RO = __ensure_readable( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x4C1 );
      }

      // Get the count
      count = CALLABLE(vertex_RO)->NumProperties( vertex_RO );
    }
    XCATCH( errcode ) {
      count = -1;
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;


  if( count >= 0 ) {
    return PyLong_FromLongLong( count );
  }
  else {
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__RemoveProperty
 *
 ******************************************************************************
 */
PyDoc_STRVAR( RemoveProperty__doc__,
  "RemoveProperty( name ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__RemoveProperty
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__RemoveProperty( PyVGX_Vertex *pyvertex, PyObject *py_name ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  // Property name
  const char *name = PyVGX_PyObject_AsString( py_name );
  if( name == NULL ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {

    CString_t *CSTR__name = NULL;
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);
      // Ensure vertex is writable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_WL;
      if( (vertex_WL = __ensure_writable_nocreate( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x4D1 );
      }

      int n_deleted;

      vgx_VertexProperty_t vertex_property = {0};

      // The property name
      if( (CSTR__name = NewEphemeralCString( vertex_WL->graph, name )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x4D2 );
      }

      // Prepare the property for lookup
      vertex_property.key = CSTR__name;

      // Delete the property
      if( (n_deleted = iV->RemoveProperty( vertex_WL, &vertex_property )) < 1 ) {
        BEGIN_PYTHON_INTERPRETER {
          if( n_deleted < 0 ) {
            PyVGXError_SetString( PyExc_Exception, "Error during property deletion" );
          }
          else {
            PyErr_Format( PyExc_LookupError, "Vertex has no property '%s'", name );
          }
        } END_PYTHON_INTERPRETER;
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4D3 );
      }

      // ok!
      py_ret = Py_None;
    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
    }
    XFINALLY {
      if( CSTR__name ) {
        CStringDelete( CSTR__name );
      }
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  if( py_ret ) {
    Py_INCREF( Py_None );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__RemoveProperties
 *
 ******************************************************************************
 */
PyDoc_STRVAR( RemoveProperties__doc__,
  "RemoveProperties() -> number\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__RemoveProperties
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__RemoveProperties( PyVGX_Vertex *pyvertex ) {
  PyObject *py_count = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  int64_t n_deleted = -1;

  BEGIN_PYVGX_THREADS {
    vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

    XTRY {
      vgx_Vertex_vtable_t *iV = CALLABLE(__vertex);

      // Ensure vertex is writable (set local locked to non-NULL if needed)
      vgx_Vertex_t *vertex_WL;
      if( (vertex_WL = __ensure_writable_nocreate( __vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x4E1 );
      }

      // Delete all properties
      if( (n_deleted = iV->RemoveProperties( vertex_WL )) < 0 ) {
        PyVGXError_SetString( PyExc_Exception, "Error during property deletion" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4E2 );
      }

    }
    XCATCH( errcode ) {
      PyVGX_SetPyErr( errcode );
    }
    XFINALLY {
      // Locally locked, so release
      if( __vertex_LOCAL_LOCK ) {
        vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
        CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
      }
    }
  } END_PYVGX_THREADS;

  if( n_deleted >= 0 ) {
    py_count = PyLong_FromLongLong( n_deleted );
  }

  return py_count;
}



/******************************************************************************
 * PyVGX_Vertex__SetExpiration
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetExpiration__doc__,
  "SetExpiration( timestamp [, relative] ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetExpiration
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetExpiration( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"expires", "relative", NULL};

  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  uint32_t expires;
  int relative = 0;
  if( !PyArg_ParseTupleAndKeywords( args, kwds, "I|i", kwlist, &expires, &relative ) ) {
    return NULL;
  }

  vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
  if( iV->Writable(vertex) ) {
    uint32_t tmx;

    // Set the absolute tmx from arguments
    if( relative ) {
      tmx = __SECONDS_SINCE_1970() + expires;
    }
    else {
      tmx = expires;
    }

    // Set the expiration
    int ret;
    BEGIN_PYVGX_THREADS {
      ret = iV->SetExpirationTime( vertex, tmx );
    } END_PYVGX_THREADS;

    if( ret < 0 ) {
      PyErr_Format( PyExc_Exception, "Failed to set expiration time for vertex: %s", iV->IDString( vertex ) );
      return NULL;
    }

    Py_RETURN_NONE;

  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex not writable" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__GetExpiration
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetExpiration__doc__,
  "GetExpiration() -> expiration timestamp in seconds since 1970\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetExpiration
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetExpiration( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  uint32_t tmx = CALLABLE( vertex )->GetExpirationTime( vertex );
  return PyLong_FromUnsignedLong( tmx );
}



/******************************************************************************
 * PyVGX_Vertex__IsExpired
 *
 ******************************************************************************
 */
PyDoc_STRVAR( IsExpired__doc__,
  "IsExpired() -> bool\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__IsExpired
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__IsExpired( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  if( CALLABLE( vertex )->IsExpired( vertex ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__ClearExpiration
 *
 ******************************************************************************
 */
PyDoc_STRVAR( ClearExpiration__doc__,
  "ClearExpiration() -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__ClearExpiration
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__ClearExpiration( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }

  vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
  if( iV->Writable(vertex) ) {
    // Set the expiration time to never
    int ret;
    BEGIN_PYVGX_THREADS {
      ret = iV->SetExpirationTime( vertex, TIME_EXPIRES_NEVER );
    } END_PYVGX_THREADS;

    if( ret < 0 ) {
      PyErr_Format( PyExc_Exception, "Failed to clear expiration time for vertex: %s", iV->IDString( vertex ) );
      return NULL;
    }
    Py_RETURN_NONE;
  }
  else {
    PyVGXError_SetString( PyVGX_AccessError, "Vertex not writable" );
    return NULL;
  }
}



/******************************************************************************
 * PyVGX_Vertex__Descriptor
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Descriptor__doc__,
  "Descriptor() -> string\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Descriptor
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Descriptor( PyVGX_Vertex *pyvertex ) {
  PyObject *py_str = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {

    char *data = NULL;
    XTRY {
      static CStringQueue_t *output = NULL;
      static CStringQueue_vtable_t *ioutput = NULL;

      if( output == NULL ) {
        output = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );
        ioutput = CALLABLE(output);
      }

      if( CALLABLE( __vertex )->Descriptor( __vertex, output ) == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Internal error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4F1 );
      }

      ioutput->NulTermNolock( output );
      if( ioutput->ReadNolock( output, (void**)&data, -1 ) < 0 || data == NULL ) {
        PyVGXError_SetString( PyExc_Exception, "Internal error" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4F2 );
      }

      BEGIN_PYTHON_INTERPRETER {
        py_str = PyUnicode_FromString( data );
      } END_PYTHON_INTERPRETER;

    }
    XCATCH( errcode ) {
    }
    XFINALLY {
      if( data ) {
        ALIGNED_FREE( data );
      }
    }
  } END_PYVGX_THREADS;

  return py_str;
}



/******************************************************************************
 * PyVGX_Vertex__SetType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( SetType__doc__,
  "SetType( type ) -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__SetType
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__SetType( PyVGX_Vertex *pyvertex, PyObject *py_type ) {
  PyObject *py_ret = NULL;
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  // Type
  const char *type = NULL;
  if( py_type != Py_None ) {
    if( (type = PyVGX_PyObject_AsString( py_type )) == NULL ) {
      return NULL;
    }
  }

  vgx_Graph_t *graph = __vertex->graph;
  CString_t *CSTR__name = NULL;
  CString_t *CSTR__type = NULL;

  XTRY {
    if( (CSTR__name = CALLABLE( __vertex )->IDCString( __vertex )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
    if( type != NULL && (CSTR__type = NewEphemeralCString( graph, type )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
    vgx_vertex_type_t tp = 0;

    BEGIN_PYVGX_THREADS {
      tp = CALLABLE( graph )->simple->VertexSetType( graph, CSTR__name, CSTR__type, 0, &reason );
    } END_PYVGX_THREADS;

    if( !__vertex_type_enumeration_valid( tp ) ) {
      PyErr_Format( PyVGX_EnumerationError, "type enumeration error: %x", tp );
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
    }

    Py_INCREF( Py_None );
    py_ret = Py_None;

  }
  XCATCH( errcode ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyExc_Exception, "internal error" );
    }
  }
  XFINALLY {
    iString.Discard( &CSTR__name );
    iString.Discard( &CSTR__type );
  }

  return py_ret;
}



/******************************************************************************
 * PyVGX_Vertex__GetType
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetType__doc__,
  "GetType() -> type\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetType
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetType( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  return __py_get_type( __vertex, NULL );
}



/******************************************************************************
 * PyVGX_Vertex__GetTypeEnum
 *
 ******************************************************************************
 */
PyDoc_STRVAR( GetTypeEnum__doc__,
  "GetTypeEnum() -> enum\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__GetTypeEnum
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__GetTypeEnum( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  vgx_vertex_type_t tp = CALLABLE( __vertex )->Type( __vertex );
  
  return PyLong_FromLong( tp );
}



/******************************************************************************
 * PyVGX_Vertex__Commit
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Commit__doc__,
  "Commit() -> opcount\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Commit
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Commit( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !vertex ) {
    return NULL;
  }
  int64_t op = -1;
  BEGIN_PYVGX_THREADS {
    if( CALLABLE( vertex )->Writable( vertex ) ) {
      vgx_Graph_t *graph = vertex->graph;
      op = CALLABLE( graph )->advanced->CommitVertex( graph, vertex );
    }
  } END_PYVGX_THREADS;  
  return PyLong_FromLongLong( op ); 
}



/******************************************************************************
 * PyVGX_Vertex__IsVirtual
 *
 ******************************************************************************
 */
PyDoc_STRVAR( IsVirtual__doc__,
  "IsVirtual() -> bool\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__IsVirtual
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__IsVirtual( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }
  
  if( __vertex_is_manifestation_virtual( __vertex ) ) {
    Py_RETURN_TRUE;
  }
  else {
    Py_RETURN_FALSE;
  }
}



/******************************************************************************
 * PyVGX_Vertex__AsDict
 *
 ******************************************************************************
 */
PyDoc_STRVAR( AsDict__doc__,
  "AsDict() -> dict of vertex attributes\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__AsDict
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__AsDict( PyVGX_Vertex *py_vertex ) {
  PyObject *py_dict = PyDict_New();
  PyObject *py_alloc = PyDict_New();
  PyObject *py_rank = PyDict_New();

  if( py_dict == NULL || py_alloc == NULL || py_rank == NULL ) {
    PyVGX_XDECREF( py_dict );
    PyVGX_XDECREF( py_alloc );
    PyVGX_XDECREF( py_rank );
    return NULL;
  }

  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( py_vertex );

  if( vertex ) {

    CString_t *CSTR__vertex_id = NULL;
    vgx_Vertex_t *vertex_LCK = NULL;

    BEGIN_PYVGX_THREADS {
      vgx_Vertex_vtable_t *iV = CALLABLE( vertex );
      vgx_Vertex_t *__vertex_LOCAL_LOCK = NULL;

      XTRY {
        int err = 0;

        if( (CSTR__vertex_id = NewEphemeralCString( vertex->graph, iV->IDString( vertex ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x501 );
        }

        // Ensure vertex is readable (set local locked to non-NULL if needed)
        if( (vertex_LCK = __ensure_readable( vertex, &__vertex_LOCAL_LOCK )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x502 );
        }

        BEGIN_PYTHON_INTERPRETER {
          // IDString
          err += PyVGX_DictStealItemString( py_dict, "id", __py_get_identifier( vertex_LCK, NULL ) );
          // InternalID
          err += PyVGX_DictStealItemString( py_dict, "internalid", __py_get_internalid( vertex_LCK, NULL ) );
          // OutDegree
          err += PyVGX_DictStealItemString( py_dict, "odeg", __py_get_outdegree( vertex_LCK, NULL ) );
          // InDegree
          err += PyVGX_DictStealItemString( py_dict, "ideg", __py_get_indegree( vertex_LCK, NULL ) );
          // Degree
          err += PyVGX_DictStealItemString( py_dict, "deg", __py_get_degree( vertex_LCK, NULL ) );
          // Descriptor
          err += PyVGX_DictStealItemString( py_dict, "descriptor", __py_get_descriptor( vertex_LCK, NULL ) );
          // TypeName 
          err += PyVGX_DictStealItemString( py_dict, "type", __py_get_type( vertex_LCK, NULL ) );
          // Manifestation
          err += PyVGX_DictStealItemString( py_dict, "man", __py_get_manifestation( vertex_LCK, NULL ) );
          // Vector
          err += PyVGX_DictStealItemString( py_dict, "vector", __py_get_vector( vertex_LCK, NULL ) );
          // Properties
          PyObject *py_prop = iPyVGXBuilder.VertexPropertiesAsDict( vertex_LCK );
          err += iPyVGXBuilder.DictMapStringToPyObject( py_dict, "properties", &py_prop );
          // tmc
          err += PyVGX_DictStealItemString( py_dict, "tmc", __py_get_TMC( vertex_LCK, NULL ) );
          // tmm
          err += PyVGX_DictStealItemString( py_dict, "tmm", __py_get_TMM( vertex_LCK, NULL ) );
          // tmx
          err += PyVGX_DictStealItemString( py_dict, "tmx", __py_get_TMX( vertex_LCK, NULL ) );
          // rank.c1
          err += PyVGX_DictStealItemString( py_rank, "c1", __py_get_rank_cX( vertex_LCK, (void*)__rank_c1 ) );
          // rank.c0
          err += PyVGX_DictStealItemString( py_rank, "c0", __py_get_rank_cX( vertex_LCK, (void*)__rank_c0 ) );
          // rank
          PyVGX_DictStealItemString( py_dict, "rank", py_rank );
          // virtual
          err += PyVGX_DictStealItemString( py_dict, "virtual", __py_get_virtual( vertex_LCK, NULL ) );
          // address
          err += PyVGX_DictStealItemString( py_dict, "address", __py_get_address( vertex_LCK, NULL ) );
          // index
          err += PyVGX_DictStealItemString( py_dict, "index", __py_get_index( vertex_LCK, NULL ) );
          // bitindex
          err += PyVGX_DictStealItemString( py_dict, "bitindex", __py_get_bitindex( vertex_LCK, NULL ) );
          // bitvector
          err += PyVGX_DictStealItemString( py_dict, "bitvector", __py_get_bitvector( vertex_LCK, NULL ) );
          // op
          err += PyVGX_DictStealItemString( py_dict, "op", __py_get_opcnt( vertex_LCK, NULL ) );
          // enum
          err += PyVGX_DictStealItemString( py_dict, "enum", PyLong_FromLong( CALLABLE( vertex_LCK )->VertexEnum( vertex_LCK ) ) );
          // readers
          err += PyVGX_DictStealItemString( py_dict, "readers", __py_get_readers( vertex_LCK, NULL ) );
          // xrecursion
          err += PyVGX_DictStealItemString( py_dict, "xrecursion", __py_get_xrecursion( vertex_LCK, NULL ) );


          // Internal things
          cxmalloc_linehead_t *linehead = _cxmalloc_linehead_from_object( vertex_LCK );
          err += PyVGX_DictStealItemString( py_alloc, "aidx", __py_get_aidx( vertex_LCK, NULL ) );
          err += PyVGX_DictStealItemString( py_alloc, "bidx", __py_get_bidx( vertex_LCK, NULL ) );
          err += PyVGX_DictStealItemString( py_alloc, "oidx", __py_get_oidx( vertex_LCK, NULL ) );
          err += PyVGX_DictStealItemString( py_alloc, "flags.invl", PyLong_FromLong( linehead->data.flags.invl ) );
          err += PyVGX_DictStealItemString( py_alloc, "flags.ovsz", PyLong_FromLong( linehead->data.flags.ovsz ) );
          err += PyVGX_DictStealItemString( py_alloc, "flags._chk", PyLong_FromLong( linehead->data.flags._chk ) );
          err += PyVGX_DictStealItemString( py_alloc, "refc", __py_get_refc( vertex_LCK, NULL ) );
          err += PyVGX_DictStealItemString( py_alloc, "size", PyLong_FromUnsignedLong( linehead->data.size ) );
          err += PyVGX_DictStealItemString( py_alloc, "handle", __py_get_handle( vertex_LCK, NULL ) );
          PyVGX_DictStealItemString( py_dict, "allocator", py_alloc );
        } END_PYTHON_INTERPRETER;
        
        if( err != 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x503 );
        }
      }
      XCATCH( errcode ) {
        BEGIN_PYTHON_INTERPRETER {
          PyVGX_XDECREF( py_dict );
        } END_PYTHON_INTERPRETER;
        py_dict = NULL;
      }
      XFINALLY {
        iString.Discard( &CSTR__vertex_id );
        // Locally locked, so release
        if( __vertex_LOCAL_LOCK ) {
          vgx_Graph_t *graph = __vertex_LOCAL_LOCK->graph;
          CALLABLE( graph )->advanced->ReleaseVertex( graph, &__vertex_LOCAL_LOCK );
        }
      }
    } END_PYVGX_THREADS;
  }

  return py_dict;
}



/******************************************************************************
 * PyVGX_Vertex__DebugVector
 *
 ******************************************************************************
 */
PyDoc_STRVAR( DebugVector__doc__,
  "DebugVector() -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__DebugVector
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__DebugVector( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  vgx_Vector_t *vector = __vertex->vector;
  if( vector ) {
    vgx_Similarity_t *sim = CALLABLE( vector )->Context( vector )->simobj;
    CALLABLE( sim )->PrintVectorAllocator( sim, vector );
  }
  else {
    printf( "No vector\n" );
  }
  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Vertex__Debug
 *
 ******************************************************************************
 */
PyDoc_STRVAR( Debug__doc__,
  "Debug() -> None\n"
);

/**************************************************************************//**
 * PyVGX_Vertex__Debug
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__Debug( PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *__vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );
  if( !__vertex ) {
    return NULL;
  }

  CALLABLE( __vertex )->PrintVertexAllocator( __vertex );

  Py_RETURN_NONE;
}



/******************************************************************************
 * PyVGX_Vertex__repr
 *
 ******************************************************************************
 */
static PyObject * PyVGX_Vertex__repr( PyVGX_Vertex *pyvertex ) {
  static const char *no_yes[2] = {"no","yes"};
  PyObject *py_repr = NULL;

  vgx_Vertex_t *vertex = __PyVGX_Vertex_as_vgx_Vertex_t( pyvertex );

  if( vertex ) {

    int64_t pyrefs = Py_REFCNT( pyvertex );
    const CString_t *CSTR__repr;

    BEGIN_PYVGX_THREADS {
      vgx_Graph_t *graph = vertex->graph;
      GRAPH_LOCK( graph ) {
        vgx_Vertex_vtable_t *iV = CALLABLE(vertex);

        int64_t indegree = iV->InDegree(vertex);
        int64_t outdegree = iV->OutDegree(vertex);
        const char *id = iV->IDString(vertex);
        const CString_t *CSTR__type = iV->TypeName(vertex);
        int64_t refcnt = Vertex_REFCNT_CS_RO( vertex );

        const char *readable = no_yes[ iV->Readable( vertex ) ];
        const char *writable = no_yes[ iV->Writable( vertex ) ];
        int semcnt = vertex->descriptor.semaphore.count;
        const char *man = __vertex_is_manifestation_real( vertex ) ? "REAL" : "VIRTUAL";
        DWORD owner = vertex->descriptor.writer.threadid;

        CSTR__repr = CStringNewFormat( "<PyVGX_Vertex: -%lld->(%s)-%lld-> type=%s man=%s refcnt=%lld pyrefs=%lld read=%s write=%s semcnt=%d owner=%d>",
                                       /*               |        \   \         |      |         |           |         |        |         |        |     */
                                                        indegree, id, outdegree, /*   |         |           |         |        |         |        |     */
                                                                               CStringValue(CSTR__type), /* |         |        |         |        |     */
                                                                                      man, /*   |           |         |        |         |        |     */
                                                                                                refcnt, /*  |         |        |         |        |     */ 
                                                                                                            pyrefs, /*|        \         \        |     */
                                                                                                                      readable, writable, semcnt,
                                                                                                                                                  owner
     
                                     );
      } GRAPH_RELEASE;
    } END_PYVGX_THREADS;

    if( CSTR__repr ) {
      py_repr = PyUnicode_FromString( CStringValue( CSTR__repr ) );
      CStringDelete( CSTR__repr );
    }
    else {
      PyVGXError_SetString( PyExc_MemoryError, "Failed to create vertex representation string" );
    }
  }

  return py_repr;
}



/******************************************************************************
 * PyVGX_Vertex__dealloc
 *
 ******************************************************************************
 */
static void PyVGX_Vertex__dealloc( PyVGX_Vertex *pyvertex ) {
  // Close the vertex if not already done earlier via explicit call and if the registry has not already been closed
  vgx_Vertex_t *vertex = _match_pyvertex_generation_guard( pyvertex );
  if( vertex && _registry_loaded ) {
    if( COMLIB_OBJECT_CLASSMATCH( vertex, COMLIB_CLASS_CODE( vgx_Vertex_t ) ) ) {
      if( __vertex_is_locked( vertex ) ) {
        bool released = false;
        BEGIN_PYVGX_THREADS {
          vgx_Vertex_vtable_t *iV = CALLABLE(vertex);
          vgx_Graph_t *graph = iV->Parent( vertex );
          vgx_Vertex_t *vertex_LCK = vertex;
          released = CALLABLE(graph)->simple->CloseVertex( graph, &vertex_LCK );  // core lock
        } END_PYVGX_THREADS;
        if( released ) {
          pyvertex->vertex = NULL;
        }
      }
      // Python wrapper references unlocked vertex
      else {
        pyvertex->vertex = NULL;
      }
    }
    else {
      pyvertex->vertex = NULL;
    }
  }
  Py_TYPE( pyvertex )->tp_free( pyvertex );
}



/******************************************************************************
 * __new
 *
 ******************************************************************************
 */
static PyObject * __new( void ) {
  PyVGX_Vertex *pyvertex = (PyVGX_Vertex*)p_PyVGX_Vertex__VertexType->tp_alloc( p_PyVGX_Vertex__VertexType, 0);
  pyvertex->vertex = NULL;
  pyvertex->pygraph = NULL;
  pyvertex->gen_guard = 0;
  return (PyObject *)pyvertex;
}



/******************************************************************************
 * PyVGX_Vertex__new
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Vertex__new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  return __new();
}



/******************************************************************************
 * __init
 *
 ******************************************************************************
 */
static int __init( PyVGX_Vertex *pyvertex, init_args *vcargs ) {

  // graph
  PyVGX_Graph *pygraph_ref = NULL;
  vgx_Graph_t *graph;
  if( vcargs->pygraph == NULL ) {
    PyVGXError_SetString( PyExc_TypeError, "Graph object required" );
    return -1;
  }
  else if( PyVGX_Graph_Check(vcargs->pygraph) ) {
    pygraph_ref = (PyVGX_Graph*)vcargs->pygraph;
    graph = (pygraph_ref)->graph;
  }
  else if( PyCapsule_CheckExact(vcargs->pygraph) ) {
    graph = (vgx_Graph_t*)PyCapsule_GetPointer( vcargs->pygraph, NULL );
    if( !COMLIB_OBJECT_ISINSTANCE( graph, vgx_Graph_t ) ) {
      PyVGXError_SetString( PyExc_ValueError, "Graph capsule does not contain a graph object" );
      return -1;
    }
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Invalid graph object" );
    return -1;
  }

  // id
  const char *vertex_id = NULL;
  Py_ssize_t id_len = 0;
  Py_ssize_t id_ucsz = 0;
  QWORD vertex_addr = 0;
  if( vcargs->py_id == NULL ) {
    PyVGXError_SetString( PyExc_TypeError, "Vertex identifier required" );
    return -1;
  }
  else if( PyVGX_PyObject_CheckString( vcargs->py_id ) ) {
    if( (vertex_id = PyVGX_PyObject_AsUTF8AndSize( vcargs->py_id, &id_len, &id_ucsz, NULL )) == NULL ) {
      return -1;
    }
    else if( id_len > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
      PyErr_Format( PyVGX_VertexError, "Vertex identifier too long (%lld), max length is %d", id_len, _VXOBALLOC_CSTRING_MAX_LENGTH );
      return -1;
    }
  }
  else if( PyLong_CheckExact( vcargs->py_id ) && (vertex_addr = PyLong_AsLongLong( vcargs->py_id )) != 0 ) {
  }
  else {
    PyVGXError_SetString( PyExc_ValueError, "Vertex id must be string or memory address" );
    return -1;
  }

  // Access mode
  vgx_VertexAccessMode_t access_mode;
  if( vcargs->py_mode == NULL || vcargs->py_mode == Py_None ) {
    access_mode = VGX_VERTEX_ACCESS_WRITABLE_NOCREATE;
  }
  else if( PyVGX_PyObject_CheckString( vcargs->py_mode ) ) {
    Py_ssize_t sz_mode_char = 0;
    Py_ssize_t _ign;
    const char *mode_char = __PyVGX_PyObject_AsStringAndSize( vcargs->py_mode, &sz_mode_char, &_ign );
    if( sz_mode_char != 1 ) {
      PyErr_Format( PyExc_ValueError, "Invalid mode: '%s'", mode_char );
      return -1;
    }
    switch( *mode_char ) {
    case 'r':
      access_mode = VGX_VERTEX_ACCESS_READONLY;
      break;
    case 'w':
      access_mode = VGX_VERTEX_ACCESS_WRITABLE;
      if( vertex_id == NULL ) {
        PyVGXError_SetString( PyExc_ValueError, "Vertex id must be string when mode is 'w'" );
        return -1;
      }
      break;
    case 'a':
      access_mode = VGX_VERTEX_ACCESS_WRITABLE_NOCREATE;
      break;
    default:
      PyErr_SetString( PyExc_ValueError, "Access mode must be one of 'r', 'w', 'a'" );
      return -1;
    }
  }
  else {
    PyErr_Format( PyExc_TypeError, "Access mode must be string, bytes or None, not %s", Py_TYPE( vcargs->py_mode )->tp_name );
    return -1;
  }

  // Vertex type
  const char *vertex_type;
  if( vcargs->py_type == NULL || vcargs->py_type == Py_None ) {
    vertex_type = NULL;
  }
  else if( PyVGX_PyObject_CheckString( vcargs->py_type ) ) {
    if( access_mode != VGX_VERTEX_ACCESS_WRITABLE ) {
      PyVGXError_SetString( PyExc_ValueError, "Vertex type argument not permitted for this access mode" );
      return -1;
    }
    vertex_type = PyVGX_PyObject_AsString( vcargs->py_type );
  }
  else {
    PyErr_Format( PyExc_TypeError, "Vertex type must be string or None, not %s", Py_TYPE( vcargs->py_type )->tp_name );
    return -1;
  }

  // Lifespan
  int lifespan;
  if( vcargs->py_lifespan == NULL ) {
    lifespan = -1;
  }
  else if( PyLong_Check( vcargs->py_lifespan ) ) {
    lifespan = PyLong_AS_LONG( vcargs->py_lifespan );
  }
  else {
    PyErr_Format( PyExc_TypeError, "Vertex lifespan must be integer, not %s", Py_TYPE( vcargs->py_lifespan )->tp_name );
    return -1;
  }

  // Timeout
  int timeout_ms;
  if( vcargs->py_timeout == NULL ) {
    timeout_ms = 0;
  }
  else if( PyLong_Check( vcargs->py_timeout ) ) {
    timeout_ms = PyLong_AS_LONG( vcargs->py_timeout );
  }
  else {
    PyErr_Format( PyExc_TypeError, "Vertex access timeout must be integer, not %s", Py_TYPE( vcargs->py_timeout )->tp_name );
    return -1;
  }
  
  vgx_Vertex_t *vertex = NULL;
  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

  BEGIN_PYVGX_THREADS {

    CString_t *CSTR__error = NULL;
    CString_t *CSTR__vertex_id = NULL;
    CString_t *CSTR__vertex_type = NULL;

    XTRY {
      
      vgx_IGraphAdvanced_t *igraphadv = CALLABLE( graph )->advanced;

      // Vertex Address
      if( vertex_addr ) {
        if( (CSTR__vertex_id = CALLABLE( graph )->advanced->GetVertexIDByAddress( graph, vertex_addr, &reason )) == NULL ) {
          CSTR__error = CStringNewFormat( "bad address: %016llx", vertex_addr );
          THROW_SILENT( CXLIB_ERR_API, 0x510 );
        }
      }
      // Vertex ID
      else if( (CSTR__vertex_id = NewEphemeralCStringLen( graph, vertex_id, (int)id_len, (int)id_ucsz )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x511 );
      }

      // Vertex Type (optional)
      if( vertex_type ) {
        if( (CSTR__vertex_type = NewEphemeralCString( graph, vertex_type )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x512 );
        }
      }

      const objectid_t *obid = CStringObid( CSTR__vertex_id );
      GRAPH_LOCK( graph ) {
        bool retry;
        do {
          retry = false;
          // Vertex exists
          if( (vertex = igraphadv->GetVertex_CS( graph, NULL, obid )) != NULL ) {
            switch( access_mode ) {
            case VGX_VERTEX_ACCESS_WRITABLE:
              // Vertex type supplied when opening existing vertex: we have to go through validation
              if( vertex_type ) {
                vertex = igraphadv->NewVertex_CS( graph, obid, CSTR__vertex_id, CSTR__vertex_type, timeout_ms, &reason, &CSTR__error );
              }
              else {
                vertex = igraphadv->AcquireVertexObjectWritable_CS( graph, vertex, timeout_ms, &reason );
              }
              // Vertex disappeared while waiting for writable access: retry
              if( vertex == NULL && __is_access_reason_noexist( reason ) ) {
                retry = true;
              }
              break;
            case VGX_VERTEX_ACCESS_WRITABLE_NOCREATE:
              vertex = igraphadv->AcquireVertexObjectWritable_CS( graph, vertex, timeout_ms, &reason );
              break;
            case VGX_VERTEX_ACCESS_READONLY:
              vertex = igraphadv->AcquireVertexObjectReadonly_CS( graph, vertex, timeout_ms, &reason );
              break;
            default:
              break;
            }
          }
          // Vertex does not exist and writable mode: Create new vertex
          else if( access_mode == VGX_VERTEX_ACCESS_WRITABLE ) {
            vertex = igraphadv->NewVertex_CS( graph, obid, CSTR__vertex_id, CSTR__vertex_type, timeout_ms, &reason, &CSTR__error );
          }
          // Vertex does not exist and other mode
          else {
            reason = VGX_ACCESS_REASON_NOEXIST;
          }
          // Lifespan
          if( lifespan > -1 && vertex && __vertex_is_writable( vertex ) ) {
            uint32_t tmx = _vgx_graph_seconds( graph );
            if( lifespan == 0 ) {
              tmx -= 1; // trick vertex into thinking it's already expired
            }
            else {
              tmx += lifespan;
            }
            if( CALLABLE( vertex )->SetExpirationTime( vertex, tmx ) < 0 ) {
              reason = VGX_ACCESS_REASON_ERROR;
              CSTR__error = CStringNewFormat( "failed to set expiration time: %u", tmx );
              igraphadv->ReleaseVertex_CS( graph, &vertex );
            }
          }
        } while( retry );
      } GRAPH_RELEASE;

      // Failed to create or open
      if( vertex == NULL ) {
        if( iPyVGXBuilder.SetPyErrorFromAccessReason( vertex_id, reason, &CSTR__error ) ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x513 );
        }
        else {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x514 );
        }
      }

    }
    XCATCH( errcode ) {
      if( reason != VGX_ACCESS_REASON_NONE || CSTR__error ) {
        if( vertex_id ) {
          iPyVGXBuilder.SetPyErrorFromAccessReason( vertex_id, reason, &CSTR__error );
        }
        else {
          if( reason == VGX_ACCESS_REASON_NOEXIST ) {
            reason = VGX_ACCESS_REASON_NOEXIST_MSG;
          }
          iPyVGXBuilder.SetPyErrorFromAccessReason( NULL, reason, &CSTR__error );
        }
      }
      else {
        PyVGX_SetPyErr( errcode );
      }
    }
    XFINALLY {
      iString.Discard( &CSTR__vertex_id );
      iString.Discard( &CSTR__vertex_type );
      iString.Discard( &CSTR__error );
    }
  } END_PYVGX_THREADS;

  // Assign the open vertex to the Python object
  if( vertex ) {
    pyvertex->vertex = vertex;
    pyvertex->pygraph = pygraph_ref;
    pyvertex->gen_guard = _get_pyvertex_generation_guard();
    return 0;
  }
  else {
    return -1;
  }
}



/******************************************************************************
 * PyVGX_Vertex__init
 *
 ******************************************************************************
 */
static int PyVGX_Vertex__init( PyVGX_Vertex *pyvertex, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"graph", "id", "type", "mode", "lifespan", "timeout", NULL};

  init_args vcargs = {0};

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "OO|OOOO", kwlist, &vcargs.pygraph, &vcargs.py_id, &vcargs.py_type, &vcargs.py_mode, &vcargs.py_lifespan, &vcargs.py_timeout ) ) {
    return -1;
  }

  return __init( pyvertex, &vcargs );
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * PyVGX_Vertex__vectorcall( PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames ) {

  static const char *kwlist[] = {
    "graph",
    "id",
    "type",
    "mode",
    "lifespan",
    "timeout",
    NULL
  };

  init_args vcargs = {0};

  int64_t nargs = PyVectorcall_NARGS( nargsf );

  if( __parse_vectorcall_args( args, nargs, kwnames, kwlist, 6, vcargs.arg ) < 0 ) {
    return NULL;
  }

  PyObject *py_vertex = __new();
  if( py_vertex ) {
    if( __init( (PyVGX_Vertex*)py_vertex, &vcargs ) < 0 ) {
      PyVGX_Vertex__dealloc( (PyVGX_Vertex*)py_vertex );
      return NULL;
    }
  }
  return py_vertex;
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN PyObject * PyVGX_Vertex__FromInstance( PyVGX_Graph *pygraph, vgx_Vertex_t *vertex_LCK ) {
  PyVGX_Vertex *pyvertex = (PyVGX_Vertex*)__new();
  if( pyvertex ) {
    pyvertex->vertex = vertex_LCK;
    pyvertex->pygraph = pygraph;
    pyvertex->gen_guard = _get_pyvertex_generation_guard();
  }
  return (PyObject*)pyvertex;
}



/******************************************************************************
 * PyVGX_Vertex__ass_subscript
 *
 ******************************************************************************
 */
static int PyVGX_Vertex__ass_subscript( PyVGX_Vertex *pyvertex, PyObject *py_key, PyObject *py_value ) {
  int ret = 0;
  PyObject *py_ret = NULL;

  if( py_value != NULL ) {

    PyObject *args[] = {
      py_key,
      py_value
    };

    py_ret = PyVGX_Vertex__SetProperty( pyvertex, args, 2, NULL );
  }
  else {
    py_ret = PyVGX_Vertex__RemoveProperty( pyvertex, py_key );
  }

  if( py_ret ) {
    PyVGX_DECREF( py_ret );
  }
  else {
    ret = -1;
  }

  return ret;
}



/******************************************************************************
 * PyVGX_Vertex__members
 *
 ******************************************************************************
 */
static PyMemberDef PyVGX_Vertex__members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
* PyVGX_Vertex__getset
*
******************************************************************************
*/
static PyGetSetDef PyVGX_Vertex__getset[] = {
  {"properties",      (getter)__PyVGX_Vertex__get_properties,           (setter)NULL,                                       "properties",     NULL },
  {"descriptor",      (getter)__PyVGX_Vertex__get_descriptor,           (setter)NULL,                                       "descriptor",     NULL },
  {"manifestation",   (getter)__PyVGX_Vertex__get_manifestation,        (setter)NULL,                                       "manifestation",  NULL },
  {"man",             (getter)__PyVGX_Vertex__get_manifestation,        (setter)NULL,                                       "manifestation",  NULL },
  {"readers",         (getter)__PyVGX_Vertex__get_readers,              (setter)NULL,                                       "readers",        NULL },
  {"xrecursion",      (getter)__PyVGX_Vertex__get_xrecursion,           (setter)NULL,                                       "xrecursion",     NULL },
  {"owner",           (getter)__PyVGX_Vertex__get_owner,                (setter)NULL,                                       "owner",          NULL },

  {"id",              (getter)__PyVGX_Vertex__get_identifier,           (setter)NULL,                                       "identifier",     NULL },
  {"internalid",      (getter)__PyVGX_Vertex__get_internalid,           (setter)NULL,                                       "internalid",     NULL },
  {"type",            (getter)__PyVGX_Vertex__get_type,                 (setter)NULL,                                       "type",           NULL },
  {"deg",             (getter)__PyVGX_Vertex__get_degree,               (setter)NULL,                                       "degree",         NULL },
  {"ideg",            (getter)__PyVGX_Vertex__get_indegree,             (setter)NULL,                                       "indegree",       NULL },
  {"odeg",            (getter)__PyVGX_Vertex__get_outdegree,            (setter)NULL,                                       "outdegree",      NULL },
  {"isolated",        (getter)__PyVGX_Vertex__get_isolated,             (setter)NULL,                                       "isolated",       NULL },
  {"vector",          (getter)__PyVGX_Vertex__get_vector,               (setter)NULL,                                       "vector",         NULL },
  {"tmc",             (getter)__PyVGX_Vertex__get_TMC,                  (setter)NULL,                                       "TMC",            NULL },
  {"tmm",             (getter)__PyVGX_Vertex__get_TMM,                  (setter)NULL,                                       "TMM",            NULL },
  {"tmx",             (getter)__PyVGX_Vertex__get_TMX,                  (setter)NULL,                                       "TMX",            NULL },
  {"rtx",             (getter)__PyVGX_Vertex__get_rtx,                  (setter)NULL,                                       "rtx",            NULL },
  {"c1",              (getter)__PyVGX_Vertex__get_rank_cX,              (setter)__PyVGX_Vertex__set_rank_cX,                "rank_c1",        (void*)__rank_c1 },
  {"c0",              (getter)__PyVGX_Vertex__get_rank_cX,              (setter)__PyVGX_Vertex__set_rank_cX,                "rank_c0",        (void*)__rank_c0 },
  {"b1",              (getter)__PyVGX_Vertex__get_rank_b1,              (setter)NULL,                                       "rank_b1",        NULL },
  {"b0",              (getter)__PyVGX_Vertex__get_rank_b0,              (setter)NULL,                                       "rank_b0",        NULL },
  {"virtual",         (getter)__PyVGX_Vertex__get_virtual,              (setter)NULL,                                       "virtual",        NULL },
  {"address",         (getter)__PyVGX_Vertex__get_address,              (setter)NULL,                                       "address",        NULL },
  {"index",           (getter)__PyVGX_Vertex__get_index,                (setter)NULL,                                       "index",          NULL },
  {"bitindex",        (getter)__PyVGX_Vertex__get_bitindex,             (setter)NULL,                                       "bitindex",       NULL },
  {"bitvector",       (getter)__PyVGX_Vertex__get_bitvector,            (setter)NULL,                                       "bitvector",      NULL },
  {"op",              (getter)__PyVGX_Vertex__get_opcnt,                (setter)NULL,                                       "op",             NULL },
  {"refc",            (getter)__PyVGX_Vertex__get_refc,                 (setter)NULL,                                       "refcount",       NULL },
  {"aidx",            (getter)__PyVGX_Vertex__get_aidx,                 (setter)NULL,                                       "aidx",           NULL },
  {"bidx",            (getter)__PyVGX_Vertex__get_bidx,                 (setter)NULL,                                       "blockindex",     NULL },
  {"oidx",            (getter)__PyVGX_Vertex__get_oidx,                 (setter)NULL,                                       "blockoffset",    NULL },
  {"handle",          (getter)__PyVGX_Vertex__get_handle,               (setter)NULL,                                       "handle",         NULL },
  {"enum",            (getter)__PyVGX_Vertex__get_enum,                 (setter)NULL,                                       "enum",           NULL },

  // Backwards compatibility
  {"identifier",      (getter)__PyVGX_Vertex__get_identifier,           (setter)NULL,                                       "identifier",     NULL },
  {"degree",          (getter)__PyVGX_Vertex__get_degree,               (setter)NULL,                                       "degree",         NULL },
  {"indegree",        (getter)__PyVGX_Vertex__get_indegree,             (setter)NULL,                                       "indegree",       NULL },
  {"outdegree",       (getter)__PyVGX_Vertex__get_outdegree,            (setter)NULL,                                       "outdegree",      NULL },
  {"TMC",             (getter)__PyVGX_Vertex__get_TMC,                  (setter)NULL,                                       "TMC",            NULL },
  {"TMM",             (getter)__PyVGX_Vertex__get_TMM,                  (setter)NULL,                                       "TMM",            NULL },
  {"TMX",             (getter)__PyVGX_Vertex__get_TMX,                  (setter)NULL,                                       "TMX",            NULL },

  {NULL}  /* Sentinel */
};



/******************************************************************************
 * PyVGX_Vertex_as_sequence
 *
 ******************************************************************************
 */
/* Hack to implement "key in table" - stolen from dictobject.c */
static PySequenceMethods PyVGX_Vertex__as_sequence = {
    .sq_length          = 0,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)pyvgx__vertex_contains,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0 
};



/******************************************************************************
 * PyVGX_Vertex__as_mapping
 *
 ******************************************************************************
 */
static PyMappingMethods PyVGX_Vertex__as_mapping = {
  NULL,                                         /* mp_length    */
  (binaryfunc)__py_get_property,                /* mp_subscript */
  (objobjargproc)PyVGX_Vertex__ass_subscript    /* mp_ass_subscript */
};



/******************************************************************************
 * PyVGX_Vertex__methods
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef PyVGX_Vertex__methods[] = {
    // VERTEX ACCESS METHODS
    {"Writable",            (PyCFunction)PyVGX_Vertex__Writable,          METH_NOARGS,                  Writable__doc__  },
    {"Readable",            (PyCFunction)PyVGX_Vertex__Readable,          METH_NOARGS,                  Readable__doc__  },
    {"Readonly",            (PyCFunction)PyVGX_Vertex__Readonly,          METH_NOARGS,                  Readonly__doc__  },
    {"Close",               (PyCFunction)PyVGX_Vertex__Close,             METH_NOARGS,                  Close__doc__     },
    {"Escalate",            (PyCFunction)PyVGX_Vertex__Escalate,          METH_VARARGS | METH_KEYWORDS, Escalate__doc__     },
    {"Relax",               (PyCFunction)PyVGX_Vertex__Relax,             METH_NOARGS,                  Relax__doc__     },

    // RANK METHODS
    {"SetRank",             (PyCFunction)PyVGX_Vertex__SetRank,           METH_VARARGS | METH_KEYWORDS, SetRank__doc__  },
    {"GetRank",             (PyCFunction)PyVGX_Vertex__GetRank,           METH_NOARGS,                  GetRank__doc__  },
    {"ArcLSH",              (PyCFunction)PyVGX_Vertex__ArcLSH,            METH_O,                       ArcLSH__doc__ },

    // VERTEX QUERY METHODS
    {"Neighborhood",        (PyCFunction)PyVGX_Vertex__Neighborhood,      METH_VARARGS | METH_KEYWORDS, pyvgx_Neighborhood__doc__  },
    {"OpenNeighbor" ,       (PyCFunction)pyvgx_OpenNeighbor,              METH_FASTCALL | METH_KEYWORDS, pyvgx_OpenNeighbor__doc__ },
    {"Adjacent" ,           (PyCFunction)PyVGX_Vertex__Adjacent,          METH_VARARGS | METH_KEYWORDS, pyvgx_Adjacent__doc__ },
    {"Aggregate",           (PyCFunction)PyVGX_Vertex__Aggregate,         METH_VARARGS | METH_KEYWORDS, pyvgx_Aggregate__doc__ },
    {"ArcValue",            (PyCFunction)pyvgx_ArcValue,                  METH_FASTCALL | METH_KEYWORDS, pyvgx_ArcValue__doc__ },
    {"Degree",              (PyCFunction)pyvgx_Degree,                    METH_FASTCALL | METH_KEYWORDS, pyvgx_Degree__doc__ },
    {"Inarcs",              (PyCFunction)PyVGX_Vertex__Inarcs,            METH_VARARGS | METH_KEYWORDS, pyvgx_Inarcs__doc__ },
    {"Outarcs",             (PyCFunction)PyVGX_Vertex__Outarcs,           METH_VARARGS | METH_KEYWORDS, pyvgx_Outarcs__doc__ },
    {"Initials",            (PyCFunction)PyVGX_Vertex__Initials,          METH_VARARGS | METH_KEYWORDS, pyvgx_Initials__doc__ },
    {"Terminals",           (PyCFunction)PyVGX_Vertex__Terminals,         METH_VARARGS | METH_KEYWORDS, pyvgx_Terminals__doc__ },
    {"Neighbors",           (PyCFunction)PyVGX_Vertex__Neighbors,         METH_VARARGS | METH_KEYWORDS, pyvgx_Neighborhood__doc__ },

    // VERTEX PROPERTY METHODS
    {"SetProperty",         (PyCFunction)PyVGX_Vertex__SetProperty,       METH_FASTCALL | METH_KEYWORDS, SetProperty__doc__  },
    {"IncProperty",         (PyCFunction)PyVGX_Vertex__IncProperty,       METH_FASTCALL | METH_KEYWORDS, IncProperty__doc__  },
    {"HasProperty",         (PyCFunction)PyVGX_Vertex__HasProperty,       METH_FASTCALL | METH_KEYWORDS, HasProperty__doc__  },
    {"GetProperty",         (PyCFunction)PyVGX_Vertex__GetProperty,       METH_FASTCALL | METH_KEYWORDS, GetProperty__doc__  },
    {"RemoveProperty",      (PyCFunction)PyVGX_Vertex__RemoveProperty,    METH_O,                       RemoveProperty__doc__  },
    {"SetProperties",       (PyCFunction)PyVGX_Vertex__SetProperties,     METH_O,                       SetProperties__doc__  },
    {"HasProperties",       (PyCFunction)PyVGX_Vertex__HasProperties,     METH_NOARGS,                  HasProperties__doc__  },
    {"NumProperties",       (PyCFunction)PyVGX_Vertex__NumProperties,     METH_NOARGS,                  NumProperties__doc__  },
    {"GetProperties",       (PyCFunction)PyVGX_Vertex__GetProperties,     METH_NOARGS,                  GetProperties__doc__  },
    {"RemoveProperties",    (PyCFunction)PyVGX_Vertex__RemoveProperties,  METH_NOARGS,                  RemoveProperties__doc__  },
    {"items",               (PyCFunction)PyVGX_Vertex__items,             METH_NOARGS,                  items__doc__  },
    {"keys",                (PyCFunction)PyVGX_Vertex__keys,              METH_NOARGS,                  keys__doc__  },
    {"values",              (PyCFunction)PyVGX_Vertex__values,            METH_NOARGS,                  values__doc__  },

    // VERTEX VECTOR METHODS
    {"SetVector",           (PyCFunction)PyVGX_Vertex__SetVector,         METH_FASTCALL | METH_KEYWORDS, SetVector__doc__  },
    {"HasVector",           (PyCFunction)PyVGX_Vertex__HasVector,         METH_NOARGS,                  HasVector__doc__  },
    {"GetVector",           (PyCFunction)PyVGX_Vertex__GetVector,         METH_NOARGS,                  GetVector__doc__  },
    {"RemoveVector",        (PyCFunction)PyVGX_Vertex__RemoveVector,      METH_NOARGS,                  RemoveVector__doc__  },

    // VERTEX TTL METHODS
    {"SetExpiration",       (PyCFunction)PyVGX_Vertex__SetExpiration,     METH_VARARGS | METH_KEYWORDS, SetExpiration__doc__ },
    {"GetExpiration",       (PyCFunction)PyVGX_Vertex__GetExpiration,     METH_NOARGS,                  GetExpiration__doc__ },
    {"IsExpired",           (PyCFunction)PyVGX_Vertex__IsExpired,         METH_NOARGS,                  IsExpired__doc__ },
    {"ClearExpiration",     (PyCFunction)PyVGX_Vertex__ClearExpiration,   METH_NOARGS,                  ClearExpiration__doc__ },

    // VERTEX MISCELLANEOUS METHODS
    {"SetType",             (PyCFunction)PyVGX_Vertex__SetType,           METH_O,                       SetType__doc__  },
    {"GetType",             (PyCFunction)PyVGX_Vertex__GetType,           METH_NOARGS,                  GetType__doc__  },
    {"GetTypeEnum",         (PyCFunction)PyVGX_Vertex__GetTypeEnum,       METH_NOARGS,                  GetTypeEnum__doc__  },
    {"Commit",              (PyCFunction)PyVGX_Vertex__Commit,            METH_NOARGS,                  Commit__doc__     },
    {"IsVirtual",           (PyCFunction)PyVGX_Vertex__IsVirtual,         METH_NOARGS,                  IsVirtual__doc__     },
    {"AsDict",              (PyCFunction)PyVGX_Vertex__AsDict,            METH_NOARGS,                  AsDict__doc__     },
    {"Descriptor",          (PyCFunction)PyVGX_Vertex__Descriptor,        METH_NOARGS,                  Descriptor__doc__  },

    // INTERNAL DEBUG
    {"DebugVector",         (PyCFunction)PyVGX_Vertex__DebugVector,       METH_NOARGS,                  DebugVector__doc__  },
    {"Debug",               (PyCFunction)PyVGX_Vertex__Debug,             METH_NOARGS,                  Debug__doc__  },

    {NULL}  /* Sentinel */
};
RESUME_WARNINGS


/******************************************************************************
 * PyVGX_Vertex__VertexType
 *
 ******************************************************************************
 */
static PyTypeObject PyVGX_Vertex__VertexType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.Vertex",
    .tp_basicsize       = sizeof(PyVGX_Vertex),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)PyVGX_Vertex__dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = (reprfunc)PyVGX_Vertex__repr,
    .tp_as_number       = 0,
    .tp_as_sequence     = &PyVGX_Vertex__as_sequence,
    .tp_as_mapping      = &PyVGX_Vertex__as_mapping,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "PyVGX Vertex objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = PyVGX_Vertex__methods,
    .tp_members         = PyVGX_Vertex__members,
    .tp_getset          = PyVGX_Vertex__getset,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)PyVGX_Vertex__init,
    .tp_alloc           = 0,
    .tp_new             = PyVGX_Vertex__new,
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
    .tp_vectorcall      = (vectorcallfunc)PyVGX_Vertex__vectorcall
};


DLL_HIDDEN PyTypeObject * p_PyVGX_Vertex__VertexType = &PyVGX_Vertex__VertexType;
