#ifndef PY_VGX_H
#define PY_VGX_H

// Include this first to disable warnings we want to ignore (Python is full of warnings)
#include "cxplat.h"


#define PY_SSIZE_T_CLEAN
IGNORE_WARNING_UNREFERENCED_FORMAL_PARAMETER
#include <Python.h>
RESUME_WARNINGS
#define TP_SLOT_9 tp_vectorcall_offset

#if (PY_MAJOR_VERSION < 3) || (PY_MINOR_VERSION < 9)
#error "Python 3.9 or higher is required"
#endif


#include "structmember.h"

#define _FORCE_VGX_IMPORT
#define _FORCE_FRAMEHASH_IMPORT

#include "_vgx.h"
#include "_framehash.h"



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
#define __PYVGX_MESSAGE( LEVEL, Component, Code, Format, ... ) LEVEL( Code, "PY::API(%s): " Format, Component, ##__VA_ARGS__ )

#define PYVGX_API_VERBOSE( Component, Code, Format, ... )   __PYVGX_MESSAGE( VERBOSE, Component, Code, Format, ##__VA_ARGS__ )
#define PYVGX_API_INFO( Component, Code, Format, ... )      __PYVGX_MESSAGE( INFO, Component, Code, Format, ##__VA_ARGS__ )
#define PYVGX_API_WARNING( Component, Code, Format, ... )   __PYVGX_MESSAGE( WARN, Component, Code, Format, ##__VA_ARGS__ )
#define PYVGX_API_REASON( Component, Code, Format, ... )    __PYVGX_MESSAGE( REASON, Component, Code, Format, ##__VA_ARGS__ )
#define PYVGX_API_CRITICAL( Component, Code, Format, ... )  __PYVGX_MESSAGE( CRITICAL, Component, Code, Format, ##__VA_ARGS__ )
#define PYVGX_API_FATAL( Component, Code, Format, ... )     __PYVGX_MESSAGE( FATAL, Component, Code, Format, ##__VA_ARGS__ )



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_Framehash_PyObject_AsStringAndSize( PyObject *py_string, Py_ssize_t *sz ) {
  if( PyBytes_CheckExact( py_string ) ) {
    *sz = PyBytes_GET_SIZE( py_string );
    return PyBytes_AS_STRING( py_string );
  }
  else {
    return PyUnicode_AsUTF8AndSize( py_string, sz );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_Framehash_PyObject_AsUTF8AndSize( PyObject *py_string, Py_ssize_t *sz ) {
  if( PyBytes_CheckExact( py_string ) ) {
    int64_t errpos = 0;
    const char *s = PyBytes_AS_STRING( py_string );
    if( COMLIB_check_utf8( (const BYTE*)s, &errpos ) ) {
      *sz = PyBytes_GET_SIZE( py_string );
      return s;
    }
    else {
      PyErr_Format( PyExc_UnicodeError, "invalid utf-8 sequence at pos %lld", errpos );
      return NULL;
    }
  }
  else {
    return PyUnicode_AsUTF8AndSize( py_string, sz );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_Framehash_PyObject_AsString( PyObject *py_string ) {
  if( PyBytes_CheckExact( py_string ) ) {
    return PyBytes_AS_STRING( py_string );
  }
  else {
    return PyUnicode_AsUTF8( py_string );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_Framehash_PyObject_AsUTF8( PyObject *py_string ) {
  if( PyBytes_CheckExact( py_string ) ) {
    int64_t errpos = 0;
    const char *s = PyBytes_AS_STRING( py_string );
    if( COMLIB_check_utf8( (const BYTE*)s, &errpos ) ) {
      return s;
    }
    else {
      PyErr_Format( PyExc_UnicodeError, "invalid utf-8 sequence at pos %lld", errpos );
      return NULL;
    }
  }
  else {
    return PyUnicode_AsUTF8( py_string );
  }
}


#define PyVGX_Framehash_PyObject_CheckString( o )   (PyBytes_CheckExact( o ) || PyUnicode_Check( o ))

#define PyVGX_Framehash_PyObject_AsStringAndSize    __PyVGX_Framehash_PyObject_AsStringAndSize
#define PyVGX_Framehash_PyObject_AsUTF8AndSize      __PyVGX_Framehash_PyObject_AsUTF8AndSize

#define PyVGX_Framehash_PyObject_AsString           __PyVGX_Framehash_PyObject_AsString
#define PyVGX_Framehash_PyObject_AsUTF8             __PyVGX_Framehash_PyObject_AsUTF8



/******************************************************************************
 * PyVGX_Framehash
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Framehash {
  PyObject_HEAD
  framehash_t *fhash;
  int nointhash;
} PyVGX_Framehash;



/******************************************************************************
 * StringQueue
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_StringQueue {
  PyObject_HEAD
  CStringQueue_t *queue;

  int64_t (*Write)( CStringQueue_t *queue, const char *elements, int64_t sz );
  int64_t (*WriteNolock)( CStringQueue_t *queue, const char *elements, int64_t sz );
  int64_t (*Read)( CStringQueue_t *queue, void **elements, int64_t sz );
  int64_t (*ReadNolock)( CStringQueue_t *queue, void **elements, int64_t sz );

} PyVGX_StringQueue;



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_PyObject_AsStringAndSize( PyObject *py_string, Py_ssize_t *sz, Py_ssize_t *ucsz ) {
  if( PyBytes_CheckExact( py_string ) ) {
    *sz = PyBytes_GET_SIZE( py_string );
    *ucsz = 0;
    return PyBytes_AS_STRING( py_string );
  }
  else {
    *ucsz = PyUnicode_GET_LENGTH( py_string );
    return PyUnicode_AsUTF8AndSize( py_string, sz );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_PyObject_AsUTF8AndSize( PyObject *py_string, Py_ssize_t *sz, Py_ssize_t *ucsz, const char *context ) {
  if( PyBytes_CheckExact( py_string ) ) {
    int64_t errpos = 0;
    const char *s = PyBytes_AS_STRING( py_string );
    if( COMLIB_check_utf8( (const BYTE*)s, &errpos ) ) {
      *sz = PyBytes_GET_SIZE( py_string );
      *ucsz = 0;
      return s;
    }
    else {
      if( context ) {
        PyErr_Format( PyExc_UnicodeError, "%s: invalid utf-8 sequence at pos %lld", context, errpos );
      }
      else {
        PyErr_Format( PyExc_UnicodeError, "invalid utf-8 sequence at pos %lld", errpos );
      }
      return NULL;
    }
  }
  else {
    *ucsz = PyUnicode_GET_LENGTH( py_string );
    return PyUnicode_AsUTF8AndSize( py_string, sz );
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_PyObject_AsString( PyObject *py_string ) {
  if( PyBytes_CheckExact( py_string ) ) {
    return PyBytes_AS_STRING( py_string );
  }
  else {
    const char *str = PyUnicode_AsUTF8( py_string );
    if( str == NULL ) {
      PyErr_Format( PyExc_TypeError, "string required, not %s", Py_TYPE( py_string )->tp_name );
    }
    return str;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
static const char * __PyVGX_PyObject_AsUTF8( PyObject *py_string, const char *context ) {
  if( PyBytes_CheckExact( py_string ) ) {
    int64_t errpos = 0;
    const char *s = PyBytes_AS_STRING( py_string );
    if( COMLIB_check_utf8( (const BYTE*)s, &errpos ) ) {
      return s;
    }
    else {
      if( context ) {
        PyErr_Format( PyExc_UnicodeError, "%s: invalid utf-8 sequence at pos %lld", context, errpos );
      }
      else {
        PyErr_Format( PyExc_UnicodeError, "invalid utf-8 sequence at pos %lld", errpos );
      }
      return NULL;
    }
  }
  else {
    return PyUnicode_AsUTF8( py_string );
  }
}


#define PyVGX_PyObject_CheckString( o )   (PyBytes_CheckExact( o ) || PyUnicode_Check( o ))

#define PyVGX_PyObject_AsStringAndSize    __PyVGX_PyObject_AsStringAndSize
#define PyVGX_PyObject_AsUTF8AndSize      __PyVGX_PyObject_AsUTF8AndSize

#define PyVGX_PyObject_AsString           __PyVGX_PyObject_AsString
#define PyVGX_PyObject_AsUTF8             __PyVGX_PyObject_AsUTF8



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyUnicode_FromStringNoErr( const char *u ) {
  PyObject *py_obj = PyUnicode_FromString( u );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyUnicode_FromStringAndSizeNoErr( const char *u, Py_ssize_t size ) {
  PyObject *py_obj = PyUnicode_FromStringAndSize( u, size );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyBytes_FromStringNoErr( const char *s ) {
  PyObject *py_obj = PyBytes_FromString( s );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyLong_FromLongNoErr( long x ) {
  PyObject *py_obj = PyLong_FromLong( x );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyLong_FromLongLongNoErr( long long x ) {
  PyObject *py_obj = PyLong_FromLongLong( x );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyLong_FromUnsignedLongLongNoErr( unsigned long long x ) {
  PyObject *py_obj = PyLong_FromUnsignedLongLong( x );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyFloat_FromDoubleNoErr( double x ) {
  PyObject *py_obj = PyFloat_FromDouble( x );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static PyObject * PyVGX_PyCapsule_NewNoErr( void *pointer, const char *name, PyCapsule_Destructor destructor ) {
  PyObject *py_obj = PyCapsule_New( pointer, name, destructor );
  if( py_obj == NULL ) {
    PyErr_Clear();
  }
  return py_obj;
}


#define PyVGX_DECREF( op ) do {   \
    IGNORE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT \
    Py_DECREF( op );              \
    RESUME_WARNINGS               \
  } WHILE_ZERO


#define PyVGX_XDECREF( op ) do {  \
    IGNORE_WARNING_CONDITIONAL_EXPRESSION_IS_CONSTANT \
    Py_XDECREF( op );             \
    RESUME_WARNINGS               \
  } WHILE_ZERO


#define PyVGX_DOC_DECLARE( Name ) DLL_HIDDEN extern const char Name[]
#define PyVGX_DOC( Name, String ) DLL_HIDDEN const char Name[] = String


#define PyVGX_VertexConditionMaxRecursion (VGX_PREDICATOR_EPH_DISTANCE_MAX-1)


__inline static PyObject * PyNone_New( void ) {
  Py_RETURN_NONE;
}


/******************************************************************************
 * PyVGX Exceptions
 ******************************************************************************
 */
DLL_HIDDEN extern PyObject * PyVGX_VertexError;
DLL_HIDDEN extern PyObject * PyVGX_EnumerationError;
DLL_HIDDEN extern PyObject * PyVGX_ArcError;
DLL_HIDDEN extern PyObject * PyVGX_AccessError;
DLL_HIDDEN extern PyObject * PyVGX_QueryError;
DLL_HIDDEN extern PyObject * PyVGX_SearchError;
DLL_HIDDEN extern PyObject * PyVGX_ResultError;
DLL_HIDDEN extern PyObject * PyVGX_RequestError;
DLL_HIDDEN extern PyObject * PyVGX_ResponseError;
DLL_HIDDEN extern PyObject * PyVGX_PluginError;
DLL_HIDDEN extern PyObject * PyVGX_InternalError;
DLL_HIDDEN extern PyObject * PyVGX_OperationTimeout;
DLL_HIDDEN extern PyObject * PyVGX_DataError;


/******************************************************************************
 * pyvgx module
 ******************************************************************************
 */
DLL_HIDDEN extern PyObject *g_pyvgx;


/******************************************************************************
 * Initialization time
 ******************************************************************************
 */
DLL_HIDDEN extern int64_t _time_init;


/******************************************************************************
 * VGX Context
 ******************************************************************************
 */
DLL_HIDDEN extern vgx_context_t *_vgx_context;


/******************************************************************************
 * VGX Default URIs
 ******************************************************************************
 */
DLL_HIDDEN extern vgx_StringList_t *_default_URIs;
DLL_HIDDEN extern void __delete_default_URIs( void );


/******************************************************************************
 * Flag indicating whether pyvgx owns the core graph registry
 ******************************************************************************
 */
DLL_HIDDEN extern bool _module_owns_registry; // true=pyvgx is owner, false=external owner


/******************************************************************************
 * Flag indicating whether core registry has been loaded
 ******************************************************************************
 */
DLL_HIDDEN extern bool _registry_loaded; // true=registry loaded, false=registry not loaded


/******************************************************************************
 * Used to guard against unintended side-effects related to CloseAll()
 ******************************************************************************
 */
DLL_HIDDEN extern __THREAD uint64_t _pyvertex_generation_guard;


/******************************************************************************
 * Globally enable/disable API
 ******************************************************************************
 */
DLL_HIDDEN extern bool _pyvgx_api_enabled;



/******************************************************************************
 * Flag indicating whether arc creation should automatically set TMC/TMM arcs.
 * When true, adding a new relationship between two vertices will also add a
 * TMC arc and a TMM arc initialized to the current graph time. Adding, removing,
 * or updating an existing relationship will also update the TMM arc to the 
 * current graph time. Note that this feature comes at the cost of adding two 
 * extra arcs to every relationship, increasing memory usage and adding overhead
 * to arc insertion and traversal.
 ******************************************************************************
 */
DLL_HIDDEN extern bool _auto_arc_timestamps;

// Some integer and string constants we often need
DLL_HIDDEN extern PyObject * g_py_zero;
DLL_HIDDEN extern PyObject * g_py_one;
DLL_HIDDEN extern PyObject * g_py_minus_one;
DLL_HIDDEN extern PyObject * g_py_char_w;
DLL_HIDDEN extern PyObject * g_py_char_a;
DLL_HIDDEN extern PyObject * g_py_char_r;
DLL_HIDDEN extern PyObject * g_py_noargs;

// Some useful system constants
DLL_HIDDEN extern PyObject * g_py_SYS_prop_id;
DLL_HIDDEN extern PyObject * g_py_SYS_prop_type;
DLL_HIDDEN extern PyObject * g_py_SYS_prop_mode;

// Currently running HTTP Server's dispatcher config (JSON)
DLL_HIDDEN extern PyObject * g_py_cfdispatcher;


/******************************************************************************
 *
 * THREADS
 *
 ******************************************************************************
 */

#define PYVGX_THREAD_STATE          __pyvgx_thread_state__


/******************************************************************************
 * Thread state for python interpreter
 ******************************************************************************
 */
DLL_HIDDEN extern __THREAD PyThreadState * PYVGX_THREAD_STATE;


#define __PYVGX_BLOCK_THREADS       PyEval_RestoreThread( PYVGX_THREAD_STATE )


#define __PYVGX_UNBLOCK_THREADS     (PYVGX_THREAD_STATE = PyEval_SaveThread())


#define PYVGX_THREADS_ACTIVE        (PYVGX_THREAD_STATE != NULL)


#define __DISALLOW_PYVGX_THREADS  \
  __PYVGX_BLOCK_THREADS;          \
  PYVGX_THREAD_STATE = NULL;


#define BEGIN_CONDITIONAL_PYVGX_THREADS_IF( Condition )   \
  do {                                                    \
    bool __pyvgx__conditional_unblocked__ = false;        \
    if( (Condition) ) {                                   \
      __PYVGX_UNBLOCK_THREADS;                            \
      __pyvgx__conditional_unblocked__ = true;            \
    }                                                     \
    do


#define END_CONDITIONAL_PYVGX_THREADS                     \
    WHILE_ZERO;                                           \
    if( __pyvgx__conditional_unblocked__ == true ) {      \
      __DISALLOW_PYVGX_THREADS                            \
    }                                                     \
  } WHILE_ZERO



#define BEGIN_PYVGX_THREADS         BEGIN_CONDITIONAL_PYVGX_THREADS_IF( !PYVGX_THREADS_ACTIVE )
#define END_PYVGX_THREADS           END_CONDITIONAL_PYVGX_THREADS


#define BEGIN_PYTHON_INTERPRETER                  \
  do {                                            \
    bool __pyvgx__suspended__ = false;            \
    PyGILState_STATE __pyvgx__gil_state__ = 0;    \
    if( PYVGX_THREADS_ACTIVE ) {                  \
      __DISALLOW_PYVGX_THREADS                    \
      __pyvgx__suspended__ = true;                \
    }                                             \
    else {                                        \
      __pyvgx__gil_state__ = PyGILState_Ensure(); \
    }                                             \
    do


#define END_PYTHON_INTERPRETER                    \
    WHILE_ZERO;                                   \
    if( __pyvgx__suspended__ ) {                  \
      __PYVGX_UNBLOCK_THREADS;                    \
    }                                             \
    else {                                        \
      PyGILState_Release( __pyvgx__gil_state__ ); \
    }                                             \
  } WHILE_ZERO



/******************************************************************************
 *
 ******************************************************************************
 */
static int __parse_vectorcall_args( PyObject *const *args, Py_ssize_t nargs, PyObject *kwdict, const char **kwlist, Py_ssize_t maxargs, PyObject **py_out ) {

  if( nargs > maxargs ) {
    PyErr_Format( PyExc_TypeError, "too many arguments" );
    return -1;
  }

  // Copy all positional args
  PyObject *const *end = args + nargs;
  PyObject **dest = py_out;
  while( args < end ) {
    *dest++ = *args++;
  }

  const char *name;

  if( kwdict ) {
    int64_t nkwds = PyTuple_Size( kwdict );
    // Process all keyword args
    const char **p = kwlist;
    dest = py_out;
    for( int64_t k=0; k<nkwds; ++k ) {
      if( (name = PyUnicode_AsUTF8( PyTuple_GET_ITEM( kwdict, k ) )) == NULL ) {
        return -1;
      }
      // Compare from start of list of expected keyword args
      int64_t i=0;
      while( p[i] != NULL ) {
        if( CharsEqualsConst( name, p[i] ) ) {
          if( dest[i] ) {
            goto duplicate;
          }
          dest[i] = *args++;
          // Matched first keyword arg in list, we can advance our list pointer to avoid re-scanning
          if( i == 0 ) {
            ++p;
            ++dest;
          }
          goto next;
        }
        ++i;
      }
      goto unexpected;

    next:
      continue;
    }
  }

  return 0;

duplicate:
  PyErr_Format( PyExc_TypeError, "duplicate keyword argument '%s'", name );
  return -1;

unexpected:
  PyErr_Format( PyExc_TypeError, "unexpected keyword argument '%s'", name );
  return -1;

}



/******************************************************************************
 *
 ******************************************************************************
 */
static void PyVGXError_SetString( PyObject *exc, const char *str ) {
  BEGIN_PYTHON_INTERPRETER {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( exc, str );
    }
  } END_PYTHON_INTERPRETER;
}


#define PyVGX_ReturnError( PyExc, Str ) { \
  PyErr_SetString( PyExc, Str ); \
  return NULL; \
}



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct s_pyvgx_VertexIdentifier_t {
  const char * id;
  int len;
  char _idbuf[33];
  char _rsv[3];
} pyvgx_VertexIdentifier_t;



__inline static void __pyvgx_reset_vertex_identifier( pyvgx_VertexIdentifier_t *ident ) {
  ident->id = NULL;
  ident->len = 0;
}



/******************************************************************************
 *
 ******************************************************************************
 */
#define __BASE_QUERY_ARGS                     \
  struct {                                    \
    vgx_Graph_t *graph;                       \
    CString_t *CSTR__error;                   \
    vgx_AccessReason_t reason;                \
    PyObject *py_err_class;                   \
    vgx_arc_direction default_arcdir;         \
    vgx_collector_mode_t collector_mode;      \
    void (*clear)( struct __s_base_query_args *args ); \
    int __debug;                              \
  } implied;                                  \
  int timeout_ms;                             \
  int limexec;                                \
  const char *pre_expr;                       \
  const char *filter_expr;                    \
  const char *post_expr;                      \
  vgx_VertexCondition_t *vertex_condition;    \
  vgx_RankingCondition_t *ranking_condition;  \
  vgx_ExpressEvalMemory_t *evalmem;



/******************************************************************************
 *
 ******************************************************************************
 */
#define __OPEN_NEIGHBOR_QUERY_ARGS            \
  pyvgx_VertexIdentifier_t anchor;            \
  vgx_ArcConditionSet_t *arc_condition_set;   \
  vgx_predicator_modifier_enum modifier;      \
  bool readonly;



/******************************************************************************
 *
 ******************************************************************************
 */
#define __ADJACENCY_QUERY_ARGS                \
  pyvgx_VertexIdentifier_t anchor;            \
  vgx_ArcConditionSet_t *arc_condition_set;   \
  vgx_predicator_modifier_enum modifier;

 
 
 /******************************************************************************
 *
 ******************************************************************************
 */
#define __QUERY_RESULT_SET_ARGS               \
  vgx_ResponseAttrFastMask result_format;     \
  vgx_ResponseAttrFastMask result_attrs;      \
  const char *select_statement;               \
  int offset;                                 \
  int64_t hits;                               \
  vgx_sortspec_t sortspec;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_base_query_args {
  __BASE_QUERY_ARGS
} __base_query_args;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_open_neighbor_query_args {
  __BASE_QUERY_ARGS
  __OPEN_NEIGHBOR_QUERY_ARGS
} __open_neighbor_query_args;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_adjacency_query_args {
  __BASE_QUERY_ARGS
  __ADJACENCY_QUERY_ARGS
} __adjacency_query_args;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_neighborhood_query_args {
  __BASE_QUERY_ARGS
  __ADJACENCY_QUERY_ARGS
  __QUERY_RESULT_SET_ARGS
  vgx_ArcConditionSet_t *collect_arc_condition_set;
  int nest;
  int64_t nested_hits;
} __neighborhood_query_args;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_aggregator_query_args {
  __BASE_QUERY_ARGS
  __ADJACENCY_QUERY_ARGS
  vgx_ArcConditionSet_t *collect_arc_condition_set;
  vgx_ResponseAttrFastMask result_format;
  vgx_ResponseAttrFastMask result_attrs;
} __aggregator_query_args;



/******************************************************************************
 *
 ******************************************************************************
 */
typedef struct __s_global_query_args {
  __BASE_QUERY_ARGS
  __QUERY_RESULT_SET_ARGS
} __global_query_args;




/******************************************************************************
 *
 * TIMED BLOCK - AUTO POPULATE PYTHON DICT
 *
 ******************************************************************************
 */
#define __PY_START_TIMED_BLOCK( DictObjectPtrPtr, KeyString ) \
do {                                                          \
  PyObject **__py_dict__ = DictObjectPtrPtr;                  \
  int64_t __t0__ = __GET_CURRENT_NANOSECOND_TICK();           \
  int64_t __t1__;                                             \
  const char *__key__ = KeyString;                            \
  do

#define __PY_END_TIMED_BLOCK                                  \
  WHILE_ZERO;                                                 \
  if( __py_dict__ && *__py_dict__ && PyDict_CheckExact( *__py_dict__ ) ) { \
    __t1__ = __GET_CURRENT_NANOSECOND_TICK();                 \
    double __t__ = (__t1__ - __t0__)/1000000000.0;            \
    iPyVGXBuilder.DictMapStringToFloat( *__py_dict__, __key__, __t__ ); \
  }                                                           \
} WHILE_ZERO



/******************************************************************************
 * output stream
 *
 ******************************************************************************
 */
DLL_HIDDEN extern int __pyvgx_set_output_stream( const char *filepath, CString_t **CSTR__error );



/******************************************************************************
 * access log
 *
 ******************************************************************************
 */
DLL_HIDDEN extern int __pyvgx_open_access_log( const char *filepath, CString_t **CSTR__error );



/******************************************************************************
 * PyVGX_Framehash
 *
 ******************************************************************************
 */
DLL_HIDDEN extern int __pyvgx_framehash__init( void );



/******************************************************************************
 * PyVGX_Graph
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Graph {
  PyObject_HEAD
  vgx_Graph_t *graph;
  struct s_PyVGX_Similarity *py_sim;
  PyObject *py_name;
  bool is_owner;
  bool constructor_complete;
  bool caller_is_dealloc;
} PyVGX_Graph;



/******************************************************************************
 * PyVGX_Vertex
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Vertex {
  PyObject_HEAD
  vgx_Vertex_t *vertex;
  PyVGX_Graph *pygraph;
  uint64_t gen_guard;
} PyVGX_Vertex;



/******************************************************************************
 * PyVGX_Similarity
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Similarity {
  PyObject_HEAD
  vgx_Similarity_t *sim;
  bool standalone;
  PyObject *py_lsh_seeds;
} PyVGX_Similarity;



/******************************************************************************
 * PyVGX_Fingerprint
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Fingerprint {
  PyObject_HEAD
} PyVGX_Fingerprint;



/******************************************************************************
 * PyVGX_Vector
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Vector {
  PyObject_HEAD
  int origin_is_ext;  // true if external origin
  vgx_Vector_t *vext; // external vector
  vgx_Vector_t *vint; // internal vector
} PyVGX_Vector;



/******************************************************************************
 * PyVGX_System
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_System {
  PyObject_HEAD
} PyVGX_System;



/******************************************************************************
 * PyVGX_Operation
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Operation {
  PyObject_HEAD
} PyVGX_Operation;



/******************************************************************************
 * PyVGX_Memory
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Memory {
  PyObject_HEAD
  PyVGX_Graph *py_parent;
  vgx_Graph_t *parent;            // graph
  vgx_ExpressEvalMemory_t *evalmem; // internal memory
  vgx_ExpressEvalString_t *str_head;
  vgx_ExpressEvalString_t *str_tail;
  vgx_ExpressEvalVector_t *vector_head;
  vgx_ExpressEvalVector_t *vector_tail;
  uint32_t threadid;              // owner thread
} PyVGX_Memory;



/******************************************************************************
 * PyVGX_PluginResponse_metas
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_PluginResponse_metas {

  // [Q1.1-2]
  x_vgx_partial__level level;

  // [Q1.3.1]
  int maxhits;

  // [Q1.3.2]
  int __rsv_1_3_2;

  // [Q1.4]
  int64_t hitcount;

  // [Q1.5.1]
  vgx_sortspec_t sortspec;

  // [Q1.5.2]
  x_vgx_partial__sortkeytype ktype;

  // [Q1.6]
  PyTypeObject *py_keytype;

  // [Q1.7]
  QWORD __rsv_1_7;

  // [Q1.8]
  QWORD __rsv_1_8;

} PyVGX_PluginResponse_metas;



/******************************************************************************
 * PyVGX_PluginResponse
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_PluginResponse {
  PyObject_HEAD
  vgx_VGXServerRequest_t *request;
  vgx_VGXServerResponse_t *response;
  PyVGX_PluginResponse_metas metas;
  x_vgx_partial__aggregator aggregator;
  PyObject *py_message;
  PyObject *py_entries;
  PyObject *py_prev_key;
} PyVGX_PluginResponse;


DLL_HIDDEN extern int __pyvgx_xvgxpartial__init( void );
DLL_HIDDEN extern int __pyvgx_PluginResponse_serialize_x_vgx_partial( PyVGX_PluginResponse *py_plugres, vgx_StreamBuffer_t *output );
DLL_HIDDEN extern PyVGX_PluginResponse * __pyvgx_PluginResponse_deserialize_x_vgx_partial( vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response );
DLL_HIDDEN extern PyObject * __pyvgx_PluginResponse_ToJSON( PyVGX_PluginResponse *py_plugres );



/******************************************************************************
 * PyVGX_PluginRequest
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_PluginRequest {
  PyObject_HEAD
  vgx_VGXServerRequest_t *request;
  bool owns_request;
  PyObject *py_params;
  PyObject *py_headers;
  PyObject *py_content;
} PyVGX_PluginRequest;


DLL_HIDDEN extern int __pyvgx_PluginRequest_Serialize( PyVGX_PluginRequest *py_plugreq, vgx_StreamBuffer_t *output );
DLL_HIDDEN extern PyVGX_PluginRequest * __pyvgx_PluginRequest_New( vgx_VGXServerRequest_t *request, PyObject *py_params, PyObject *py_headers, PyObject *py_content );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static PyObject * __vgx_PyBytes_FromHTTPHeaders( vgx_HTTPHeaders_t *headers ) {
  int64_t sz = headers->sz > 0 ? *headers->_cursor - headers->_buffer : 0;
  return PyBytes_FromStringAndSize( headers->_buffer, sz );
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static PyObject * __vgx_PyUnicode_FromHTTPHeaders( vgx_HTTPHeaders_t *headers ) {
  int64_t sz = headers->sz > 0 ? *headers->_cursor - headers->_buffer : 0;
  return PyUnicode_FromStringAndSize( headers->_buffer, sz );
}



/******************************************************************************
 * PyVGX_Query
 *
 ******************************************************************************
 */
typedef struct s_PyVGX_Query {
  PyObject_HEAD
  PyVGX_Graph *py_parent;
  vgx_Graph_t *parent;            // graph
  uint32_t threadid;              // owner thread
  vgx_BaseQuery_t *query; 
  vgx_QueryType qtype;
  __base_query_args *p_args;
  int64_t cache_opid;
  PyObject *py_error;
} PyVGX_Query;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static void __invalidate_query_cache( PyVGX_Query *py_query ) {
  py_query->cache_opid = -1;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static int64_t __set_query_cache( PyVGX_Query *py_query ) {
  return py_query->cache_opid = py_query->query->parent_opid;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static bool __query_result_cache_valid__NOGIL( PyVGX_Query *py_query ) {
  int64_t cache_opid;
  if( (cache_opid = py_query->cache_opid) > 0 ) {
    int64_t opid;
    GRAPH_LOCK( py_query->parent ) {
      opid = iOperation.GetId_LCK( &py_query->parent->operation );
    } GRAPH_RELEASE;
    return cache_opid == opid;
  }
  else {
    return false;
  }
}



/******************************************************************************
 * Global Default Similarity object
 ******************************************************************************
 */
DLL_HIDDEN extern PyVGX_Similarity *_global_default_similarity;


/******************************************************************************
 * Global System object
 ******************************************************************************
 */
DLL_HIDDEN extern PyVGX_System *_global_system_object;


/******************************************************************************
 * Global Operation object
 ******************************************************************************
 */
DLL_HIDDEN extern PyVGX_Operation *_global_operation_object;



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
typedef int (*f_keyval_setfunc)( PyObject *py_container, int64_t index, const char *key, int64_t sz_key, const char *value, int64_t sz_value );



/******************************************************************************
 * Plug-in dict
 ******************************************************************************
 */
DLL_HIDDEN extern PyObject *g_py_plugins;

DLL_HIDDEN extern int                     __pyvgx_plugin__init_pyvgx( void );
DLL_HIDDEN extern int                     __pyvgx_plugin__init_plugins( void );
DLL_HIDDEN extern int                     __pyvgx_plugin__delete( void );
DLL_HIDDEN extern PyObject *              __pyvgx_plugin__get_argspec( PyObject *py_function );
DLL_HIDDEN extern f_vgx_ServicePluginCall __pyvgx_plugin__get_call( void );
DLL_HIDDEN extern int                     __pyvgx_plugin__add( const char *plugin_name, vgx_server_plugin_phase phase, PyObject *py_plugin, PyObject *py_bound_graph );
DLL_HIDDEN extern int                     __pyvgx_plugin__remove( const char *name );
DLL_HIDDEN extern PyObject *              __pyvgx_plugin__get_plugins( bool user, const char *onlyname );
DLL_HIDDEN extern HTTPStatus              __pyvgx_plugin__map_keyval_to_dict( vgx_KeyVal_t *kv, PyObject *py_dict );
DLL_HIDDEN extern int                     __pyvgx_plugin__set_dict_keyval( PyObject *py_dict, int64_t __ign, const char *key, int64_t sz_key, const char *value, int64_t sz_value );
DLL_HIDDEN extern PyObject *              __pyvgx_plugin__update_object_from_headers( vgx_HTTPHeaders_t *headers, PyObject *py_object, f_keyval_setfunc setfunc );





static void PyVGX_SetPyErr( int errcode ) {
  static char codebuf[512] = {0};
#define FORMAT_ERROR_CODE( Message, Errcode ) \

  BEGIN_PYTHON_INTERPRETER {

    if( !PyErr_Occurred() ) {
      int msg_sub = cxlib_exc_subtype( errcode );
      cxlib_format_code( errcode, codebuf, 511 );

      switch( msg_sub ) {
      case CXLIB_ERR_INITIALIZATION:
        PyErr_Format( PyVGX_DataError, "Initialization Error: %s", codebuf );
        break;
      case CXLIB_ERR_CONFIG:
        PyErr_Format( PyVGX_DataError, "Configuration Error: %s", codebuf );
        break;
      case CXLIB_ERR_FORMAT:
        PyErr_Format( PyExc_Exception, "Format Error: %s", codebuf );
        break;
      case CXLIB_ERR_LOOKUP:
        PyErr_SetNone( PyExc_LookupError );
        break;
      case CXLIB_ERR_BUG:
        PyErr_Format( PyExc_Exception, "Code Error: %s", codebuf );
        break;
      case CXLIB_ERR_FILESYSTEM:
        PyErr_Format( PyExc_IOError, "Filesystem Error: %s", codebuf );
        break;
      case CXLIB_ERR_CAPACITY:
        PyErr_Format( PyExc_MemoryError, "System Capacity Error: %s", codebuf );
        break;
      case CXLIB_ERR_MEMORY:
        PyErr_SetNone( PyExc_MemoryError );
        break;
      case CXLIB_ERR_CORRUPTION:
        PyErr_Format( PyExc_Exception, "Data Corruption Error: %s", codebuf );
        break;
      case CXLIB_ERR_SEMAPHORE:
        PyErr_Format( PyExc_Exception, "Semaphore Error: %s", codebuf );
        break;
      case CXLIB_ERR_MUTEX:
        PyErr_Format( PyExc_Exception, "Mutex Error: %s", codebuf );
        break;
      case CXLIB_ERR_GENERAL:
        PyErr_Format( PyExc_Exception, "Error: %s", codebuf );
        break;
      case CXLIB_ERR_API:
        PyErr_Format( PyExc_Exception, "API Error: %s", codebuf );
        break;
      case CXLIB_ERR_ASSERTION:
        PyErr_SetNone( PyExc_AssertionError );
        break;
      case CXLIB_ERR_IGNORE:
        break;
      default:
        PyErr_Format( PyExc_Exception, "Unknown Error: %s", codebuf );
      }
    }
  } END_PYTHON_INTERPRETER;
}



PyVGX_DOC_DECLARE( pyvgx_Neighborhood__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Initials__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Terminals__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Inarcs__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Outarcs__doc__ );
PyVGX_DOC_DECLARE( pyvgx_OpenNeighbor__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Adjacent__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Aggregate__doc__ );
PyVGX_DOC_DECLARE( pyvgx_ArcValue__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Degree__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Arcs__doc__ );
PyVGX_DOC_DECLARE( pyvgx_Vertices__doc__ );



DLL_HIDDEN extern PyObject * pyvgx_ArcValue( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames );
DLL_HIDDEN extern PyObject * pyvgx_Degree( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames );

// OpenNeighborQuery
DLL_HIDDEN extern PyObject * pyvgx_OpenNeighbor( PyObject *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames );

// AdjacencyQuery
DLL_HIDDEN extern PyObject * pyvgx_NewAdjacencyQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_ExecuteAdjacencyQuery( PyVGX_Query *py_query );
DLL_HIDDEN extern PyObject * pyvgx_Adjacent( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );

// NeighborhoodQuery
DLL_HIDDEN extern PyObject * pyvgx_NewNeighborhoodQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_ExecuteNeighborhoodQuery( PyVGX_Query *py_query );
DLL_HIDDEN extern PyObject * pyvgx_Neighborhood( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_Initials( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_Terminals( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_Inarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_Outarcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );

// AggregatorQuery
DLL_HIDDEN extern PyObject * pyvgx_NewAggregatorQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_ExecuteAggregatorQuery( PyVGX_Query *py_query );
DLL_HIDDEN extern PyObject * pyvgx_Aggregate( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );

// GlobalQuery
DLL_HIDDEN extern PyObject * pyvgx_NewArcsQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_NewVerticesQuery( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_ExecuteGlobalQuery( PyVGX_Query *py_query );
DLL_HIDDEN extern PyObject * pyvgx_Arcs( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );
DLL_HIDDEN extern PyObject * pyvgx_Vertices( PyVGX_Graph *pygraph, PyObject *args, PyObject *kwds );


DLL_HIDDEN extern int64_t pyvgx_SetVertexProperties( vgx_Vertex_t *vertex_WL, PyObject *py_properties );
DLL_HIDDEN extern PyObject * pyvgx__vertex_keys_and_values( PyVGX_Vertex *pyvertex, bool _keys, bool _values );
DLL_HIDDEN extern int pyvgx__vertex_contains( PyVGX_Vertex *pyvertex, PyObject *py_key );
DLL_HIDDEN extern PyObject * PyVGX_Vertex__FromInstance( PyVGX_Graph *pygraph, vgx_Vertex_t *vertex_LCK );

DLL_HIDDEN extern PyObject * PyVGX_Vector__FromVector( vgx_Vector_t *vector );


DLL_HIDDEN extern int64_t pyvgx_SystemProfile( int64_t n_hash, int64_t n_mem, bool quiet );
DLL_HIDDEN extern PyObject * pyvgx_GraphStatus( vgx_Graph_t *graph, PyObject *args );

DLL_HIDDEN extern int pyvgx_System_Initialize_Default( void );


typedef struct s_IPyVGXSearchResult {
  PyObject * (*PyResultList_FromSearchResult)( vgx_SearchResult_t *search_result, bool nested, int64_t nested_hits );
  PyObject * (*PyPredicatorValue_FromArcHead)( const vgx_ArcHead_t *archead );
  PyObject * (*PyDict_FromVertexProperties)( vgx_Vertex_t *vertex );
} IPyVGXSearchResult;



typedef struct s_IPyVGXParser {
  int (*Initialize)( void );
  void (*Destroy)( void );
  int (*GetVertexID)( PyVGX_Graph *py_graph, PyObject *py_vertex, pyvgx_VertexIdentifier_t *ident, vgx_Vertex_t **vertex, bool use_obid, const char *context );
  vgx_Relation_t * (*NewRelation)( vgx_Graph_t *graph, const char *initial, PyObject *py_arc, const char *terminal );
  vgx_value_comparison (*ParseValueCondition)( PyObject *py_valcond, vgx_value_condition_t *value_condition, const vgx_value_constraint_t vconstraint, const vgx_value_comparison vcomp_default );
  vgx_ArcConditionSet_t * (*NewArcConditionSet)( vgx_Graph_t *graph, PyObject *py_arc_condition, vgx_arc_direction default_direction );
  vgx_VertexCondition_t * (*NewVertexCondition)( vgx_Graph_t *graph, PyObject *py_vertex_condition, vgx_collector_mode_t collector_mode );
  vgx_RankingCondition_t * (*NewRankingCondition)( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, vgx_Vector_t *probe_vector );
  vgx_RankingCondition_t * (*NewRankingConditionEx)( vgx_Graph_t *graph, PyObject *py_rankspec, PyObject *py_aggregate, vgx_sortspec_t sortspec, vgx_predicator_modifier_enum modifier, PyObject *py_rank_vector_object, vgx_VertexCondition_t *vertex_condition );
  vgx_ExpressEvalMemory_t * (*NewExpressEvalMemory)( vgx_Graph_t *graph, PyObject *py_object );
  int (*ExternalMapElements)( PyObject *py_elements, ext_vector_feature_t **parsed_elements );
  int (*ExternalEuclideanElements)( PyObject *py_elements, float **parsed_elements );
  vgx_Vector_t * (*InternalVectorFromPyObject)( vgx_Similarity_t *simcontext, PyObject *py_object, PyObject *py_alpha, bool ephemeral );
  vgx_StringList_t * (*NewStringListFromVertexPyList)( PyObject *py_list );
} IPyVGXParser;



typedef struct s_IPyVGXCodec {
  CString_t * (*NewEncodedObjectFromPyObject)( PyObject *py_key, PyObject *py_object, object_allocator_context_t *allocator_context, bool allow_oversized );
  PyObject * (*NewPyObjectFromEncodedObject)( const CString_t *CSTR__encoded, PyObject **py_retkey );
  bool (*IsCStringCompressible)( const CString_t *CSTR__string );
  PyObject * (*NewSerializedPyBytesFromPyObject)( PyObject *py_object );
  PyObject * (*NewPyObjectFromSerializedPyBytes)( PyObject *py_bytes );
  PyObject * (*NewPyObjectFromSerializedBytes)( const char *bytes, int64_t sz_bytes );
  PyObject * (*NewCompressedPyBytesFromPyObject)( PyObject *py_object );
  PyObject * (*NewPyObjectFromCompressedPyBytes)( PyObject *py_bytes );
  PyObject * (*NewJsonPyStringFromPyObject)( PyObject *py_object );
  PyObject * (*NewJsonPyBytesFromPyObject)( PyObject *py_object );
  PyObject * (*NewPyObjectFromJsonPyString)( PyObject *py_json );
  PyObject * (*NewPyObjectFromJsonBytes)( const char *bytes, int64_t sz_bytes );
  bool (*IsTypeJson)( const PyObject *py_type );
  int (*RenderPyObjectByMediatype)( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, vgx_StreamBuffer_t *output );
  PyObject * (*ConvertPyObjectByMediatype)( vgx_MediaType mtype, PyObject *py_plugin_return_type, PyObject *py_obj, const char **rstr, int64_t *rsz );
} IPyVGXCodec;



typedef struct s_IPyVGXBuilder {
  PyObject * (*InternalVector)( vgx_Vector_t *vector );
  PyObject * (*InternalVectorArray)( vgx_Vector_t *vector );
  PyObject * (*ExternalVector)( vgx_Vector_t *vector );
  PyObject * (*StringListFromTptrList)( CtptrList_t *CSTR__strings );
  int (*DictMapStringToLongLong)( PyObject *py_dict, const char *key, const int64_t value );
  int (*DictMapStringToUnsignedLongLong)( PyObject *py_dict, const char *key, const uint64_t value );
  int (*DictMapStringToInt)( PyObject *py_dict, const char *key, const int value );
  int (*DictMapStringToString)( PyObject *py_dict, const char *key, const char *value );
  int (*DictMapStringToFloat)( PyObject *py_dict, const char *key, const double value );
  int (*DictMapStringToPyObject)( PyObject *py_dict, const char *key, PyObject **py_object );
  PyObject * (*NumberListFromBytes)( const char *data, int sz, bool isfloat );
  PyObject * (*NumberListFromCString)( const CString_t *CSTR__str );
  PyObject * (*NumberMapFromBytes)( const char *data );
  PyObject * (*NumberMapFromCString)( const CString_t *CSTR__str );
  int64_t (*MapIntegerConstants)( PyObject *py_dict, vgx_KeyVal_char_int64_t *data );
  PyObject * (*TupleFromCStringMapKeyVal)( const QWORD *item_bits );
  PyObject * (*VertexPropertiesAsDict)( vgx_Vertex_t *vertex_RO );
  void (*SetErrorFromMessages)( vgx_StringTupleList_t *messages );
  bool (*SetPyErrorFromAccessReason)( const char *object_name, vgx_AccessReason_t reason, CString_t **CSTR__error );
  int (*CatchPyExceptionIntoOutput)( const char *wrap, vgx_MediaType *mediatype, CString_t **CSTR__output, vgx_StreamBuffer_t *output );
} IPyVGXBuilder;



typedef struct s_IPyVGXPersist {
  int64_t (*Serialize)( vgx_Graph_t *graph, int timeout_ms, bool force, bool remote );
} IPyVGXPersist;


typedef struct s_IPyVGXDebug {
  void (*PrintVertexAllocator)( vgx_Vertex_t *vertex );
  void (*PrintVectorAllocator)( vgx_Vector_t *vector );
} IPyVGXDebug;




DLL_HIDDEN extern IPyVGXSearchResult iPyVGXSearchResult;

DLL_HIDDEN extern IPyVGXParser iPyVGXParser;

DLL_HIDDEN extern IPyVGXCodec iPyVGXCodec;
DLL_HIDDEN extern int _ipyvgx_codec__init( void );
DLL_HIDDEN extern int _ipyvgx_codec__delete( void );

DLL_HIDDEN extern IPyVGXBuilder iPyVGXBuilder;
DLL_HIDDEN extern int _ipyvgx_builder__init( void );
DLL_HIDDEN extern int _ipyvgx_builder__delete( void );

DLL_HIDDEN extern IPyVGXPersist iPyVGXPersist;

DLL_HIDDEN extern IPyVGXDebug iPyVGXDebug;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Framehash__FramehashType;
DLL_HIDDEN extern PyTypeObject * p_PyVGX_StringQueue__StringQueueType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Graph__GraphType;
DLL_HIDDEN extern int PyVGX_Graph_Init( void );
DLL_HIDDEN extern int PyVGX_Graph_Unload( void );
DLL_HIDDEN extern PyObject * PyVGX_Graph__get_open( PyObject *py_name );

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Vertex__VertexType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Vector__VectorType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Similarity__SimilarityType;
DLL_HIDDEN extern PyObject * PyVGX_Similarity__Similarity( PyObject *pysim, PyObject *args );

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Memory__MemoryType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_PluginRequestType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_PluginResponseType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Query__QueryType;

DLL_HIDDEN extern PyTypeObject * p_PyVGX_System__SystemType;
DLL_HIDDEN extern PyObject * PyVGX_System_GetNewConstantsDict( void );

DLL_HIDDEN extern PyTypeObject * p_PyVGX_Operation__OperationType;
DLL_HIDDEN extern PyObject * PyVGX_Operation_GetNewConstantsDict( void );



#define PyVGX_Graph_CheckExact( op )          Py_IS_TYPE(op, p_PyVGX_Graph__GraphType)
#define PyVGX_Graph_Check( op )               PyObject_TypeCheck(op, p_PyVGX_Graph__GraphType)
#define PyVGX_Similarity_CheckExact( op )     Py_IS_TYPE(op, p_PyVGX_Similarity__SimilarityType)

#define PyVGX_Vector_CheckExact( op )         Py_IS_TYPE(op, p_PyVGX_Vector__VectorType)
#define PyVGX_Vertex_CheckExact( op )         Py_IS_TYPE(op, p_PyVGX_Vertex__VertexType)

#define PyVGX_Memory_CheckExact( op )         Py_IS_TYPE(op, p_PyVGX_Memory__MemoryType)

#define PyVGX_PluginRequest_CheckExact( op )  Py_IS_TYPE(op, p_PyVGX_PluginRequestType)

#define PyVGX_PluginResponse_CheckExact( op ) Py_IS_TYPE(op, p_PyVGX_PluginResponseType)

#define PyVGX_Query_CheckExact( op )          Py_IS_TYPE(op, p_PyVGX_Query__QueryType)

#define PyVGX_Vector_AsComparable( pyvgx_vector ) ((vgx_Comparable_t)((PyVGX_Vector*)pyvgx_vector)->vint)
#define PyVGX_Vertex_AsComparable( pyvgx_vertex ) ((vgx_Comparable_t)((PyVGX_Vertex*)pyvgx_vertex)->vertex)
#define PyVGX_PyObject_AsComparable( op )         (PyVGX_Vector_CheckExact( op ) ? PyVGX_Vector_AsComparable( op ) : PyVGX_Vertex_CheckExact( op ) ? PyVGX_Vertex_AsComparable( op ) : NULL)


typedef uint64_t (*f_generic_enum_encoder)( vgx_Graph_t *graph, const CString_t *CSTR__string );


DLL_HIDDEN extern PyObject * pyvgx__enumerator_as_dict(
  vgx_Graph_t *graph,
  int (*GetStrings)( vgx_Graph_t *graph, CtptrList_t **CSTR__strings ),
  bool use_qwo,
  f_generic_enum_encoder EncodeString_CS );


DLL_HIDDEN extern int64_t pyvgx__enumerator_size( vgx_Graph_t *graph, int64_t (*GetSize)( vgx_Graph_t *graph ) );



typedef float (*f_similarity_method)( vgx_Similarity_t *sim, const vgx_Comparable_t A, const vgx_Comparable_t B );


/******************************************************************************
 * __new_query_args
 *
 ******************************************************************************
 */
static __base_query_args * __new_query_args( size_t struct_size, vgx_Graph_t *graph, vgx_arc_direction default_arcdir, vgx_collector_mode_t collector_mode, void (*clear)( __base_query_args *clear ) ) {
  __base_query_args *param;
  // Allocate Neighborhood Arguments  
  if( (param = calloc( 1, struct_size )) == NULL ) {
    PyErr_SetNone( PyExc_MemoryError );
    return NULL;
  }

  param->implied.graph = graph;
  param->implied.default_arcdir = default_arcdir;
  param->implied.collector_mode = collector_mode;
  param->implied.py_err_class = PyExc_Exception;
  param->implied.clear = clear;

  return param;
}

#define PyVGX_NewQueryArgs( QueryArgsStruct, GraphPtr, DefaultArcDirection, CollectorMode, ClearCallback ) \
  (QueryArgsStruct*)__new_query_args( sizeof( QueryArgsStruct ), GraphPtr, DefaultArcDirection, CollectorMode, ClearCallback )




/******************************************************************************
 * __pyquery_from_base_query
 *
 ******************************************************************************
 */
static PyVGX_Query * __pyquery_from_base_query( PyVGX_Graph *pygraph, vgx_BaseQuery_t *query, __base_query_args **param ) {
  // Create Python Query Object
  PyObject *py_query_args;
  if( (py_query_args = PyTuple_New( 3 )) == NULL ) {
    return NULL;
  }

  // Graph object
  Py_INCREF( (PyObject*)pygraph ); // prepare for stealing
  PyTuple_SET_ITEM( py_query_args, 0, (PyObject*)pygraph ); // steals pygraph

  // Query capsule
  PyTuple_SET_ITEM( py_query_args, 1, PyVGX_PyCapsule_NewNoErr( query, NULL, NULL ) );

  // Params capsule
  PyTuple_SET_ITEM( py_query_args, 2, PyVGX_PyCapsule_NewNoErr( *param, NULL, NULL ) );

  // Create Python Query Object
  PyVGX_Query *py_query;
  if( (py_query = (PyVGX_Query*)PyObject_CallObject( (PyObject*)p_PyVGX_Query__QueryType, py_query_args )) != NULL ) {
    // pyvgx.Query object not owns the param struct
    *param = NULL; // stolen
  }

  Py_XDECREF( py_query_args );

  return py_query;
}

#define PyVGX_PyQuery_From_BaseQuery( GraphPtr, QueryPtr, QueryArgsPtrPtr ) \
  __pyquery_from_base_query( GraphPtr, (vgx_BaseQuery_t*)(QueryPtr), (__base_query_args**)(QueryArgsPtrPtr) )




/******************************************************************************
 * __as_adjacency_query
 *
 ******************************************************************************
 */
__inline static vgx_AdjacencyQuery_t * __as_adjacency_query( vgx_BaseQuery_t *base ) {
  // Do we have the complete feature set for adjacency queries?
  if( base && (base->type & __VGX_QUERY_FEATURE_SET_ADJACENCY) == __VGX_QUERY_FEATURE_SET_ADJACENCY ) {
    return (vgx_AdjacencyQuery_t*)base;
  }
  else {
    return NULL;
  }
}


#define PyVGX_PyQuery_As_AdjacencyQuery( PyQueryPtr ) __as_adjacency_query( (PyQueryPtr)->query )



/******************************************************************************
 * __base_query_from_pyquery
 *
 ******************************************************************************
 */
__inline static vgx_BaseQuery_t * __base_query_from_pyquery( PyVGX_Query *py_query, __base_query_args **param ) {
  if( py_query->threadid != GET_CURRENT_THREAD_ID() ) {
    PyVGXError_SetString( PyVGX_AccessError, "not owner thread" );
    return NULL;
  }
  vgx_BaseQuery_t *query = py_query->query;
  *param = py_query->p_args;
  if( query && *param ) {
    return query;
  }
  else {
    PyErr_SetString( PyExc_Exception, "internal error" );
    return NULL;
  }
}

#define PyVGX_BaseQuery_From_PyQuery( PyQueryPtr, BaseQueryArgsPtr ) __base_query_from_pyquery( PyQueryPtr, &(BaseQueryArgsPtr) )



/******************************************************************************
 * __adjacency_query_from_pyquery
 *
 ******************************************************************************
 */
__inline static vgx_AdjacencyQuery_t * __adjacency_query_from_pyquery( PyVGX_Query *py_query, __adjacency_query_args **param ) {
  return __as_adjacency_query( __base_query_from_pyquery( py_query, (__base_query_args**)param ) );
}

#define PyVGX_AdjacencyQuery_From_PyQuery( PyQueryPtr, AdjacencyQueryArgsPtr ) __adjacency_query_from_pyquery( PyQueryPtr, &(AdjacencyQueryArgsPtr) )



/******************************************************************************
 * __capture_query_error
 *
 ******************************************************************************
 */
static void __capture_query_error( vgx_BaseQuery_t *query, __base_query_args *param ) {
  // *** ERROR ***
  // Transfer error string from query to param
  iString.Discard( &param->implied.CSTR__error );
  param->implied.CSTR__error = query->CSTR__error;
  query->CSTR__error = NULL;
  // Copy reason
  if( (query->type & __VGX_QUERY_FEATURE_ERR_REASON) != 0 ) {
    param->implied.reason = ((vgx_AdjacencyQuery_t*)query)->access_reason;
  }
  // SearchError
  param->implied.py_err_class = PyVGX_SearchError;
}


#define PyVGX_CAPTURE_QUERY_ERROR( QueryPtr, ParamPtr ) __capture_query_error( (vgx_BaseQuery_t*)(QueryPtr), (__base_query_args*)(ParamPtr) )



/******************************************************************************
 *
 ******************************************************************************
 */
static void __pyvgx_set_query_error( PyVGX_Query *py_query, __base_query_args *param, const char *object_name ) {
  if( !iPyVGXBuilder.SetPyErrorFromAccessReason( object_name, param->implied.reason, &param->implied.CSTR__error ) ) {
    PyErr_SetString( param->implied.py_err_class, param->implied.CSTR__error ? CStringValue( param->implied.CSTR__error ) : "unknown internal error" );
  }
  if( py_query ) {
    PyObject *type, *traceback;
    Py_XDECREF( py_query->py_error );
    PyErr_Fetch( &type, &py_query->py_error, &traceback );
    Py_XINCREF( py_query->py_error );
    PyErr_Restore( type, py_query->py_error, traceback );
  }
}


#define PyVGX_SET_QUERY_ERROR( PyQueryPtr, ParamPtr, ObjectName )  __pyvgx_set_query_error( PyQueryPtr, (__base_query_args*)(ParamPtr), ObjectName )



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static uint64_t _get_pyvertex_generation_guard( void ) {
  if( _pyvertex_generation_guard == 0 ) {
    _pyvertex_generation_guard = ihash64( GET_CURRENT_THREAD_ID() );
  }
  return _pyvertex_generation_guard;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static uint64_t _next_pyvertex_generation_guard( void ) {
  return _pyvertex_generation_guard = ihash64( _get_pyvertex_generation_guard() + GET_CURRENT_THREAD_ID() );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * _match_pyvertex_generation_guard( PyVGX_Vertex *pyvertex ) {
  if( pyvertex && pyvertex->vertex && pyvertex->gen_guard == _pyvertex_generation_guard ) {
    return pyvertex->vertex;
  }
  else {
    return NULL;
  }
}



/******************************************************************************
 * __PyVGX__comparable_from_pyobject
 *
 ******************************************************************************
 */
__inline static PyObject * __PyVGX__comparable_from_pyobject( PyObject *py_obj, vgx_Similarity_t *simcontext, vgx_Comparable_t *pC ) {
  *pC = PyVGX_PyObject_AsComparable( py_obj );
  PyObject *py_parent = NULL;
  if( *pC ) {
    Py_INCREF( py_obj );
    py_parent = py_obj;
  }
  else {
    vgx_Vector_t *vector = iPyVGXParser.InternalVectorFromPyObject( simcontext, py_obj, NULL, true );
    if( vector ) {
      *pC = (vgx_Comparable_t)vector;
    }
  }
  return py_parent; // This is the comparable's parent object and caller will own a new reference to it! (If not NULL.) Remember to decref.
}



/******************************************************************************
 * __PyVGX_Graph__compare_vectors
 *
 ******************************************************************************
 */
static PyObject * __PyVGX__compare_vectors( PyObject *args, vgx_Similarity_t *simcontext, f_similarity_method method ) {
  PyObject *py_sim = NULL;
  PyObject *py_A, *py_B;
  vgx_Comparable_t A = NULL, B = NULL;
  
  if( !PyArg_ParseTuple( args, "OO", &py_A, &py_B ) ) {
    return NULL;
  }

  PyObject *py_parent_A = __PyVGX__comparable_from_pyobject( py_A, simcontext, &A );
  PyObject *py_parent_B = __PyVGX__comparable_from_pyobject( py_B, simcontext, &B );

  // Compute similarity of comparable objects
  if( A && B ) {
    float sim;
    BEGIN_PYVGX_THREADS {
      sim = method( simcontext, A, B );
    } END_PYVGX_THREADS;
    py_sim = PyFloat_FromDouble( sim );
  }
  // Objects are not comparable
  else {
    PyErr_SetString( PyExc_ValueError, "Objects are not comparable" );
  }

  // Clean up
  if( py_parent_A ) {
    PyVGX_DECREF( py_parent_A );
  }
  else if( A ) {
    CALLABLE(( vgx_Vector_t*)A )->Decref( (vgx_Vector_t*)A );
  }

  // Clean up B
  if( py_parent_B ) {
    PyVGX_XDECREF( py_parent_B );
  }
  else if( B ) {
    CALLABLE(( vgx_Vector_t*)B )->Decref( (vgx_Vector_t*)B );
  }

  return py_sim;
}



/******************************************************************************
 * __PyVGX_Graph__hamdist_vectors
 *
 ******************************************************************************
 */
static PyObject * __PyVGX__hamdist_vectors( PyObject *args, vgx_Similarity_t *simcontext ) {
  PyObject *py_ham = NULL;
  PyObject *py_A, *py_B;
  vgx_Comparable_t A = NULL, B = NULL;
  
  if( !PyArg_ParseTuple( args, "OO", &py_A, &py_B ) ) {
    return NULL;
  }

  PyObject *py_parent_A = __PyVGX__comparable_from_pyobject( py_A, simcontext, &A );
  PyObject *py_parent_B = __PyVGX__comparable_from_pyobject( py_B, simcontext, &B );

  // Compute similarity of comparable objects
  if( A && B ) {
    int ham;
    BEGIN_PYVGX_THREADS {
      ham = CALLABLE( simcontext )->HammingDistance( simcontext, A, B );
    } END_PYVGX_THREADS;
    py_ham = PyLong_FromLong( ham );
  }
  // Objects are not comparable
  else {
    PyErr_SetString( PyExc_ValueError, "Objects are not comparable" );
  }

  // Clean up
  if( py_parent_A ) {
    PyVGX_DECREF( py_parent_A );
  }
  else if( A ) {
    CALLABLE(( vgx_Vector_t*)A )->Decref( (vgx_Vector_t*)A );
  }
  if( py_parent_B ) {
    PyVGX_XDECREF( py_parent_B );
  }
  else if( B ) {
    CALLABLE(( vgx_Vector_t*)B )->Decref( (vgx_Vector_t*)B );
  }

  return py_ham;
}



#define ENSURE_FEATURE_VECTORS_OR_RETURN_NULL    \
  do {                                      \
    if( igraphfactory.EuclideanVectors() ) {    \
      PyErr_SetString( PyExc_NotImplementedError, "not available in selected vector mode (euclidean)" );  \
      return NULL;  \
    }               \
  } WHILE_ZERO



/******************************************************************************
 * Set the python error string based on query's access reason, or if that is not
 * a conclusive reason set the python error string to any query error string that
 * that was set deeper in the core execution of the query.
 * Returns:
 *    true    : error reason was the vertex does not exist
 *    false   : any other error
 ******************************************************************************
 */
static bool __py_set_query_error_check_is_noexist( vgx_AdjacencyQuery_t *query ) {
  bool is_nonexist = false;
  bool is_conclusive = iPyVGXBuilder.SetPyErrorFromAccessReason( CStringValue(query->CSTR__anchor_id), query->access_reason, &query->CSTR__error );
  if( is_conclusive ) {
    if( __is_access_reason_noexist( query->access_reason ) ) {
      is_nonexist = true;
    }
  }
  else {
    BEGIN_PYTHON_INTERPRETER {
      PyErr_SetString( PyVGX_SearchError, query->CSTR__error ? CStringValue(query->CSTR__error) : "unknown adjacency query error" );
    } END_PYTHON_INTERPRETER;
  }
  return is_nonexist;
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static bool __py_set_vertex_error_check_is_noexist( const char *vertex_name, vgx_AccessReason_t reason ) {
  iPyVGXBuilder.SetPyErrorFromAccessReason( vertex_name, reason, NULL );
  return __is_access_reason_noexist( reason ) ? true : false;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_Graph_t * __get_vertex_graph( PyVGX_Graph *pygraph, PyVGX_Vertex *pyvertex ) {
  vgx_Vertex_t *vertex = pyvertex->vertex;
  vgx_Graph_t *graph = pygraph->graph;
  if( graph && vertex && graph == vertex->graph ) {
    return graph;
  }
  else {
    if( graph && vertex ) {
      PyErr_Format( PyVGX_AccessError, "Vertex '%s' not a member of graph '%s'", CALLABLE( vertex )->IDString( vertex ), CStringValue( CALLABLE( graph )->Name( graph ) ) );
    }
    else if( vertex == NULL ) {
      PyErr_SetString( PyVGX_AccessError, "Vertex is not accessible" );
    }
    else {
      PyErr_SetString( PyVGX_AccessError, "Invalid graph" );
    }
    return NULL;
  }
}



/******************************************************************************
 *
 ******************************************************************************
 */
__inline static const char * PyVGX_VertexIDAsString( PyObject *py_graph, PyObject *py_vertex ) {
  if( PyVGX_Vertex_CheckExact( py_vertex ) ) {
    if( py_graph && __get_vertex_graph( (PyVGX_Graph*)py_graph, (PyVGX_Vertex*)py_vertex ) == NULL ) {
      return NULL;
    }
    vgx_Vertex_t *vertex = ((PyVGX_Vertex*)py_vertex)->vertex;
    if( vertex ) {
      return CALLABLE( vertex )->IDString( vertex );
    }
  }
  else if( PyVGX_PyObject_CheckString( py_vertex ) ) {
    return PyVGX_PyObject_AsUTF8( py_vertex, NULL );
  }
  return NULL;
}



/******************************************************************************
 *
 ******************************************************************************
 */
static void __py_set_vertex_pair_error_ident( PyVGX_Graph *pygraph, PyObject *py_initial, PyObject *py_terminal, vgx_AccessReason_t reason, CString_t **CSTR__error ) {
  CString_t *CSTR__ident = NULL;
  const char *str_init = PyVGX_VertexIDAsString( (PyObject*)pygraph, py_initial );
  const char *str_term = PyVGX_VertexIDAsString( (PyObject*)pygraph, py_terminal );
  if( str_init && str_term ) {
    CSTR__ident = CStringNewFormat( "(%s)->(%s)", str_init, str_term );
  }
  iPyVGXBuilder.SetPyErrorFromAccessReason( CSTR__ident ? CStringValue( CSTR__ident ) : "?", reason, CSTR__error );
  iString.Discard( &CSTR__ident );
}



/******************************************************************************
 * __PyVGX_Vertex_as_vgx_Vertex_t
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __PyVGX_Vertex_as_vgx_Vertex_t( PyVGX_Vertex *py_vertex ) {
  vgx_Vertex_t *vertex = _match_pyvertex_generation_guard( py_vertex );
  if( vertex && __vertex_is_locked( vertex ) ) {
    return vertex;
  }
  else {
    PyErr_SetString( PyVGX_AccessError, "Vertex cannot be accessed" );
    return NULL;
  }
}



/******************************************************************************
 * 
 *
 ******************************************************************************
 */
static bool __PyVGX_Graph_try_enable_api( void ) {
  BEGIN_PYVGX_THREADS {
    if( igraphfactory.IsSyncActive() == false ) {
      _pyvgx_api_enabled = true;
    }
  } END_PYVGX_THREADS;
  if( _pyvgx_api_enabled == false ) {
    PyErr_SetString( PyVGX_AccessError, "API not available at this time" );
  }
  return _pyvgx_api_enabled;
}



/******************************************************************************
 * __PyVGX_Graph_as_vgx_Graph_t
 *
 ******************************************************************************
 */
__inline static vgx_Graph_t * __PyVGX_Graph_as_vgx_Graph_t( PyVGX_Graph *pygraph ) {
  if( !_pyvgx_api_enabled ) {
    if( __PyVGX_Graph_try_enable_api() == false ) {
      return NULL;
    }
  }
  vgx_Graph_t *graph = pygraph ? pygraph->graph : NULL;
  if( graph == NULL && pygraph && pygraph->constructor_complete ) {
    if( !PyErr_Occurred() ) {
      PyErr_SetString( PyVGX_AccessError, "Graph cannot be accessed" );
    }
  }
  return graph;
}


#define GET_GRAPH_FROM_PYGRAPH( Graph, PyGraph )  \
  vgx_Graph_t *Graph = __PyVGX_Graph_as_vgx_Graph_t( PyGraph ); \
  if( !Graph ) {    \
    return NULL;    \
  }



/******************************************************************************
 * PyVGX_DictStealItemString
 *
 ******************************************************************************
 */
static int PyVGX_DictStealItemString( PyObject *py_map, const char *key, PyObject *py_value ) {
  if( py_value != NULL ) {
    int result = PyDict_SetItemString( py_map, key, py_value );
    PyVGX_DECREF( py_value );
    return result;
  }
  return -1;
}

#endif


