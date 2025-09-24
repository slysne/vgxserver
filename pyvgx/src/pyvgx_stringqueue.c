/*######################################################################
 *#
 *# pyvgx_stringqueue.c
 *#
 *#
 *######################################################################
 */


#include "pyvgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_GENERAL );



/******************************************************************************
 * PyVGX_StringQueue method definitions
 * (type definitions and other standard Python C-API stuff in include/fluxlib.h)
 *
 ******************************************************************************
 */



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void StringQueue_dealloc( PyVGX_StringQueue *self ) {
  COMLIB_OBJECT_DESTROY( self->queue );
  self->queue = NULL;
  Py_TYPE( self )->tp_free( self );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * StringQueue_new( PyTypeObject *type, PyObject *args, PyObject *kwds ) {
  PyVGX_StringQueue *self;

  self = (PyVGX_StringQueue *)type->tp_alloc(type, 0);
  if (self != NULL) {
    BEGIN_PYVGX_THREADS {
      self->queue = COMLIB_OBJECT_NEW( CStringQueue_t, NULL, NULL );
      self->Write = CALLABLE( self->queue )->Write;
      self->WriteNolock = CALLABLE( self->queue )->WriteNolock;
      self->Read = CALLABLE( self->queue )->Read;
      self->ReadNolock = CALLABLE( self->queue )->ReadNolock;
    } END_PYVGX_THREADS;
    if( self->queue == NULL ) {
      return NULL;
    }
  }
  return (PyObject *)self;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int StringQueue_init( PyVGX_StringQueue *self, PyObject *args, PyObject *kwds ) {
  static char *kwlist[] = {"capacity", NULL};
  int64_t capacity = 0, ret;

  if( !PyArg_ParseTupleAndKeywords(args, kwds, "|L", kwlist, &capacity) ) {
    return -1; 
  }

  if( capacity > 4096 ) {
    BEGIN_PYVGX_THREADS {
      ret = CALLABLE( self->queue )->SetCapacity( self->queue, capacity );
    } END_PYVGX_THREADS;
  }
  else {
    ret = CALLABLE( self->queue )->SetCapacity( self->queue, capacity );
  }
  
  if( ret < 0 ) {
    PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
    return -1;
  }
  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_write( PyVGX_StringQueue *self, PyObject *args ) {
  PyObject *py_ret = NULL;
  const char *data;
  int64_t length;
  int64_t nwritten;

  if( !PyArg_ParseTuple(args, "s#", &data, &length) ) {
    return NULL;
  }

  XTRY {
  
    BEGIN_PYVGX_THREADS {
      nwritten = self->Write( self->queue, data, length );
    } END_PYVGX_THREADS;
  
    if( nwritten != length ) {
      PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x401 );
    }
    
    Py_INCREF( Py_None );
    py_ret = Py_None;
  }
  XCATCH( errcode ) {
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
static PyObject * StringQueue_read( PyVGX_StringQueue *self, PyObject *args ) {
  PyObject *py_result = NULL;
  int64_t count = -1;
  char *rstr = NULL; // Read() will allocate ALIGNED memory
  
  if( !PyArg_ParseTuple(args, "|L", &count) ) {
    return NULL;
  }

  XTRY {
    int64_t nread;
    
    BEGIN_PYVGX_THREADS {
      nread = self->Read( self->queue, (void**)&rstr, count );
    } END_PYVGX_THREADS;
  
    if( nread < 0 ) {
      PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x411 );
    }

    if( rstr ) {
      if( (py_result = PyBytes_FromStringAndSize( rstr, nread )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x412 );
      }
    }
    else {
      py_result = PyBytes_FromString( "" );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( rstr ) {
      ALIGNED_FREE( rstr ); // ALIGNED
    }
  }

  return py_result;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * StringQueue_readline( PyVGX_StringQueue *self, PyObject *args ) {
  PyObject *py_result = NULL;
  char *rstr = NULL; // Readline() will allocate (NOT ALIGNED)

  XTRY {
    int64_t nread;

    BEGIN_PYVGX_THREADS {
      nread = CALLABLE( self->queue )->Readline( self->queue, (void**)&rstr );
    } END_PYVGX_THREADS;
    
    if( nread < 0 ) {
      PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x421 );
    }
    
    if( (py_result = PyBytes_FromStringAndSize( rstr, nread )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x422 );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( rstr ) {
      free( rstr ); // not aligned
    }
  }
    
  return py_result;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_readuntil( PyVGX_StringQueue *self, PyObject *args ) {
  PyObject *py_result = NULL;
  char *rstr = NULL; // ReadUntil() will allocate (NOT ALIGNED)
  const char *probe;
  Py_ssize_t plen;
  int exclude_probe = 1;

  if( !PyArg_ParseTuple(args, "s#|i", &probe, &plen, &exclude_probe) ) {
    return NULL;
  }

  XTRY {
    int64_t nread;

    BEGIN_PYVGX_THREADS {
      nread = CALLABLE( self->queue )->ReadUntil( self->queue, (void**)&rstr, probe, plen, exclude_probe );
    } END_PYVGX_THREADS;
    
    if( nread < 0 ) {
      PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x431 );
    }
    else if( rstr != NULL ) {
      if( (py_result = PyBytes_FromStringAndSize( rstr, nread )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x432 );
      }
    }
    else {
      if( (py_result = PyBytes_FromStringAndSize( "", 0 )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x433 );
      }
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( rstr ) {
      free( rstr ); // not aligned
    }
  }
    
  return py_result;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_unread( PyVGX_StringQueue *self, PyObject *args ) {
  int64_t count = -1;

  if( !PyArg_ParseTuple(args, "|L", &count) ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    CALLABLE( self->queue )->Unread( self->queue, count );
  } END_PYVGX_THREADS;
  
  Py_INCREF( Py_None );
  return Py_None;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_clear( PyVGX_StringQueue *self ) {

  BEGIN_PYVGX_THREADS {
    CALLABLE( self->queue )->Clear( self->queue );
  } END_PYVGX_THREADS;
    
  Py_INCREF( Py_None );
  return Py_None;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_getvalue( PyVGX_StringQueue *self ) {
  PyObject *py_value = NULL;
  char *rstr = NULL; // GetValue() will allocate (ALIGNED)

  XTRY {
    int64_t nread;
    
    BEGIN_PYVGX_THREADS {
      nread = self->queue->vtable->GetValue( self->queue, (void**)&rstr );
    } END_PYVGX_THREADS;
    
    if( nread < 0 ) {
      PyErr_SetString( PyExc_Exception, self->queue->vtable->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x441 );
    }
      
    if( (py_value = PyBytes_FromStringAndSize( rstr, nread )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x442 );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( rstr ) {
      ALIGNED_FREE( rstr ); // ALIGNED
    }
  }

  return py_value;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int StringQueue_contains(PyVGX_StringQueue *self, PyObject *py_probe) {
  const char *probe;
  Py_ssize_t plen;
  int match;

  if( PyBytes_CheckExact( py_probe ) ) {
    probe = PyBytes_AS_STRING( py_probe );
    plen = PyBytes_GET_SIZE( py_probe );
  }
  else if( (probe = PyUnicode_AsUTF8AndSize( py_probe, &plen )) == NULL ) {
    return -1;
  }

  BEGIN_PYVGX_THREADS {
    match = CALLABLE( self->queue )->Index( self->queue, probe, plen ) >= 0;
  } END_PYVGX_THREADS;
  
  return match;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_index(PyVGX_StringQueue *self, PyObject *args) {
  const char *probe;
  Py_ssize_t plen;
  int64_t index;

  if( !PyArg_ParseTuple( args, "s#", &probe, &plen ) ) {
    return NULL;
  }
  
  BEGIN_PYVGX_THREADS {
    index = CALLABLE( self->queue )->Index( self->queue, probe, plen );
  } END_PYVGX_THREADS;
  
  if( index < 0 ) {
    PyErr_SetString( PyExc_ValueError, "substring not found" );
    return NULL;
  }

  return PyLong_FromLongLong( index );
}
  


/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_count(PyVGX_StringQueue *self, PyObject *args) {
  const char *probe;
  Py_ssize_t plen;
  int64_t occ;

  if( !PyArg_ParseTuple( args, "s#", &probe, &plen) ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    occ = CALLABLE( self->queue )->Occ( self->queue, probe, plen );
  } END_PYVGX_THREADS;

  return PyLong_FromLongLong( occ );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_peek(PyVGX_StringQueue *self, PyObject *args) {
  PyObject *py_result = NULL;
  Py_ssize_t index;
  Py_ssize_t count = -1;
  char *rstr = NULL; // delegate allocation to CStringQueue by passing NULL (ALIGNED)

  if( !PyArg_ParseTuple( args, "L|L", &index, &count) ) {
    return NULL;
  }

  XTRY {
    Py_ssize_t nread;

    BEGIN_PYVGX_THREADS {
      nread = CALLABLE( self->queue )->Peek( self->queue, (void**)&rstr, index, count );
    } END_PYVGX_THREADS;

    if( nread < 0 ) {
      PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x451 );
    }
    
    if( (py_result = PyBytes_FromStringAndSize( rstr, nread )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x452 );
    }
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
    if( rstr ) {
      ALIGNED_FREE( rstr ); // ALIGNED
    }
  }
    
  return py_result;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_discard(PyVGX_StringQueue *self, PyObject *args) {
  int64_t count = -1;

  if( !PyArg_ParseTuple(args, "|L", &count ) ) {
    return NULL;
  }

  BEGIN_PYVGX_THREADS {
    CALLABLE( self->queue )->Discard( self->queue, count );
  } END_PYVGX_THREADS;
  
  Py_INCREF( Py_None );
  return Py_None;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static PyObject * StringQueue_optimize( PyVGX_StringQueue *self, PyObject *args ) {

  BEGIN_PYVGX_THREADS {
    CALLABLE( self->queue )->Optimize( self->queue );
  } END_PYVGX_THREADS;
  
  Py_INCREF( Py_None );
  return Py_None;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_absorb( PyVGX_StringQueue *self, PyObject *args )  {
  PyObject *py_other = NULL;
  PyObject *py_nabs = NULL;

  if( !PyArg_ParseTuple(args, "O", &py_other) ) {
    return NULL;
  }

  XTRY {
    int64_t nabs = 0;

    if( py_other ) {
      if( !PyObject_TypeCheck( py_other, p_PyVGX_StringQueue__StringQueueType ) ) {
        PyErr_Format( PyExc_TypeError, "argument must be StringQueue, got %s", py_other->ob_type->tp_name );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x461 );
      }

      BEGIN_PYVGX_THREADS {
        nabs = CALLABLE( self->queue )->Absorb( self->queue, ((PyVGX_StringQueue*)py_other)->queue, -1 );
      } END_PYVGX_THREADS;

      if( nabs < 0 ) {
        PyErr_SetString( PyExc_Exception, CALLABLE( self->queue )->GetError( self->queue ) );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x462 );
      }
    }

    py_nabs = PyLong_FromLongLong( nabs );
  }
  XCATCH( errcode ) {
  }
  XFINALLY {
  }

  return py_nabs;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_capacity(PyVGX_StringQueue* self) {
  int64_t n = CALLABLE( self->queue )->Capacity( self->queue );
  return PyLong_FromLongLong( n );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyObject * StringQueue_remain(PyVGX_StringQueue* self) {
  int64_t n = CALLABLE( self->queue )->Remain( self->queue );
  return PyLong_FromLongLong( n );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static Py_ssize_t StringQueue_length(PyVGX_StringQueue *self) {
  return CALLABLE( self->queue )->Length( self->queue );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PyMemberDef StringQueue_members[] = {
  {NULL}  /* Sentinel */
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static PySequenceMethods StringQueue_as_sequence = {
    .sq_length          = (lenfunc)StringQueue_length,
    .sq_concat          = 0,
    .sq_repeat          = 0,
    .sq_item            = 0,
    .was_sq_slice       = 0,
    .sq_ass_item        = 0,
    .was_sq_ass_slice   = 0,
    .sq_contains        = (objobjproc)StringQueue_contains,
    .sq_inplace_concat  = 0,
    .sq_inplace_repeat  = 0 
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
IGNORE_WARNING_UNSAFE_FUNCTION_POINTER_CAST
static PyMethodDef StringQueue_methods[] = {
  {"write",     (PyCFunction)StringQueue_write,     METH_VARARGS, "write( data ) -> None\n"
                                                                  "write data to end of buffer" },

  {"read",      (PyCFunction)StringQueue_read,      METH_VARARGS, "read( [n] ) -> string\n"
                                                                  "read n bytes from the read position, default all" },

  {"readline",  (PyCFunction)StringQueue_readline,  METH_NOARGS,  "readline() -> string\n"
                                                                  "read up to and including first newline, or end of queue" },

  {"readuntil", (PyCFunction)StringQueue_readuntil, METH_VARARGS, "readuntil( str [,exclude=True] ) -> string\n"
                                                                  "read until first occurrence of str, or end of queue, excluding the probe string by default" },

  {"unread",    (PyCFunction)StringQueue_unread,    METH_VARARGS, "unread( [n] ) -> None\n"
                                                                  "unread n bytes back from the read position, default all of last read" },

  {"clear",     (PyCFunction)StringQueue_clear,     METH_NOARGS,  "clear() -> None\n"
                                                                  "remove all data from queue" },

  {"absorb",    (PyCFunction)StringQueue_absorb,    METH_VARARGS, "absorb( P ) -> None\n"
                                                                  "consume all data in other queue P and append to self" },

  {"getvalue",  (PyCFunction)StringQueue_getvalue,  METH_NOARGS,  "getvalue() -> string\n"
                                                                  "return data in queue without consuming" },

  {"capacity",  (PyCFunction)StringQueue_capacity,  METH_NOARGS,  "capacity() -> long\n"
                                                                  "return current capacity of queue" },

  {"remain",    (PyCFunction)StringQueue_remain,    METH_NOARGS,  "remain() -> long\n"
                                                                  "return the amount of free space in current queue" },

  {"index",     (PyCFunction)StringQueue_index,     METH_VARARGS, "index( str ) -> long\n"
                                                                  "return index of first occurrence of str" },

  {"count",     (PyCFunction)StringQueue_count,     METH_VARARGS, "count( str ) -> long\n"
                                                                  "return the number of non-overlapping occurrence of str" },

  {"peek",      (PyCFunction)StringQueue_peek,      METH_VARARGS, "peek( index [,n] ) -> long\n"
                                                                  "return n bytes starting at index without consuming, default to end of data" },

  {"discard",   (PyCFunction)StringQueue_discard,   METH_VARARGS, "discard( [n] ) -> None\n"
                                                                  "skip n bytes ahead, entire queue by default" },

  {"optimize",  (PyCFunction)StringQueue_optimize,  METH_VARARGS, "optimize() -> None\n"
                                                                  "resize the queue to fit data" },
  {NULL}  /* Sentinel */
};
RESUME_WARNINGS


static PyTypeObject PyVGX_StringQueue__StringQueueType = {
    PyVarObject_HEAD_INIT(NULL,0)
    .tp_name            = "pyvgx.StringQueue",
    .tp_basicsize       = sizeof(PyVGX_StringQueue),
    .tp_itemsize        = 0,
    .tp_dealloc         = (destructor)StringQueue_dealloc,
    .tp_vectorcall_offset = 0,
    .tp_getattr         = 0,
    .tp_setattr         = 0,
    .tp_as_async        = 0,
    .tp_repr            = 0,
    .tp_as_number       = 0,
    .tp_as_sequence     = &StringQueue_as_sequence,
    .tp_as_mapping      = 0,
    .tp_hash            = 0,
    .tp_call            = 0,
    .tp_str             = 0,
    .tp_getattro        = 0,
    .tp_setattro        = 0,
    .tp_as_buffer       = 0,
    .tp_flags           = Py_TPFLAGS_DEFAULT,
    .tp_doc             = "StringQueue objects",
    .tp_traverse        = 0,
    .tp_clear           = 0,
    .tp_richcompare     = 0,
    .tp_weaklistoffset  = 0,
    .tp_iter            = 0,
    .tp_iternext        = 0,
    .tp_methods         = StringQueue_methods,
    .tp_members         = StringQueue_members,
    .tp_getset          = 0,
    .tp_base            = 0,
    .tp_dict            = 0,
    .tp_descr_get       = 0,
    .tp_descr_set       = 0,
    .tp_dictoffset      = 0,
    .tp_init            = (initproc)StringQueue_init,
    .tp_alloc           = 0,
    .tp_new             = StringQueue_new,
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





DLL_HIDDEN PyTypeObject * p_PyVGX_StringQueue__StringQueueType = &PyVGX_StringQueue__StringQueueType;
