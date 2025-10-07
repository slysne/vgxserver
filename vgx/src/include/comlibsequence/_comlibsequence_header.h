/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    _comlibsequence_header.h
 * Author:  Stian Lysne slysne.dev@gmail.com
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

#ifndef _COMLIBSEQUENCE_HEADER_H_INCLUDED
#define _COMLIBSEQUENCE_HEADER_H_INCLUDED


#include "_comlib.h"





/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIBSEQUENCE );

#define COMLIB_SEQUENCE_TYPENAME(QueueTypeName) QueueTypeName##_t
#define X_COMLIB_SEQUENCE_TYPENAME(QueueTypeName) COMLIB_SEQUENCE_TYPENAME(QueueTypeName)

#define COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE(QueueTypeName) QueueTypeName##_constructor_args_t
#define X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE(QueueTypeName) COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE(QueueTypeName)

#define COMLIB_SEQUENCE_VTABLETYPE(QueueTypeName) QueueTypeName##_vtable_t
#define X_COMLIB_SEQUENCE_VTABLETYPE(QueueTypeName) COMLIB_SEQUENCE_VTABLETYPE(QueueTypeName)

#define COMLIB_SEQUENCE_GLOBAL_VTABLE(QueueTypeName) g_##QueueTypeName##_vtable
#define X_COMLIB_SEQUENCE_GLOBAL_VTABLE(QueueTypeName) COMLIB_SEQUENCE_GLOBAL_VTABLE(QueueTypeName)

#define COMLIB_SEQUENCE_ELEMENT_TYPE(QueueElementType) QueueElementType
#define X_COMLIB_SEQUENCE_ELEMENT_TYPE(QueueElementType) COMLIB_SEQUENCE_ELEMENT_TYPE(QueueElementType)

#define COMLIB_SEQUENCE_REGISTER_CLASS(QueueTypeName) QueueTypeName##_RegisterClass
#define X_COMLIB_SEQUENCE_REGISTER_CLASS(QueueTypeName) COMLIB_SEQUENCE_REGISTER_CLASS(QueueTypeName)

#define COMLIB_SEQUENCE_UNREGISTER_CLASS(QueueTypeName) QueueTypeName##_UnregisterClass
#define X_COMLIB_SEQUENCE_UNREGISTER_CLASS(QueueTypeName) COMLIB_SEQUENCE_UNREGISTER_CLASS(QueueTypeName)


#define COMLIBSEQUENCE_EXPAND_EXP           1   // double size when expanding
#define COMLIBSEQUENCE_REDUCE_EXP           3   // shrink by 8 times when reducing
#define COMLIBSEQUENCE_MAX_ERRSTRING_SIZE   100
#define COMLIBSEQUENCE_FD_NONE              -1  // no file descriptor


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#if defined( _CSEQ_IMPLEMENTATION_BASE )

#define _CSEQ_LINEAR_BUFFER

/* Supported Methods */
#define _CSEQ_FEATURE_SETCAPACITY_METHOD
#define _CSEQ_FEATURE_CAPACITY_METHOD
#define _CSEQ_FEATURE_LENGTH_METHOD
#define _CSEQ_FEATURE_REMAIN_METHOD

#endif


/*******************************************************************//**
 *
 * QUEUE
 *
 ***********************************************************************
 */
#if defined( _CSEQ_IMPLEMENTATION_GENERIC_QUEUE )

#define _CSEQ_CIRCULAR_BUFFER

#define _CSEQ_FEATURE_STREAM_IO
#define _CSEQ_FEATURE_ERRORS
#define _CSEQ_FEATURE_SYNCHRONIZED_API
#define _CSEQ_FEATURE_SORTABLE

/* Supported Methods */
#define _CSEQ_FEATURE_SETCAPACITY_METHOD

#define _CSEQ_FEATURE_ATTACHINPUTDESCRIPTOR_METHOD
#define _CSEQ_FEATURE_ATTACHOUTPUTDESCRIPTOR_METHOD
#define _CSEQ_FEATURE_ATTACHINPUTSTREAM_METHOD
#define _CSEQ_FEATURE_ATTACHOUTPUTSTREAM_METHOD
#define _CSEQ_FEATURE_DETACHINPUT_METHOD
#define _CSEQ_FEATURE_DETACHOUTPUT_METHOD
#define _CSEQ_FEATURE_DETACHOUTPUTNOFLUSH_METHOD
#define _CSEQ_FEATURE_HASINPUTDESCRIPTOR_METHOD
#define _CSEQ_FEATURE_HASOUTPUTDESCRIPTOR_METHOD

#define _CSEQ_FEATURE_CAPACITY_METHOD
#define _CSEQ_FEATURE_LENGTH_METHOD
#define _CSEQ_FEATURE_REMAIN_METHOD

#define _CSEQ_FEATURE_INDEX_METHOD
#define _CSEQ_FEATURE_OCC_METHOD
#define _CSEQ_FEATURE_INITIALIZE_METHOD
#define _CSEQ_FEATURE_WRITE_METHOD
#define _CSEQ_FEATURE_APPEND_METHOD
#define _CSEQ_FEATURE_SET_METHOD
#define _CSEQ_FEATURE_NULTERM_METHOD
#define _CSEQ_FEATURE_FORMAT_METHOD
#define _CSEQ_FEATURE_NEXT_METHOD
#define _CSEQ_FEATURE_READ_METHOD
#define _CSEQ_FEATURE_GET_METHOD
#define _CSEQ_FEATURE_READUNTIL_METHOD
#define _CSEQ_FEATURE_READLINE_METHOD
#define _CSEQ_FEATURE_PEEK_METHOD
#define _CSEQ_FEATURE_EXPECT_METHOD
#define _CSEQ_FEATURE_UNREAD_METHOD
#define _CSEQ_FEATURE_TRUNCATE_METHOD
#define _CSEQ_FEATURE_CLEAR_METHOD
#define _CSEQ_FEATURE_RESET_METHOD
#define _CSEQ_FEATURE_GETVALUE_METHOD
#define _CSEQ_FEATURE_ABSORB_METHOD
#define _CSEQ_FEATURE_ABSORBUNTIL_METHOD
#define _CSEQ_FEATURE_HEAPIFY_METHOD
#define _CSEQ_FEATURE_HEAPTOP_METHOD
#define _CSEQ_FEATURE_HEAPPUSH_METHOD
#define _CSEQ_FEATURE_HEAPPOP_METHOD
#define _CSEQ_FEATURE_HEAPREPLACE_METHOD
#define _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD
#define _CSEQ_FEATURE_CLONEINTO_METHOD
#define _CSEQ_FEATURE_TRANSPLANTFROM_METHOD
#define _CSEQ_FEATURE_DISCARD_METHOD
#define _CSEQ_FEATURE_UNWRITE_METHOD
#define _CSEQ_FEATURE_POP_METHOD
#define _CSEQ_FEATURE_DUMP_METHOD
#define _CSEQ_FEATURE_FLUSH_METHOD
#define _CSEQ_FEATURE_SORT_METHOD
#define _CSEQ_FEATURE_REVERSE_METHOD
#define _CSEQ_FEATURE_OPTIMIZE_METHOD
#define _CSEQ_FEATURE_GETERROR_METHOD

#endif



/*******************************************************************//**
 *
 * BUFFER
 *
 ***********************************************************************
 */
#if defined( _CSEQ_IMPLEMENTATION_GENERIC_BUFFER )

#define _CSEQ_CIRCULAR_BUFFER

/* Supported Methods */
#define _CSEQ_FEATURE_SETCAPACITY_METHOD

#define _CSEQ_FEATURE_CAPACITY_METHOD
#define _CSEQ_FEATURE_LENGTH_METHOD
#define _CSEQ_FEATURE_REMAIN_METHOD

#define _CSEQ_FEATURE_INDEX_METHOD
#define _CSEQ_FEATURE_OCC_METHOD
#define _CSEQ_FEATURE_INITIALIZE_METHOD
#define _CSEQ_FEATURE_WRITE_METHOD
#define _CSEQ_FEATURE_APPEND_METHOD
#define _CSEQ_FEATURE_SET_METHOD
#define _CSEQ_FEATURE_NULTERM_METHOD
#define _CSEQ_FEATURE_FORMAT_METHOD
#define _CSEQ_FEATURE_NEXT_METHOD
#define _CSEQ_FEATURE_READ_METHOD
#define _CSEQ_FEATURE_GET_METHOD
#define _CSEQ_FEATURE_READUNTIL_METHOD
#define _CSEQ_FEATURE_READLINE_METHOD
#define _CSEQ_FEATURE_PEEK_METHOD
#define _CSEQ_FEATURE_EXPECT_METHOD
#define _CSEQ_FEATURE_TRUNCATE_METHOD
#define _CSEQ_FEATURE_CLEAR_METHOD
#define _CSEQ_FEATURE_RESET_METHOD
#define _CSEQ_FEATURE_GETVALUE_METHOD
#define _CSEQ_FEATURE_ABSORB_METHOD
#define _CSEQ_FEATURE_ABSORBUNTIL_METHOD
#define _CSEQ_FEATURE_CLONEINTO_METHOD
#define _CSEQ_FEATURE_TRANSPLANTFROM_METHOD
#define _CSEQ_FEATURE_DISCARD_METHOD
#define _CSEQ_FEATURE_UNWRITE_METHOD
#define _CSEQ_FEATURE_POP_METHOD
#define _CSEQ_FEATURE_REVERSE_METHOD
#define _CSEQ_FEATURE_OPTIMIZE_METHOD

#endif



/*******************************************************************//**
 *
 * HEAP
 *
 ***********************************************************************
 */
#if defined( _CSEQ_IMPLEMENTATION_HEAP )

#define _CSEQ_LINEAR_BUFFER
#define _CSEQ_FEATURE_SORTABLE

/* Supported Methods */
#define _CSEQ_FEATURE_SETCAPACITY_METHOD
#define _CSEQ_FEATURE_CAPACITY_METHOD
#define _CSEQ_FEATURE_LENGTH_METHOD
#define _CSEQ_FEATURE_REMAIN_METHOD
#define _CSEQ_FEATURE_INITIALIZE_METHOD
#define _CSEQ_FEATURE_WRITE_METHOD
#define _CSEQ_FEATURE_APPEND_METHOD
#define _CSEQ_FEATURE_CLEAR_METHOD
#define _CSEQ_FEATURE_RESET_METHOD
#define _CSEQ_FEATURE_HEAPIFY_METHOD
#define _CSEQ_FEATURE_HEAPTOP_METHOD
#define _CSEQ_FEATURE_HEAPPUSH_METHOD
#define _CSEQ_FEATURE_HEAPPOP_METHOD
#define _CSEQ_FEATURE_HEAPREPLACE_METHOD
#define _CSEQ_FEATURE_HEAPPUSHTOPK_METHOD
#define _CSEQ_FEATURE_CLONEINTO_METHOD
#define _CSEQ_FEATURE_TRANSPLANTFROM_METHOD

#endif


/*******************************************************************//**
 *
 * LIST
 *
 ***********************************************************************
 */
#if defined( _CSEQ_IMPLEMENTATION_LIST )

#define _CSEQ_LINEAR_BUFFER
#define _CSEQ_FEATURE_SORTABLE

/* Supported Methods */
#define _CSEQ_FEATURE_SETCAPACITY_METHOD
#define _CSEQ_FEATURE_CAPACITY_METHOD
#define _CSEQ_FEATURE_LENGTH_METHOD
#define _CSEQ_FEATURE_REMAIN_METHOD
#define _CSEQ_FEATURE_INITIALIZE_METHOD
#define _CSEQ_FEATURE_DEADSPACE_METHOD
#define _CSEQ_FEATURE_WRITE_METHOD
#define _CSEQ_FEATURE_APPEND_METHOD
#define _CSEQ_FEATURE_SET_METHOD
#define _CSEQ_FEATURE_CURSOR_METHOD
#define _CSEQ_FEATURE_GET_METHOD
#define _CSEQ_FEATURE_TRUNCATE_METHOD
#define _CSEQ_FEATURE_CLEAR_METHOD
#define _CSEQ_FEATURE_RESET_METHOD
#define _CSEQ_FEATURE_ABSORB_METHOD
#define _CSEQ_FEATURE_ABSORBUNTIL_METHOD
#define _CSEQ_FEATURE_CLONEINTO_METHOD
#define _CSEQ_FEATURE_TRANSPLANTFROM_METHOD
#define _CSEQ_FEATURE_YANKBUFFER_METHOD
#define _CSEQ_FEATURE_DISCARD_METHOD
#define _CSEQ_FEATURE_SORT_METHOD
#define _CSEQ_FEATURE_REVERSE_METHOD
#define _CSEQ_FEATURE_OPTIMIZE_METHOD

#endif


typedef int (*f_compare_t)( const void *, const void *);

#endif
