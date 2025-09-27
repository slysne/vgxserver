/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxaptr.c
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

#include "cxaptr.h"


SET_EXCEPTION_MODULE( CXLIB_MSG_MOD_APTR );



/* 16-bit double */
double REINTERPRET_CAST_DOUBLE_16( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d16 = qw;
  return bits.d;
}

/* 20-bit double */
double REINTERPRET_CAST_DOUBLE_20( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d20 = qw;
  return bits.d;
}

/* 24-bit double */
double REINTERPRET_CAST_DOUBLE_24( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d24 = qw;
  return bits.d;
}

/* 28-bit double */
double REINTERPRET_CAST_DOUBLE_28( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d28 = qw;
  return bits.d;
}

/* 32-bit double */
double REINTERPRET_CAST_DOUBLE_32( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d32 = qw;
  return bits.d;
}

/* 36-bit double */
double REINTERPRET_CAST_DOUBLE_36( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d36 = qw;
  return bits.d;
}

/* 40-bit double */
double REINTERPRET_CAST_DOUBLE_40( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d40 = qw;
  return bits.d;
}

/* 44-bit double */
double REINTERPRET_CAST_DOUBLE_44( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d44 = qw;
  return bits.d;
}

/* 48-bit double */
double REINTERPRET_CAST_DOUBLE_48( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d48 = qw;
  return bits.d;
}

/* 52-bit double */
double REINTERPRET_CAST_DOUBLE_52( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d52 = qw;
  return bits.d;
}

/* 56-bit double */
double REINTERPRET_CAST_DOUBLE_56( QWORD qw ) {
  double_bits_t bits = {0};
  bits.d56 = qw;
  return bits.d;
}

/* 64-bit double */
double REINTERPRET_CAST_DOUBLE( QWORD qw ) {
  return ((double_bits_t*)&qw)->d;
}



/* 16-bit float */
float REINTERPRET_CAST_FLOAT_16( DWORD dw ) {
  float_bits_t bits = {0};
  bits.f16 = dw;
  return bits.f;
}

/* 20-bit float */
float REINTERPRET_CAST_FLOAT_20( DWORD dw ) {
  float_bits_t bits = {0};
  bits.f20 = dw;
  return bits.f;
}

/* 24-bit float */
float REINTERPRET_CAST_FLOAT_24( DWORD dw ) {
  float_bits_t bits = {0};
  bits.f24 = dw;
  return bits.f;
}

/* 28-bit float */
float REINTERPRET_CAST_FLOAT_28( DWORD dw ) {
  float_bits_t bits = {0};
  bits.f28 = dw;
  return bits.f;
}

/* 32-bit float */
float REINTERPRET_CAST_FLOAT( DWORD dw ) {
  return ((float_bits_t*)&dw)->f;
}





/*******************************************************************//**
 *
 ***********************************************************************
 */

/* tagged pointer initialization */
void _TPTR_INIT( tptr_t *TP ) {
  TP->qword = 0;
}


/* annotated  pointer initialization */
void _APTR_INIT( aptr_t *AP ) {
  AP->annotation = 0;
  _TPTR_INIT( &AP->tptr );
}

/* tagged pointer getters */
void * _TPTR_GET_POINTER( tptr_t *TP ) {
  return (void*)__TPTR_UNPACK( TP->ncptr.qwo );   /* remove tag, sign extend, and cast data to void pointer */
}


/**************************************************************************//**
 * _TPTR_GET_IDHIGH
 *
 ******************************************************************************
 */
uint64_t _TPTR_GET_IDHIGH( tptr_t *TP ) {
  return TP->data.uval56;
}


/**************************************************************************//**
 * _TPTR_GET_BOOLEAN
 *
 ******************************************************************************
 */
bool _TPTR_GET_BOOLEAN( tptr_t *TP ) {
  return (bool)TP->data.uval56 != 0;
}


/**************************************************************************//**
 * _TPTR_GET_REAL
 *
 ******************************************************************************
 */
double _TPTR_GET_REAL( tptr_t *TP ) {
  return REINTERPRET_CAST_DOUBLE_56( TP->data.uval56 );
}


/**************************************************************************//**
 * _TPTR_GET_PTR56
 *
 ******************************************************************************
 */
void * _TPTR_GET_PTR56( tptr_t *TP ) {
  return (void*)((intptr_t)TP->data.ival56);
}


/**************************************************************************//**
 * _TPTR_GET_OBJ56
 *
 ******************************************************************************
 */
void * _TPTR_GET_OBJ56( tptr_t *TP ) {
  return (void*)((intptr_t)TP->data.ival56);
}

/* tagged pointer setters */
void _TPTR_SET_POINTER( tptr_t *TP, const void *p ) {
  TP->ncptr.qwo = __TPTR_PACK( p );
  TP->tag.bit.nonptr = 0;
}


/**************************************************************************//**
 * _TPTR_SET_POINTER_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_POINTER_AND_TAG( tptr_t *TP, const void *p, int tag ) {
  TP->ncptr.qwo = __TPTR_PACK( p );
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_IDHIGH
 *
 ******************************************************************************
 */
void _TPTR_SET_IDHIGH( tptr_t *TP, QWORD id ) {
  TP->data.uval56 = id;    /* truncation implied */
  TP->data.type = TAGGED_DTYPE_ID56;
  TP->tag.bit.nonptr = 1;
}


/**************************************************************************//**
 * _TPTR_SET_IDHIGH_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_IDHIGH_AND_TAG( tptr_t *TP, QWORD id, int tag ) {
  TP->data.uval56 = id;    /* truncation implied */
  TP->data.type = TAGGED_DTYPE_ID56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_BOOLEAN
 *
 ******************************************************************************
 */
void _TPTR_SET_BOOLEAN( tptr_t *TP, bool b ) {
  TP->data.uval56 = (QWORD)(b != 0 ? true : false);
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_BOOL;
}


/**************************************************************************//**
 * _TPTR_SET_BOOLEAN_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_BOOLEAN_AND_TAG( tptr_t *TP, bool b, int tag ) {
  TP->data.uval56 = (QWORD)(b != 0 ? true : false);
  TP->data.type = TAGGED_DTYPE_BOOL;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_UNSIGNED
 *
 ******************************************************************************
 */
void _TPTR_SET_UNSIGNED( tptr_t *TP, uint64_t u ) {
  TP->data.uval56 = u;     /* truncation implied */
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_UINT56;
}


/**************************************************************************//**
 * _TPTR_SET_UNSIGNED_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_UNSIGNED_AND_TAG( tptr_t *TP, uint64_t u, int tag ) {
  TP->data.uval56 = u;     /* truncation implied */
  TP->data.type = TAGGED_DTYPE_UINT56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_INTEGER
 *
 ******************************************************************************
 */
void _TPTR_SET_INTEGER( tptr_t *TP, int64_t i ) {
  TP->data.ival56 = i;     /* truncation implied */
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_INT56;
}


/**************************************************************************//**
 * _TPTR_SET_INTEGER_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_INTEGER_AND_TAG( tptr_t *TP, int64_t i, int tag ) {
  TP->data.ival56 = i;     /* truncation implied */
  TP->data.type = TAGGED_DTYPE_INT56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_REAL
 *
 ******************************************************************************
 */
void _TPTR_SET_REAL( tptr_t *TP, double r ) {
  TP->data.uval56 = ((double_bits_t*)&r)->d56;
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_REAL56;
}


/**************************************************************************//**
 * _TPTR_SET_REAL_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_REAL_AND_TAG( tptr_t *TP, double r, int tag ) {
  TP->data.uval56 = ((double_bits_t*)&r)->d56;
  TP->data.type = TAGGED_DTYPE_REAL56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_PTR56
 *
 ******************************************************************************
 */
void _TPTR_SET_PTR56( tptr_t *TP, const void *p ) {
  TP->data.ival56 = (int64_t)p;
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_PTR56;
}


/**************************************************************************//**
 * _TPTR_SET_PTR56_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_PTR56_AND_TAG( tptr_t *TP, const void *p, int tag ) {
  TP->data.ival56 = (int64_t)p;
  TP->data.type = TAGGED_DTYPE_PTR56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_OBJ56
 *
 ******************************************************************************
 */
void _TPTR_SET_OBJ56( tptr_t *TP, const void *op ) {
  TP->data.ival56 = (int64_t)op;
  TP->tag.bit.nonptr = 1;
  TP->data.type = TAGGED_DTYPE_OBJ56;
}


/**************************************************************************//**
 * _TPTR_SET_OBJ56_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_OBJ56_AND_TAG( tptr_t *TP, const void *op, int tag ) {
  TP->data.ival56 = (int64_t)op;
  TP->data.type = TAGGED_DTYPE_OBJ56;
  TP->tag.value = tag;
}


/**************************************************************************//**
 * _TPTR_SET_HANDLE
 *
 ******************************************************************************
 */
void _TPTR_SET_HANDLE( tptr_t *TP, const uint64_t handle_value ) {
  TP->handle.value = handle_value;
  TP->data.type = TAGGED_DTYPE_HANDLE;
  TP->tag.bit.nonptr = 1;
}


/**************************************************************************//**
 * _TPTR_SET_HANDLE_AND_TAG
 *
 ******************************************************************************
 */
void _TPTR_SET_HANDLE_AND_TAG( tptr_t *TP, const uint64_t handle_value, int tag ) {
  TP->handle.value = handle_value;
  TP->data.type = TAGGED_DTYPE_HANDLE;
  TP->tag.value = tag;
}


/* annotated pointer getters */
void * _APTR_GET_ANNOTATION_POINTER( aptr_t *AP ) {
  return (void*)&AP->annotation;
}


/**************************************************************************//**
 * _APTR_GET_POINTER
 *
 ******************************************************************************
 */
void * _APTR_GET_POINTER( aptr_t *AP ) {
  return _TPTR_GET_POINTER( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_GET_IDHIGH
 *
 ******************************************************************************
 */
uint64_t _APTR_GET_IDHIGH( aptr_t *AP ) {
  return _TPTR_GET_IDHIGH( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_GET_BOOLEAN
 *
 ******************************************************************************
 */
bool _APTR_GET_BOOLEAN( aptr_t *AP ) {
  return _TPTR_GET_BOOLEAN( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_GET_REAL
 *
 ******************************************************************************
 */
double _APTR_GET_REAL( aptr_t *AP ) {
  return _TPTR_GET_REAL( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_GET_PTR56
 *
 ******************************************************************************
 */
void * _APTR_GET_PTR56( aptr_t *AP ) {
  return _TPTR_GET_PTR56( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_GET_OBJ56
 *
 ******************************************************************************
 */
void * _APTR_GET_OBJ56( aptr_t *AP ) {
  return _TPTR_GET_OBJ56( &AP->tptr );
}


/* annotated pointer setters */
void _APTR_SET_POINTER( aptr_t *AP, const void *p ) {
  _TPTR_SET_POINTER( &AP->tptr, p );
}


/**************************************************************************//**
 * _APTR_SET_POINTER_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_POINTER_AND_TAG( aptr_t *AP, const void *p, int tag ) {
  _TPTR_SET_POINTER_AND_TAG( &AP->tptr, p, tag );
}


/**************************************************************************//**
 * _APTR_SET_IDHIGH
 *
 ******************************************************************************
 */
void _APTR_SET_IDHIGH( aptr_t *AP, QWORD id ) {
  _TPTR_SET_IDHIGH( &AP->tptr, id );
}


/**************************************************************************//**
 * _APTR_SET_IDHIGH_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_IDHIGH_AND_TAG( aptr_t *AP, QWORD id, int tag ) {
  _TPTR_SET_IDHIGH_AND_TAG( &AP->tptr, id, tag );
}


/**************************************************************************//**
 * _APTR_SET_BOOLEAN
 *
 ******************************************************************************
 */
void _APTR_SET_BOOLEAN( aptr_t *AP, bool b ) {
  _TPTR_SET_BOOLEAN( &AP->tptr, b );
}


/**************************************************************************//**
 * _APTR_SET_BOOLEAN_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_BOOLEAN_AND_TAG( aptr_t *AP, bool b, int tag ) {
  _TPTR_SET_BOOLEAN_AND_TAG( &AP->tptr, b, tag );
}


/**************************************************************************//**
 * _APTR_SET_UNSIGNED
 *
 ******************************************************************************
 */
void _APTR_SET_UNSIGNED( aptr_t *AP, uint64_t u ) {
  _TPTR_SET_UNSIGNED( &AP->tptr, u );
}


/**************************************************************************//**
 * _APTR_SET_UNSIGNED_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_UNSIGNED_AND_TAG( aptr_t *AP, uint64_t u, int tag ) {
  _TPTR_SET_UNSIGNED_AND_TAG( &AP->tptr, u, tag );
}


/**************************************************************************//**
 * _APTR_SET_INTEGER
 *
 ******************************************************************************
 */
void _APTR_SET_INTEGER( aptr_t *AP, int64_t i ) {
  _TPTR_SET_INTEGER( &AP->tptr, i );
}


/**************************************************************************//**
 * _APTR_SET_INTEGER_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_INTEGER_AND_TAG( aptr_t *AP, int64_t i, int tag ) {
  _TPTR_SET_INTEGER_AND_TAG( &AP->tptr, i, tag );
}


/**************************************************************************//**
 * _APTR_SET_REAL
 *
 ******************************************************************************
 */
void _APTR_SET_REAL( aptr_t *AP, double r ) {
  _TPTR_SET_REAL( &AP->tptr, r );
}


/**************************************************************************//**
 * _APTR_SET_REAL_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_REAL_AND_TAG( aptr_t *AP, double r, int tag ) {
  _TPTR_SET_REAL_AND_TAG( &AP->tptr, r, tag );
}


/**************************************************************************//**
 * _APTR_SET_PTR56
 *
 ******************************************************************************
 */
void _APTR_SET_PTR56( aptr_t *AP, const void *p ) {
  _TPTR_SET_PTR56( &AP->tptr, p );
}


/**************************************************************************//**
 * _APTR_SET_PTR56_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_PTR56_AND_TAG( aptr_t *AP, const void *p, int tag ) {
  _TPTR_SET_PTR56_AND_TAG( &AP->tptr, p, tag );
}


/**************************************************************************//**
 * _APTR_SET_OBJ56
 *
 ******************************************************************************
 */
void _APTR_SET_OBJ56( aptr_t *AP, const void *op ) {
  _TPTR_SET_OBJ56( &AP->tptr, op );
}


/**************************************************************************//**
 * _APTR_SET_OBJ56_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_OBJ56_AND_TAG( aptr_t *AP, const void *op, int tag ) {
  _TPTR_SET_OBJ56_AND_TAG( &AP->tptr, op, tag );
}


/**************************************************************************//**
 * _APTR_SET_HANDLE
 *
 ******************************************************************************
 */
void _APTR_SET_HANDLE( aptr_t *AP, const uint64_t handle_value ) {
  _TPTR_SET_HANDLE( &AP->tptr, handle_value );
}


/**************************************************************************//**
 * _APTR_SET_HANDLE_AND_TAG
 *
 ******************************************************************************
 */
void _APTR_SET_HANDLE_AND_TAG( aptr_t *AP, const uint64_t handle_value, int tag ) {
  _TPTR_SET_HANDLE_AND_TAG( &AP->tptr, handle_value, tag );
}
 

/* tagged pointer matchers */
bool _TPTR_MATCH_POINTER( const tptr_t *TP, const void *p ) {
  return __TPTR_UNPACK( TP->ncptr.qwo ) == (intptr_t)p;
}


/**************************************************************************//**
 * _TPTR_MATCH_IDHIGH
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_IDHIGH( const tptr_t *TP, const QWORD id ) {
  return TP->data.uval56 == (id & 0x00FFFFFFFFFFFFFFULL);
}


/**************************************************************************//**
 * _TPTR_MATCH_BOOLEAN
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_BOOLEAN( const tptr_t *TP, const bool b ) {
  return (TP->data.uval56 != 0) == (b != 0);
}


/**************************************************************************//**
 * _TPTR_MATCH_UNSIGNED
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_UNSIGNED( const tptr_t *TP, const uint64_t u ) {
  return TP->data.uval56 == (u & 0x00FFFFFFFFFFFFFFULL);
}


/**************************************************************************//**
 * _TPTR_MATCH_INTEGER
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_INTEGER( const tptr_t *TP, const int64_t i ) {
  return TP->data.ival56 == i;
}


/**************************************************************************//**
 * _TPTR_MATCH_REAL
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_REAL( const tptr_t *TP, const double r ) {
  return TP->data.uval56 == ((double_bits_t*)&r)->d56; /* effective epsilon: 2^-44 */
}


/**************************************************************************//**
 * _TPTR_MATCH_PTR56
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_PTR56( const tptr_t *TP, const void *p ) {
  return TP->data.ival56 == (int64_t)p;
}


/**************************************************************************//**
 * _TPTR_MATCH_OBJ56
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_OBJ56( const tptr_t *TP, const void *op ) {
  return TP->data.ival56 == (int64_t)op;
}


/**************************************************************************//**
 * _TPTR_MATCH_HANDLE
 *
 ******************************************************************************
 */
bool _TPTR_MATCH_HANDLE( const tptr_t *TP, const uint64_t handle_value ) {
  return TP->handle.value == handle_value;
}


/**************************************************************************//**
 * _TPTR_MATCH
 *
 ******************************************************************************
 */
bool _TPTR_MATCH( const tptr_t *TP1, const tptr_t *TP2 ) {
  return TP1->qword == TP2->qword;
}

/* annotated pointer matchers */
bool _APTR_MATCH_ANNOTATION( const aptr_t *AP, const QWORD a ) {
  return AP->annotation == a;
}


/**************************************************************************//**
 * _APTR_MATCH_POINTER
 *
 ******************************************************************************
 */
bool _APTR_MATCH_POINTER( const aptr_t *AP, const void *p ) {
  return _TPTR_MATCH_POINTER( &AP->tptr, p );
}


/**************************************************************************//**
 * _APTR_MATCH_IDHIGH
 *
 ******************************************************************************
 */
bool _APTR_MATCH_IDHIGH( const aptr_t *AP, const QWORD id ) {
  return _TPTR_MATCH_IDHIGH( &AP->tptr, id );
}


/**************************************************************************//**
 * _APTR_MATCH_BOOLEAN
 *
 ******************************************************************************
 */
bool _APTR_MATCH_BOOLEAN( const aptr_t *AP, const bool b ) {
  return _TPTR_MATCH_BOOLEAN( &AP->tptr, b );
}


/**************************************************************************//**
 * _APTR_MATCH_UNSIGNED
 *
 ******************************************************************************
 */
bool _APTR_MATCH_UNSIGNED( const aptr_t *AP, const uint64_t u ) {
  return _TPTR_MATCH_UNSIGNED( &AP->tptr, u );
}


/**************************************************************************//**
 * _APTR_MATCH_INTEGER
 *
 ******************************************************************************
 */
bool _APTR_MATCH_INTEGER( const aptr_t *AP, const int64_t i ) {
  return _TPTR_MATCH_INTEGER( &AP->tptr, i );
}


/**************************************************************************//**
 * _APTR_MATCH_REAL
 *
 ******************************************************************************
 */
bool _APTR_MATCH_REAL( const aptr_t *AP, const double r ) {
  return _TPTR_MATCH_REAL( &AP->tptr, r );
}


/**************************************************************************//**
 * _APTR_MATCH_PTR56
 *
 ******************************************************************************
 */
bool _APTR_MATCH_PTR56( const aptr_t *AP, const void *p ) {
  return _TPTR_MATCH_PTR56( &AP->tptr, p );
}


/**************************************************************************//**
 * _APTR_MATCH_OBJ56
 *
 ******************************************************************************
 */
bool _APTR_MATCH_OBJ56( const aptr_t *AP, const void *op ) {
  return _TPTR_MATCH_OBJ56( &AP->tptr, op );
}


/**************************************************************************//**
 * _APTR_MATCH_HANDLE
 *
 ******************************************************************************
 */
bool _APTR_MATCH_HANDLE( const aptr_t *AP, const uint64_t handle_value ) {
  return _TPTR_MATCH_HANDLE( &AP->tptr, handle_value );
}


/**************************************************************************//**
 * _APTR_MATCH
 *
 ******************************************************************************
 */
bool _APTR_MATCH( const aptr_t *AP1, const aptr_t *AP2 ) {
  return AP1->annotation == AP2->annotation && TPTR_MATCH( &AP1->tptr, &AP2->tptr );
}

/* tagged pointer tests */
bool _TPTR_IS_NULL( const tptr_t *TP ) {
  return TP->ncptr.qwo == 0 && TP->tag.bit.nonptr == 0;
}


/**************************************************************************//**
 * _TPTR_IS_VALID
 *
 ******************************************************************************
 */
bool _TPTR_IS_VALID( const tptr_t *TP ) {
  return TP->tag.bit.valid == 1;
}


/**************************************************************************//**
 * _TPTR_IS_INVALID
 *
 ******************************************************************************
 */
bool _TPTR_IS_INVALID( const tptr_t *TP ) {
  return TP->tag.bit.valid == 0;
}


/**************************************************************************//**
 * _TPTR_IS_DIRTY
 *
 ******************************************************************************
 */
bool _TPTR_IS_DIRTY( const tptr_t *TP ) {
  return TP->tag.bit.dirty == 1;
}


/**************************************************************************//**
 * _TPTR_IS_CLEAN
 *
 ******************************************************************************
 */
bool _TPTR_IS_CLEAN( const tptr_t *TP ) {
  return TP->tag.bit.dirty == 0;
}


/**************************************************************************//**
 * _TPTR_IS_NONPTR
 *
 ******************************************************************************
 */
bool _TPTR_IS_NONPTR( const tptr_t *TP ) {
  return TP->tag.bit.nonptr == 1;
}


/**************************************************************************//**
 * _TPTR_IS_POINTER
 *
 ******************************************************************************
 */
bool _TPTR_IS_POINTER( const tptr_t *TP ) {
  return TP->tag.bit.nonptr == 0;
}


/* annotated pointer tests */
bool _APTR_IS_NULL( const aptr_t *AP ) {
  return _TPTR_IS_NULL( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_VALID
 *
 ******************************************************************************
 */
bool _APTR_IS_VALID( const aptr_t *AP ) {
  return _TPTR_IS_VALID( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_INVALID
 *
 ******************************************************************************
 */
bool _APTR_IS_INVALID( const aptr_t *AP ) {
  return _TPTR_IS_INVALID( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_DIRTY
 *
 ******************************************************************************
 */
bool _APTR_IS_DIRTY( const aptr_t *AP ) {
  return _TPTR_IS_DIRTY( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_CLEAN
 *
 ******************************************************************************
 */
bool _APTR_IS_CLEAN( const aptr_t *AP ) {
  return _TPTR_IS_CLEAN( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_NONPTR
 *
 ******************************************************************************
 */
bool _APTR_IS_NONPTR( const aptr_t *AP ) {
  return _TPTR_IS_NONPTR( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_IS_POINTER
 *
 ******************************************************************************
 */
bool _APTR_IS_POINTER( const aptr_t *AP ) {
  return _TPTR_IS_POINTER( &AP->tptr );
}


/* tagged pointer tag setters */
void _TPTR_MAKE_VALID( tptr_t *TP ) {
  TP->tag.bit.valid = 1;
}


/**************************************************************************//**
 * _TPTR_MAKE_INVALID
 *
 ******************************************************************************
 */
void _TPTR_MAKE_INVALID( tptr_t *TP ) {
  TP->tag.bit.valid = 0;
}


/**************************************************************************//**
 * _TPTR_MAKE_DIRTY
 *
 ******************************************************************************
 */
void _TPTR_MAKE_DIRTY( tptr_t *TP ) {
  TP->tag.bit.dirty = 1;
}


/**************************************************************************//**
 * _TPTR_MAKE_CLEAN
 *
 ******************************************************************************
 */
void _TPTR_MAKE_CLEAN( tptr_t *TP ) {
  TP->tag.bit.dirty = 0;
}


/**************************************************************************//**
 * _TPTR_MAKE_NONPTR
 *
 ******************************************************************************
 */
void _TPTR_MAKE_NONPTR( tptr_t *TP ) {
  TP->tag.bit.nonptr = 1;
}


/**************************************************************************//**
 * _TPTR_MAKE_POINTER
 *
 ******************************************************************************
 */
void _TPTR_MAKE_POINTER( tptr_t *TP ) {
  TP->tag.bit.nonptr = 0;
}


/* annotated pointer tag setters */
void _APTR_MAKE_VALID( aptr_t *AP ) {
  _TPTR_MAKE_VALID( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_MAKE_INVALID
 *
 ******************************************************************************
 */
void _APTR_MAKE_INVALID( aptr_t *AP ) {
  _TPTR_MAKE_INVALID( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_MAKE_DIRTY
 *
 ******************************************************************************
 */
void _APTR_MAKE_DIRTY( aptr_t *AP ) {
  _TPTR_MAKE_DIRTY( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_MAKE_CLEAN
 *
 ******************************************************************************
 */
void _APTR_MAKE_CLEAN( aptr_t *AP ) {
  _TPTR_MAKE_CLEAN( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_MAKE_NONPTR
 *
 ******************************************************************************
 */
void _APTR_MAKE_NONPTR( aptr_t *AP ) {
  _TPTR_MAKE_NONPTR( &AP->tptr );
}


/**************************************************************************//**
 * _APTR_MAKE_POINTER
 *
 ******************************************************************************
 */
void _APTR_MAKE_POINTER( aptr_t *AP ) {
  _TPTR_MAKE_POINTER( &AP->tptr );
}


/* tagged pointer copy */
void _TPTR_COPY( tptr_t *Dest, const tptr_t *Src ) {
  Dest->qword = Src->qword;
}

/* tagged pointer swap */
void _TPTR_SWAP( tptr_t *TP1, tptr_t *TP2 ) {
  elemswap( QWORD, TP1->qword, TP2->qword );
}

/* annotated pointer copy */
void _APTR_COPY( aptr_t *Dest, const aptr_t *Src ) {
  Dest->annotation = Src->annotation;
  _TPTR_COPY( &Dest->tptr, &Src->tptr );
}

/* annotated pointer swap */
void _APTR_SWAP( aptr_t *AP1, aptr_t *AP2 ) {
  elemswap( uint64_t, AP1->annotation, AP2->annotation );
  _TPTR_SWAP( &AP1->tptr, &AP2->tptr );
}
