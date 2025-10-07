/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    demo_class.c
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

#include "_comlib.h"
#include "demo_class.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );


/*******************************************************************//**
 * STEP 4:
 * Implement all the class methods, including any basic interface methods
 * that don't have a suitable default implementation.
 ***********************************************************************
 */



/*******************************************************************//**
 * signature compatible with f_object_identifier: vm_getid
 ***********************************************************************
 */
static char * DemoClass_identifier( DemoClass_t *self ) {
  return self->id;
}



/*******************************************************************//**
 * signature compatible with f_object_constructor: vm_construct
 ***********************************************************************
 */
static DemoClass_t * DemoClass_constructor( const void *identifier, const DemoClass_constructor_args_t *args ) {
  DemoClass_t *obj;
  if( (obj = (DemoClass_t*)calloc( 1, sizeof(DemoClass_t) )) == NULL ) {
    return NULL;
  }
  if( COMLIB_OBJECT_INIT( DemoClass_t, obj, identifier ) == NULL ) {
    free( obj );
    return NULL;
  }

  if( args ) {
    obj->a = args->a;
  }

  return obj;
}



/*******************************************************************//**
 * signature compatible with f_object_destructor: vm_destroy
 ***********************************************************************
 */
static void DemoClass_destructor( DemoClass_t *self ) {
  free( self );
}



/*******************************************************************//**
 * signature compatible with f_object_serializer: vm_serialize
 ***********************************************************************
 */
static int64_t DemoClass_serializer( const DemoClass_t *self, CQwordQueue_t *out_queue ) {
  int64_t n_elems = 0;
  uint64_t sz_id;
  int64_t (*write)( CQwordQueue_t *Q, const QWORD *data, int64_t length ) = CALLABLE(out_queue)->WriteNolock;

  // Object serialization format:
  // | TYPEINFO | SZ_DATA  |  DATA  |
  n_elems += write( out_queue, &self->typeinfo.qword, 1 ); // <- needed by framework to pick deserializer later
  // serialize the actual object data
  sz_id = strnlen( self->id, _DEMO_CLASS_OBJECT_ID_LENGTH );

  QWORD *wide_id = (QWORD*)malloc( sz_id * sizeof(QWORD) );
  if( wide_id == NULL ) {
    return -1;
  }

  for( size_t i=0; i<sz_id; i++ ) {
    wide_id[i] = self->id[i];
  }

  n_elems += write( out_queue, &sz_id, 1 );  // write size of member: id
  n_elems += write( out_queue, wide_id, sz_id );        // write member: id
  n_elems += write( out_queue, &self->a, 1 );  // write member: a

  free( wide_id );

  return n_elems;
}


/*******************************************************************//**
 * signature compatible with f_object_deserializer: vm_deserialize
 ***********************************************************************
 */
static DemoClass_t * DemoClass_deserializer( comlib_object_t *container, CQwordQueue_t *in_queue ) {
  int64_t sz_id, *psz_id = &sz_id;
  int64_t (*read)( CQwordQueue_t *Q, void **dest, int64_t count ) = CALLABLE(in_queue)->ReadNolock;
  DemoClass_t *self;
  QWORD *wide_id = NULL;
  char *id = NULL;
  
  if( container ) {
    WARN( 0xDE0, "container not supported" );
  }

  XTRY {
    if( (self = DemoClass_constructor( NULL, NULL )) == NULL ) {
      XTHROW(0);  // populate below
    }
    QWORD *pa = &self->a;
    if( read( in_queue, (void**)&psz_id, 1 ) != 1 ) {
      XTHROW(0); // size of member: id
    }
    if( (id = (char*)malloc( sz_id * sizeof(char) + 1 )) == NULL ) {
      XTHROW(0);
    }
    TALIGNED_ARRAY( wide_id, QWORD, sz_id );
    if( wide_id == NULL ) {
      XTHROW(0);
    }
    if( read( in_queue, (void**)&wide_id, sz_id ) != sz_id ) {
      XTHROW(0);                     // populate member: id
    }
    if( read( in_queue, (void**)&pa, 1 ) != 1 ) {
      XTHROW(0);         // populate member: a
    }

    for( int i=0; i<sz_id; i++ ) {
      id[i] = (char)wide_id[i];
    }
    id[sz_id] = '\0';

    if( COMLIB_ObjectSetIdentifier( COMLIB_OBJECT(self), id ) == NULL ) {
      XTHROW(0);
    }

  }
  XCATCH( errcode ) {
    COMLIB_OBJECT_DESTROY( self );
    self = NULL;
  }
  XFINALLY {
    free( id );
    ALIGNED_FREE( wide_id );
  }
  return self;
}
 

/*******************************************************************//**
 * SetValue
 ***********************************************************************
 */
static QWORD DemoClass_SetValue( DemoClass_t *self, QWORD x ) {
  self->a = x;
  return self->a;
}


/*******************************************************************//**
 * GetValue
 ***********************************************************************
 */
static QWORD DemoClass_GetValue( DemoClass_t *self ) {
  return self->a;
}


/*******************************************************************//**
 * STEP 5:
 * Instantiate a single static global vtable populated with the
 * methods implemented above. NOTE: THE ORDER MATTERS.
 *
 ***********************************************************************
 */
static DemoClass_vtable_t DemoClassMethods = {
  /* base methods */
  .vm_cmpid       = NULL,
  .vm_getid       = (f_object_identifier_t)DemoClass_identifier,
  .vm_serialize   = (f_object_serializer_t)DemoClass_serializer,
  .vm_deserialize = (f_object_deserializer_t)DemoClass_deserializer,
  .vm_construct   = (f_object_constructor_t)DemoClass_constructor,
  .vm_destroy     = (f_object_destructor_t)DemoClass_destructor,
  .vm_represent   = NULL,
  .vm_allocator   = NULL,
  /* extended methods */
  DemoClass_SetValue,                               /* GetValue */
  DemoClass_GetValue                                /* SetValue */
};


/*******************************************************************//**
 * STEP 6:
 * Define the class type and base type, hook up the vtable, and define
 * the how objects of this class should be identified
 ***********************************************************************
 */
void DemoClass_RegisterClass( void ) {
  // DemoClass_t                      : the type we are defining
  // CXLIB_OBTYPE_GENERIC             : this class with have the generic base type 
  // DemoClassMethods                 : the single instance of the class vtable
  // OBJECT_IDENTIFIED_BY_SHORTSTRING : objects will use a 16-char string identifier
  // _DEMO_CLASS_OBJECT_ID_LENGTH-1   : the maximum length of the string identifier
  COMLIB_REGISTER_CLASS( DemoClass_t, CXLIB_OBTYPE_GENERIC, &DemoClassMethods, OBJECT_IDENTIFIED_BY_SHORTSTRING, _DEMO_CLASS_OBJECT_ID_LENGTH );
}


/*******************************************************************//**
 * STEP 7:
 * Un-registration method to be called before program exit
 ***********************************************************************
 */
void DemoClass_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( DemoClass_t );
}


/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void DemoClass_DemonstrateUsage( void ) {

  DemoClass_constructor_args_t args;
  DemoClass_t *demo1;
  DemoClass_t *demo2;
  DemoClass_t *demo3;

  args.a = 100;
  demo1 = COMLIB_OBJECT_NEW( DemoClass_t, "demo1", &args );

  args.a = 200;
  demo2 = NEW( DemoClass_t, "demo2", &args );
  QWORD val;

  // Access using CALLABLE
  val = CALLABLE(demo1)->GetValue(demo1); // 0
  CALLABLE(demo1)->SetValue(demo1, 100);
  val = CALLABLE(demo1)->GetValue(demo1); // 100

  // Access using CALL
  val = CALL(demo2, GetValue); // 0
  CALL(demo2, SetValue, 200);
  val = CALL(demo2, GetValue); // 200

  // Serialize and deserialize
  CQwordQueue_t *Q = NEW( CQwordQueue_t, NULL, NULL );
  SERIALIZE( demo1, Q );
  demo3 = (DemoClass_t*)COMLIB_DeserializeObject( NULL, NULL, Q );


  COMLIB_OBJECT_DESTROY( demo1 );
  COMLIB_OBJECT_DESTROY( demo2 );
  COMLIB_OBJECT_DESTROY( demo3 );

}
