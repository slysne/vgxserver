/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    pyvgx_persist.c
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

#include "pyvgx.h"
#include "_vxsim.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );


/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void PrintReprForAllAllocators( vgx_Graph_t *graph ) {
  BEGIN_PYVGX_THREADS {

    CStringQueue_t *string = COMLIB_OBJECT_NEW_DEFAULT( CStringQueue_t );

    cxmalloc_family_t *allocator = graph->vertex_allocator;


    // vertex_allocator
    COMLIB_OBJECT_REPR( graph->vertex_allocator,                  string );
    
    // vxtable
    COMLIB_OBJECT_REPR( graph->vxtable->_dynamic.falloc,          string );
    COMLIB_OBJECT_REPR( graph->vxtable->_dynamic.balloc,          string );
    
    // rel_encoder
    COMLIB_OBJECT_REPR( graph->rel_encoder->_dynamic.falloc,      string );
    COMLIB_OBJECT_REPR( graph->rel_encoder->_dynamic.balloc,      string );
    // rel_decoder
    COMLIB_OBJECT_REPR( graph->rel_decoder->_dynamic.falloc,      string );
    COMLIB_OBJECT_REPR( graph->rel_decoder->_dynamic.balloc,      string );

    // vxtype_encoder
    COMLIB_OBJECT_REPR( graph->vxtype_encoder->_dynamic.falloc,   string );
    COMLIB_OBJECT_REPR( graph->vxtype_encoder->_dynamic.balloc,   string );
    // vxtype_decoder
    COMLIB_OBJECT_REPR( graph->vxtype_decoder->_dynamic.falloc,   string );
    COMLIB_OBJECT_REPR( graph->vxtype_decoder->_dynamic.balloc,   string );

    // arcvector_fhdyn
    COMLIB_OBJECT_REPR( graph->arcvector_fhdyn.falloc,            string );
    COMLIB_OBJECT_REPR( graph->arcvector_fhdyn.balloc,            string );
    
    // property_fhdyn
    COMLIB_OBJECT_REPR( graph->property_fhdyn.falloc,             string );
    COMLIB_OBJECT_REPR( graph->property_fhdyn.balloc,             string );

    // vtxmap_fhdyn
    COMLIB_OBJECT_REPR( graph->vtxmap_fhdyn.falloc,               string );
    COMLIB_OBJECT_REPR( graph->vtxmap_fhdyn.balloc,               string );

    // dim_decoder
    if( graph->similarity->dim_decoder ) {
      COMLIB_OBJECT_REPR( graph->similarity->dim_decoder->_dynamic.falloc,  string );
      COMLIB_OBJECT_REPR( graph->similarity->dim_decoder->_dynamic.balloc,  string );
    }
    // dim_encoder
    if( graph->similarity->dim_encoder ) {
      COMLIB_OBJECT_REPR( graph->similarity->dim_encoder->_dynamic.falloc,  string );
      COMLIB_OBJECT_REPR( graph->similarity->dim_encoder->_dynamic.balloc,  string );
    }

    // internal vector allocator
    COMLIB_OBJECT_REPR( graph->similarity->int_vector_allocator,  string );
    // external vector allocator
    COMLIB_OBJECT_REPR( graph->similarity->ext_vector_allocator,  string );


    CALLABLE(string)->NulTermNolock(string);

    char *line = NULL;
    CALLABLE(string)->GetValueNolock( string, (void**)&line );

    printf( "%s", line );

    COMLIB_OBJECT_DESTROY( string );
    string = NULL;

    CQwordQueue_t *output = COMLIB_OBJECT_NEW_DEFAULT( CQwordQueue_t );

    int64_t sz = COMLIB_OBJECT_SERIALIZE( allocator, output );

    QWORD qword=0, *pqword=&qword;

    for( int i=0; i<sz; i++ ) {
      CALLABLE(output)->ReadNolock(output, (void**)&pqword, 1 );
      putc( (char)qword, stdout );
    } 
    putc( '\n', stdout );

    COMLIB_OBJECT_DESTROY( output );

  } END_PYVGX_THREADS;

}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int64_t _ipyvgx_persist__serialize( vgx_Graph_t *graph, int timeout_ms, bool force, bool remote ) {
  int64_t nqwords = 0;
  CString_t *CSTR__error = NULL;

  vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;
  BEGIN_PYVGX_THREADS {
    nqwords = CALLABLE(graph)->BulkSerialize( graph, timeout_ms, force, remote, &reason, &CSTR__error );
  } END_PYVGX_THREADS;

  if( nqwords < 0 ) {
    BEGIN_PYTHON_INTERPRETER {
      if( !iPyVGXBuilder.SetPyErrorFromAccessReason( CALLABLE( graph )->FullPath( graph ), reason, &CSTR__error ) ) {
        const char *e = CSTR__error ? CStringValue( CSTR__error ) : "check logs";
        PyErr_Format( PyExc_Exception, "Graph serialization error: %s", e );
      }
    } END_PYTHON_INTERPRETER;
  }

  if( CSTR__error ) {
    CStringDelete( CSTR__error );
  }

  return nqwords;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN IPyVGXPersist iPyVGXPersist = {
  .Serialize           = _ipyvgx_persist__serialize,
};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void _ipyvgx_debug__print_vertex_allocator( vgx_Vertex_t *vertex ) {
  // TODO: Implement
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void _ipyvgx_debug__print_vector_allocator( vgx_Vector_t *vector ) {
  vgx_Similarity_t *simcontext = CALLABLE( vector )->Context( vector )->simobj;
  CALLABLE( simcontext )->PrintVectorAllocator( simcontext, vector );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN IPyVGXDebug iPyVGXDebug = {
  .PrintVertexAllocator         = _ipyvgx_debug__print_vertex_allocator,
  .PrintVectorAllocator         = _ipyvgx_debug__print_vector_allocator,
};
