/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxinit.c
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

#include "_vgx.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );





/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_context_t * vgx_INIT( const char *sysroot ) {
  vgx_context_t *context = NULL;

  XTRY {
    #if defined CXPLAT_WINDOWS_X64
    #elif defined CXPLAT_LINUX_ANY
    #elif defined CXPLAT_MAC_ARM64
    size_t size = sizeof(uint64_t);
    uint64_t flag = 0;
    // Require DotProd
    const char *required[] = {
      "hw.optional.arm.FEAT_DotProd",
      0
    };
    const char **feature = required;
    while( *feature ) {
      const char *name = *feature++;
      sysctlbyname( name, &flag, &size, NULL, 0 );
      if( !flag ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x000, "Missing required CPU feature: %s", name );
      }
    }
    #endif
 
    // Context
    if( (context = calloc( 1, sizeof( vgx_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
    }

    // Sysroot
    if( sysroot ) {
      strncpy( context->sysroot, sysroot, 254 );
    }
    else {
      context->sysroot[0] = '\0';
    }

    // cxlib
    context->cxlib.init = 0;
    if( (context->cxlib.version.simple = CharsNew( cxlib_version( false ) )) == NULL ||
        (context->cxlib.version.extended = CharsNew( cxlib_version( true ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x002 );
    }

    // comlib
    if( (context->comlib.init = comlib_INIT()) < 0 ||
        (context->comlib.version.simple = CharsNew( comlib_version( false ) )) == NULL ||
        (context->comlib.version.extended = CharsNew( comlib_version( true ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
    }

    // cxmalloc
    if( (context->cxmalloc.init = cxmalloc_INIT()) < 0 ||
        (context->cxmalloc.version.simple = CharsNew( cxmalloc_version( false ) )) == NULL ||
        (context->cxmalloc.version.extended = CharsNew( cxmalloc_version( true ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x004 );
    }

    // framehash
    if( (context->framehash.init  = framehash_INIT()) < 0 ||
        (context->framehash.version.simple = CharsNew( iFramehash.Version( false ) )) == NULL ||
        (context->framehash.version.extended = CharsNew( iFramehash.Version( true ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x005 );
    }

    // graph
    if( (context->graph.init = vgx_GRAPH_INIT()) < 0 ||
        (context->graph.version.simple = CharsNew( CStringValue( igraphinfo.Version( 0 ) ) )) == NULL ||
        (context->graph.version.extended = CharsNew( CStringValue( igraphinfo.Version( 1 ) ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x006 );
    }

    // similarity
    if( (context->similarity.init = vgx_SIM_INIT()) < 0 ||
        (context->similarity.version.simple = CharsNew( CStringValue( igraphinfo.Version( 0 ) ) )) == NULL ||
        (context->similarity.version.extended = CharsNew( CStringValue( igraphinfo.Version( 1 ) ) )) == NULL )
    {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x007 );
    }

    // socket
#if defined CXPLAT_WINDOWS_X64
    WORD wVersionRequested = MAKEWORD(2,2);
    WSADATA wsaData;
    if( WSAStartup( wVersionRequested, &wsaData ) != 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_INITIALIZATION, 0x008, "WSAStartup failed" );
    }
    else {
      context->socket.init = 1;
      VERBOSE( 0x009, "%s: %s", wsaData.szDescription, wsaData.szSystemStatus );
      CString_t *CSTR__ver = CStringNewFormat( "WSAVersion: %d", wsaData.wVersion );
      if( CSTR__ver ) {
        if( (context->socket.version.simple = CharsNew( CStringValue( CSTR__ver ) )) == NULL ||
            (context->socket.version.extended = CharsNew( CStringValue( CSTR__ver ) )) == NULL )
        {
          iString.Discard( &CSTR__ver );
        }
      }
      if( CSTR__ver ) {
        CStringDelete( CSTR__ver );
      }
      else {
        THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x00A );
      }
    }
#else
    // TODO: IMPLEMENT
#endif

  }
  XCATCH( errcode ) {
    vgx_DESTROY( &context );
  }
  XFINALLY {
  }
  return context;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT void vgx_DESTROY( vgx_context_t **context ) {
  if( context && *context ) {
    vgx_component_context_t *socket = &(*context)->socket;
    vgx_component_context_t *similarity = &(*context)->similarity;
    vgx_component_context_t *graph = &(*context)->graph;
    vgx_component_context_t *framehash = &(*context)->framehash;
    vgx_component_context_t *cxmalloc = &(*context)->cxmalloc;
    vgx_component_context_t *comlib = &(*context)->comlib;
    vgx_component_context_t *cxlib = &(*context)->cxlib;
    

    igraphfactory.Shutdown();

    // socket
    if( socket->init > 0 ) {
#if defined CXPLAT_WINDOWS_X64
      WSACleanup();
#else
#endif
    }
    CharsDelete( socket->version.simple );
    CharsDelete( socket->version.extended );

    // similarity
    if( similarity->init > 0 ) {
      vgx_SIM_DESTROY();
    }
    CharsDelete( similarity->version.extended );
    CharsDelete( similarity->version.simple );
      
    // graph
    if( graph->init > 0 ) {
      vgx_GRAPH_DESTROY();
    }
    CharsDelete( graph->version.extended );
    CharsDelete( graph->version.simple );

    // framehash
    if( framehash->init > 0 ) {
      framehash_DESTROY();
    }
    CharsDelete( framehash->version.extended );
    CharsDelete( framehash->version.simple );

    // cxmalloc
    if( cxmalloc->init > 0 ) {
      cxmalloc_DESTROY();
    }
    CharsDelete( cxmalloc->version.extended );
    CharsDelete( cxmalloc->version.simple );

    // comlib
    if( comlib->init > 0 ) {
      comlib_DESTROY();
    }
    CharsDelete( comlib->version.extended );
    CharsDelete( comlib->version.simple );

    // cxlib
    CharsDelete( cxlib->version.extended );
    CharsDelete( cxlib->version.simple );


    free( *context );
    *context = NULL;
  }
}
