/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_resource.c
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

#include "_vgx.h"
#include "_vxserver.h"



/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );








static char *                 __resource__get_path_until_query( const char *path, char **rpath, size_t n_max );
static const char *           __resource__get_suffix( const CString_t *CSTR__name );
static vgx_MediaType          __resource__get_mediatype_by_filepath( const CString_t *CSTR__filepath );
static int64_t                __resource__read_file_contents_into_response_body( const char *path, vgx_VGXServerResponse_t *response );
static bool                   __resource__load_artifact( vgx_VGXServer_t *server, vgx_VGXServerResponse_t *response, const char *resource );
static int                    __resource__load_file( vgx_VGXServer_t *server, vgx_VGXServerResponse_t *response, const char *resource, CString_t **CSTR__error );
static const char *           __resource__service_prefix( vgx_server_pathspec_t *pathspec );
static vgx_server_plugin_type __resource__plugin_type( vgx_server_pathspec_t *pathspec );
static const char *           __resource__path( const char *plugin_path, char **rpath, size_t path_skip );




#define __VGX_SERVER_RESOURCE_MESSAGE( LEVEL, VGXServer, Code, Format, ... ) LEVEL( Code, "IO::VGX::%c(%s): " Format, __ident_letter( VGXServer ), __full_path( VGXServer ), ##__VA_ARGS__ )

#define VGX_SERVER_RESOURCE_VERBOSE( VGXServer, Code, Format, ... )   __VGX_SERVER_RESOURCE_MESSAGE( VERBOSE, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_RESOURCE_INFO( VGXServer, Code, Format, ... )      __VGX_SERVER_RESOURCE_MESSAGE( INFO, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_RESOURCE_WARNING( VGXServer, Code, Format, ... )   __VGX_SERVER_RESOURCE_MESSAGE( WARN, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_RESOURCE_REASON( VGXServer, Code, Format, ... )    __VGX_SERVER_RESOURCE_MESSAGE( REASON, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_RESOURCE_CRITICAL( VGXServer, Code, Format, ... )  __VGX_SERVER_RESOURCE_MESSAGE( CRITICAL, VGXServer, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_RESOURCE_FATAL( VGXServer, Code, Format, ... )     __VGX_SERVER_RESOURCE_MESSAGE( FATAL, VGXServer, Code, Format, ##__VA_ARGS__ )





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char * __resource__get_path_until_query( const char *path, char **rpath, size_t n_max ) {
  char *dest = *rpath;
  char *end = dest + n_max;
  // Copy until first '?'
  const char *p = path;
  char c;
  while( *p && (c = *p++) != '?' ) {
    if( dest < end ) {
      *dest++ = c;
    }
    else {
      return NULL;
    }
  }
  *dest = '\0';
  return *rpath;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __resource__get_suffix( const CString_t *CSTR__name ) {
  int sz = CStringLength( CSTR__name );
  const char *str = CStringValue( CSTR__name );
  const char *suffix = str + sz;
  while( suffix >= str ) {
    if( *suffix == '.' ) {
      break;
    }
    --suffix;
  }
  return suffix;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t *g_keymap = NULL;
static framehash_dynamic_t g_keymap_dyn = {0};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t * __resource__new_keyval_map( framehash_dynamic_t *dyn ) {
  framehash_cell_t *keymap = NULL;

  static const char *IMAGE[] = { ".gif", ".jpg", ".jpeg", ".png", ".ico", ".bmp", ".tiff", ".svg", NULL };

  const char **p;

  if( (keymap = iMapping.NewIntegerMap( dyn, "vgxserver_resource.dyn" )) != NULL ) {

    // txt
    iMapping.IntegerMapAdd( &keymap, dyn, ".txt",           MEDIA_TYPE__text_plain );
    
    // html
    iMapping.IntegerMapAdd( &keymap, dyn, ".html",          MEDIA_TYPE__text_html );
    iMapping.IntegerMapAdd( &keymap, dyn, ".htm",           MEDIA_TYPE__text_html );
    
    // images
    p = IMAGE;
    while( *p != NULL ) {
      iMapping.IntegerMapAdd( &keymap, dyn, *p,             MEDIA_TYPE__image_ANY );
      ++p;
    }
    // css
    iMapping.IntegerMapAdd( &keymap, dyn, ".css",           MEDIA_TYPE__text_css );
    // js
    iMapping.IntegerMapAdd( &keymap, dyn, ".js",            MEDIA_TYPE__application_javascript );
    // pdf
    iMapping.IntegerMapAdd( &keymap, dyn, ".pdf",           MEDIA_TYPE__application_pdf );
    // adoc
    iMapping.IntegerMapAdd( &keymap, dyn, ".adoc",          MEDIA_TYPE__text_plain );
    // xml
    iMapping.IntegerMapAdd( &keymap, dyn, ".xml",           MEDIA_TYPE__application_xml );
    
    if( !(iMapping.IntegerMapSize( keymap ) > 0 ) ) {
      iMapping.DeleteIntegerMap( &keymap, dyn );
    }
  }
  return keymap;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__init( void ) {
  if( g_keymap ) {
    return 0; // already initialized
  }
  if( (g_keymap = __resource__new_keyval_map( &g_keymap_dyn )) == NULL ) { 
    return -1; // error
  }
  return 1; // initialized ok
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN void vgx_server_resource__clear( void ) {
  if( g_keymap ) {
    iMapping.DeleteIntegerMap( &g_keymap, &g_keymap_dyn );
    iFramehash.dynamic.ClearDynamic( &g_keymap_dyn );
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t *g_plugin_map = NULL;
static framehash_dynamic_t g_plugin_map_dyn = {0};



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static framehash_cell_t * __resource__new_plugin_map( framehash_dynamic_t *dyn ) {
  return iMapping.NewIntegerMap( dyn, "vgxserver_resource.dyn" );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__add_plugin( const char *plugin_name, vgx_server_plugin_phase phase ) {
  int x = vgx_server_resource__get_plugin_phases( plugin_name );
  x |= phase;
  return iMapping.IntegerMapAdd( &g_plugin_map, &g_plugin_map_dyn, plugin_name, x );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__del_plugin( const char *plugin_name ) {
  return iMapping.IntegerMapDel( &g_plugin_map, &g_plugin_map_dyn, plugin_name );
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN uint8_t vgx_server_resource__get_plugin_phases( const char *plugin_name ) {
  int64_t x = 0;
  iMapping.IntegerMapGet( g_plugin_map, &g_plugin_map_dyn, plugin_name, &x );
  return (uint8_t)x;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__has_pre_plugin( const char *plugin_name ) {
  return vgx_server_resource__get_plugin_phases( plugin_name ) & VGX_SERVER_PLUGIN_PHASE__PRE;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__has_exec_plugin( const char *plugin_name ) {
  return vgx_server_resource__get_plugin_phases( plugin_name ) & VGX_SERVER_PLUGIN_PHASE__EXEC;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__has_post_plugin( const char *plugin_name ) {
  return vgx_server_resource__get_plugin_phases( plugin_name ) & VGX_SERVER_PLUGIN_PHASE__POST;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN int vgx_server_resource__plugin_init( void ) {
  if( g_plugin_map ) {
    return 0; // already initialized
  }
  if( (g_plugin_map = __resource__new_plugin_map( &g_plugin_map_dyn )) == NULL ) { 
    return -1; // error
  }
  return 1; // initialized ok
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_HIDDEN void vgx_server_resource__plugin_clear( void ) {
  if( g_plugin_map ) {
    iMapping.DeleteIntegerMap( &g_plugin_map, &g_plugin_map_dyn );
    iFramehash.dynamic.ClearDynamic( &g_plugin_map_dyn );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_MediaType __resource__get_mediatype_by_filepath( const CString_t *CSTR__filepath ) {
  int64_t mt;
  if( iMapping.IntegerMapGet( g_keymap, &g_keymap_dyn, __resource__get_suffix( CSTR__filepath ), &mt ) ) {
    return (vgx_MediaType)mt;
  }
  return MEDIA_TYPE__text_html;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __resource__read_file_contents_into_response_body( const char *path, vgx_VGXServerResponse_t *response ) {
  CString_t *CSTR__path = NULL;
  int fd = 0;
  int64_t n_total = 0;

  XTRY {

    // Read file from disk
    if( n_total == 0 ) {
      if( file_exists( path ) && OPEN_R_SEQ( &fd, path ) == 0 ) {
        char buffer[4096];
        int64_t n = 0;
        n_total = 0;
        while( (n = CX_READ( buffer, 1, 4096, fd )) > 0 ) {
          if( iStreamBuffer.Write( response->buffers.content, buffer, n ) == n ) {
            n_total += n;
          }
          else {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
          }
        }
      }
      // File not found
      else {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x003 );
      }
    }
  }
  XCATCH( errcode ) {
    n_total = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__path );
    if( fd > 0 ) {
      CX_CLOSE( fd );
    }
  }
  
  return n_total;
}



/*******************************************************************//**
 * Load named resource into response body
 * Returns: 
 *          true  : resource exists and was loaded into response body
 *          false : resource does not exists
 * 
 ***********************************************************************
 */
static bool __resource__load_artifact( vgx_VGXServer_t *server, vgx_VGXServerResponse_t *response, const char *resource ) {
  QWORD rhash;
  if( CharsEndsWithConst( resource, ".html" ) ) {
    rhash = hash64( (const BYTE*)resource, strlen( resource ) - 5 );
  }
  else if( CharsEqualsConst( resource, "/" ) ) {
    rhash = CharsHash64( "/index" );
  }
  else {
    rhash = CharsHash64( resource );
  }

  int64_t artifact_addr = 0;

  if( CALLABLE( server->resource.artifacts )->GetInt56( server->resource.artifacts, CELL_KEY_TYPE_HASH64, rhash, &artifact_addr ) == CELL_VALUE_TYPE_INTEGER ) {
    vgx_server_artifact_t *A = (vgx_server_artifact_t*)artifact_addr;
    iStreamBuffer.Write( response->buffers.content, (const char*)A->servable.bytes, A->servable.sz );
    response->mediatype = A->MediaType;
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __resource__load_file( vgx_VGXServer_t *server, vgx_VGXServerResponse_t *response, const char *resource, CString_t **CSTR__error ) {
  int ret = 0;

#define THROW_InternalServerError( Code, Reason, Response ) do { \
    response->info.http_errcode = HTTP_STATUS__InternalServerError; \
    __format_error_string( CSTR__error, "%03d Internal Server Error: " Reason, (Response)->info.http_errcode ); \
  } WHILE_ZERO; \
  THROW_ERROR( CXLIB_ERR_GENERAL, Code )

#define THROW_Forbidden( Code, Reason, Response ) do { \
    response->info.http_errcode = HTTP_STATUS__Forbidden; \
    __format_error_string( CSTR__error, "%03d Forbidden: " Reason, (Response)->info.http_errcode ); \
  } WHILE_ZERO; \
  THROW_SILENT( CXLIB_ERR_API, Code )

#define THROW_NotFound( Code, Reason, Response ) do { \
    response->info.http_errcode = HTTP_STATUS__NotFound; \
    __format_error_string( CSTR__error, "%03d Not Found: " Reason, (Response)->info.http_errcode ); \
  } WHILE_ZERO; \
  THROW_SILENT( CXLIB_ERR_API, Code )


  char *abs_vgxroot = NULL;
  char *abs_filepath = NULL;
  CString_t *CSTR__filepath = NULL;

  XTRY {
    if( resource == NULL ) {
      THROW_InternalServerError( 0x001, "resource = NULL", response );
    }

    // Get system root absolute path
    response->mediatype = MEDIA_TYPE__NONE;
    const CString_t *CSTR__root = igraphfactory.SystemRoot();
    if( CSTR__root ) {
      if( (abs_vgxroot = get_abspath( CStringValue( CSTR__root ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
    }
    else {
      THROW_CRITICAL( CXLIB_ERR_GENERAL, 0x003 );
    }

    // Default
    if( CharsEqualsConst( resource, "" ) || CharsEqualsConst( resource, "/" ) ) {
      CSTR__filepath = CStringNewFormat( "%s/WEB-ROOT", abs_vgxroot );
      response->mediatype = MEDIA_TYPE__text_html;
    }
    // File resource under vgxroot
    else if( CharsStartsWithConst( resource, "/~" ) ) {
      CSTR__filepath = CStringNewFormat( "%s/%s", abs_vgxroot, resource+2 );
    }
    // Any other path under webroot
    else {
      CSTR__filepath = CStringNewFormat( "%s/WEB-ROOT%s", abs_vgxroot, resource );
    }

    // Memory error
    if( CSTR__filepath == NULL ) {
      THROW_InternalServerError( 0x004, "out of memory", response );
    }

    // Get mediatype
    if( response->mediatype == MEDIA_TYPE__NONE ) {
      response->mediatype = __resource__get_mediatype_by_filepath( CSTR__filepath );
    }

    // Make sure file path is under vgxroot
    if( CharsContainsConst( resource, ".." ) && (abs_filepath = get_abspath( CStringValue( CSTR__filepath ) )) == NULL ) {
      THROW_Forbidden( 0x005, "invalid path", response );
    }

    // Forbidden path
    if( abs_filepath && !CharsStartsWithConst( abs_filepath, abs_vgxroot ) ) {
      THROW_Forbidden( 0x006, "forbidden path", response );
    }

    // Load file data
    const char *filepath = CStringValue( CSTR__filepath );
    if( __resource__read_file_contents_into_response_body( CStringValue( CSTR__filepath ), response ) < 0 ) {
      VGX_SERVER_RESOURCE_WARNING( server, 0x007, "Resource not found: %s", filepath );
      THROW_NotFound( 0x008, "unknown service or file", response );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( abs_vgxroot );
    free( abs_filepath );
    iString.Discard( &CSTR__filepath );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __resource__service_prefix( vgx_server_pathspec_t *pathspec ) {
  vgx_VGXServerFrontConfig_t *ucf = pathspec->server->config.cf_server->front;

  if( ucf->CSTR__prefix ) {
    pathspec->sz_prefix = CStringLength( ucf->CSTR__prefix );
    return pathspec->prefix = CStringValue( ucf->CSTR__prefix );
  }
  else {
    pathspec->sz_prefix = 0;
    return pathspec->prefix = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_server_plugin_type __resource__plugin_type( vgx_server_pathspec_t *pathspec ) {
  if( pathspec->prefix && CharsStartsWithConst( pathspec->path, pathspec->prefix ) ) {
    const char *suffix = pathspec->path + pathspec->sz_prefix;
    if( CharsStartsWithConst( suffix, VGX_SERVER_RESOURCE_SERVICE ) || CharsStartsWithConst( suffix, VGX_SERVER_RESOURCE_PLUGIN ) ) {
      return pathspec->type = VGX_SERVER_PLUGIN_TYPE__CUSTOM_PREFIX;
    }
  }
  else if( CharsStartsWithConst( pathspec->path, VGX_SERVER_RESOURCE_PLUGIN_PATH_PREFIX ) ) {
    return pathspec->type = VGX_SERVER_PLUGIN_TYPE__USER;
  }
  else if( CharsStartsWithConst( pathspec->path, VGX_SERVER_RESOURCE_BUILTIN_PATH_PREFIX ) ) {
    return pathspec->type = VGX_SERVER_PLUGIN_TYPE__BUILTIN;
  }
  else if( CharsEqualsConst( pathspec->path, VGX_SERVER_RESOURCE_PLUGINS_PATH ) ) {
    return pathspec->type = VGX_SERVER_PLUGIN_TYPE__LIST_USER;
  }
  else if( CharsEqualsConst( pathspec->path, VGX_SERVER_RESOURCE_BUILTINS_PATH ) ) {
    return pathspec->type = VGX_SERVER_PLUGIN_TYPE__LIST_BUILTIN;
  }
  else if( CharsEqualsConst( pathspec->path, VGX_SERVER_RESOURCE_WILDCARD ) ) {
    return pathspec->type = VGX_SERVER_PLUGIN_TYPE__NONE;
  }
  return pathspec->type = VGX_SERVER_PLUGIN_TYPE__NONE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __resource__path( const char *plugin_path, char **rpath, size_t path_skip ) {
  const char *resource = __resource__get_path_until_query( plugin_path, rpath, SZ_VGX_RESOURCE_PATH_BUFFER - path_skip );
  if( resource ) {
    return resource - path_skip;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN const char * vgx_server_resource__plugin_name( vgx_server_pathspec_t *pathspec ) {

  if( pathspec->type & __VGX_SERVER_PLUGIN_TYPE__EXEC_MASK ) {
    const char *plugin_path = pathspec->path;
    switch( pathspec->type ) {
    case VGX_SERVER_PLUGIN_TYPE__CUSTOM_PREFIX:
      //                    8
      //                ________
      // /my-application/plugin/...
      //                        ^
      //               path-----'
      //
      plugin_path += pathspec->sz_prefix;
      if( CharsStartsWithConst( plugin_path, VGX_SERVER_RESOURCE_SERVICE ) ) {
        plugin_path += sz_VGX_SERVER_RESOURCE_SERVICE;
      }
      else {
        plugin_path += sz_VGX_SERVER_RESOURCE_PLUGIN;
      }
      return __resource__path( plugin_path, &pathspec->presource, 0 );
    case VGX_SERVER_PLUGIN_TYPE__USER:
      // /vgx/plugin/...
      //             ^
      //    path-----'
      //
      plugin_path += sz_VGX_SERVER_RESOURCE_PLUGIN_PATH_PREFIX;
      return __resource__path( plugin_path, &pathspec->presource, 0 );
    case VGX_SERVER_PLUGIN_TYPE__BUILTIN:
      // /vgx/builtin/...
      //              ^
      //     path-----'
      //
      plugin_path += sz_VGX_SERVER_RESOURCE_BUILTIN_PATH_PREFIX;
      strcpy( pathspec->presource, VGX_SERVER_RESOURCE_BUILTIN_PLUGIN_PREFIX );  // prepend special prefix for builtins
      pathspec->presource += sz_VGX_SERVER_RESOURCE_BUILTIN_PLUGIN_PREFIX;       // adjust resource buffer pointer
      return __resource__path( plugin_path, &pathspec->presource, sz_VGX_SERVER_RESOURCE_BUILTIN_PLUGIN_PREFIX );
    default:
      return NULL;
    }
  }
  else if( pathspec->type & __VGX_SERVER_PLUGIN_TYPE__LIST_MASK ) {
    switch( pathspec->type ) {
    case VGX_SERVER_PLUGIN_TYPE__LIST_USER:
      // /vgx/plugins
      return VGX_SERVER_RESOURCE_BUILTIN_LIST_PLUGINS;
    case VGX_SERVER_PLUGIN_TYPE__LIST_BUILTIN:
      // /vgx/builtins
      return VGX_SERVER_RESOURCE_BUILTIN_LIST_BUILTINS;
    default:
      return NULL;
    }
  }

  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_server_pathspec_t * vgx_server_resource__init_pathspec( vgx_server_pathspec_t *pathspec, vgx_VGXServer_t *server, vgx_VGXServerRequest_t *request ) {
  // Pointer to resource buffer (part of request path up to first '?')
  pathspec->presource = pathspec->_resource;

  // Server object
  pathspec->server = server;

  // Raw request path
  pathspec->path = request->path;

  // Set service prefix, if configured
  __resource__service_prefix( pathspec );

  // Determine plugin type from request path
  __resource__plugin_type( pathspec );

  return pathspec;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int  vgx_server_resource__call( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error ) {

  vgx_Graph_t *SYSTEM = server->sysgraph;

  vgx_server_pathspec_t pathspec;
  vgx_server_resource__init_pathspec( &pathspec, server, request );

  // Methods initially allowed
  response->allowed_methods_mask = HTTP_OPTIONS | HTTP_HEAD | HTTP_GET | HTTP_POST;

  // ------------------------------------
  // plugin
  // ------------------------------------
  if( pathspec.type != VGX_SERVER_PLUGIN_TYPE__NONE ) {

    // HEAD not supported for plugins
    response->allowed_methods_mask ^= HTTP_HEAD;

    // Get the plugin name
    const char *plugin_name = vgx_server_resource__plugin_name( &pathspec );

    // Check
    if( plugin_name == NULL ) {
      __set_error_string( CSTR__error, "invalid plugin name" );
      goto error;
    }

    // Prepare the response body with any content that needs to appear before the plugin response payload.
    if( vgx_server_response__prepare_body( response ) < 0 ) {
      goto error;
    }

    // Check if http method is a supported plugin method
    if( (request->method & response->allowed_methods_mask) == 0 ) {
      goto method_not_allowed;
    }

    if( request->method == HTTP_OPTIONS ) {
      goto http_options;
    }

    // Call plugin
    response->info.execution.plugin = true;
    return server->resource.pluginf( plugin_name, false, params, request, response, CSTR__error );
  }

  // ------------------------------------
  // non-plugin service
  // ------------------------------------

  const char *resource = __resource__get_path_until_query( pathspec.path, &pathspec.presource, 255 );
  if( resource == NULL ) {
    __set_error_string( CSTR__error, "invalid request" );
    goto error;
  }

  //-----------------------------------
  // *
  //-----------------------------------
  if( CharsEqualsConst( resource, VGX_SERVER_RESOURCE_WILDCARD ) && request->method == HTTP_OPTIONS ) {
    goto http_options;
  }

  // Disallow POST
  response->allowed_methods_mask ^= HTTP_POST;

  //-----------------------------------
  // vgx
  //-----------------------------------
  if( CharsStartsWithConst( resource, "/vgx/" ) ) {

    // HEAD not supported for builtin services
    response->allowed_methods_mask ^= HTTP_HEAD;

    // Check if http method is supported for builtin service
    if( (request->method & response->allowed_methods_mask) == 0 ) {
      goto method_not_allowed;
    }

    if( request->method == HTTP_OPTIONS ) {
      goto http_options;
    }

    response->info.execution.system = true;

    // /vgx/hc
    if( CharsEqualsConst( resource, VGX_SERVER_RESOURCE_HC_PATH ) ) {
      response->info.execution.nometrics = true;
    }

    // /vgx/subscribe
    if( CharsStartsWithConst( resource, VGX_SERVER_RESOURCE_SUBSCRIBE_PATH_PREFIX ) ) {
      response->mediatype = MEDIA_TYPE__text_plain;
      return vgx_server_sync__subscribe_and_sync( SYSTEM, params, request->buffers.content, response->buffers.content, response->mediatype, CSTR__error );
    }
    // /vgx/...
    else {
      return vgx_server_endpoint__service( server, params, response, resource+5 );
    }

  }

  //-----------------------------------
  // SPECIAL treatment of /my-application/vgx/ping and /my-application/vgx/hc
  //-----------------------------------
  if( pathspec.prefix && CharsStartsWithConst( resource, pathspec.prefix ) ) {
    int hc = 0;
    if( CharsEqualsConst( resource + pathspec.sz_prefix, "/vgx/ping" ) || (hc = CharsEqualsConst( resource + pathspec.sz_prefix, VGX_SERVER_RESOURCE_HC_PATH )) != 0 ) {
      // HEAD not supported for ping or hc
      response->allowed_methods_mask ^= HTTP_HEAD;

      // Check if http method is supported for ping or hc
      if( (request->method & response->allowed_methods_mask) == 0 ) {
        goto method_not_allowed;
      }

      if( request->method == HTTP_OPTIONS ) {
        goto http_options;
      }

      response->info.execution.system = true;

      if( hc ) {
        response->info.execution.nometrics = true;
        return vgx_server_endpoint__service( server, params, response, "hc" );
      }
      else {
        return vgx_server_endpoint__service( server, params, response, "ping" );
      }
    }
  }


  //-----------------------------------
  // Everything else
  //-----------------------------------

  // Check http method for artifact or file
  if( (request->method & response->allowed_methods_mask) == 0 ) {
    goto method_not_allowed;
  }

  if( request->method == HTTP_OPTIONS ) {
    goto http_options;
  }

  //-----------------------------------
  // artifact
  //-----------------------------------
  response->info.execution.fileio = true;
  if( __resource__load_artifact( server, response, resource ) ) {
    return 0;
  }

  //-----------------------------------
  // file
  //-----------------------------------
  if( pathspec.prefix && CharsStartsWithConst( resource, pathspec.prefix ) ) {
    resource += pathspec.sz_prefix;
  }
  return __resource__load_file( server, response, resource, CSTR__error );




method_not_allowed:
  response->info.http_errcode = HTTP_STATUS__MethodNotAllowed;
  __format_error_string( CSTR__error, "%03d Method Not Allowed", response->info.http_errcode );
  goto error;

http_options:
  response->mediatype = MEDIA_TYPE__NONE;
  return 0;

error:
  return -1;

}
