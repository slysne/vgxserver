/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_endpoint.c
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
#include "_vxserver.h"
#include "versiongen.h"


#ifndef VGX_VERSION
#define VGX_VERSION ?.?
#endif

static const char *g_version_info = GENERATE_VERSION_INFO_STR( "vgx", VERSIONGEN_XSTR( VGX_VERSION ) );
static const char *g_version_info_ext = GENERATE_VERSION_INFO_EXT_STR( "vgx", VERSIONGEN_XSTR( VGX_VERSION ) );
static const char *g_build_info = GENERATE_BUILD_INFO;


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static char *           __endpoint__write_chars_limit( char *ptr, const char *end, const char *data );
static char *           __endpoint__write_decimal_limit( char *ptr, const char *end, int64_t value );
static char *           __endpoint__write_double_limit( char *ptr, const char *end, double value, int decimals );
static int              __endpoint__format_duration( int seconds, char *buffer, size_t sz );
static int              __endpoint__service_hc(       vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_ping(     vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_time(     vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_storage(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_graphsum( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_objcnt(   vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_status(   vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_txstat(   vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_peerstat( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_meminfo(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_nodestat( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_matrix(   vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_dispatch( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_inspect(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_randstr(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_randint(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );
static int              __endpoint__service_unknown(  vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );



typedef int (*__f_service)( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response );



typedef struct __s_service_t {
  const char *name;
  int64_t key;
  __f_service func;
} __service_t;



static __service_t g_services[] = {
  { "hc",       0,  __endpoint__service_hc },
  { "ping",     0,  __endpoint__service_ping },
  { "time",     0,  __endpoint__service_time },
  { "storage",  0,  __endpoint__service_storage },
  { "graphsum", 0,  __endpoint__service_graphsum },
  { "objcnt",   0,  __endpoint__service_objcnt },
  { "status",   0,  __endpoint__service_status },
  { "txstat",   0,  __endpoint__service_txstat },
  { "peerstat", 0,  __endpoint__service_peerstat },
  { "meminfo",  0,  __endpoint__service_meminfo },
  { "nodestat", 0,  __endpoint__service_nodestat },
  { "matrix",   0,  __endpoint__service_matrix },
  { "dispatch", 0,  __endpoint__service_dispatch },
  { "inspect",  0,  __endpoint__service_inspect },
  { "randstr",  0,  __endpoint__service_randstr },
  { "randint",  0,  __endpoint__service_randint },
  { NULL,       0,  __endpoint__service_unknown }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_endpoint__init( void ) {
  __service_t *s = g_services;
  do {
    s->key = strhash64( (BYTE*)s->name );
  } while( (++s)->name );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_endpoint__service( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response, const char *name ) {

  int64_t key = strhash64( (BYTE*)name );
  __service_t *s = g_services;
  
  do {
    if( key == s->key ) {
      return s->func( server, params, response );
    }
  } while( (++s)->key );

  return s->func( server, params, response );

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static char * __endpoint__write_chars_limit( char *ptr, const char *end, const char *data ) {
  char *cp = ptr;
  const char *rp = data;
  while( *rp != '\0' && cp < end ) {
    *cp++ = *rp++;
  }
  return cp;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static char * __endpoint__write_decimal_limit( char *ptr, const char *end, int64_t value ) {
  if( end - ptr > 20 ) {
    return write_decimal( ptr, value );
  }
  return ptr;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static char * __endpoint__write_double_limit( char *ptr, const char *end, double value, int decimals ) {
  if( end - ptr > 32 ) {
    return write_double( ptr, value, decimals );
  }
  return ptr;
}



#define out_ptr_name  __OUTPTR
#define out_end_name  __OUTEND
#define out_buf_name  __OUTBUF

#define out_txt( Txt )    (out_ptr_name = __endpoint__write_chars_limit( out_ptr_name, out_end_name, Txt ))
#define out_int( Int )    (out_ptr_name = __endpoint__write_decimal_limit( out_ptr_name, out_end_name, (int64_t)(Int) ))
#define out_dbl( Dbl, N ) (out_ptr_name = __endpoint__write_double_limit( out_ptr_name, out_end_name, (double)(Dbl), N ))
#define out_bol( Bool )   (out_ptr_name = __endpoint__write_chars_limit( out_ptr_name, out_end_name, (Bool) ? "true" : "false" ))
#define out_nul()         (*out_ptr_name = '\0')
#define bdy_out( Resp )   iStreamBuffer.Write( (Resp)->buffers.content, out_buf_name, out_ptr_name - out_buf_name );

#define first_key( Key )            out_txt( "\"" Key "\": " )
#define next_key( Key )             out_txt( ", \"" Key "\": " )

#define first_str( Str )            out_txt( "\"" ); out_txt( Str ); out_txt( "\"" )
#define next_str( Str )             out_txt( ", \"" ); out_txt( Str ); out_txt( "\"" )

#define first_str_key( Str )        out_txt( "\"" ); out_txt( Str ); out_txt( "\": " )
#define next_str_key( Str )         out_txt( ", \"" ); out_txt( Str ); out_txt( "\": " )

#define first_key_str( Key, Str )   out_txt( "\"" Key "\": \"" ); out_txt( Str ); out_txt( "\"" )
#define next_key_str( Key, Str )    out_txt( ", \"" Key "\": \"" ); out_txt( Str ); out_txt( "\"" )

#define begin_first_key_direct( Key ) \
do { \
  out_txt( "\"" Key "\": \"" ); \
  {

#define begin_next_key_direct( Key ) \
do { \
  out_txt( ", \"" Key "\": \"" ); \
  {

#define end_key_direct \
  } \
  out_txt( "\"" ); \
} WHILE_ZERO


#define first_key_int( Key, Int )         first_key( Key ); out_int( Int )
#define next_key_int( Key, Int )          next_key( Key ); out_int( Int )
#define first_key_iqt( Key, Int )  first_key( Key ); out_txt( "\"" ); out_int( Int ); out_txt( "\"" )
#define next_key_iqt( Key, Int )   next_key( Key ); out_txt( "\"" ); out_int( Int ); out_txt( "\"" )
#define first_key_dbl( Key, Val, N )      first_key( Key ); out_dbl( Val, N )
#define next_key_dbl( Key, Val, N )       next_key( Key ); out_dbl( Val, N )
#define first_key_bol( Key, Bool )        first_key( Key ); out_bol( Bool )
#define next_key_bol( Key, Bool )         next_key( Key ); out_bol( Bool )
#define first_key_null( Key )             first_key( Key ); out_txt( "null" )
#define next_key_null( Key )              next_key( Key ); out_txt( "null" )

#define open_dict()                 out_txt( "{" )
#define open_next_dict()            out_txt( ", {" )
#define close_dict()                out_txt( "}" )

#define open_array()                out_txt( "[" )
#define open_next_array()           out_txt( ", [" )
#define close_array()               out_txt( "]" )

#define begin_first_key_dict( Key ) \
do { \
  out_txt( "\"" Key "\": {" ); \
  {

#define begin_next_key_dict( Key ) \
do { \
  out_txt( ", \"" Key "\": {" ); \
  {

#define end_key_dict \
  } \
  close_dict(); \
} WHILE_ZERO


#define begin_first_key_array( Key ) \
do { \
  out_txt( "\"" Key "\": [" ); \
  {

#define begin_next_key_array( Key ) \
do { \
  out_txt( ", \"" Key "\": [" ); \
  {

#define end_key_array \
  } \
  close_array(); \
} WHILE_ZERO


#define begin_json_static( Response, BufferSize, Type )  \
do {                \
  char out_buf_name[BufferSize]; \
  char *out_ptr_name = out_buf_name; \
  char *out_end_name = out_buf_name + (BufferSize) - 1; \
  vgx_VGXServerResponse_t *__response__ = Response; \
  char __type__ = Type; \
  __response__->mediatype = MEDIA_TYPE__application_json; \
  vgx_server_response__prepare_body( __response__ ); \
  switch( __type__ ) { \
  case '{': open_dict(); break;  \
  case '[': open_array(); break;  \
  } \
  {


#define end_json_static     \
  }                         \
  switch( __type__ ) {      \
  case '{': close_dict(); break;  \
  case '[': close_array(); break;  \
  }                         \
  out_nul();                \
  bdy_out( __response__ );  \
} WHILE_ZERO




#define try_json_dynamic( Response, BufferSize, Type )  \
do {                \
  char *out_buf_name = calloc( BufferSize, 1 ); \
  if( out_buf_name != NULL ) { \
    char *out_ptr_name = out_buf_name; \
    char *out_end_name = out_buf_name + (BufferSize) - 1; \
    vgx_VGXServerResponse_t *__response__ = Response; \
    char __type__ = Type; \
    __response__->mediatype = MEDIA_TYPE__application_json; \
    vgx_server_response__prepare_body( __response__ ); \
    switch( __type__ ) { \
    case '{': open_dict(); break;  \
    case '[': open_array(); break;  \
    } \
    {


#define catch_json_dynamic    \
    }                         \
    switch( __type__ ) {      \
    case '{': close_dict(); break;  \
    case '[': close_array(); break;  \
    }                         \
    out_nul();                \
    bdy_out( __response__ );  \
    free( out_buf_name );     \
  }                           \
  else {


#define end_json_dynamic      \
  }                           \
} WHILE_ZERO






/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __endpoint__format_duration( int seconds, char *buffer, size_t sz ) {
  int days =    seconds / 86400;
  int hours = ( seconds % 86400 ) / 3600;
  int min =   ( seconds % 3600  ) / 60;
  int sec =     seconds % 60;
  // 112:04:34:14
  return snprintf( buffer, sz, "%d:%02d:%02d:%02d", days, hours, min, sec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_hc( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {
  response->mediatype = MEDIA_TYPE__text_plain;
  vgx_server_response__prepare_body( response );

  char buf[] = VGX_SERVER_HEADER;
  iStreamBuffer.Write( response->buffers.content, buf, 5 );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_ping( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;
  vgx_VGXServerPerfCounters_t counters = {0};
  CString_t *CSTR__now = NULL;
  CString_t *CSTR__fqdn = NULL;
  CString_t *CSTR__cpu = NULL;
  char *local_ip = NULL;
  const char *ip = NULL;

  int timeout_ms = 10000;


  XTRY {

    int err = 0;
    GRAPH_LOCK( SYSTEM ) {
      err = iVGXServer.Counters.Get( SYSTEM, &counters, timeout_ms );
      ip = iVGXServer.Service.GetAdminIP( SYSTEM );
    } GRAPH_RELEASE;

    if( err < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    if( ip == NULL ) {
      if( (ip = local_ip = cxgetip( NULL )) == NULL ) {
        ip = "0.0.0.0";
      }
    }

    CSTR__fqdn = iURI.NewFqdn();
    const char *fqdn = CStringValueDefault( CSTR__fqdn, "?" );

    CSTR__cpu = iVGXProfile.CPU.GetBrandString();
    const char *cpu = CStringValueDefault( CSTR__cpu, "?" );
    
    int64_t mem = 0;
    get_system_physical_memory( &mem, NULL, NULL );

    CSTR__now = igraphinfo.CTime(-1, false);
    const char *now = CStringValueDefault( CSTR__now, "1970-01-01 00:00:00.000" );

    int up_t = (int)(counters.server_uptime_ns / 1000000000);

    begin_json_static( response, 512, '{' ) {

      /*  "host": {
            "ip":           "127.0.0.1",
            "name":         "localhost.local",
            "uptime":       "112:04:34:14",
            "current_time": "2022-06-19 11:35:11.657",
            "cpu":          "Intel(R) Xeon(R) W-2255 CPU @ 3.70GHz",
            "memory":       137438953472,
            "version":      "vgx v3.3"
          }
      */
      begin_first_key_dict( "host" ) {
        first_key_str( "ip", ip );
        next_key_str( "name", fqdn );
        begin_next_key_direct( "uptime" ) {
          int nc = __endpoint__format_duration( up_t, out_ptr_name, 16 );
          if( nc > 0 ) {
            out_ptr_name += nc;
          }
        } end_key_direct;
        next_key_str( "current_time", now );
        next_key_str( "cpu", cpu );
        next_key_int( "memory", mem );
        next_key_str( "version", g_version_info_ext );
      } end_key_dict;
    } end_json_static;

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__now );
    iString.Discard( &CSTR__fqdn );
    iString.Discard( &CSTR__cpu );
    free( local_ip );
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_time( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;
  vgx_VGXServerPerfCounters_t counters = {0};
  CString_t *CSTR__start = NULL;
  CString_t *CSTR__now = NULL;

  int timeout_ms = 10000;


  XTRY {
    if( iVGXServer.Counters.Get( SYSTEM, &counters, timeout_ms ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    int64_t tms = __MILLISECONDS_SINCE_1970();
    int64_t t_start = tms - counters.server_uptime_ns / 1000000;

    if( (CSTR__start = igraphinfo.CTime(t_start, false)) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    if( (CSTR__now = igraphinfo.CTime(tms, false)) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }

    int up_t = (int)(counters.server_uptime_ns / 1000000000);

    begin_json_static( response, 512, '{' ) {

      /*  "time": {
            "up":      "99:01:19:18",
            "start":   "2022-03-12 10:15:53"
            "current": "2022-06-19 11:35:11"
          }
      */

      begin_first_key_dict( "time" ) {
        begin_first_key_direct( "up" ) {
          int nc = __endpoint__format_duration( up_t, out_ptr_name, 16 );
          if( nc > 0 ) {
            out_ptr_name += nc;
          }
        } end_key_direct;
        next_key_str( "start", CStringValue( CSTR__start ) );
        next_key_str( "current", CStringValue( CSTR__now ) );

      } end_key_dict;

    } end_json_static;

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__start );
    iString.Discard( &CSTR__now );
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_storage( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  char *sysroot_full = NULL;

  XTRY {

    const CString_t *CSTR__root = igraphfactory.SystemRoot();
    if( CSTR__root == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    const char *sysroot = "";
    if( (sysroot_full = get_abspath( CStringValue( CSTR__root ) )) != NULL ) {
      char *p = sysroot_full;
      while( *p != '\0' ) {
        if( *p == '\\' ) {
          *p = '/';
        }
        ++p;
      }
      sysroot = sysroot_full;
    }

    /*  {
          "sysroot":      "/usr/data/instanceX",
        }
    */
    begin_json_static( response, 512, '{' ) {
      first_key_str( "sysroot", sysroot );
    } end_json_static;


  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( sysroot_full );
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_graphsum( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;

  char digest[33] = {0};
  int64_t master_serial = iSystem.MasterSerial();
  int64_t order = 0;
  int64_t size = 0;
  int64_t properties = 0;
  int64_t vectors = 0;
  int64_t nrel = 0;
  int64_t nvtx = 0;
  int64_t ndim = 0;
  int64_t nkey = 0;
  int64_t nval = 0;
  int64_t vprop_bytes = 0;
  int64_t vprop_count = 0;
  int64_t qcnt = 0;
  int64_t qns = 0;
  int64_t nWarning = 0;
  int64_t nError = 0;
  int64_t nCritical = 0;

  cxlib_exception_counters( &nWarning, &nError, &nCritical );
  /*
    {
        "graphsum": {
            "digest": "c1261113054d271e02ffc8c2ee95612d",
            "master-serial": "1689703829703000",
            "names": ["g1", "g2", "secret"],
            "local-only": ["secret"],
            "order": 4,
            "size": 5,
            "properties": 2,
            "vectors": 3,
            "enumerator": {
                "relationship": 2,
                "vertextype": 2,
                "key": 2,
                "string": 1,
                "dimension": 0
            },
            "vprop": {
                "bytes": 12345,
                "count": 17
            },
            "query": {
                "count": 0,
                "ns-acc": 0
            },
            "engine": {
                "exceptions": {
                    "warning": 0,
                    "error": 0,
                    "critical": 0
                }
            }
        }
    }
  */

  begin_json_static( response, 1024, '{' ) {

    begin_first_key_dict( "graphsum" ) {

      GRAPH_FACTORY_ACQUIRE {
        objectid_t obid = igraphfactory.Fingerprint();
        idtostr( digest, &obid );

        first_key_str( "digest", digest );
        next_key_iqt( "master-serial", master_serial );

        vgx_StringList_t *CSTR__names = iString.List.New( NULL, 0 );
        vgx_StringList_t *CSTR__local_names = iString.List.New( NULL, 0 );

        vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
        if( graphs ) {
          vgx_Graph_t **cursor = graphs;
          vgx_Graph_t *graph;
          while( (graph = *cursor++) != NULL ) {
            bool local_only;
            GRAPH_LOCK( graph ) {
              local_only = _vgx_graph_is_local_only_CS( graph );
              // Basic objects
              order += GraphOrder( graph );
              size += GraphSize( graph );
              properties += GraphPropCount( graph );
              vectors += GraphVectorCount( graph );
              // Enumerators
              if( graph->similarity && graph->similarity->parent == graph ) {
                nrel += iEnumerator_CS.Relationship.Count( graph );
                nvtx += iEnumerator_CS.VertexType.Count( graph );
                if( !igraphfactory.EuclideanVectors() ) {
                  ndim += iEnumerator_CS.Dimension.Count( graph->similarity );
                }
              }
              nkey += iEnumerator_CS.Property.Key.Count( graph );
              nval += iEnumerator_CS.Property.Value.Count( graph );
              // Internal queries
              qcnt += CALLABLE( graph )->QueryCountNolock( graph );
              qns += CALLABLE( graph )->QueryTimeNanosecAccNolock( graph );
            } GRAPH_RELEASE;
            SYNCHRONIZE_ON( graph->vprop.lock ) {
              vprop_bytes += graph->vprop.bytes;
              vprop_count += graph->vprop.count;
            } RELEASE;
            const char *name = CStringValue( CALLABLE( graph )->Name( graph ) );

            if( CSTR__names && CSTR__local_names ) {
              iString.List.Append( CSTR__names, name );
              if( local_only ) {
                iString.List.Append( CSTR__local_names, name );
              }
            }
          }
          free( (void*)graphs );
        }

        begin_next_key_array( "names" ) {
          if( CSTR__names ) {
            int64_t n_names = iString.List.Size( CSTR__names );
            for( int64_t i=0; i<n_names; ++i ) {
              const char *name = iString.List.GetChars( CSTR__names, i );
              if( i==0 ) {
                first_str( name );
              }
              else {
                next_str( name );
              }
            }
            iString.List.Discard( &CSTR__names );
          }
        } end_key_array;

        begin_next_key_array( "local-only" ) {
          if( CSTR__local_names ) {
            int64_t n_local_names = iString.List.Size( CSTR__local_names );
            for( int64_t i=0; i<n_local_names; ++i ) {
              const char *name = iString.List.GetChars( CSTR__local_names, i );
              if( i==0 ) {
                first_str( name );
              }
              else {
                next_str( name );
              }
            }
            iString.List.Discard( &CSTR__local_names );
          }
        } end_key_array;

      } GRAPH_FACTORY_RELEASE;

      next_key_int( "order", order );
      next_key_int( "size", size );
      next_key_int( "properties", properties );
      next_key_int( "vectors", vectors );
      begin_next_key_dict( "enumerator" ) {
        first_key_int( "relationship", nrel );
        next_key_int( "vertextype", nvtx );
        next_key_int( "key", nkey );
        next_key_int( "string", nval );
        next_key_int( "dimension", ndim );
      } end_key_dict;
      begin_next_key_dict( "vprop" ) {
        first_key_int( "bytes", vprop_bytes );
        next_key_int( "count", vprop_count );
      } end_key_dict;
      begin_next_key_dict( "query" ) {
        first_key_int( "count", qcnt );
        next_key_int( "ns-acc", qns );
      } end_key_dict;
      begin_next_key_dict( "engine" ) {
        begin_first_key_dict( "exceptions" ) {
          first_key_int( "warning", nWarning );
          next_key_int( "error", nError );
          next_key_int( "critical", nCritical );
        } end_key_dict;
      } end_key_dict;
    } end_key_dict;
  } end_json_static;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_objcnt( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;

  int64_t master_serial = iSystem.MasterSerial();
  int64_t order = 0;
  int64_t size = 0;
  int64_t properties = 0;
  int64_t vectors = 0;

  GRAPH_FACTORY_ACQUIRE {
    vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
    if( graphs ) {
      vgx_Graph_t **cursor = graphs;
      vgx_Graph_t *graph;
      while( (graph = *cursor++) != NULL ) {
        order += GraphOrder( graph );
        size += GraphSize( graph );
        properties += GraphPropCount( graph );
        vectors += GraphVectorCount( graph );
      }
      free( (void*)graphs );
    }
  } GRAPH_FACTORY_RELEASE;

  begin_json_static( response, 1024, '{' ) {
    first_key_iqt( "master-serial", master_serial );
    next_key_int( "order", order );
    next_key_int( "size", size );
    next_key_int( "properties", properties );
    next_key_int( "vectors", vectors );
  } end_json_static;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_status( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;
  vgx_VGXServerPerfCounters_t counters = {0};
  CString_t *CSTR__now = NULL;
  CString_t *CSTR__service_name = NULL;
  int timeout_ms = 10000;

  XTRY {
    // Generate perf counters
    if( iVGXServer.Counters.Get( SYSTEM, &counters, timeout_ms ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Front/Matrix config
    uint16_t port = 0;
    int width = 0;
    int height = 0;
    bool allow_incomplete = false;
    iVGXServer.Config.Get( SYSTEM->vgxserverA, &port, &width, &height, &allow_incomplete );
    bool has_matrix = width > 0 && height > 0;
    bool is_proxy = width == 1;
    int64_t matrix_backlog_size = iVGXServer.Dispatcher.Matrix.BacklogSize( SYSTEM->vgxserverA );
    int64_t matrix_backlog_count = iVGXServer.Dispatcher.Matrix.BacklogCount( SYSTEM->vgxserverA );
    int64_t nopen_channels = 0;
    int64_t nmax_channels = 0;
    if( DISPATCHER_MATRIX_ENABLED( SYSTEM->vgxserverA ) ) {
      vgx_VGXServerDispatcherMatrix_t *matrix = &SYSTEM->vgxserverA->matrix;
      nopen_channels = matrix->partition.nopen_channels_MCS;
      nmax_channels = matrix->partition.nmax_channels_MCS;
    }

    const char *service_name;
    if( (CSTR__service_name = iVGXServer.Service.GetName( SYSTEM )) != NULL ) {
      service_name = CStringValue( CSTR__service_name );
    }
    else {
      service_name = "(interactive)";
    }

    // Compute latency percentiles
    typedef struct __s_bucket {
      float pctX;
      double latency;
    } __bucket;

    __bucket buckets[] = {
      { 0.500f, 0.0 },
      { 0.900f, 0.0 },
      { 0.950f, 0.0 },
      { 0.990f, 0.0 },
      { 0.999f, 0.0 },
      {0}
    };

    __bucket *percentile = buckets;
    while( percentile->pctX > 0.0 ) {
      iVGXServer.Counters.GetLatencyPercentile( &counters, percentile->pctX, &percentile->latency, NULL );
      ++percentile;
    }


    begin_json_static( response, 1024, '{' ) {

      /* "name" : "MyService"
      */
      first_key_str( "name", service_name );

      /* "data" : { 
          "in":  4237523,
          "out": 65262273
        }
      */
      begin_next_key_dict( "data" ) {
        first_key_int( "in", counters.bytes_in );
        next_key_int( "out", counters.bytes_out );
      } end_key_dict;

      // "connected_clients": 5
      next_key_int( "connected_clients", counters.connected_clients );

      // "total_clients": 171
      next_key_int( "total_clients", counters.total_clients );

      /*  "request": {
            "rate": 123.45,
            "serving": 1,
            "waiting": 13,
            "working": 8,
            "executors": 8,
            "count": 54321,
            "plugin": 52067
          }
      */
      begin_next_key_dict( "request" ) {
        first_key_dbl( "rate", counters.average_rate_short, 2 );
        next_key_int( "serving", counters.service_in );
        next_key_int( "waiting", counters.sz_dispatch );
        next_key_int( "working", counters.sz_working );
        next_key_int( "executors", counters.n_executors );
        next_key_int( "count", counters.request_count_total );
        next_key_int( "plugin", counters.request_count_plugin );
      } end_key_dict;

      /*  "dispatcher": {
            "enabled": true,
            "front-port": 9010,
            "matrix-width": 1,
            "matrix-height": 5,
            "matrix-active-channels": 17,
            "matrix-total-channels": 40,
            "matrix-allow-incomplete": false,
            "matrix-backlog": 0,
            "matrix-backlog-count": 0,
            "proxy": true
          }
      */
      begin_next_key_dict( "dispatcher" ) {
        first_key_bol( "enabled", has_matrix );
        next_key_int( "front-port", port );
        next_key_int( "matrix-width", width );
        next_key_int( "matrix-height", height );
        next_key_int( "matrix-active-channels", nopen_channels );
        next_key_int( "matrix-total-channels", nmax_channels );
        next_key_bol( "matrix-allow-incomplete", allow_incomplete );
        next_key_int( "matrix-backlog", matrix_backlog_size );
        next_key_int( "matrix-backlog-count", matrix_backlog_count );
        next_key_bol( "proxy", is_proxy );
      } end_key_dict;

      /*  "errors" : {
            "service" : 4123,
            "http"    : 11,
            "rate"    : 0.1,
            "codes" : {
              "400" : 3,
              "405" : 1,
              "413" : 1,
              "414" : 2,
              "429" : 3,
              "500" : 1
            }
          }
      */
      begin_next_key_dict( "errors" ) {
        first_key_int( "service", counters.error_count.service );
        next_key_int( "http", counters.error_count.http );
        next_key_dbl( "rate", counters.error_rate, 2 );
        begin_next_key_dict( "codes" ) {
          const uint16_t *pcode = counters.__http_400_527;
          const uint16_t *end_code = counters.__http_400_527 + 128;
          int code = 400;
          int freq = 0;
          bool first = true;
          while( pcode < end_code ) {
            if( (freq = *pcode++) != 0 ) {
              if( first ) {
                out_txt( "\"" );
                first = false;
              }
              else {
                out_txt( ", \"" );
              }
              out_int( code );
              out_txt( "\": " );
              out_int( freq );
            }
            ++code;
          }
        } end_key_dict;
      } end_key_dict;

      /*  "response_ms": {
            "mean": 0.73,
            "50.0": 0.28,
            "90.0": 0.36,
            "95.0": 1.57,
            "99.0": 8.5,
            "99.9": 12.84
          }
      */
      begin_next_key_dict( "response_ms" ) {
        first_key_dbl( "mean", 1000.0 * counters.average_duration_short, 2 );
        percentile = buckets;
        while( percentile->pctX > 0.0 ) {
          out_txt( ", \"" );
          out_dbl( 100.0 * percentile->pctX, 1 );
          out_txt( "\": " );
          out_dbl( 1000.0 * percentile->latency, 2 );
          ++percentile;
        }
      } end_key_dict;

    } end_json_static;
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__now );
    iString.Discard( &CSTR__service_name );
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_txstat( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;
  
  int64_t master_serial = 0;
  vgx_OperationCounters_t tx_in;
  vgx_OperationCounters_t tx_out;
  int64_t tx_lag_ms = 0;
  float tx_in_rate = 0.0;
  float tx_out_rate = 0.0;
  bool tx_in_halted = false;
  bool tx_out_halted = false;


  vgx_OperationFeedRates_t limits = {0};

  GRAPH_LOCK( SYSTEM ) {
    master_serial = iSystem.MasterSerial();
    tx_in = SYSTEM->OP.system.in_counters_CS;
    tx_out = SYSTEM->OP.system.out_counters_CS;
    tx_lag_ms = SYSTEM->tx_input_lag_ms_CS;
    tx_in_rate = SYSTEM->OP.system.running_tx_input_rate_CS;
    tx_out_rate = SYSTEM->OP.system.running_tx_output_rate_CS;
    tx_in_halted = iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) > 0;
    tx_out_halted = iOperation.System_SYS_CS.IsAgentSuspended( SYSTEM ) > 0;
    limits = *SYSTEM->OP.system.in_feed_limits_CS;
  } GRAPH_RELEASE;



  begin_json_static( response, 1024, '{' ) {


    /* 
       "data" : {
         "bytes" : {
           "throttle" : 10000000.0,
           "in"       : 123456,
           "out"      : 654321
         },
         "rate" : {
           "in" : 9949.0,
           "out" : 0
         },
         "opcodes" : {
           "throttle" : 1000000.0,
           "in"       : 12345,
           "out"      : 54321
         },
         "operations" : {
           "throttle" : 100000.0,
           "in"       : 1234,
           "out"      : 4321
         },
         "transactions" : {
           "throttle" : 10000.0,
           "in"       : 123,
           "out"      : 321
         }
       },
       "halted" : {
         "in"  : false,
         "out" : false
       },
       "input-lag" : 5113,
       "master-serial": "1688785967139285"
    */

    begin_first_key_dict( "data" ) {
      begin_first_key_dict( "bytes" ) {
        first_key_dbl( "throttle", 1000.0 * limits.bpms, 1 );
        next_key_int( "in", tx_in.n_bytes );
        next_key_int( "out", tx_out.n_bytes );
      } end_key_dict;
      begin_next_key_dict( "rate" ) {
        first_key_dbl( "in", tx_in_rate, 0 );
        next_key_dbl( "out", tx_out_rate, 0 );
      } end_key_dict;
      begin_next_key_dict( "opcodes" ) {
        first_key_dbl( "throttle", 1000.0 * limits.cpms, 1 );
        next_key_int( "in", tx_in.n_opcodes );
        next_key_int( "out", tx_out.n_opcodes );
      } end_key_dict;
      begin_next_key_dict( "operations" ) {
        first_key_dbl( "throttle", 1000.0 * limits.opms, 1 );
        next_key_int( "in", tx_in.n_operations );
        next_key_int( "out", tx_out.n_operations );
      } end_key_dict;
      begin_next_key_dict( "transactions" ) {
        first_key_dbl( "throttle", 1000.0 * limits.tpms, 1 );
        next_key_int( "in", tx_in.n_transactions );
        next_key_int( "out", tx_out.n_transactions );
      } end_key_dict;
    } end_key_dict;
    begin_next_key_dict( "halted" ) {
      first_key_bol( "in", tx_in_halted );
      next_key_bol( "out", tx_out_halted );
    } end_key_dict;
    next_key_int( "input-lag", tx_lag_ms );
    next_key_iqt( "master-serial", master_serial );

  } end_json_static;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_peerstat( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;

  CString_t *CSTR__service_name = iVGXServer.Service.GetName( SYSTEM );
  const char *service_name = CSTR__service_name ? CStringValue( CSTR__service_name ) : "(interactive)";

  CString_t *CSTR__provider = iSystem.AttachedInput();

  CString_t *CSTR__local_fqdn = iURI.NewFqdn();
  const char *local_fqdn = CStringValueDefault( CSTR__local_fqdn, "" );
  char *local_ip = NULL;
  const char *ip = NULL;
  int adminport = -1;

  vgx_StringList_t *descriptions = NULL;
  vgx_StringList_t *outputs = iSystem.AttachedOutputs( &descriptions, 250 );

  bool txlog = false;
  int txport = 0;
  bool events = igraphfactory.EventsEnabled();
  bool events_running = events;
  if( events ) {
    if( igraphfactory.EventsResumable() > 0 ) {
      events_running = false;
    }
  }

  bool readonly = igraphfactory.CountAllReadonly() > 0;

  bool executing = false;
  bool synchronizing = false;
  int sync_progress = 100;
  int64_t master_serial = 0;

  objectid_t durable_tx = {0};
  int64_t durable_sn = 0;
  int64_t durable_ts = 0;
  int n_serializing = 0;
  igraphfactory.DurabilityPoint( &durable_tx, &durable_sn, &durable_ts, &n_serializing );
  
  char provider_digest[33] = {0};
  GRAPH_LOCK( SYSTEM ) {
    ip = iVGXServer.Service.GetAdminIP( SYSTEM );
    adminport = iVGXServer.Service.GetAdminPort(SYSTEM);
    vgx_TransactionalConsumerService_t *consumer_service = SYSTEM->OP.system.consumer;
    if( consumer_service ) {
      txlog = consumer_service->CSTR__tx_location != NULL;
      txport = consumer_service->bind_port;
      executing = !iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM );
      idtostr( provider_digest, &consumer_service->provider.fingerprint );
    }
    if( (synchronizing = SYSTEM->OP.system.state_CS.flags.sync_in_progress) == true ) {
      double obj_counter = (double)SYSTEM->OP.system.progress_CS->obj_counter;
      double obj_total = (double)SYSTEM->OP.system.progress_CS->obj_total;
      if( obj_total > 0.0 && obj_counter < obj_total ) {
        sync_progress = (int)round( 100.0 * obj_counter / obj_total );
      }
    }
    master_serial = iSystem.MasterSerial();
  } GRAPH_RELEASE;

  if( ip == NULL ) {
    if( (ip = local_ip = cxgetip( NULL )) == NULL ) {
      ip = "0.0.0.0";
    }
  }

  objectid_t fingerprint = igraphfactory.Fingerprint();
  char digest[33] = {0};
  idtostr( digest, &fingerprint );
  int64_t idle_ms = igraphfactory.FingerprintAge();


  size_t sz_buf = 1536;
  if( outputs ) {
    int64_t sz = iString.List.Size( outputs );
    for( int64_t i=0; i<sz; i++ ) {
      const CString_t *CSTR__subscriber = iString.List.GetItem( outputs, i );
      const CString_t *CSTR__description = iString.List.GetItem( descriptions, i );
      if( CSTR__subscriber ) {
        sz_buf += CStringLength( CSTR__subscriber );
      }
      if( CSTR__description ) {
        sz_buf += CStringLength( CSTR__description );
      }
      sz_buf += 32;
    }
  }

  try_json_dynamic( response, sz_buf, '{' ) {

    /* 
       "name" : "MyService",
       "host" : "somewhere.local",
       "ip": 192.168.1.100",
       "adminport" : 9081,
       "port" : 10081,
       "events" : false,
       "events-running" : true,
       "readonly" : false,
       "durable" : true,
       "executing" : true,
       "digest" : "0348a020e49760fc382f1569552eff5f",
       "master-serial": "1688785967139285",
       "idle-ms": 5782,
       "persist-age" : "0:07:13:45",
       "persist-ts" : 1669316305,
       "persisting" : 0,
       "synchronizing" : 1,
       "sync-progress" : 83,
       "provider" : "vgx://192.168.1.101:10081",
       "provider-digest" : "0348a020e49760fc382f1569552eff5f",
       "subscribers" : [
         ["vgx://192.168.1.201:10081", {
            "host": "192.168.1.201",
            "ip": "192.168.1.201",
            "adminport": 9001,
            "txport": 10081,
            "mode": "NORMAL",
            "status": "CONNECTED",
            "digest": "0348a020e49760fc382f1569552eff5f",
            "master-serial": "1688785967139285",
            "lag": {
              "ms": 0,
              "tx": 0
            }
          }],
         ["vgx://192.168.1.202:10081", {...}],
         ["vgx://192.168.1.203:10081", {...}]
       ]
    */

    first_key_str( "name", service_name );

    next_key_str( "host", local_fqdn );
    next_key_str( "ip", ip );
    next_key_int( "adminport", adminport );
    next_key_int( "port", txport );
    next_key_bol( "events", events );
    next_key_bol( "events-running", events_running );
    next_key_bol( "readonly", readonly );
    next_key_bol( "durable", txlog );
    next_key_bol( "executing", executing );
    next_key_str( "digest", digest );
    next_key_iqt( "master-serial", master_serial );
    next_key_int( "idle-ms", idle_ms );
    begin_next_key_direct( "persist-age" ) {
      if( durable_ts > 0 ) {
        int64_t persist_age = (int64_t)__SECONDS_SINCE_1970() - durable_ts;
        int nc = __endpoint__format_duration( (int)persist_age, out_ptr_name, 16 );
        if( nc > 0 ) {
          out_ptr_name += nc;
        }
      }
      else {
        out_txt( "-:--:--:--" );
      }
    } end_key_direct;
    next_key_int( "persist-ts", durable_ts );
    next_key_int( "persisting", n_serializing  );
    next_key_int( "synchronizing", synchronizing  );
    next_key_int( "sync-progress", sync_progress  );
    if( CSTR__provider ) {
      next_key_str( "provider", CStringValue( CSTR__provider ) );
    }
    else {
      next_key_null( "provider" );
    }
    next_key_str( "provider-digest", provider_digest );
    begin_next_key_array( "subscribers" ) {
      if( outputs ) {
        int64_t sz = iString.List.Size( outputs );
        int64_t dsz = descriptions ? iString.List.Size( descriptions ) : 0;
        bool first = true;
        for( int64_t i=0; i<sz; i++ ) {
          const char *subscriber = iString.List.GetChars( outputs, i );
          const char *info = "";
          if( i < dsz ) {
            info = iString.List.GetChars( descriptions, i );
          }
          if( first ) {
            out_txt( "[\"" );
            first = false;
          }
          else {
            out_txt( ", [\"" );
          }
          out_txt( subscriber );
          out_txt( "\", " );
          out_txt( info );
          out_txt( "]");
        }
      }
    } end_key_array;
  } catch_json_dynamic {
    ret = -1;
  } end_json_dynamic;

  iString.Discard( &CSTR__provider );
  iString.List.Discard( &outputs );
  iString.List.Discard( &descriptions );
  iString.Discard( &CSTR__service_name );
  iString.Discard( &CSTR__local_fqdn );
  free( local_ip );

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_meminfo( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;

  XTRY {
    int64_t total_physical = 0;
    int64_t current_use_pct = 0;
    int64_t current_process_use = 0;

    if( get_system_physical_memory( &total_physical, &current_use_pct, &current_process_use ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }

    vgx_Graph_t *SYSTEM = server->sysgraph;
    GRAPH_LOCK( SYSTEM ) {
      if( current_process_use > server->counters.mem_max_process_use ) {
        server->counters.mem_max_process_use = current_process_use;
      }
      if( current_use_pct > (int64_t)server->counters.byte.mem_max_use_pct ) {
        uint8_t p = (uint8_t)(current_use_pct & 0xFF);
        server->counters.byte.mem_max_use_pct = p > 100 ? 100 : p;
      }
    } GRAPH_RELEASE;

    int64_t current_available = (int64_t)((1.0 - (current_use_pct / 100.0)) * total_physical);
    int64_t min_available = (int64_t)((1.0 - ((double)server->counters.byte.mem_max_use_pct / 100.0)) * total_physical);

    begin_json_static( response, 1024, '{' ) {
      begin_first_key_dict( "memory" ) { 
        first_key_int( "total", total_physical );
        begin_next_key_dict( "current" ) {
          first_key_int( "available", current_available );
          next_key_int( "process", current_process_use );
        } end_key_dict;
        begin_next_key_dict( "worst-case" ) {
          first_key_int( "available", min_available );
          next_key_int( "process", server->counters.mem_max_process_use );
        } end_key_dict;
      } end_key_dict;
    } end_json_static;
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_nodestat( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = server->sysgraph;

  // (ready)
  bool ready = true;
  // host
  const char *local_host = server->Listen ? iURI.NameInfo( server->Listen ) : "?";
  // ips
  char **local_ips = cxgetip_list( NULL );
  // ip
  const char *local_ip = NULL;
  // (adminport)
  int adminport = -1;
  // (port)
  int txport = 0;
  // (uptime)
  int uptime = 0;
  // (pid)
  uint32_t pid = GET_CURRENT_PROCESS_ID();
  // service-name
  CString_t *CSTR__service_name = iVGXServer.Service.GetName( SYSTEM );
  const char *service_name = CStringValueDefault( CSTR__service_name, NULL );
  // service-label
  char service_label[33] = {0};
  objectid_t unique_label = igraphfactory.UniqueLabel();
  idtostr( service_label, &unique_label );
  // digest
  char digest[33] = {0};
  objectid_t fingerprint = igraphfactory.Fingerprint();
  idtostr( digest, &fingerprint );
  // (master-serial)
  int64_t master_serial = 0;
  // idle-ms
  int64_t idle_ms = igraphfactory.FingerprintAge();
  // (synchronizing)
  bool synchronizing = false;
  // (sync-progress)
  int sync_progress = 100;
  // provider
  CString_t *CSTR__provider = iSystem.AttachedInput();
  // subscribers
  vgx_StringList_t *subscribers = iSystem.AttachedOutputsSimple();
  // (tx-halted-in)
  bool tx_in_halted = false;
  // (tx-halted-out)
  bool tx_out_halted = false;
  // (tx-in-rate)
  float tx_in_rate = 0.0;
  // (tx-out-rate)
  float tx_out_rate = 0.0;
  // (graph-order)
  int64_t order = 0;
  // (graph-size)
  int64_t size = 0;
  // (graph-properties)
  int64_t properties = 0;
  // (graph-vectors)
  int64_t vectors = 0;
  // (graph-nrel)
  int64_t nrel = 0;
  // (graph-nvtx)
  int64_t nvtx = 0;
  // (graph-ndim)
  int64_t ndim = 0;
  // (graph-nkey)
  int64_t nkey = 0;
  // (graph-nval)
  int64_t nval = 0;
  // matrix: width
  int width = 0;
  // matrix: height
  int height = 0;
  // matrix: allow-incomplete
  bool allow_incomplete = false;
  iVGXServer.Config.Get( SYSTEM->vgxserverA, NULL, &width, &height, &allow_incomplete );
  // (matrix: active-channels)
  int active_channels = 0;
  // (matrix: total-channels)
  int total_channels = 0;
  // (service-in)
  int service_in = 0;
  // (service-rate)
  double service_rate = -1.0;
  // (service-95th-ms)
  double service_95th_ms = -1.0;
  // (connected-clients)
  int64_t connected_clients = 0;
  // cpu
  CString_t *CSTR__cpu = iVGXProfile.CPU.GetBrandString();
  const char *CPU = CStringValueDefault( CSTR__cpu, "" );
  // (memory-total)
  int64_t memory_total = 0;
  // (memory-process)
  int64_t memory_process = 0;
  // memory-total
  // memory-process
  int64_t __current_use_pct = 0;
  get_system_physical_memory( &memory_total, &__current_use_pct, &memory_process );
  // memory-available
  int64_t memory_available = (int64_t)((1.0 - (__current_use_pct / 100.0)) * memory_total);
  // durable
  bool durable = iOperation.System_OPEN.ConsumerService.IsDurable(SYSTEM);
  // (durable-txlog)
  int64_t durable_txlog = 0;
  // (durable-max-txlog)
  int64_t durable_max_txlog = 0;
  // snapshot-writing
  objectid_t __durable_tx = {0};
  int64_t __durable_sn = 0;
  int64_t __durable_ts = 0;
  int __n_serializing = 0;
  igraphfactory.DurabilityPoint( &__durable_tx, &__durable_sn, &__durable_ts, &__n_serializing );
  bool snapshot_writing = __n_serializing > 0;
  // snapshot-age
  int64_t snapshot_age = (__MILLISECONDS_SINCE_1970() / 1000) - __durable_ts;
  // readonly
  bool readonly = igraphfactory.CountAllReadonly() > 0;
  // (local-only)
  int local_only = 0;

  GRAPH_LOCK( SYSTEM ) {
    // ip
    if( (local_ip = iVGXServer.Service.GetAdminIP( SYSTEM )) == NULL ) {
      if( local_ips && *local_ips ) {
        local_ip = *local_ips;
      }
      else {
        local_ip = "0.0.0.0";
      }
    }
    // adminport
    adminport = iVGXServer.Service.GetAdminPort(SYSTEM);
    // txport
    vgx_TransactionalConsumerService_t *consumer_service = SYSTEM->OP.system.consumer;
    if( consumer_service ) {
      txport = consumer_service->bind_port;
      if( txport > 0 && iOperation.System_OPEN.ConsumerService.IsInitializing(SYSTEM) ) {
        ready = false;
      }
      // durable-txlog
      durable_txlog = consumer_service->tx_log_total_sz_SYS_CS;
      // durable-max-txlog
      durable_max_txlog = consumer_service->snapshot.tx_log_threshold_SYS_CS;
    }
    // master-serial
    master_serial = iSystem.MasterSerial();
    // synchronizing
    // sync-progress
    if( (synchronizing = SYSTEM->OP.system.state_CS.flags.sync_in_progress) == true ) {
      double obj_counter = (double)SYSTEM->OP.system.progress_CS->obj_counter;
      double obj_total = (double)SYSTEM->OP.system.progress_CS->obj_total;
      if( obj_total > 0.0 && obj_counter < obj_total ) {
        sync_progress = (int)round( 100.0 * obj_counter / obj_total );
      }
    }
    // tx-in-halted
    tx_in_halted = iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) > 0;
    // tx-out-halted
    tx_out_halted = iOperation.System_SYS_CS.IsAgentSuspended( SYSTEM ) > 0;
    // tx-in-rate
    tx_in_rate = SYSTEM->OP.system.running_tx_input_rate_CS;
    // tx-out-rate
    tx_out_rate = SYSTEM->OP.system.running_tx_output_rate_CS;
  } GRAPH_RELEASE;

  GRAPH_FACTORY_ACQUIRE {
    vgx_Graph_t **graphs = (vgx_Graph_t**)igraphfactory.ListGraphs( NULL );
    if( graphs ) {
      vgx_Graph_t **cursor = graphs;
      vgx_Graph_t *graph;
      while( (graph = *cursor++) != NULL ) {
        GRAPH_LOCK( graph ) {
          if( _vgx_graph_is_local_only_CS( graph ) ) {
            ++local_only;
          }
          // Basic objects
          order += GraphOrder( graph );
          size += GraphSize( graph );
          properties += GraphPropCount( graph );
          vectors += GraphVectorCount( graph );
          // Enumerators
          if( graph->similarity && graph->similarity->parent == graph ) {
            nrel += iEnumerator_CS.Relationship.Count( graph );
            nvtx += iEnumerator_CS.VertexType.Count( graph );
            if( !igraphfactory.EuclideanVectors() ) {
              ndim += iEnumerator_CS.Dimension.Count( graph->similarity );
            }
          }
          nkey += iEnumerator_CS.Property.Key.Count( graph );
          nval += iEnumerator_CS.Property.Value.Count( graph );
        } GRAPH_RELEASE;
      }
      free( (void*)graphs );
    }
  } GRAPH_FACTORY_RELEASE;

  // Generate perf counters
  vgx_VGXServerPerfCounters_t counters = {0};
  if( iVGXServer.Counters.Get( SYSTEM, &counters, 1000 ) == 0 ) {
    // uptime
    uptime = (int)(counters.server_uptime_ns / 1000000000);
    // service-in
    service_in = counters.service_in;
    // service-rate
    service_rate = counters.average_rate_short;
    // service-95th-ms
    iVGXServer.Counters.GetLatencyPercentile( &counters, 0.950f, &service_95th_ms, NULL );
    // connected-clients
    connected_clients = counters.connected_clients;
  }
  
  if( DISPATCHER_MATRIX_ENABLED( SYSTEM->vgxserverA ) ) {
    vgx_VGXServerDispatcherMatrix_t *matrix = &SYSTEM->vgxserverA->matrix;
    SYNCHRONIZE_ON( matrix->lock ) {
      // matrix: active-channels
      active_channels = matrix->partition.nopen_channels_MCS;
      // matrix: total-channels
      total_channels = matrix->partition.nmax_channels_MCS;
    } RELEASE;
  }

  // Compute size of buffer
  size_t sz_buf = 1536;
  if( subscribers ) {
    int64_t nsubs = iString.List.Size( subscribers );
    for( int64_t i=0; i<nsubs; i++ ) {
      const CString_t *CSTR__subscriber = iString.List.GetItem( subscribers, i );
      if( CSTR__subscriber ) {
        sz_buf += CStringLength( CSTR__subscriber );
      }
      sz_buf += 32;
    }
  }

  try_json_dynamic( response, sz_buf, '{' ) {

    /* { 
       "ready": true,
       "host": "something.local",
       "ip": 192.168.1.100",
       "ips": [
         "10.10.1.115",
         "192.168.1.100"
       ],
       "adminport" : 9081,
       "txport" : 10081,
       "uptime" : 1994812,
       "pid" : 1234,
       "service-name" : "My Service 1",
       "service-label" : "cf9af3b43af1839cf0138ca8bd1bc830",
       "digest" : "0348a020e49760fc382f1569552eff5f",
       "master-serial": "1688785967139285",
       "idle-ms": 5782,
       "synchronizing" : 1,
       "sync-progress" : 83,
       "subscribers" : [
          {
            "ip": "192.168.1.201",
            "adminport": 9001,
            "status": "CONNECTED",
            "lag_ms": 0
          },
          {...}
       ],
       "tx-in-halted": true,
       "tx-out-halted": false,
       "tx-in-rate": 0,
       "tx-out-rate": 654321,
       "graph-order": 1234567,
       "graph-size": 2345678,
       "graph-properties": 5678,
       "graph-vectors": 277892,
       "graph-nrel": 4,
       "graph-nvtx": 2,
       "graph-ndim": 0,
       "graph-nkey": 16,
       "graph-nval": 4451,
       "matrix": {
          "mode": "dispatch"
          "width": 5,
          "height": 2,
          "active-channels": 27,
          "total-channels": 160,
          "allow-incomplete"
       },
       "service-in": 1,
       "service-rate": 125.1,
       "service-95th-ms": 5.7,
       "connected-clients": 24,
       "cpu": "Intel(R) Xeon(R) W-2255 CPU @ 3.70GHz",
       "memory-total": 4000000000,
       "memory-process": 3000000000,
       "memory-available": 1000000000,
       "durable": true,
       "durable-txlog": 5571273,
       "durable-max-txlog": 2147483648,
       "snapshot-writing": false,
       "snapshot-age": 55616,
       "readonly": false,
       "local-only": 0
       }
    */
    first_key_bol( "ready", ready );
    next_key_str( "host", local_host );
    next_key_str( "ip", local_ip );
    begin_next_key_array( "ips" ) {
      if( local_ips && *local_ips ) {
        const char **ip = (const char**)local_ips;
        bool first = true;
        while( *ip ) {
          if( first ) {
            first_str( *ip );
            first = false;
          }
          else {
            next_str( *ip );
          }
          ++ip;
        }
      }
    } end_key_array;
    next_key_int( "adminport", adminport );
    next_key_int( "txport", txport );
    next_key_int( "uptime", uptime );
    next_key_int( "pid", pid );
    if( service_name ) {
      next_key_str( "service-name", service_name );
    }
    else {
      next_key_null( "service-name" );
    }
    next_key_str( "service-label", service_label );
    next_key_str( "digest", digest );
    next_key_iqt( "master-serial", master_serial );
    next_key_int( "idle-ms", idle_ms );
    next_key_int( "synchronizing", synchronizing );
    next_key_int( "sync-progress", sync_progress );
    if( CSTR__provider ) {
      next_key_str( "provider", CStringValue( CSTR__provider ) );
    }
    else {
      next_key_null( "provider" );
    }
    begin_next_key_array( "subscribers" ) {
      if( subscribers ) {
        int64_t nsubs = iString.List.Size( subscribers );
        for( int64_t i=0; i<nsubs; i++ ) {
          const char *subscriber = iString.List.GetChars( subscribers, i );
          if( i > 0 ) {
            out_txt( ", " );
          }
          out_txt( subscriber );
        }
      }
    } end_key_array;
    next_key_bol( "tx-in-halted", tx_in_halted );
    next_key_bol( "tx-out-halted", tx_out_halted );
    next_key_dbl( "tx-in-rate", tx_in_rate, 0 );
    next_key_dbl( "tx-out-rate", tx_out_rate, 0 );
    next_key_int( "graph-order", order );
    next_key_int( "graph-size", size );
    next_key_int( "graph-properties", properties );
    next_key_int( "graph-vectors", vectors );
    next_key_int( "graph-nrel", nrel );
    next_key_int( "graph-nvtx", nvtx );
    next_key_int( "graph-ndim", ndim );
    next_key_int( "graph-nkey", nkey );
    next_key_int( "graph-nval", nval );
    begin_next_key_dict( "matrix" ) {
      first_key( "mode" );
      if( width > 0 && height > 0 ) {
        if( width == 1 ) {
          first_str( "proxy" );
        }
        else {
          first_str( "dispatch" );
        }
      }
      else {
        out_txt( "\"engine\"" );
      }
      out_txt( ", \"width\": " );
      out_int( width );
      out_txt( ", \"height\": " );
      out_int( height );
      out_txt( ", \"active-channels\": " );
      out_int( active_channels );
      out_txt( ", \"total-channels\": " );
      out_int( total_channels );
      out_txt( ", \"allow-incomplete\": " );
      out_bol( allow_incomplete );
    } end_key_dict;
    next_key_int( "service-in", service_in );
    next_key_dbl( "service-rate", service_rate, 2 );
    next_key_dbl( "service-95th-ms", 1000.0 * service_95th_ms, 2 );
    next_key_int( "connected-clients", connected_clients );
    next_key_str( "cpu", CPU );
    next_key_int( "memory-total", memory_total );
    next_key_int( "memory-process", memory_process );
    next_key_int( "memory-available", memory_available );
    next_key_bol( "durable", durable );
    next_key_int( "durable-txlog", durable_txlog );
    next_key_int( "durable-max-txlog", durable_max_txlog );
    next_key_bol( "snapshot-writing", snapshot_writing );
    next_key_int( "snapshot-age", snapshot_age );
    next_key_bol( "readonly", readonly );
    next_key_int( "local-only", local_only );
  } catch_json_dynamic {
    ret = -1;
  } end_json_dynamic;


  iString.Discard( &CSTR__cpu );
  iString.Discard( &CSTR__service_name );
  iString.Discard( &CSTR__provider );
  iString.List.Discard( &subscribers );
  if( local_ips ) {
    char **ip = local_ips;
    while( *ip ) {
      free(*ip);
      ++ip;
    }
  }
  free( local_ips );

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_matrix( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();


  /*
  [
    [
      { "ip": "127.0.0.1", "port": 9110, "channels": 32, "priority": 1, "primary": true },
      { "ip": "127.0.0.1", "port": 9120, "channels": 32, "priority": 1 },
      { "ip": "127.0.0.1", "port": 9130, "channels": 32, "priority": 1 }
    ],
    [
      { "ip": "127.0.0.1", "port": 9210, "channels": 32, "priority": 1, "primary": true },
      { "ip": "127.0.0.1", "port": 9220, "channels": 32, "priority": 1 },
      { "ip": "127.0.0.1", "port": 9230, "channels": 32, "priority": 1 }
    ]
  ]
  */

  int n_hosts = 0;
  int sz = 128;

  vgx_VGXServerConfig_t *cf = iVGXServer.Config.Clone( SYSTEM->vgxserverA );
  if( cf == NULL ) {
    return -1;
  }

  if( cf->dispatcher ) {
    n_hosts = (int)cf->dispatcher->shape.width * (int)cf->dispatcher->shape.height;
  }

  sz += 128 * n_hosts; // plenty

  try_json_dynamic( response, sz, '[' ) {

    if( cf->dispatcher ) {
      vgx_VGXServerDispatcherConfig_t *d = cf->dispatcher;
      for( int w=0; w<d->shape.width; w++ ) {
        if( w > 0 ) {
          open_next_array();
        }
        else {
          open_array();
        }
        vgx_VGXServerDispatcherConfigPartition_t *p = &d->partitions[w];
        for( int h=0; h<d->shape.height; h++ ) {
          if( h > 0 ) {
            open_next_dict();
          }
          else {
            open_dict();
          }

          vgx_VGXServerDispatcherConfigReplica_t *r = &p->replicas[h];
          if( r->uri ) {
            first_key_str( "ip", iURI.HostIP( r->uri ) );
          }
          else {
            first_key_null( "ip" );
          }
          next_key_int( "port", r->port );
          next_key_int( "channels", r->settings.channels );
          next_key_int( "priority", r->settings.priority );
          if( r->settings.flag.writable ) {
            next_key_bol( "primary", true );
          }
          close_dict();
        }
        close_array();
      }
    }

  } catch_json_dynamic {
    ret = -1;
  } end_json_dynamic;

  iVGXServer.Config.Delete( &cf );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_dispatch( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();
  vgx_VGXServer_t *serverA = SYSTEM->vgxserverA;
  vgx_VGXServer_t *serverB = SYSTEM->vgxserverB;

 
  /*
  {
    "A": {
      "dispatch": {
        "dispatched": 548,
        "completed": 547,
        "signals": 547
      },
      "executor": {
        "0": 156,
        "1": 157,
        "2": 129,
        "3": 106
      },
      "matrix": {
        "width": 2,
        "height": 3,
        "active-channels": 19,
        "total-channels": 96,
        "allow-incomplete": false,
        "proxy": false,
        "partitions": [
          [
            { "host": "h1.1", "port": 9110, "channels": 32, "priority": 1 },
            { "host": "h1.2", "port": 9120, "channels": 32, "priority": 1 },
            { "host": "h1.3", "port": 9130, "channels": 32, "priority": 1 }
          ],
          [
            { "host": "h2.1", "port": 9210, "channels": 32, "priority": 1 },
            { "host": "h2.2", "port": 9220, "channels": 32, "priority": 1 },
            { "host": "h2.3", "port": 9230, "channels": 32, "priority": 1 }
          ]
        ]
      }
    },
    "B": {
      "dispatch": {
        "dispatched": 548,
        "completed": 547,
        "signals": 547
      },
      "executor": {
        "0": 156,
        "1": 157,
        "2": 129,
        "3": 106
      }
    }
  }
  */

  int bsz = 1024;
  vgx_VGXServerConfig_t *main_cf = iVGXServer.Config.Clone( serverA );
  if( main_cf && main_cf->dispatcher ) {
    int n_hosts = (int)main_cf->dispatcher->shape.width * (int)main_cf->dispatcher->shape.height;
    bsz += 128 * n_hosts; // plenty
  }
  iVGXServer.Config.Delete( &main_cf );

  try_json_dynamic( response, bsz, '{' ) {


    vgx_VGXServer_t *servers[] = { serverA, serverB, NULL };
    vgx_VGXServer_t **cursor = servers;
    vgx_VGXServer_t *s;
    char ident[] = "A";
    while( (s = *cursor++) != NULL ) {

      int64_t *executor_count = NULL;
      double *executor_busy = NULL;
      int sz_wpool = 0;

      vgx_VGXServerWorkDispatch_t *dispatch = &s->dispatch;
      vgx_VGXServerExecutorCompletion_t *completion = &dispatch->completion;
      int64_t n_dispatched;
      vgx_VGXServerExecutorPool_t *pool = s->pool.executors;
      if( pool ) {
        sz_wpool = pool->sz;
        executor_count = calloc( sz_wpool, sizeof(int64_t) );
        executor_busy = calloc( sz_wpool, sizeof(double) );
      }
      // 
      vgx_VGXServerConfig_t *cf = iVGXServer.Config.Clone( s );

      n_dispatched = dispatch->n_total; // unlocked, updated by server loop

      if( executor_count ) {
        vgx_VGXServerExecutor_t **pexecutor = pool->executors;
        vgx_VGXServerExecutor_t *exec;
        for( int i=0; i<sz_wpool; i++, pexecutor++ ) {
          if( (exec = *pexecutor) == NULL ) {
            continue;
          }
          executor_count[i] = ATOMIC_READ_i64( &exec->count_atomic );
        }
      }

      int64_t nopen_channels = 0;
      int64_t nmax_channels = 0;
      if( cf && cf->dispatcher && DISPATCHER_MATRIX_ENABLED( s ) ) {
        vgx_VGXServerDispatcherMatrix_t *matrix = &s->matrix;
        SYNCHRONIZE_ON( matrix->lock ) {
          nopen_channels = matrix->partition.nopen_channels_MCS;
          nmax_channels = matrix->partition.nmax_channels_MCS;
        } RELEASE;
      }

      int64_t n_completed;
      int64_t n_signals;
      SYNCHRONIZE_ON( completion->lock ) {
        n_completed = completion->completion_count;
        n_signals = completion->n_blocked_poll_signals;
      } RELEASE;

      if( executor_busy ) {
        vgx_VGXServerExecutor_t **executor = pool->executors;
        for( int i=0; i<sz_wpool; i++, executor++ ) {
          vgx_VGXServerExecutor_t *exec = *executor;
          if( exec && exec->TASK ) {
            double exec_idle = COMLIB_TASK__LifetimeIdle( exec->TASK );
            executor_busy[i] = 100.0 * (1.0 - exec_idle);
          }
        }
      }

      if( *ident == 'A' ) {
        first_str( ident );
      }
      else {
        next_str( ident );
      }
      out_txt( ": " );
      open_dict();
      begin_first_key_dict( "dispatch" ) {
        first_key_int( "dispatched", n_dispatched );
        next_key_int( "completed", n_completed );
        next_key_int( "signals", n_signals );
      } end_key_dict;
      begin_next_key_dict( "executor" ) {
        if( executor_count && executor_busy ) {
          for( int i=0; i<sz_wpool; i++ ) {
            // "n": [
            if( i == 0 ) {
              out_txt( "\"" );
            }
            else {
              out_txt( ", \"" );
            }
            out_int( i );
            out_txt( "\": [" );
            // 123, 17.3]
            out_int( executor_count[i] );
            out_txt( ", " );
            out_dbl( executor_busy[i], 1 );
            out_txt( "]" );
          }
        }
      } end_key_dict;
      if( cf && cf->dispatcher ) {
        begin_next_key_dict( "matrix" ) {
          first_key_int( "width", cf->dispatcher->shape.width );
          next_key_int( "height", cf->dispatcher->shape.height );
          next_key_int( "active-channels", nopen_channels );
          next_key_int( "total-channels", nmax_channels );
          next_key_bol( "allow-incomplete", cf->dispatcher->allow_incomplete );
          next_key_bol( "proxy", cf->dispatcher->shape.width == 1 );
          begin_next_key_array( "partitions" ) {
            vgx_VGXServerDispatcherConfig_t *d = cf->dispatcher;
            for( int w=0; w<d->shape.width; w++ ) {
              if( w > 0 ) {
                open_next_array();
              }
              else {
                open_array();
              }
              vgx_VGXServerDispatcherConfigPartition_t *p = &d->partitions[w];
              for( int h=0; h<d->shape.height; h++ ) {
                if( h > 0 ) {
                  open_next_dict();
                }
                else {
                  open_dict();
                }
                vgx_VGXServerDispatcherConfigReplica_t *r = &p->replicas[h];
                if( r->uri ) {
                  first_key_str( "host", iURI.HostIP( r->uri ) );
                }
                else {
                  first_key_null( "host" );
                }
                next_key_int( "port", r->port );
                next_key_int( "channels", r->settings.channels );
                next_key_int( "priority", r->settings.priority );
                if( r->settings.flag.writable ) {
                  next_key_bol( "primary", true );
                }
                close_dict();
              }
              close_array();
            }
          } end_key_array;
        } end_key_dict;
      }
      close_dict();

      ++(*ident);
      free( executor_count );
      free( executor_busy );
      iVGXServer.Config.Delete( &cf );
    }

  } catch_json_dynamic {
    ret = -1;
  } end_json_dynamic;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_inspect( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {

  int ret = 0;
  vgx_Graph_t *SYSTEM = iSystem.GetSystemGraph();

  int64_t level = 0;
  vgx_KeyVal_t *cursor = params->keyval;
  vgx_KeyVal_t *kv_end = cursor + params->sz;
  while( cursor < kv_end ) {
    vgx_KeyVal_t *kv = cursor++;
    const char *key = kv->key;
    vgx_value_t val = kv->val;
    if( CharsEqualsConst( key, "level" ) ) {
      if( val.type == VGX_VALUE_TYPE_STRING ) {
        char *e = NULL;
        int64_t q = strtoll( val.data.simple.string, &e, 0 );
        if( !e || *e == '\0' ) {
          level = q;
        }
      }
      else if( val.type == VGX_VALUE_TYPE_INTEGER ) {
        level = val.data.simple.integer;
      }
    }
  }

  int64_t executor_job_count[A_EXECUTOR_POOL_SIZE] = {0}; // max pool sizxe

  vgx_VGXServerExecutorPool_t *pool = server->pool.executors;

  for( int i=0; i<pool->sz; i++ ) {
    vgx_VGXServerExecutor_t *exec = pool->executors[i];
    executor_job_count[i] = ATOMIC_READ_i64( &exec->count_atomic );
  }

  int port_offset = server->config.cf_server->front->port.offset;

  vgx_VGXServerMatrixInspect_t *inspect = iVGXServer.Counters.GetMatrixInspect( SYSTEM, port_offset, 100 );

  if( inspect == NULL ) {
    return -1;
  }

  size_t bsz = 1024ULL;

  int n_clients = inspect->n_clients;
  for( int i=0; i<n_clients; i++ ) {
    vgx_VGXServerClientInspect_t *client_inspect = &inspect->clients[i];
    // base client data
    bsz += 1024ULL;
    // path
    bsz += strlen( client_inspect->request.path );
    // headers
    const char *hdata = client_inspect->request.headers.data;
    bsz += 2*strnlen( hdata, client_inspect->request.headers._end - hdata );
    // channels
    bsz += 512ULL * client_inspect->n_channels;
  }

  char buf[1024];

#define include_level( Level ) if( level >= (Level) )

  try_json_dynamic( response, bsz, '{' ) {

    first_key_int( "n-connections", n_clients );
    // "connections": [...]
    begin_next_key_array( "connections" ) {
      for( int i=0; i<n_clients; i++ ) {
        vgx_VGXServerClientInspect_t *client_inspect = &inspect->clients[i];
        if( i > 0 ) {
          open_next_dict();
        }
        else {
          open_dict();
        }

        int64_t dt = client_inspect->delta_t_ns;
        int64_t t0 = client_inspect->io_t0_ns;
        int64_t t1 = client_inspect->io_t1_ns;
        int64_t xt = t1 > t0 ? (t1 - t0) : dt;
        const char *client_state = __vgx_server_client_state( client_inspect->request.state );
        if( client_state == NULL || __vgx_server_client_state_error( client_inspect->request.state ) ) {
          write_term( write_HEX_dword( buf, client_inspect->request.state ) );
          client_state = buf;
        }

        // "client": {...}
        begin_first_key_dict( "client" ) {
          first_key_int( "id", client_inspect->id );
          next_key_str( "state", client_state );
          include_level(2) {
            next_key_str( "host", iURI.Host( client_inspect->uri ) );
            next_key_str( "host-ip", iURI.HostIP( client_inspect->uri ) );
            next_key_int( "port", iURI.PortInt( client_inspect->uri ) );
            next_key_dbl( "age-ms", dt / 1e6, 3 );
          }
          include_level(3) {
            next_key_int( "socket-in", client_inspect->socket.s );
          }
        } end_key_dict;

        // "request": {...}
        begin_next_key_dict( "request" ) {
          first_key_dbl( "exec-ms", xt / 1e6, 3 );
          include_level(1) {
            next_key_str( "method", __vgx_http_request_method( client_inspect->request.method ) );
            next_key_str( "path", client_inspect->request.path );
          }
          include_level(3) {
            next_key_int( "t0-ns", client_inspect->io_t0_ns );
            next_key_int( "t1-ns", client_inspect->io_t1_ns );
            next_key_int( "sn", client_inspect->request.headers.sn );
            idtostr( buf, &client_inspect->request.headers.signature );
            next_key_str( "signature", buf );
          }
        } end_key_dict;

        // "partial": {...}
        include_level(2) {
          begin_next_key_dict( "partial" ) {
            x_vgx_partial_ident ident = client_inspect->partial_ident;
            if( ident.defined ) {
              begin_first_key_dict( "ident" ) {
                snprintf( buf, 1023, "%d.%d.%d", (int)ident.matrix.width, (int)ident.matrix.height, (int)ident.matrix.depth );
                first_key_str( "matrix", buf );
                snprintf( buf, 1023, "%d.%d.%d", (int)ident.position.partition, (int)ident.position.replica, (int)ident.position.channel );
                next_key_str( "position", buf );
                next_key_bol( "primary", ident.position.primary );
              } end_key_dict;
            }
            else {
              first_key_null( "ident" );
            }
          } end_key_dict;
        }

        // "executor": {...}
        include_level(2) {
          int exec_id = client_inspect->request.executor_id;
          begin_next_key_dict( "executor" ) {
            first_key_int( "id", exec_id );
            if( exec_id >=0 && exec_id < A_EXECUTOR_POOL_SIZE ) {
              int64_t job_count = executor_job_count[ exec_id ];
              next_key_str( "status", "assigned" );
              next_key_int( "job-count", job_count );
            }
            else {
              next_key_null( "status");
              next_key_null( "job-count");
            }
          } end_key_dict;
        }

        // "headers": { ... }
        include_level(2) {
          begin_next_key_dict( "headers" ) {
            first_key_str( "content-type", __get_http_mediatype( client_inspect->request.content_type ) );
            next_key_str( "accept-type", __get_http_mediatype( client_inspect->request.accept_type ) );
            next_key_int( "content-length", client_inspect->request.headers.content_length );
            next_key_int( "n-headers", client_inspect->request.headers.sz );
            // "fields": {...}
            include_level(3) {
              begin_next_key_dict( "fields" ) {
                const char *hdata = client_inspect->request.headers.data;
                char *e = buf + 1022;
                for( int h=0; h<client_inspect->request.headers.sz; h++ ) {
                  // field name
                  char *p = buf;
                  char c;
                  while( (c=*hdata++) != ':' && c != '\0' && p < e ) {
                    *p++ = c;
                  }
                  *p = '\0';

                  if( h == 0 ) {
                    first_str_key( buf );
                  }
                  else {
                    next_str_key( buf );
                  }

                  // skip space
                  while( *hdata == ' ' ) {
                    ++hdata;
                  }

                  // field value
                  p = buf;
                  while( *hdata != '\r' && *hdata != '\0' && p < e ) {
                    c = *hdata++;
                    if( c == '"' || c == '\\' ) {
                      *p++ = '\\';
                    }
                    *p++ = c;
                  }
                  *p = '\0';
                  first_str( buf );

                  // skip to next header
                  while( *hdata != '\0' ) {
                    if( *hdata == '\r' && *(hdata+1) == '\n' ) {
                      hdata += 2;
                      break;
                    }
                  }

                  // last header
                  if( *hdata == '\0' ) {
                    break;
                  }

                }
              } end_key_dict;
            }
          } end_key_dict;
        }

        // "matrix": [...]
        include_level(1) {
          begin_next_key_dict( "matrix" ) {
            first_key_int( "target-partial", client_inspect->request.target_partial );
            next_key_int( "n-channels", client_inspect->n_channels );
            // "channels": [...]
            include_level(2) {
              begin_next_key_array( "channels" ) {
                for( int ch=0; ch<client_inspect->n_channels; ch++ ) {
                  vgx_VGXServerChannelInspect_t *channel_inspect = &client_inspect->channels[ch];
                  if( ch > 0 ) {
                    open_next_dict();
                  }
                  else {
                    open_dict();
                  }

                  snprintf( buf, 1023, "%d.%d.%d", (int)channel_inspect->partition, (int)channel_inspect->replica, (int)channel_inspect->channel );

                  first_key_str( "matrix-id", buf );
                  const char *channel_state = __vgx_server_channel_state( channel_inspect->state );
                  if( channel_state == NULL || __vgx_server_channel_state_error( channel_inspect->state ) ) {
                    write_term( write_HEX_dword( buf, channel_inspect->state ) );
                    channel_state = buf;
                  }
                  next_key_str( "state", channel_state );
                  next_key_int( "cost", channel_inspect->cost );
                  include_level(3) {
                    next_key_int( "socket-out", channel_inspect->socket.s );
                    next_key_int( "bytes-sent", channel_inspect->n_bytes_sent );
                    next_key_int( "bytes-recv", channel_inspect->n_bytes_recv );
                    next_key_int( "total-requests", channel_inspect->n_requests );
                  }
                  close_dict();
                }
              } end_key_array;
            }
          } end_key_dict;
        }

        close_dict();
      }
    } end_key_array;

  } catch_json_dynamic {
    ret = -1;
  } end_json_dynamic;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_randstr( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {
  response->mediatype = MEDIA_TYPE__text_plain;
  vgx_server_response__prepare_body( response );

  char buf[65];
  int64_t n = (int64_t)(rand64() & 0x7ff) + 1;
  while( n-- > 0 ) {
    int64_t x64 = rand64();
    sha256_t h256 = sha256_len( (char*)&x64, 8 );
    char *p = buf;
    bytestohex( &p, (BYTE*)h256.str, 32 );
    iStreamBuffer.Write( response->buffers.content, buf, 32 );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_randint( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {
  response->mediatype = MEDIA_TYPE__text_plain;
  vgx_server_response__prepare_body( response );

  char buf[17];
  char *p = buf;
  int64_t x = rand64();
  bytestohex( &p, (BYTE*)&x, 8 );
  iStreamBuffer.Write( response->buffers.content, buf, 16 );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __endpoint__service_unknown( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response ) {
  begin_json_static( response, 64, '{' ) {
    first_key_str( "message", "unknown service" );
  } end_json_static;
  return 0;
}
