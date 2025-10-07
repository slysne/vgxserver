/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxserver.h
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

#ifndef _VXSERVER_H
#define _VXSERVER_H

#include "vxapiservice.h"



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_HTTPRequestHeaderField {
  HTTP_REQUEST_HEADER__END_OF_HEADERS                 = 0,
  HTTP_REQUEST_HEADER_IGNORED                         = 1,
  HTTP_REQUEST_HEADER_ERROR                           = 9,
  __HTTP_REQUEST_HEADER_BEGIN_STANDARD_FIELDS         = 1000,
  HTTP_REQUEST_HEADER_FIELD__A_IM,
  HTTP_REQUEST_HEADER_FIELD__Accept,
  HTTP_REQUEST_HEADER_FIELD__AcceptCharset,
  HTTP_REQUEST_HEADER_FIELD__AcceptDatetime,
  HTTP_REQUEST_HEADER_FIELD__AcceptEncoding,
  HTTP_REQUEST_HEADER_FIELD__AcceptLanguage,
  HTTP_REQUEST_HEADER_FIELD__AccessControlRequestMethod,
  HTTP_REQUEST_HEADER_FIELD__AccessControlRequestHeaders,
  HTTP_REQUEST_HEADER_FIELD__Authorization,
  HTTP_REQUEST_HEADER_FIELD__CacheControl,
  HTTP_REQUEST_HEADER_FIELD__Connection,
  HTTP_REQUEST_HEADER_FIELD__ContentEncoding,
  HTTP_REQUEST_HEADER_FIELD__ContentLength,
  HTTP_REQUEST_HEADER_FIELD__ContentMD5,
  HTTP_REQUEST_HEADER_FIELD__ContentType,
  HTTP_REQUEST_HEADER_FIELD__Cookie,
  HTTP_REQUEST_HEADER_FIELD__Date,
  HTTP_REQUEST_HEADER_FIELD__Expect,
  HTTP_REQUEST_HEADER_FIELD__Forwarded,
  HTTP_REQUEST_HEADER_FIELD__From,
  HTTP_REQUEST_HEADER_FIELD__Host,
  HTTP_REQUEST_HEADER_FIELD__HTTP2Settings,
  HTTP_REQUEST_HEADER_FIELD__IfMatch,
  HTTP_REQUEST_HEADER_FIELD__IfModifiedSince,
  HTTP_REQUEST_HEADER_FIELD__IfNoneMatch,
  HTTP_REQUEST_HEADER_FIELD__IfRange,
  HTTP_REQUEST_HEADER_FIELD__IfUnmodifiedSince,
  HTTP_REQUEST_HEADER_FIELD__MaxForwards,
  HTTP_REQUEST_HEADER_FIELD__Origin,
  HTTP_REQUEST_HEADER_FIELD__Pragma,
  HTTP_REQUEST_HEADER_FIELD__Prefer,
  HTTP_REQUEST_HEADER_FIELD__ProxyAuthorization,
  HTTP_REQUEST_HEADER_FIELD__Range,
  HTTP_REQUEST_HEADER_FIELD__Referer,
  HTTP_REQUEST_HEADER_FIELD__TE,
  HTTP_REQUEST_HEADER_FIELD__Trailer,
  HTTP_REQUEST_HEADER_FIELD__TransferEncoding,
  HTTP_REQUEST_HEADER_FIELD__UserAgent,
  HTTP_REQUEST_HEADER_FIELD__Upgrade,
  HTTP_REQUEST_HEADER_FIELD__Via,
  HTTP_REQUEST_HEADER_FIELD__Warning,
  __HTTP_REQUEST_HEADER_BEGIN_NONSTANDARD_FIELDS      = 2000,
  HTTP_REQUEST_HEADER_FIELD__UpgradeInsecureRequests,
  HTTP_REQUEST_HEADER_FIELD__XRequestedWith,
  HTTP_REQUEST_HEADER_FIELD__DNT,
  HTTP_REQUEST_HEADER_FIELD__XForwardedFor,
  HTTP_REQUEST_HEADER_FIELD__XForwardedHost,
  HTTP_REQUEST_HEADER_FIELD__XForwardedProto,
  HTTP_REQUEST_HEADER_FIELD__FrontEndHttps,
  HTTP_REQUEST_HEADER_FIELD__XHttpMethodOverride,
  HTTP_REQUEST_HEADER_FIELD__XATTDeviceId,
  HTTP_REQUEST_HEADER_FIELD__XWapProfile,
  HTTP_REQUEST_HEADER_FIELD__ProxyConnection,
  HTTP_REQUEST_HEADER_FIELD__XUIDH,
  HTTP_REQUEST_HEADER_FIELD__XCsrfToken,
  HTTP_REQUEST_HEADER_FIELD__XRequestID,
  HTTP_REQUEST_HEADER_FIELD__XCorrelationID,
  HTTP_REQUEST_HEADER_FIELD__SaveData,
  __HTTP_REQUEST_HEADER_BEGIN_VGX_FIELDS              = 3000,
  HTTP_REQUEST_HEADER_FIELD__XVgxPartialTarget,
  HTTP_REQUEST_HEADER_FIELD__XVgxBuiltinExecutor,
  HTTP_REQUEST_HEADER_FIELD__XVgxBypassSOUT,
  HTTP_REQUEST_HEADER_FIELD__XVgxRsv4,
  HTTP_REQUEST_HEADER_FIELD__XVgxRsv5,
  HTTP_REQUEST_HEADER_FIELD__XVgxRsv6,
  HTTP_REQUEST_HEADER_FIELD__XVgxRsv7,
  HTTP_REQUEST_HEADER_FIELD__XVgxRsv8

} vgx_HTTPRequestHeaderField;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_HTTPResponseHeaderField {
  HTTP_RESPONSE_HEADER__END_OF_HEADERS                 = 0,
  HTTP_RESPONSE_HEADER_IGNORED                         = 1,
  __HTTP_RESPONSE_HEADER_BEGIN_STANDARD_FIELDS         = 1000,
  HTTP_RESPONSE_HEADER_FIELD__AcceptCH,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlAllowOrigin,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlAllowCredentials,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlExposeHeaders,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlMaxAge,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlAllowMethods,
  HTTP_RESPONSE_HEADER_FIELD__AccessControlAllowHeaders,
  HTTP_RESPONSE_HEADER_FIELD__AcceptPatch,
  HTTP_RESPONSE_HEADER_FIELD__Age,
  HTTP_RESPONSE_HEADER_FIELD__Allow,
  HTTP_RESPONSE_HEADER_FIELD__AltSvc,
  HTTP_RESPONSE_HEADER_FIELD__CacheControl,
  HTTP_RESPONSE_HEADER_FIELD__Connection,
  HTTP_RESPONSE_HEADER_FIELD__ContentDisposition,
  HTTP_RESPONSE_HEADER_FIELD__ContentEncoding,
  HTTP_RESPONSE_HEADER_FIELD__ContentLanguage,
  HTTP_RESPONSE_HEADER_FIELD__ContentLength,
  HTTP_RESPONSE_HEADER_FIELD__ContentLocation,
  HTTP_RESPONSE_HEADER_FIELD__ContentMD5,
  HTTP_RESPONSE_HEADER_FIELD__ContentRange,
  HTTP_RESPONSE_HEADER_FIELD__ContentType,
  HTTP_RESPONSE_HEADER_FIELD__Date,
  HTTP_RESPONSE_HEADER_FIELD__DeltaBase,
  HTTP_RESPONSE_HEADER_FIELD__ETag,
  HTTP_RESPONSE_HEADER_FIELD__Location,
  HTTP_RESPONSE_HEADER_FIELD__P3P,
  HTTP_RESPONSE_HEADER_FIELD__Pragma,
  HTTP_RESPONSE_HEADER_FIELD__PreferenceApplied,
  HTTP_RESPONSE_HEADER_FIELD__ProxyAuthenticate,
  HTTP_RESPONSE_HEADER_FIELD__PublicKeyPins,
  HTTP_RESPONSE_HEADER_FIELD__RetryAfter,
  HTTP_RESPONSE_HEADER_FIELD__Server,
  HTTP_RESPONSE_HEADER_FIELD__SetCookie,
  HTTP_RESPONSE_HEADER_FIELD__StrictTransportSecurity,
  HTTP_RESPONSE_HEADER_FIELD__Trailer,
  HTTP_RESPONSE_HEADER_FIELD__TransferEncoding,
  HTTP_RESPONSE_HEADER_FIELD__Tk,
  HTTP_RESPONSE_HEADER_FIELD__Upgrade,
  HTTP_RESPONSE_HEADER_FIELD__Vary,
  HTTP_RESPONSE_HEADER_FIELD__Via,
  HTTP_RESPONSE_HEADER_FIELD__Warning,
  HTTP_RESPONSE_HEADER_FIELD__WWWAuthenticate,
  HTTP_RESPONSE_HEADER_FIELD__XFrameOptions,
  __HTTP_RESPONSE_HEADER_BEGIN_NONSTANDARD_FIELDS      = 2000,
  HTTP_RESPONSE_HEADER_FIELD__ContentSecurityPolicy,
  HTTP_RESPONSE_HEADER_FIELD__XContentSecurityPolicy,
  HTTP_RESPONSE_HEADER_FIELD__XWebKitCSP,
  HTTP_RESPONSE_HEADER_FIELD__ExpectCT,
  HTTP_RESPONSE_HEADER_FIELD__NEL,
  HTTP_RESPONSE_HEADER_FIELD__PermissionsPolicy,
  HTTP_RESPONSE_HEADER_FIELD__Refresh,
  HTTP_RESPONSE_HEADER_FIELD__ReportTo,
  HTTP_RESPONSE_HEADER_FIELD__Status,
  HTTP_RESPONSE_HEADER_FIELD__TimingAllowOrigin,
  HTTP_RESPONSE_HEADER_FIELD__XContentDuration,
  HTTP_RESPONSE_HEADER_FIELD__XContentTypeOptions,
  HTTP_RESPONSE_HEADER_FIELD__XPoweredBy,
  HTTP_RESPONSE_HEADER_FIELD__XRedirectBy,
  HTTP_RESPONSE_HEADER_FIELD__XRequestID,
  HTTP_RESPONSE_HEADER_FIELD__XCorrelationID,
  HTTP_RESPONSE_HEADER_FIELD__XUACompatible,
  HTTP_RESPONSE_HEADER_FIELD__XXSSProtection,
  __HTTP_RESPONSE_HEADER_BEGIN_VGX_FIELDS              = 3000,
  HTTP_RESPONSE_HEADER_FIELD__XVgxBacklog,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv2,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv3,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv4,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv5,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv6,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv7,
  HTTP_RESPONSE_HEADER_FIELD__XVgxRsv8

} vgx_HTTPResponseHeaderField;




// 2xx
static const char HTTP_STATUS_REASON__OK[] = "OK";

// 4xx
static const char HTTP_STATUS_REASON__BadRequest[]              = "Bad Request";
static const char HTTP_STATUS_REASON__NotFound[]                = "Not Found";
static const char HTTP_STATUS_REASON__MethodNotAllowed[]        = "Method Not Allowed";
static const char HTTP_STATUS_REASON__RequestTimeout[]          = "Request Timeout";
static const char HTTP_STATUS_REASON__PayloadTooLarge[]         = "Payload Too Large";
static const char HTTP_STATUS_REASON__URITooLong[]              = "URI Too Long";
static const char HTTP_STATUS_REASON__TooManyRequests[]         = "Too Many Requests";

// 5xx
static const char HTTP_STATUS_REASON__InternalServerError[]     = "Internal Server Error";
static const char HTTP_STATUS_REASON__NotImplemented[]          = "Not Implemented";
static const char HTTP_STATUS_REASON__ServiceUnavailable[]      = "Service Unavailable";
static const char HTTP_STATUS_REASON__HTTPVersionNotSupported[] = "HTTP Version Not Supported";
static const char HTTP_STATUS_REASON__NoResponse[]              = "No Response";
static const char HTTP_STATUS_REASON__RequestHeaderTooLarge[]   = "Request Header Too Large";



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const vgx_HTTPStatus_t http_status_tuples[] = {
  // 2xx
  { .code = HTTP_STATUS__OK,                      .reason = HTTP_STATUS_REASON__OK },

  // 4xx
  { .code = HTTP_STATUS__BadRequest,              .reason = HTTP_STATUS_REASON__BadRequest },
  { .code = HTTP_STATUS__NotFound,                .reason = HTTP_STATUS_REASON__NotFound },
  { .code = HTTP_STATUS__MethodNotAllowed,        .reason = HTTP_STATUS_REASON__MethodNotAllowed },
  { .code = HTTP_STATUS__RequestTimeout,          .reason = HTTP_STATUS_REASON__RequestTimeout },
  { .code = HTTP_STATUS__PayloadTooLarge,         .reason = HTTP_STATUS_REASON__PayloadTooLarge },
  { .code = HTTP_STATUS__URITooLong,              .reason = HTTP_STATUS_REASON__URITooLong },
  { .code = HTTP_STATUS__TooManyRequests,         .reason = HTTP_STATUS_REASON__TooManyRequests },

  // 5xx
  { .code = HTTP_STATUS__InternalServerError,     .reason = HTTP_STATUS_REASON__InternalServerError },
  { .code = HTTP_STATUS__NotImplemented,          .reason = HTTP_STATUS_REASON__NotImplemented },
  { .code = HTTP_STATUS__ServiceUnavailable,      .reason = HTTP_STATUS_REASON__ServiceUnavailable },
  { .code = HTTP_STATUS__HTTPVersionNotSupported, .reason = HTTP_STATUS_REASON__HTTPVersionNotSupported },
  { .code = HTTP_STATUS__NoResponse,              .reason = HTTP_STATUS_REASON__NoResponse },
  { .code = HTTP_STATUS__RequestHeaderTooLarge,   .reason = HTTP_STATUS_REASON__RequestHeaderTooLarge },

  { .code = HTTP_STATUS__NONE,                    .reason = NULL }

};


static const vgx_HTTPStatus_t default_http_error_status = { .code = HTTP_STATUS__InternalServerError, .reason = HTTP_STATUS_REASON__InternalServerError };



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__get_http_response_reason( HTTPStatus code ) {
  const vgx_HTTPStatus_t *s = http_status_tuples;
  while( s->reason ) {
    if( s->code == code ) {
      return s->reason;
    }
    ++s;
  }
  return default_http_error_status.reason;
}


#define HTTP_MEDIA_TYPE__charset_UTF8  "charset=UTF-8"

#define vgx_HTTP_charset "; " HTTP_MEDIA_TYPE__charset_UTF8

static const char MEDIA_TYPE_STRING__application_x_vgx_partial[]  = "application/x-vgx-partial" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__application_octet_stream[]   = "application/octet-stream" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__application_javascript[]     = "application/javascript" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__application_pdf[]            = "application/pdf" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__application_xml[]            = "application/xml" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__application_json[]           = "application/json" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__text_plain[]                 = "text/plain" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__text_css[]                   = "text/css" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__text_html[]                  = "text/html" vgx_HTTP_charset;
static const char MEDIA_TYPE_STRING__image_ANY[]                  = "image/*";
static const char MEDIA_TYPE_STRING__image_ico[]                  = "image/ico";
static const char MEDIA_TYPE_STRING__image_gif[]                  = "image/gif";
static const char MEDIA_TYPE_STRING__image_png[]                  = "image/png";
static const char MEDIA_TYPE_STRING__image_jpg[]                  = "image/jpg";



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const vgx_MediaTypeMap_t http_mediatype_tuples[] = {
  { .type = MEDIA_TYPE__application_x_vgx_partial,  .text = MEDIA_TYPE_STRING__application_x_vgx_partial },
  { .type = MEDIA_TYPE__application_octet_stream,   .text = MEDIA_TYPE_STRING__application_octet_stream },
  { .type = MEDIA_TYPE__application_json,           .text = MEDIA_TYPE_STRING__application_json },
  { .type = MEDIA_TYPE__application_xml,            .text = MEDIA_TYPE_STRING__application_xml },
  { .type = MEDIA_TYPE__text_plain,                 .text = MEDIA_TYPE_STRING__text_plain },
  { .type = MEDIA_TYPE__text_html,                  .text = MEDIA_TYPE_STRING__text_html },
  { .type = MEDIA_TYPE__text_css,                   .text = MEDIA_TYPE_STRING__text_css },
  { .type = MEDIA_TYPE__application_javascript,     .text = MEDIA_TYPE_STRING__application_javascript },
  { .type = MEDIA_TYPE__application_pdf,            .text = MEDIA_TYPE_STRING__application_pdf },
  { .type = MEDIA_TYPE__image_ANY,                  .text = MEDIA_TYPE_STRING__image_ANY },
  { .type = MEDIA_TYPE__image_ico,                  .text = MEDIA_TYPE_STRING__image_ico },
  { .type = MEDIA_TYPE__image_gif,                  .text = MEDIA_TYPE_STRING__image_gif },
  { .type = MEDIA_TYPE__image_png,                  .text = MEDIA_TYPE_STRING__image_png },
  { .type = MEDIA_TYPE__image_jpg,                  .text = MEDIA_TYPE_STRING__image_jpg },
  { .type = MEDIA_TYPE__NONE,                       .text = NULL }
};


static const vgx_MediaTypeMap_t default_http_mediatype = { .type = MEDIA_TYPE__text_plain, .text = MEDIA_TYPE_STRING__text_plain };



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static const char *__get_http_mediatype( vgx_MediaType mtype ) {
  const vgx_MediaTypeMap_t *t = http_mediatype_tuples;
  while( t->text ) {
    if( t->type == mtype ) {
      return t->text;
    }
    ++t;
  }
  return default_http_mediatype.text;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_SubscriberSynchronizer_t {

  // [Q1.1]
  comlib_task_t *TASK;

  vgx_Graph_t *sysgraph;

  struct {
    struct {
      int perform_sync;
      int terminate_on_completion;
      int perform_cleanup;
    } command;
    struct {
      int ready_to_sync;
      int performing_sync;
      int sync_complete;
    } state;
  } stage_TCS;

  bool sync_hard;
  bool resume_tx_input;

  vgx_StringList_t *subscribers_URIs;

  CString_t *CSTR__new_subscriber_uri;
} vgx_SubscriberSynchronizer_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_server_artifact_t {
  const char *name;
  QWORD namehash;

  vgx_MediaType MediaType;

  struct {
    const char **data;
  } raw;

  struct {
    BYTE *bytes;
    int64_t sz;
    bool public;
  } servable;


} vgx_server_artifact_t;



DLL_HIDDEN extern framehash_t * vgx_server_artifacts__map;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef enum e__plugin_type {
    //                                              
    //                                           ||||
    __VGX_SERVER_PLUGIN_TYPE__PLUG_MASK      = 0x0010,
    __VGX_SERVER_PLUGIN_TYPE__EXEC_MASK      = 0x0100,
    __VGX_SERVER_PLUGIN_TYPE__LIST_MASK      = 0x0200,
    //                                           ||||
    VGX_SERVER_PLUGIN_TYPE__NONE             = 0x0000,
    VGX_SERVER_PLUGIN_TYPE__CUSTOM_PREFIX    = 0x0111,
    VGX_SERVER_PLUGIN_TYPE__USER             = 0x0112,
    VGX_SERVER_PLUGIN_TYPE__BUILTIN          = 0x0113,
    VGX_SERVER_PLUGIN_TYPE__LIST_USER        = 0x0204,
    VGX_SERVER_PLUGIN_TYPE__LIST_BUILTIN     = 0x0205
} vgx_server_plugin_type;





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_pathspec{
  char _resource[ SZ_VGX_RESOURCE_PATH_BUFFER + 1 ];
  char *presource;
  vgx_VGXServer_t *server;
  const char *path;
  const char *prefix;
  int sz_prefix;
  vgx_server_plugin_type type;
} vgx_server_pathspec_t;





// executor
DLL_HIDDEN extern vgx_VGXServerExecutorPool_t *   vgx_server_executor__new_pool( vgx_VGXServer_t *server, int sz_pool );
DLL_HIDDEN extern int                             vgx_server_executor__start_all( vgx_VGXServer_t *server );
DLL_HIDDEN extern void                            vgx_server_executor__delete_pool( vgx_VGXServer_t *server, vgx_VGXServerExecutorPool_t **executor_pool );

// endpoint
DLL_HIDDEN extern void                          vgx_server_endpoint__init( void );
DLL_HIDDEN extern int                           vgx_server_endpoint__service( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerResponse_t *response, const char *name);

// resource
DLL_HIDDEN extern int                           vgx_server_resource__init( void );
DLL_HIDDEN extern void                          vgx_server_resource__clear( void );
DLL_HIDDEN extern int                           vgx_server_resource__plugin_init( void );
DLL_HIDDEN extern void                          vgx_server_resource__plugin_clear( void );
DLL_HIDDEN extern int                           vgx_server_resource__add_plugin( const char *plugin_name, vgx_server_plugin_phase phase );
DLL_HIDDEN extern int                           vgx_server_resource__del_plugin( const char *plugin_name );
DLL_HIDDEN extern uint8_t                       vgx_server_resource__get_plugin_phases( const char *plugin_name );
DLL_HIDDEN extern int                           vgx_server_resource__has_pre_plugin( const char *plugin_name );
DLL_HIDDEN extern int                           vgx_server_resource__has_exec_plugin( const char *plugin_name );
DLL_HIDDEN extern int                           vgx_server_resource__has_post_plugin( const char *plugin_name );
DLL_HIDDEN extern const char *                  vgx_server_resource__plugin_name( vgx_server_pathspec_t *pathspec );
DLL_HIDDEN extern vgx_server_pathspec_t *       vgx_server_resource__init_pathspec( vgx_server_pathspec_t *pathspec, vgx_VGXServer_t *server, vgx_VGXServerRequest_t *request );
DLL_HIDDEN extern int                           vgx_server_resource__call( vgx_VGXServer_t *server, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error );

// sync
DLL_HIDDEN extern int                           vgx_server_sync__subscribe_and_sync( vgx_Graph_t *SYSTEM, vgx_URIQueryParameters_t *params, vgx_StreamBuffer_t *request_content, vgx_StreamBuffer_t *response_body, vgx_MediaType mediatype, CString_t **CSTR__error );

// counters
DLL_HIDDEN extern int                             vgx_server_counters__get_bucket_by_duration( double duration );
DLL_HIDDEN extern void                            vgx_server_counters__delete_matrix_inspect( vgx_VGXServerMatrixInspect_t **inspect );
DLL_HIDDEN extern vgx_VGXServerMatrixInspect_t *  vgx_server_counters__new_matrix_inspect( vgx_VGXServer_t *server );
DLL_HIDDEN extern vgx_VGXServerPerfCounters_t *   vgx_server_counters__new( void );
DLL_HIDDEN extern void                            vgx_server_counters__delete( vgx_VGXServerPerfCounters_t **counters );
DLL_HIDDEN extern const vgx_VGXServerPerfCounters_t * vgx_server_counters__take_snapshot( vgx_VGXServerPerfCounters_t *counters );
DLL_HIDDEN extern void                            vgx_server_counters__measure( vgx_VGXServer_t *server, int64_t now_ns, bool reset );
DLL_HIDDEN extern void                            vgx_server_counters__inspect_TCS( vgx_VGXServer_t *server );
DLL_HIDDEN extern int                             vgx_server_counters__get( vgx_Graph_t *SYSTEM, vgx_VGXServerPerfCounters_t *counters, int timeout_ms );
DLL_HIDDEN extern int                             vgx_server_counters__reset( vgx_Graph_t *SYSTEM );
DLL_HIDDEN extern int                             vgx_server_counters__get_latency_percentile( const vgx_VGXServerPerfCounters_t *counters, float pctX, double *pctX_short, double *pctX_long );
DLL_HIDDEN extern vgx_VGXServerMatrixInspect_t *  vgx_server_counters__inspect_matrix( vgx_Graph_t *SYSTEM, int port_offset, int timeout_ms );

// artifacts
DLL_HIDDEN extern int                           vgx_server_artifacts__create( const char *prefix );
DLL_HIDDEN extern void                          vgx_server_artifacts__destroy( void );

// parser
DLL_HIDDEN extern int                           vgx_server_parser__parse_request_initial_line( vgx_VGXServerRequest_t *request, const char *line );
DLL_HIDDEN extern vgx_HTTPRequestHeaderField    vgx_server_parser__parse_request_header_line( vgx_VGXServer_t *server, vgx_VGXServerRequest_t *request, const char *line );
DLL_HIDDEN extern int                           vgx_server_parser__parse_response_initial_line( const char *line, vgx_VGXServerResponse_t *response );
DLL_HIDDEN extern vgx_HTTPResponseHeaderField   vgx_server_parser__parse_response_header_line( const char *line, vgx_VGXServerResponse_t *response );

// io
DLL_HIDDEN extern int                           vgx_server_io__front_send( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void                          vgx_server_io__process_socket_events( vgx_VGXServer_t *server, int64_t max_ns );

// request
DLL_HIDDEN extern void                          vgx_server_request__dump( const vgx_VGXServerRequest_t *request, const char *fatal_message );
DLL_HIDDEN extern int                           vgx_server_request__handle( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void                          vgx_server_request__copy_clean( vgx_VGXServerRequest_t *dest, const vgx_VGXServerRequest_t *src );
DLL_HIDDEN extern void                          vgx_server_request__copy_all( vgx_VGXServerRequest_t *dest, const vgx_VGXServerRequest_t *src );
DLL_HIDDEN extern vgx_VGXServerRequest_t *      vgx_server_request__new( HTTPRequestMethod method, const char *path );
DLL_HIDDEN extern void                          vgx_server_request__delete( vgx_VGXServerRequest_t **request );
DLL_HIDDEN extern int                           vgx_server_request__init( vgx_VGXServerRequest_t *request, const char *label );
DLL_HIDDEN extern void                          vgx_server_request__destroy( vgx_VGXServerRequest_t *request );
DLL_HIDDEN extern void                          vgx_server_request__client_request_ready( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int                           vgx_server_request__add_header( vgx_VGXServerRequest_t *request, const char *field, const char *value );
DLL_HIDDEN extern int64_t                       vgx_server_request__add_content( vgx_VGXServerRequest_t *request, const char *data, int64_t sz );

// response
DLL_HIDDEN extern void                          vgx_server_response__dump( const vgx_VGXServerResponse_t *response, const char *fatal_message );
DLL_HIDDEN extern int                           vgx_server_response__prepare_body( vgx_VGXServerResponse_t *response );
DLL_HIDDEN extern int                           vgx_server_response__complete_body( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int                           vgx_server_response__prepare_body_error( vgx_VGXServerResponse_t *response, CString_t *CSTR__error );
DLL_HIDDEN extern int                           vgx_server_response__produce( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, HTTPStatus code );
DLL_HIDDEN extern int                           vgx_server_response__produce_error( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, HTTPStatus code, const char *message, bool errlog );
DLL_HIDDEN extern int                           vgx_server_response__init( vgx_VGXServerResponse_t *response, const char *label );
DLL_HIDDEN extern void                          vgx_server_response__destroy( vgx_VGXServerResponse_t *response );
DLL_HIDDEN extern vgx_VGXServerResponse_t *     vgx_server_response__new( const char *label );
DLL_HIDDEN extern void                          vgx_server_response__delete( vgx_VGXServerResponse_t **response );
DLL_HIDDEN extern void                          vgx_server_response__reset( vgx_VGXServerResponse_t *response );
DLL_HIDDEN extern void                          vgx_server_response__client_response_reset( vgx_VGXServerClient_t *client );

// client
DLL_HIDDEN extern int                           vgx_server_client__create_pool( vgx_VGXServer_t *server, int capacity );
DLL_HIDDEN extern void                          vgx_server_client__destroy_pool( vgx_VGXServer_t *server );
DLL_HIDDEN extern vgx_VGXServerClient_t *       vgx_server_client__register( vgx_VGXServer_t *server, vgx_URI_t *ClientURI );
DLL_HIDDEN extern void                          vgx_server_client__append_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern vgx_VGXServerClient_t *       vgx_server_client__yank_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void                          vgx_server_client__accept_into_iochain( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_URI_t *ClientURI );
DLL_HIDDEN extern void                          vgx_server_client__close_all( vgx_VGXServer_t *server );
DLL_HIDDEN extern int                           vgx_server_client__close( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int                           vgx_server_client__count_TCS( vgx_VGXServer_t *server );
DLL_HIDDEN extern int                           vgx_server_client__count_active_TCS( vgx_VGXServer_t *server );
DLL_HIDDEN extern void                          vgx_server_client__dump( vgx_VGXServerClient_t *client, const char *fatal_message );

// dispatch
DLL_HIDDEN extern int                           vgx_server_dispatch__dispatch( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int                           vgx_server_dispatch__stage_executor( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern vgx_VGXServerClient_t *       vgx_server_dispatch__fetch( vgx_VGXServer_t *server, vgx_VGXServerExecutor_t *executor, int *n_waiting, int64_t *t_slept_ns );
DLL_HIDDEN extern int                           vgx_server_dispatch__return( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int                           vgx_server_dispatch__executor_complete( vgx_VGXServer_t *server );
DLL_HIDDEN extern int                           vgx_server_dispatch__bypass_to_front( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void                          vgx_server_dispatch__dispose_and_disconnect_TCS( comlib_task_t *self, vgx_VGXServer_t *server );
DLL_HIDDEN extern float                         vgx_server_dispatch__drain_pct( comlib_task_t *self );
DLL_HIDDEN extern int                           vgx_server_dispatch__create( vgx_VGXServer_t *server, CString_t **CSTR__error );
DLL_HIDDEN extern int                           vgx_server_dispatch__start_monitor( vgx_VGXServer_t *server, CString_t **CSTR__error );
DLL_HIDDEN extern void                          vgx_server_dispatch__destroy( vgx_VGXServer_t *server );

// plugin
DLL_HIDDEN extern int                           vgx_server_plugin__none( vgx_Graph_t *sysgraph, const char *plugin, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error );

// util
DLL_HIDDEN extern int                           vgx_server_util__sendall( vgx_URI_t *URI, vgx_VGXServerRequest_t *request, int timeout_ms );
DLL_HIDDEN extern int                           vgx_server_util__recvall_sock( CXSOCKET *psock, vgx_VGXServerResponse_t *response, int timeout_ms );
DLL_HIDDEN extern int                           vgx_server_util__recvall( vgx_URI_t *URI, vgx_VGXServerResponse_t *response, int timeout_ms );

// config
DLL_HIDDEN extern vgx_VGXServerConfig_t *       vgx_server_config__new( void );
DLL_HIDDEN extern void                          vgx_server_config__delete( vgx_VGXServerConfig_t **cf );
DLL_HIDDEN extern vgx_VGXServerConfig_t *       vgx_server_config__clone( vgx_VGXServer_t *server );
DLL_HIDDEN extern void                          vgx_server_config__set_front( vgx_VGXServerConfig_t *cf, vgx_VGXServerFrontConfig_t **cf_front );
DLL_HIDDEN extern void                          vgx_server_config__set_dispatcher( vgx_VGXServerConfig_t *cf, vgx_VGXServerDispatcherConfig_t **cf_dispatcher );
DLL_HIDDEN extern const vgx_VGXServerConfig_t * vgx_server_config__get( vgx_VGXServer_t *server, uint16_t *rport, int *rwidth, int *rheight, bool *rallow_incomplete );
DLL_HIDDEN extern void                          vgx_server_config__set_executor_plugin_entrypoint( vgx_VGXServerConfig_t *config, f_vgx_ServicePluginCall pluginf );
// (front)
DLL_HIDDEN extern vgx_VGXServerFrontConfig_t *  vgx_server_config__front_new( const char *ip, uint16_t port, uint16_t offset, const char *prefix );
DLL_HIDDEN extern void                          vgx_server_config__front_delete( vgx_VGXServerFrontConfig_t **cf );
// (dispatcher)
DLL_HIDDEN extern vgx_VGXServerDispatcherConfig_t * vgx_server_config__dispatcher_new( int width, int height );
DLL_HIDDEN extern void                          vgx_server_config__dispatcher_delete( vgx_VGXServerDispatcherConfig_t **cf );
DLL_HIDDEN extern int                           vgx_server_config__dispatcher_set_replica_address( vgx_VGXServerDispatcherConfig_t *cf, int partition_id, int replica_id, const char *host, int port, CString_t **CSTR__error );
DLL_HIDDEN extern int                           vgx_server_config__dispatcher_set_replica_access( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, bool writable );
DLL_HIDDEN extern int                           vgx_server_config__dispatcher_set_replica_channels( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, int channels );
DLL_HIDDEN extern int                           vgx_server_config__dispatcher_set_replica_priority( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, int priority );
DLL_HIDDEN extern bool                          vgx_server_config__dispatcher_verify( const vgx_VGXServerDispatcherConfig_t *cf, CString_t **CSTR__error );
DLL_HIDDEN extern int                           vgx_server_config__dispatcher_channels( const vgx_VGXServerDispatcherConfig_t *cf );
DLL_HIDDEN extern void                          vgx_server_config__dispatcher_dump( const vgx_VGXServerDispatcherConfig_t *cf );




// ===============================================================
// Dispatcher
// ===============================================================

// Matrix
DLL_HIDDEN extern int     vgx_server_dispatcher_matrix__init( vgx_VGXServer_t *server, CString_t **CSTR__error );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__clear( vgx_VGXServer_t *server );
DLL_HIDDEN extern vgx_VGXServerDispatcherStreamSet_t *
                          vgx_server_dispatcher_matrix__new_stream_set( vgx_VGXServerDispatcherMatrix_t *matrix );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__dump( const vgx_VGXServerDispatcherMatrix_t *matrix, const char *fatal_message );
DLL_HIDDEN extern int     vgx_server_dispatcher_matrix__assign_client_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client, bool *defunct, bool *alldown );
DLL_HIDDEN extern vgx_VGXServerDispatcherChannel_t *
                          vgx_server_dispatcher_matrix__get_partition_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherPartition_t *partition, int replica_affinity, bool *defunct );
DLL_HIDDEN extern vgx_VGXServerDispatcherChannel_t *
                          vgx_server_dispatcher_matrix__get_primary_channel( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherPartition_t *partition, bool *defunct );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__client_append_matrix( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern vgx_VGXServerClient_t *
                          vgx_server_dispatcher_matrix__client_yank_matrix( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__delete_stream_set( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherStreamSet_t **stream_set );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__set_replica_defunct( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__deboost_replica( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherReplica_t *replica );
DLL_HIDDEN extern int     vgx_server_dispatcher_matrix__width( const vgx_VGXServer_t *server );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__abort_channels( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void    vgx_server_dispatcher_matrix__channel_close( vgx_VGXServerDispatcherMatrix_t *matrix, vgx_VGXServerDispatcherChannel_t *channel );


// Dispatch
DLL_HIDDEN extern int     vgx_server_dispatcher_dispatch__forward( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern void    vgx_server_dispatcher_dispatch__complete( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int     vgx_server_dispatcher_dispatch__append_backlog( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
DLL_HIDDEN extern int     vgx_server_dispatcher_dispatch__apply_backlog( vgx_VGXServer_t *server );
DLL_HIDDEN extern int     vgx_server_dispatcher_dispatch__backlog_size( vgx_VGXServer_t *server );
DLL_HIDDEN extern int     vgx_server_dispatcher_dispatch__backlog_count( vgx_VGXServer_t *server );

// IO
DLL_HIDDEN extern int     vgx_server_dispatcher_io__send( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern int     vgx_server_dispatcher_io__recv( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern int     vgx_server_dispatcher_io__handle_exception( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel, HTTPStatus code, const char *message );

// Streams
DLL_HIDDEN extern void    vgx_server_dispatcher_streams__trim_set( vgx_VGXServerDispatcherStreamSet_t *stream_set );
DLL_HIDDEN extern void    vgx_server_dispatcher_streams__dump_sets( const vgx_VGXServerDispatcherStreamSets_t *sets, const char *fatal_message );
DLL_HIDDEN extern vgx_VGXServerDispatcherStreamSets_t *
                          vgx_server_dispatcher_streams__new_sets( vgx_VGXServer_t *server, vgx_VGXServerDispatcherConfig_t *cf );
DLL_HIDDEN extern void    vgx_server_dispatcher_streams__delete_sets( vgx_VGXServerDispatcherStreamSets_t **sets );
DLL_HIDDEN extern void    vgx_server_dispatcher_streams__set_request( vgx_VGXServerDispatcherStreamSet_t *set, vgx_VGXServerRequest_t *request );
DLL_HIDDEN extern vgx_VGXServerRequest_t *
                          vgx_server_dispatcher_streams__get_request( vgx_VGXServerDispatcherStreamSet_t *set );

DLL_HIDDEN extern x_vgx_partial__header *
                          vgx_server_dispatcher_streams__get_header( vgx_VGXServerDispatcherStreamSet_t *set, int i );
DLL_HIDDEN extern vgx_VGXServerResponse_t *
                          vgx_server_dispatcher_streams__get_reset_response( vgx_VGXServerDispatcherStreamSet_t *set, int i );

// Partial
DLL_HIDDEN extern void    vgx_server_dispatcher_partial__dump_header( const x_vgx_partial__header *header, const char *fatal_message );
DLL_HIDDEN extern int     vgx_server_dispatcher_partial__aggregate_partials( vgx_VGXServerClient_t *client, CString_t **CSTR__error );

// Channel
DLL_HIDDEN extern void    vgx_server_dispatcher_channel__dump( const vgx_VGXServerDispatcherChannel_t *channel, const char *fatal_message );
DLL_HIDDEN extern int     vgx_server_dispatcher_channel__init( vgx_VGXServer_t *server, int8_t channel_id, vgx_VGXServerDispatcherChannel_t *channel, vgx_VGXServerDispatcherReplica_t *replica, vgx_VGXServerDispatcherPartition_t *partition );
DLL_HIDDEN extern void    vgx_server_dispatcher_channel__clear( vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern void    vgx_server_dispatcher_channel__invalid_push( const vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern void    vgx_server_dispatcher_channel__return( vgx_VGXServerDispatcherChannel_t *channel );

// Replica
DLL_HIDDEN extern void    vgx_server_dispatcher_replica__dump( const vgx_VGXServerDispatcherReplica_t *replica, const char *fatal_message );
DLL_HIDDEN extern int     vgx_server_dispatcher_replica__init( vgx_VGXServer_t *server, BYTE replica_id, vgx_VGXServerDispatcherReplica_t *replica, const vgx_VGXServerDispatcherConfigReplica_t *cf_replica, vgx_VGXServerDispatcherPartition_t *partition );
DLL_HIDDEN extern void    vgx_server_dispatcher_replica__clear( vgx_VGXServerDispatcherReplica_t *replica );
DLL_HIDDEN extern void    vgx_server_dispatcher_replica__push_channel( vgx_VGXServerDispatcherReplica_t *replica, vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern vgx_VGXServerDispatcherChannel_t *
                          vgx_server_dispatcher_replica__pop_channel( vgx_VGXServerDispatcherReplica_t *replica );
DLL_HIDDEN extern int     vgx_server_dispatcher_replica__cost( const vgx_VGXServerDispatcherReplica_t *replica );

// Partition
DLL_HIDDEN extern void    vgx_server_dispatcher_partition__dump( const vgx_VGXServerDispatcherPartition_t *partition, const char *fatal_message );
DLL_HIDDEN extern int     vgx_server_dispatcher_partition__init( vgx_VGXServer_t *server, BYTE partition_id, vgx_VGXServerDispatcherPartition_t *partition, const vgx_VGXServerDispatcherConfigPartition_t *cf_partition );
DLL_HIDDEN extern void    vgx_server_dispatcher_partition__clear( vgx_VGXServerDispatcherPartition_t *partition );

// Response
DLL_HIDDEN extern int     vgx_server_dispatcher_response__handle( vgx_VGXServer_t *server, vgx_VGXServerDispatcherChannel_t *channel );

// Client
DLL_HIDDEN extern void    vgx_server_dispatcher_client__channel_append( vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern vgx_VGXServerDispatcherChannel_t *
                          vgx_server_dispatcher_client__channel_yank( vgx_VGXServerClient_t *client, vgx_VGXServerDispatcherChannel_t *channel );
DLL_HIDDEN extern bool    vgx_server_dispatcher_client__no_channels( const vgx_VGXServerClient_t *client );

// AsyncTask
DLL_HIDDEN extern vgx_VGXServerDispatcherAsyncTask_t *
                          vgx_server_dispatcher_asynctask__new( vgx_VGXServer_t *server );
DLL_HIDDEN extern int     vgx_server_dispatcher_asynctask__destroy( vgx_VGXServer_t *server, vgx_VGXServerDispatcherAsyncTask_t **asynctask );





#define CRLF "\r\n"
#define sz_CRLF 2

#define VGX_SERVER_VERSION      "3"
#define VGX_SERVER_HEADER       "VGX/" VGX_SERVER_VERSION


#define HTTP_PATH_MAX           8171
#define HTTP_LINE_MAX           8192

#define HTTP_MAX_HEADERS        100

#if defined VGXSERVER_RECV_CHUNK_ORDER
#define RECV_CHUNK_ORDER        VGXSERVER_RECV_CHUNK_ORDER
#else
#define RECV_CHUNK_ORDER        13
#endif
#define RECV_CHUNK_SZ           (1 << RECV_CHUNK_ORDER)

#if defined VGXSERVER_SEND_CHUNK_ORDER
#define SEND_CHUNK_ORDER        VGXSERVER_SEND_CHUNK_ORDER
#else
#define SEND_CHUNK_ORDER        13
#endif
#define SEND_CHUNK_SZ           (1 << SEND_CHUNK_ORDER)

#define WORKBUFFER_ORDER        16

#define MAX_EXECUTOR_POOL_ORDER   5
#define MAX_EXECUTOR_POOL_SIZE    (1<<MAX_EXECUTOR_POOL_ORDER)

#define A_EXECUTOR_POOL_ORDER     MAX_EXECUTOR_POOL_ORDER
#define A_EXECUTOR_POOL_SIZE      (1<<A_EXECUTOR_POOL_ORDER)

#define B_EXECUTOR_POOL_ORDER     ((A_EXECUTOR_POOL_ORDER+1)/2)
#define B_EXECUTOR_POOL_SIZE      (1<<B_EXECUTOR_POOL_ORDER)

#define C_EXECUTOR_POOL_ORDER     ((B_EXECUTOR_POOL_ORDER+1)/2)
#define C_EXECUTOR_POOL_SIZE      (1<<C_EXECUTOR_POOL_ORDER)


#define MAX_NCLIENT_SLOTS_ORDER 10
#define MAX_NCLIENT_SLOTS       (1<<MAX_NCLIENT_SLOTS_ORDER)

#define A_NCLIENT_SLOTS_ORDER   MAX_NCLIENT_SLOTS_ORDER
#define A_NCLIENT_SLOTS         (1<<A_NCLIENT_SLOTS_ORDER)

#define B_NCLIENT_SLOTS_ORDER   6
#define B_NCLIENT_SLOTS         (1<<B_NCLIENT_SLOTS_ORDER)

#define C_NCLIENT_SLOTS_ORDER   2
#define C_NCLIENT_SLOTS         (1<<C_NCLIENT_SLOTS_ORDER)

#define USE_EVENTFD_IF_AVAILABLE
#if defined(CXPLAT_FEATURE_EVENTFD) && defined(USE_EVENTFD_IF_AVAILABLE)
#define VGXSERVER_USE_LINUX_EVENTFD
#endif

#define LISTEN_FD_SLOT          0
#define WAKE_FD_SLOT            1
#define CLIENT_FD_START         2

#define VGXSERVER_GLOBAL_SERVICE_SET( VGXServer, State )  ((VGXServer)->control.serving_any = (State))
#define VGXSERVER_GLOBAL_SERVICE_IN( VGXServer )          VGXSERVER_GLOBAL_SERVICE_SET( VGXServer, 1 )
#define VGXSERVER_GLOBAL_SERVICE_OUT( VGXServer )         VGXSERVER_GLOBAL_SERVICE_SET( VGXServer, 0 )
#define VGXSERVER_GLOBAL_IS_SERVING( VGXServer )          ((VGXServer)->control.serving_any != 0)

#define VGXSERVER_ENABLE_rsv1_TCS( VGXServer )      ((VGXServer)->control.flag_TCS._rsv1 = 1)
#define VGXSERVER_DISABLE_rsv1_TCS( VGXServer )     ((VGXServer)->control.flag_TCS._rsv1 = 0)
#define VGXSERVER_IS_ENABLED_rsv1_TCS( VGXServer )  ((VGXServer)->control.flag_TCS._rsv1 != 0)


#define CLIENT_REQUEST_STREAM_BUFFER_MAX_IDLE_CAPACITY      0x2000
#define CLIENT_REQUEST_CONTENT_BUFFER_MAX_IDLE_CAPACITY     0x100000
#define CLIENT_RESPONSE_STREAM_BUFFER_MAX_IDLE_CAPACITY     0x2000
#define CLIENT_RESPONSE_CONTENT_BUFFER_MAX_IDLE_CAPACITY    0x100000

#define CHANNEL_REQUEST_STREAM_BUFFER_MAX_IDLE_CAPACITY     0x2000
#define CHANNEL_REQUEST_CONTENT_BUFFER_MAX_IDLE_CAPACITY    0x80000
#define CHANNEL_RESPONSE_STREAM_BUFFER_MAX_IDLE_CAPACITY    0x2000
#define CHANNEL_RESPONSE_CONTENT_BUFFER_MAX_IDLE_CAPACITY   0x80000

#define REPLICA_MIN_CHANNELS          1
#define REPLICA_MAX_CHANNELS          127
#define REPLICA_DEFAULT_CHANNELS      MAX_EXECUTOR_POOL_SIZE

#define REPLICA_PRIORITY_HIGHEST      0
#define REPLICA_PRIORITY_NORMAL       2
#define REPLICA_PRIORITY_LOWEST       127
#define REPLICA_PRIORITY_DEFAULT      REPLICA_PRIORITY_NORMAL

// The maximum acceptable cost for a replica to be selected.
// Replicas whose cost exceeds this level will not be used for I/O
// 16256
#define REPLICA_MAX_COST              ( (REPLICA_PRIORITY_LOWEST+1) * REPLICA_MAX_CHANNELS )

// See: typedef struct s_vgx_VGXServerDispatcherReplica_t
// resource.priority.mix = (resource.priority.deboost << 8) + resource.priority.base
//
// 0x40 << 8 = 16384  is greater than REPLICA_MAX COST which means replica will never be selected
// 0x41 << 8 = 16640  when combined with deboost one
#define REPLICA_PRIORITY_DEBOOST_MAX  0x40

// 0x01 << 8 = 256    is high which means other replicas are likely to be selected instead
#define REPLICA_PRIORITY_DEBOOST_ONE  0x01

// Replica cost is computed normally
#define REPLICA_PRIORITY_DEBOOST_NONE 0x00


#define REPLICA_DEFUNCT_COST_FLOOR    (REPLICA_PRIORITY_DEBOOST_MAX << 8)

// Mark replica as defunct and set max deboost
#define REPLICA_SET_DEFUNCT_MCS( Replica )  \
  do { \
    (Replica)->resource.priority.deboost |= REPLICA_PRIORITY_DEBOOST_MAX;   \
    (Replica)->flags_MCS.defunct_raised = true;                             \
  } WHILE_ZERO

// Make replica available and remove max deboost
#define REPLICA_CLEAR_DEFUNCT_MCS( Replica )  \
  do { \
    (Replica)->resource.priority.deboost &= ~REPLICA_PRIORITY_DEBOOST_MAX;  \
    (Replica)->flags_MCS.defunct_raised = false;                            \
  } WHILE_ZERO

// True if replica is set defunct and has deboost set
#define REPLICA_IS_DEFUNCT_MCS( Replica )     ((Replica)->flags_MCS.defunct_raised && ((Replica)->resource.priority.deboost != REPLICA_PRIORITY_DEBOOST_NONE))

// Deboost replica to make it less likely to be selected when other replicas are available
#define REPLICA_SET_TMP_DEBOOST_MCS( Replica )  \
  do { \
    (Replica)->resource.priority.deboost |= REPLICA_PRIORITY_DEBOOST_ONE;   \
    (Replica)->flags_MCS.tmp_deboost = true;                                \
  } WHILE_ZERO

// Remove deboost to restore state prior to deboost
#define REPLICA_CLEAR_TMP_DEBOOST_MCS( Replica )  \
  do { \
    (Replica)->resource.priority.deboost &= !REPLICA_PRIORITY_DEBOOST_ONE;  \
    (Replica)->flags_MCS.tmp_deboost = false;                               \
  } WHILE_ZERO

// True if replica has been deboosted
#define REPLICA_IS_TMP_DEBOOST_MCS( Replica )     ((Replica)->flags_MCS.tmp_deboost && ((Replica)->resource.priority.deboost != REPLICA_PRIORITY_DEBOOST_NONE))





// Unused channel will have socket closed after X seconds of inactivity
#define CHANNEL_MAX_IDLE_SECONDS      15



#define SERVER_MATRIX_WIDTH_NONE        0
#define SERVER_MATRIX_WIDTH_MAX         127


#define SERVER_MATRIX_HEIGHT_NONE       0
#define SERVER_MATRIX_HEIGHT_MAX        127


#define EXECUTOR_DISPATCH_QUEUE_MAX_WAITING 4




#ifdef VGX_CONSISTENCY_CHECK
DLL_HIDDEN extern void vgx_server_client__trap_illegal_access( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );

#define TRAP_ILLEGAL_CLIENT_ACCESS( Server, Client )  \
  do { if( CLIENT_STATE__IN_EXECUTOR( Client ) ) { \
    vgx_server_client__trap_illegal_access( Server, Client );   \
  } } WHILE_ZERO



#else

#define TRAP_ILLEGAL_CLIENT_ACCESS( Server, Client )  ((void)0)

#endif

DLL_HIDDEN extern void vgx_server_client__assert_client_reset( vgx_VGXServerClient_t *client );
#define ASSERT_CLIENT_RESET( Client ) vgx_server_client__assert_client_reset( Client )


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char *__full_path( vgx_VGXServer_t *server ) {
  vgx_Graph_t *SYSTEM = server->sysgraph;
  return SYSTEM ? CALLABLE( SYSTEM )->FullPath( SYSTEM ) : "?";
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static uint16_t __server_port_offset( const vgx_VGXServer_t *server ) {
  return server->config.cf_server->front->port.offset;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static uint16_t __server_port( const vgx_VGXServer_t *server ) {
  vgx_VGXServerFrontConfig_t *ucf = server->config.cf_server->front;
  return ucf->port.base + ucf->port.offset;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const char * __server_prefix( const vgx_VGXServer_t *server ) {
  vgx_VGXServerFrontConfig_t *ucf = server->config.cf_server->front;
  return CStringValueDefault( ucf->CSTR__prefix, NULL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char __ident_letter( const vgx_VGXServer_t *server ) {
  return (char)('A' + __server_port_offset( server ));
}





#endif
