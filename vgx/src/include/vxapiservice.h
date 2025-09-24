/*
###################################################
#
# File:   vxapiservice.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VXAPISERVICE_H
#define _VXAPISERVICE_H


#include "vxbase.h"


struct s_vgx_VGXServer_t;
struct s_vgx_VGXServerClient_t;
struct s_vgx_VGXServerDispatcherConfig_t;

struct s_vgx_VGXServerDispatcherStreamSet_t;
struct s_vgx_VGXServerDispatcherChannel_t;
struct s_vgx_VGXServerDispatcherReplica_t;
struct s_vgx_VGXServerDispatcherPartition_t;
struct s_vgx_VGXServerDispatcherMatrix_t;
struct s_vgx_VGXServerDispatcherAsyncTask_t;


/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_StreamBuffer_t {

  // [Q1.1]
  // Buffer
  char *data;
  
  // [Q1.2]
  //
  char *__allocated;

  // [Q1.3]
  // Capacity
  int64_t capacity;

  // [Q1.4]
  // Write pointer
  char *wp;

  // [Q1.5]
  // Read pointer
  const char *rp;

  // [Q1.6]
  CString_t *CSTR__name;

  // [Q1.7]
  QWORD __rsv_1_7;

  // [Q1.8]
  QWORD __rsv_1_8;

  // ---------------------------

} vgx_StreamBuffer_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_IStreamBuffer_t {

  vgx_StreamBuffer_t * (*New)( int order );
  void (*Delete)( vgx_StreamBuffer_t **buffer );

  void (*SetName)( vgx_StreamBuffer_t *buffer, const char *name );
  const char * (*GetName)( const vgx_StreamBuffer_t *buffer );

  int64_t (*Capacity)( const vgx_StreamBuffer_t *buffer );
  int64_t (*Size)( const vgx_StreamBuffer_t *buffer );
  bool (*Empty)( vgx_StreamBuffer_t *buffer );
  bool (*IsReadable)( const vgx_StreamBuffer_t *buffer );
  int64_t (*Writable)( const vgx_StreamBuffer_t *buffer );

  int64_t (*ReadUntil)( vgx_StreamBuffer_t *buffer, int64_t max, char **data, const char probe );
  const char * (*GetLinearLine)( const vgx_StreamBuffer_t *buffer, int offset, int max_sz, int *rsz );

  int (*Expand)( vgx_StreamBuffer_t *buffer );
  int (*Trim)( vgx_StreamBuffer_t *buffer, int64_t max_sz );
  int64_t (*Write)( vgx_StreamBuffer_t *buffer, const char *data, int64_t n );
  int64_t (*WriteString)( vgx_StreamBuffer_t *buffer, const char *data );
  int (*TermString)( vgx_StreamBuffer_t *buffer );
  int64_t (*Copy)( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other );

  int64_t (*Absorb)( vgx_StreamBuffer_t *buffer, vgx_StreamBuffer_t *other, int64_t n );
  void (*Swap)( vgx_StreamBuffer_t *A, vgx_StreamBuffer_t *B );
  int64_t (*WritableSegment)( vgx_StreamBuffer_t *buffer, int64_t max, char **segment, char **end );
  char * (*WritableSegmentEx)( vgx_StreamBuffer_t *buffer, int64_t sz, int64_t *wsz );
  int64_t (*AdvanceWrite)( vgx_StreamBuffer_t *buffer, int64_t n );
  bool  (*IsSingleSegment)( const vgx_StreamBuffer_t *buffer );
  const char * (*EndSingleSegment)( const vgx_StreamBuffer_t *buffer );
  int64_t (*ReadableSegment)( vgx_StreamBuffer_t *buffer, int64_t max, const char **segment, const char **end );
  int64_t (*AdvanceRead)( vgx_StreamBuffer_t *buffer, int64_t n );
  int64_t (*Clear)( vgx_StreamBuffer_t *buffer );
  void (*Dump)( const vgx_StreamBuffer_t *buffer );



} vgx_IStreamBuffer_t;


DLL_VISIBLE extern vgx_IStreamBuffer_t iStreamBuffer;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_x_vgx_partial__status {
  X_VGX_PARTIAL_STATUS__RESET        = 0x00000000,
  X_VGX_PARTIAL_STATUS__OK           = 0x00000001,
  X_VGX_PARTIAL_STATUS__EMPTY        = 0x0000E001,
  X_VGX_PARTIAL_STATUS__ERROR        = 0x000F0000
} x_vgx_partial__status;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_x_vgx_partial__sortkeytype {
  __X_VGX_PARTIAL_SORTKEYTYPE_MASK_NUMERIC  = 0x10,
  __X_VGX_PARTIAL_SORTKEYTYPE_MASK_STRING   = 0x20,
  X_VGX_PARTIAL_SORTKEYTYPE__NONE           = 0x00,
  X_VGX_PARTIAL_SORTKEYTYPE__double         = 0x11,
  X_VGX_PARTIAL_SORTKEYTYPE__int64          = 0x12,
  X_VGX_PARTIAL_SORTKEYTYPE__bytes          = 0x23,
  X_VGX_PARTIAL_SORTKEYTYPE__unicode        = 0x24
} x_vgx_partial__sortkeytype;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int x_vgx_partial__is_sortkeytype_numeric( x_vgx_partial__sortkeytype ktype ) {
  return (ktype & __X_VGX_PARTIAL_SORTKEYTYPE_MASK_NUMERIC) == __X_VGX_PARTIAL_SORTKEYTYPE_MASK_NUMERIC;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int x_vgx_partial__is_sortkeytype_string( x_vgx_partial__sortkeytype ktype ) {
  return (ktype & __X_VGX_PARTIAL_SORTKEYTYPE_MASK_STRING) == __X_VGX_PARTIAL_SORTKEYTYPE_MASK_STRING;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_x_vgx_partial__message {
  X_VGX_PARTIAL_MESSAGE__NONE       = 0x00,
  X_VGX_PARTIAL_MESSAGE__UTF8       = 0x01,
  X_VGX_PARTIAL_MESSAGE__OBJECT     = 0x02
} x_vgx_partial__message;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_x_vgx_partial__level {
  // Current level number
  int number;
  // Number of partitions aggregated at current level
  int parts;
  // Number of total aggregated parts from the lowest level
  int deep_parts;
  // Number of ignored parts
  int incomplete_parts;
} x_vgx_partial__level;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_x_vgx_partial__aggregator {
  int64_t int_aggr[2];
  double dbl_aggr[2];
} x_vgx_partial__aggregator;



/*******************************************************************//**
 * 
 *
 *  HEADER:
 *  [status, __xxx, ktype, sdir, n_entries, segment.message, segment.keys, segment.strings, segment.items, segment.end, hitcount, exec_ms, __xxx, __xxx, __xxx, __xxx, __xxx, __xxx]
 *  struct: iiiiqqqqqqqqqqqqqq
 * 
 *  ENTRY_MSG:
 *  (sz_msg, msg)
 * 
 *  ENTRY_KEYS:
 *  [(offset_str1, offset_entry1), (offset_str1, offset_entry2), ..., (offset_strN, offset_entryN)]
 *  [(sortkey1,item1.offset), (sortkey2,item2.offset), ..., (sortkeyN,itemN.offset)]
 *  struct: dqdq...dq       # float sortkey
 *  struct: qqqq...qq       # int sortkey or string offset sortkey
 *   
 *  STRINGS (skipped for numeric sortkeys)
 *  [(sz_str1, str1), (sz_str2, str2), ..., (sz_strN, strN)]
 *  struct: qRbqRb...qRb   (where R equals the number of bytes sz_stri)
 * 
 *  ENTRIES
 *  [(sz_entry1, entry1), (sz_entry2, entry2), ..., (sz_entryN, entryN)]
 *  struct: qRbqRb...qRb   (where R equals the number of bytes sz_entryi)
 * 
 * 
 *
 ***********************************************************************
 */
typedef union u_x_vgx_partial__header {
  struct {
    // [Q1.1.1]
    // Response status code
    x_vgx_partial__status status;
    // [Q1.1.2]
    // Max number of hits to include in result
    int maxhits;
    // [Q1.2.1]
    // Data type of the field used for sorting
    x_vgx_partial__sortkeytype ktype;
    // [Q1.2.2]
    // Sortspec
    vgx_sortspec_t sortspec;
    // [Q1.3]
    // Number of hit entries in the result
    int64_t n_entries;
    struct {
      // [Q1.4]
      // Byte offset to start of message string
      int64_t message;
      // [Q1.5]
      // Byte offset to entry key segment
      int64_t keys;
      // [Q1.6]
      // Byte offset to strings used for sorting (if applicable)
      int64_t strings;
      // [Q1.7]
      // Byte offset to serialized item data
      int64_t items;
      // [Q1.8]
      // Byte offset one beyond the last byte of the last item
      int64_t end;
    } segment;
    // [Q2.1]
    // Total number of hits that could have been returned
    int64_t hitcount;
    
    // [Q2.2.1]
    x_vgx_partial__message message_type;
    
    // [Q2.2.2]
    DWORD __rsv_2_2_2;
    
    // [Q2.3-4]
    x_vgx_partial__level level;

    // [Q2.5-8]
    // General purpose aggregator slots
    x_vgx_partial__aggregator aggregator;
  };
  char bytes[128];
} x_vgx_partial__header;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union __u_x_vgx_partial__entry_key {
  __m128i m128i;
  struct {
    union {
      double dval;
      int64_t ival;
      int64_t offset;
      void *ptr;
      QWORD bits;
    } sortkey;
    union {
      int64_t offset;
      void *ptr;
    } item;
  };
} x_vgx_partial__entry_key;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_x_vgx_partial__binentry {
  union {
    int val;
    char bytes[sizeof(int)];
  } sz;
  const char *data;
} x_vgx_partial__binentry;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_x_vgx_partial__entry {
  x_vgx_partial__entry_key key;
  void *obj;
  x_vgx_partial__binentry sortkey;
  x_vgx_partial__binentry item;
} x_vgx_partial__entry;



DLL_VISIBLE int       vgx_server_dispatcher_partial__deserialize_header( const char *data, int64_t sz_data, x_vgx_partial__header *dest );
DLL_VISIBLE void      vgx_server_dispatcher_partial__reset_header( x_vgx_partial__header *header );
DLL_VISIBLE int64_t   vgx_server_dispatcher_partial__write_output_binary( const x_vgx_partial__header *header, const char *msg, int sz_msg, const x_vgx_partial__entry *entries, vgx_StreamBuffer_t *output );
DLL_VISIBLE int64_t   vgx_server_dispatcher_partial__serialize_partial_error( const char *msg, int64_t sz_msg, vgx_StreamBuffer_t *output );



#define SZ_VGX_RESOURCE_PATH_BUFFER 255
#define VGX_PLUGIN_PATH_PREFIX      "/vgx/plugin/"

#define DEFINE_RESOURCE( Name, Value )  \
static const char Name[] = Value;       \
static const size_t sz_##Name = sizeof( Name ) - 1LL

DEFINE_RESOURCE( VGX_SERVER_RESOURCE_WILDCARD,                "*" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_PLUGIN,                  "/plugin/" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_SERVICE,                 "/service/" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_PLUGIN_PATH_PREFIX,      VGX_PLUGIN_PATH_PREFIX );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_BUILTIN_PATH_PREFIX,     "/vgx/builtin/" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_PLUGINS_PATH,            "/vgx/plugins" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_BUILTINS_PATH,           "/vgx/builtins" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_SUBSCRIBE_PATH_PREFIX,   "/vgx/subscribe" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_HC_PATH,                 "/vgx/hc" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_BUILTIN_PLUGIN_PREFIX,   "sysplugin__" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_BUILTIN_LIST_PLUGINS,    "sysplugin__plugins" );
DEFINE_RESOURCE( VGX_SERVER_RESOURCE_BUILTIN_LIST_BUILTINS,   "sysplugin__builtins" );






/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef enum e__plugin_phase {
    VGX_SERVER_PLUGIN_PHASE__PRE      = 0x01,
    VGX_SERVER_PLUGIN_PHASE__EXEC     = 0x02,
    VGX_SERVER_PLUGIN_PHASE__POST     = 0x04
} vgx_server_plugin_phase;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_HTTPRequestMethod {
  HTTP_NONE     = 0x0001,
  HTTP_OPTIONS  = 0x0002,
  HTTP_GET      = 0x0004,
  HTTP_HEAD     = 0x0008,
  HTTP_POST     = 0x0010,
  HTTP_PUT      = 0x0020,
  HTTP_DELETE   = 0x0040,
  HTTP_CONNECT  = 0x0080,
  HTTP_TRACE    = 0x0100,
  HTTP_PATCH    = 0x0200,
  XVGX_IDENT    = 0x0400,
  XVGX_rsv1     = 0x0800,
  XVGX_rsv2     = 0x1000,
  XVGX_rsv3     = 0x2000,
  XVGX_rsv4     = 0x4000,
  XVGX_rsv5     = 0x8000,
  __HTTP_MIN_MASK = HTTP_GET,
  __HTTP_MAX_MASK = HTTP_PATCH
} HTTPRequestMethod;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char *__HTTP_METHOD_STRINGS[] = {
  "",
  "OPTIONS",
  "GET",
  "HEAD",
  "POST",
  "PUT",
  "DELETE",
  "CONNECT",
  "TRACE",
  "PATCH",
  "XVGXIDENT",
  "?",
  "?",
  "?",
  "?",
  "?"
};



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char * __vgx_http_request_method( HTTPRequestMethod method ) {
  return __HTTP_METHOD_STRINGS[ (unsigned)ilog2( (int)method & 0xFFFF ) & 0xF ];
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_HTTPStatus {
  HTTP_STATUS__NONE                         = 0,
  HTTP_STATUS__INTERNAL_SOCKET_ERROR        = 5,

  // 1xx Informational
  HTTP_STATUS__Continue                     = 100,
  HTTP_STATUS__SwitchingProtocols           = 101,
  HTTP_STATUS__Processing                   = 102,
  HTTP_STATUS__EarlyHints                   = 103,

  // 2xx Success
  HTTP_STATUS__OK                           = 200,
  HTTP_STATUS__Created                      = 201,
  HTTP_STATUS__Accepted                     = 202,
  HTTP_STATUS__NonAuthoritativeInformation  = 203,
  HTTP_STATUS__NoContent                    = 204,
  HTTP_STATUS__ResetContent                 = 205,
  HTTP_STATUS__PartialContent               = 206,
  HTTP_STATUS__MultiStatus                  = 207,
  HTTP_STATUS__AlreadyReported              = 208,
  HTTP_STATUS__IMUsed                       = 226,

  // 3xx Redirection
  HTTP_STATUS__MultipleChoices              = 300,
  HTTP_STATUS__MovedPermanently             = 301,
  HTTP_STATUS__Found                        = 302,
  HTTP_STATUS__SeeOther                     = 303,
  HTTP_STATUS__NotModified                  = 304,
  HTTP_STATUS__UseProxy                     = 305,
  HTTP_STATUS__SwitchProxy                  = 306,
  HTTP_STATUS__TemporaryRedirect            = 307,
  HTTP_STATUS__PermanentRedirect            = 308,

  // 4xx Client error
  HTTP_STATUS__BadRequest                   = 400,
  HTTP_STATUS__Unauthorized                 = 401,
  HTTP_STATUS__PaymentRequired              = 402,
  HTTP_STATUS__Forbidden                    = 403,
  HTTP_STATUS__NotFound                     = 404,
  HTTP_STATUS__MethodNotAllowed             = 405,
  HTTP_STATUS__NotAcceptable                = 406,
  HTTP_STATUS__ProxyAuthenticationRequired  = 407,
  HTTP_STATUS__RequestTimeout               = 408,
  HTTP_STATUS__Conflict                     = 409,
  HTTP_STATUS__Gone                         = 410,
  HTTP_STATUS__LengthRequired               = 411,
  HTTP_STATUS__PreconditionFailed           = 412,
  HTTP_STATUS__PayloadTooLarge              = 413,
  HTTP_STATUS__URITooLong                   = 414,
  HTTP_STATUS__UnsupportedMediaType         = 415,
  HTTP_STATUS__RangeNotSatisfiable          = 416,
  HTTP_STATUS__ExpectationFailed            = 417,
  HTTP_STATUS__MisdirectedRequest           = 421,
  HTTP_STATUS__UnprocessableEntity          = 422,
  HTTP_STATUS__Locked                       = 423,
  HTTP_STATUS__FailedDependency             = 424,
  HTTP_STATUS__TooEarly                     = 425,
  HTTP_STATUS__UpgradeRequired              = 426,
  HTTP_STATUS__PreconditionRequired         = 428,
  HTTP_STATUS__TooManyRequests              = 429,
  HTTP_STATUS__RequestHeaderFieldsTooLarge  = 431,
  HTTP_STATUS__UnavailableForLegalReasons   = 451,

  // 5xx Server error
  HTTP_STATUS__InternalServerError          = 500,
  HTTP_STATUS__NotImplemented               = 501,
  HTTP_STATUS__BadGateway                   = 502,
  HTTP_STATUS__ServiceUnavailable           = 503,
  HTTP_STATUS__GatewayTimeout               = 504,
  HTTP_STATUS__HTTPVersionNotSupported      = 505,
  HTTP_STATUS__VariantAlsoNegotiates        = 506,
  HTTP_STATUS__InsufficientStorage          = 507,
  HTTP_STATUS__LoopDetected                 = 508,
  HTTP_STATUS__NotExtended                  = 510,
  HTTP_STATUS__NetworkAuthenticationRequired = 511,

  // nginx codes
  HTTP_STATUS__NoResponse                   = 444,
  HTTP_STATUS__RequestHeaderTooLarge        = 494,

} HTTPStatus;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_MediaType {
  MEDIA_TYPE__NONE                          = 0x00000000,
  MEDIA_TYPE__application_javascript        = 0x00001001,
  MEDIA_TYPE__application_json              = 0x00001002,
  MEDIA_TYPE__application_pdf               = 0x00001003,
  MEDIA_TYPE__application_xml               = 0x00001004,
  MEDIA_TYPE__application_octet_stream      = 0x00001005,
  MEDIA_TYPE__application_x_vgx_partial     = 0x10002001,
  MEDIA_TYPE__text_plain                    = 0x00004001,
  MEDIA_TYPE__text_css                      = 0x00004002,
  MEDIA_TYPE__text_html                     = 0x00004003,
  MEDIA_TYPE__image_ANY                     = 0x00008000,
  MEDIA_TYPE__image_ico                     = 0x00008001,
  MEDIA_TYPE__image_gif                     = 0x00008002,
  MEDIA_TYPE__image_png                     = 0x00008003,
  MEDIA_TYPE__image_jpg                     = 0x00008004,
  MEDIA_TYPE__ANY                           = 0x7FFFFFFF
} vgx_MediaType;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_VGXServerClientState {
  /*
  //
  //                                   ERROR______________
  //                                   IOCHAIN___________ \  
  //                                   EXECUTOR_________ \ |
  //                                   DISPATCHER______ \ ||
  //                                                   \ |||
  //                                                    ||||
  //                         Advancing state value: ssss||||
  //                                                ----||||
  */
  __VGXSERVER_CLIENT_STATE__MASK_ERROR          = 0x0000000f,
  __VGXSERVER_CLIENT_STATE__MASK_NOERROR        = 0x7ffffff0,
  __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN        = 0x00000010,
  __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR       = 0x00000100,
  __VGXSERVER_CLIENT_STATE__MASK_COLLECTED      = 0x00000200,
  __VGXSERVER_CLIENT_STATE__MASK_DISPATCHER     = 0x00001000,
  __VGXSERVER_CLIENT_STATE__MASK_STATE          = 0x7FFF0000,

  VGXSERVER_CLIENT_STATE__RESET                 = 0x00000000, // Client is idle and not associated with a connected socket
  VGXSERVER_CLIENT_STATE__READY                 = 0x00000000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN, // Client is associated with a connected socket and ready for a new request
  VGXSERVER_CLIENT_STATE__EXPECT_INITIAL        = 0x00010000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN, //
  VGXSERVER_CLIENT_STATE__EXPECT_HEADERS        = 0x00020000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN, //
  VGXSERVER_CLIENT_STATE__EXPECT_CONTENT        = 0x00040000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN, //
  VGXSERVER_CLIENT_STATE__HANDLE_REQUEST        = 0x00080000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN, //
  VGXSERVER_CLIENT_STATE__AWAIT_DISPATCH        = 0x00100000 | __VGXSERVER_CLIENT_STATE__MASK_DISPATCHER,
  VGXSERVER_CLIENT_STATE__DISPATCH              = 0x00100000 | __VGXSERVER_CLIENT_STATE__MASK_DISPATCHER | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN,
  VGXSERVER_CLIENT_STATE__DISPATCH_COMPLETE     = 0x00200000 | __VGXSERVER_CLIENT_STATE__MASK_DISPATCHER,
  VGXSERVER_CLIENT_STATE__PREPROCESS            = 0x01000000 | __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR,
  VGXSERVER_CLIENT_STATE__EXECUTE               = 0x02000000 | __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR,
  VGXSERVER_CLIENT_STATE__MERGE                 = 0x04000000 | __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR,
  VGXSERVER_CLIENT_STATE__POSTPROCESS           = 0x08000000 | __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR,
  VGXSERVER_CLIENT_STATE__RESUBMIT              = 0x10000000 | __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR,
  VGXSERVER_CLIENT_STATE__COLLECTED             = 0x40000000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN | __VGXSERVER_CLIENT_STATE__MASK_COLLECTED, //
  VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE     = 0x7FFF0000 | __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN  //
} vgx_VGXServerClientState;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char * __vgx_server_client_state( vgx_VGXServerClientState state ) {
  switch( (state & __VGXSERVER_CLIENT_STATE__MASK_NOERROR) ) {
  case VGXSERVER_CLIENT_STATE__RESET:
    return "RESET";
  case VGXSERVER_CLIENT_STATE__READY:
    return "READY";
  case VGXSERVER_CLIENT_STATE__EXPECT_INITIAL:
    return "EXPECT_INITIAL";
  case VGXSERVER_CLIENT_STATE__EXPECT_HEADERS:
    return "EXPECT_HEADERS";
  case VGXSERVER_CLIENT_STATE__EXPECT_CONTENT:
    return "EXPECT_CONTENT";
  case VGXSERVER_CLIENT_STATE__HANDLE_REQUEST:
    return "HANDLE_REQUEST";
  case VGXSERVER_CLIENT_STATE__AWAIT_DISPATCH:
    return "AWAIT_DISPATCH";
  case VGXSERVER_CLIENT_STATE__DISPATCH:
    return "DISPATCH";
  case VGXSERVER_CLIENT_STATE__DISPATCH_COMPLETE:
    return "DISPATCH_COMPLETE";
  case VGXSERVER_CLIENT_STATE__PREPROCESS:
    return "PREPROCESS";
  case VGXSERVER_CLIENT_STATE__EXECUTE:
    return "EXECUTE";
  case VGXSERVER_CLIENT_STATE__MERGE:
    return "MERGE";
  case VGXSERVER_CLIENT_STATE__POSTPROCESS:
    return "POSTPROCESS";
  case VGXSERVER_CLIENT_STATE__COLLECTED:
    return "COLLECTED";
  case VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE:
    return "RESPONSE_COMPLETE";
  default:
    return NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static bool __vgx_server_client_state_error( vgx_VGXServerClientState state ) {
  return (state & __VGXSERVER_CLIENT_STATE__MASK_ERROR) != 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef enum e_vgx_VGXServerChannelState {

  //                                   ERROR     _________
  //                                   OUTPUT    ________ \
  //                                   INPUT     _______ \ |
  //                                 INCHAIN     ______ \ ||
  //                                                   \ |||
  //                                                    ||||
  //                         Advancing state value: ssss||||
  //                                                ||||||||
  __VGXSERVER_CHANNEL_STATE__ACTIVE_MASK        = 0xfffffff0,
  __VGXSERVER_CHANNEL_STATE__ERROR_MASK         = 0x0000000f,
  __VGXSERVER_CHANNEL_STATE__NOERROR_MASK       = 0x7ffffff0,
  __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK       = 0x00001000,
  __VGXSERVER_CHANNEL_STATE__SEND_MASK          = 0x00000010,
  __VGXSERVER_CHANNEL_STATE__RECV_MASK          = 0x00000100,

  VGXSERVER_CHANNEL_STATE__RESET                = 0x00000000, // Channel is idle and not connected
  VGXSERVER_CHANNEL_STATE__ERROR                = 0x00000000 | __VGXSERVER_CHANNEL_STATE__ERROR_MASK,
  VGXSERVER_CHANNEL_STATE__READY                = 0x00010000, // Channel is not assigned to any client
  VGXSERVER_CHANNEL_STATE__ASSIGNED             = 0x00100000 | __VGXSERVER_CHANNEL_STATE__SEND_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is assigned but no I/O yet
  VGXSERVER_CHANNEL_STATE__IDENT                = 0x00200000 | __VGXSERVER_CHANNEL_STATE__SEND_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is sending ident data
  VGXSERVER_CHANNEL_STATE__SEND                 = 0x00400000 | __VGXSERVER_CHANNEL_STATE__SEND_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is sending data
  VGXSERVER_CHANNEL_STATE__RECV_INITIAL         = 0x01000000 | __VGXSERVER_CHANNEL_STATE__RECV_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is receiving initial response line
  VGXSERVER_CHANNEL_STATE__RECV_HEADERS         = 0x02000000 | __VGXSERVER_CHANNEL_STATE__RECV_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is receiving header line(s)
  VGXSERVER_CHANNEL_STATE__RECV_CONTENT         = 0x04000000 | __VGXSERVER_CHANNEL_STATE__RECV_MASK | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK,  // Channel is receiving content payload
  VGXSERVER_CHANNEL_STATE__COMPLETE             = 0x10000000 | __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK
} vgx_VGXServerChannelState;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static const char * __vgx_server_channel_state( vgx_VGXServerChannelState state ) {
  switch( (state & __VGXSERVER_CHANNEL_STATE__NOERROR_MASK) ) {
  case VGXSERVER_CHANNEL_STATE__RESET:
    return "RESET";
  case VGXSERVER_CHANNEL_STATE__READY:
    return "READY";
  case VGXSERVER_CHANNEL_STATE__ASSIGNED:
    return "ASSIGNED";
  case VGXSERVER_CHANNEL_STATE__IDENT:
    return "IDENT";
  case VGXSERVER_CHANNEL_STATE__SEND:
    return "SEND";
  case VGXSERVER_CHANNEL_STATE__RECV_INITIAL:
    return "RECV_INITIAL";
  case VGXSERVER_CHANNEL_STATE__RECV_HEADERS:
    return "RECV_HEADERS";
  case VGXSERVER_CHANNEL_STATE__RECV_CONTENT:
    return "RECV_CONTENT";
  case VGXSERVER_CHANNEL_STATE__COMPLETE:
    return "COMPLETE";
  default:
    return NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static bool __vgx_server_channel_state_error( vgx_VGXServerChannelState state ) {
  return (state & __VGXSERVER_CHANNEL_STATE__ERROR_MASK) != 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_HTTPStatus_t {
  HTTPStatus code;
  DWORD __rsv;
  const char *reason;
} vgx_HTTPStatus_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_MediaTypeMap_t {
  vgx_MediaType type;
  DWORD __rsv;
  const char *text;
} vgx_MediaTypeMap_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef union u_vgx_HTTPVersion_t {
  uint16_t bits;
  struct {
    int8_t major;
    int8_t minor;
  };
} vgx_HTTPVersion_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef union u_vgx_HTTPHeadersCapsule_t {
  QWORD __bits[2];
  struct {
    void *data;
    void (*destroyf)( union u_vgx_HTTPHeadersCapsule_t *capsule );
  };
} vgx_HTTPHeadersCapsule_t;


#define DESTROY_HEADERS_CAPSULE( CapsulePtr ) \
  do {                                        \
    if( (CapsulePtr)->destroyf ) {            \
      (CapsulePtr)->destroyf( (CapsulePtr) ); \
    }                                         \
  } WHILE_ZERO



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_HTTPHeaders_t {
  // [Q1.1-2]
  // Pre-allocated buffer holding all header data as a consecutive sequence
  // of bytes, into which a list of pointers refer to individual substrings.
  char *_buffer;
  char *_end;

  // [Q1.3]
  // Pointer to next substring in _buffer where data can be written when
  // building the headers object.
  char *_wp;

  // [Q1.4-5]
  // List of pointers referencing substrings in _buffer
  char **list;
  char **_cursor;

  // [Q1.6.1]
  // Number of headers
  int sz;

  // [Q1.6.2]
  // Start of content payload in request buffers.content
  int content_offset;

  // [Q1.7]
  // Pre-parsed Content-Length: <n>
  int64_t content_length;

  // [Q1.8]
  // Client reference
  struct s_vgx_VGXServerClient_t *client;

  // [Q2.1-2]
  // Request parameter digest
  objectid_t signature;

  // [Q2.3]
  // Unique request serial number
  int64_t sn;

  // [Q2.4]
  // Settable flag per request to carry user info from request to response
  union {
    QWORD __bits;
    char value[8];
  } flag;
  
  // [Q2.5-6]
  vgx_HTTPHeadersCapsule_t capsule;
  
  // [Q2.7.1]
  struct {
    int8_t bypass_sout;
    int8_t resubmit;
    int8_t _rsv_2_7_1_3;
    int8_t _rsv_2_7_1_4;
  } control;

  // [Q2.7.2]
  int nresubmit;

  // [Q2.8]
  QWORD __rsv_2_8;

} vgx_HTTPHeaders_t;


#define REQUEST_MAX_RESUBMIT 9999


/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerBufferPair_t {
  vgx_StreamBuffer_t *stream;
  vgx_StreamBuffer_t *content;
} vgx_VGXServerBufferPair_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerRequest_t {
  // [Q1.1.1]
  // HTTP request state
  vgx_VGXServerClientState state;

  // [Q1.1.2]
  // HTTP request method
  HTTPRequestMethod method;

  // [Q1.2]
  // Nanosecond timestamp for start of next execution segment (relative to thread performing execution)
  int64_t exec_t0_ns;

  // [Q1.3.1]
  // HTTP version
  vgx_HTTPVersion_t version;

  // [Q1.3.2]
  // HTTP request path length
  int16_t sz_path;
  
  // [Q1.3.3.1]
  int8_t executor_id;

  // [Q1.3.3.2]
  int8_t min_executor_pool;

  // [Q1.3.3.3]
  int8_t replica_affinity;

  // [Q1.3.3.4]
  // Target partial (used by dispatcher matrix when positive)
  int8_t target_partial;

  // [Q1.4]
  // HTTP request path (pre-allocated buffer)
  char *path;

  // [Q1.5/6]
  vgx_VGXServerBufferPair_t buffers;

  // [Q1.7]
  // HTTP request headers
  vgx_HTTPHeaders_t *headers;

  // [Q1.8.1]
  vgx_MediaType content_type;

  // [Q1.8.2]
  vgx_MediaType accept_type;

} vgx_VGXServerRequest_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __vgxserver_client_state__busy( const vgx_VGXServerRequest_t *request ) {
  return request->state > VGXSERVER_CLIENT_STATE__READY && request->state < VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state_pre_content( const vgx_VGXServerRequest_t *request ) {
  return request->state < VGXSERVER_CLIENT_STATE__EXPECT_CONTENT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClientState __vgxserver_request_state__get_noerror( const vgx_VGXServerRequest_t *request ) {
  return request->state & __VGXSERVER_CLIENT_STATE__MASK_NOERROR;
}
#define CLIENT_STATE_NOERROR( Client ) __vgxserver_request_state__get_noerror( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClientState __vgxserver_request_state__set_state( vgx_VGXServerRequest_t *request, vgx_VGXServerClientState state ) {
  return request->state = state;
}
#define CLIENT_STATE__UPDATE( Client, State ) __vgxserver_request_state__set_state( &(Client)->request, State )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClientState __vgxserver_request_state__add_error( vgx_VGXServerRequest_t *request ) {
  return request->state |= __VGXSERVER_CLIENT_STATE__MASK_ERROR;
}
#define CLIENT_STATE__SET_ERROR( Client ) __vgxserver_request_state__add_error( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClientState __vgxserver_request_state__add_iochain( vgx_VGXServerRequest_t *request ) {
  return request->state |= __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN;
}
#define CLIENT_STATE__ADD_IOCHAIN( Client ) __vgxserver_request_state__add_iochain( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerClientState __vgxserver_request_state__remove_iochain( vgx_VGXServerRequest_t *request ) {
  return request->state = (int)request->state & (~(int)__VGXSERVER_CLIENT_STATE__MASK_IOCHAIN);
}
#define CLIENT_STATE__REMOVE_IOCHAIN( Client ) __vgxserver_request_state__remove_iochain( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state__in_iochain( const vgx_VGXServerRequest_t *request ) {
  return (((int)request->state) & __VGXSERVER_CLIENT_STATE__MASK_IOCHAIN) != 0;
}
#define CLIENT_STATE__IN_IOCHAIN( Client ) __vgxserver_request_state__in_iochain( &(Client)->request )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state__executor( const vgx_VGXServerRequest_t *request ) {
  return (((int)request->state) & __VGXSERVER_CLIENT_STATE__MASK_EXECUTOR) != 0;
}
#define CLIENT_STATE__IN_EXECUTOR( Client ) __vgxserver_request_state__executor( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state__has_error( const vgx_VGXServerRequest_t *request ) {
  return (((int)request->state) & __VGXSERVER_CLIENT_STATE__MASK_ERROR) != 0;
}
#define CLIENT_STATE__HAS_ERROR( Client ) __vgxserver_request_state__has_error( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state__collected( const vgx_VGXServerRequest_t *request ) {
  return ((int)request->state & (int)__VGXSERVER_CLIENT_STATE__MASK_COLLECTED) != 0;
}
#define CLIENT_STATE__IS_COLLECTED( Client ) __vgxserver_request_state__collected( &(Client)->request )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vgxserver_request_state__dispatcher( const vgx_VGXServerRequest_t *request ) {
  return ((int)request->state & (int)__VGXSERVER_CLIENT_STATE__MASK_DISPATCHER) != 0;
}
#define CLIENT_STATE__IN_DISPATCHER( Client ) __vgxserver_request_state__dispatcher( &(Client)->request )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __vgxserver_request_unfilled_content_buffer( const vgx_VGXServerRequest_t *request ) {
  int64_t sz_request_payload_data = iStreamBuffer.Size( request->buffers.content ) - request->headers->content_offset;
  return request->headers->content_length - sz_request_payload_data;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerResponse_t {

  // [Q1.1-2]
  vgx_HTTPStatus_t status;

  // [Q1.3]
  // Nanosecond aggregator for use by executor thread to report total
  int64_t exec_ns;

  // [Q1.4]
  // Content Length as specified in header
  int64_t content_length;

  // [Q1.5/6]
  vgx_VGXServerBufferPair_t buffers;

  // [Q1.7.1]
  vgx_MediaType mediatype;
  
  // [Q1.7.2.1]
  //vgx_HTTPVersion_t version;
  int16_t x_vgx_backlog;

  // [Q1.7.2.2]
  int16_t allowed_methods_mask;

  // [Q1.8.1]
  // Start of content payload in buffers.content
  int content_offset;

  // [Q1.8.2]
  union {
    DWORD _bits;
    struct {
      int16_t http_errcode;
      struct {
        uint8_t svc_exe   : 1;
        uint8_t _rsv06    : 1;
        uint8_t _rsv07    : 1;
        uint8_t _rsv08    : 1;
        uint8_t mem_err   : 1;
        uint8_t _rsv14    : 1;
        uint8_t _rsv15    : 1;
        uint8_t _rsv16    : 1;
      } error;
      struct {
        uint8_t system    : 1;
        uint8_t fileio    : 1;
        uint8_t plugin    : 1;
        uint8_t nometrics : 1;
        uint8_t nometas   : 1;
        uint8_t prewrap   : 1;
        uint8_t dispatch  : 1;
        uint8_t complete  : 1;
      } execution;
    };
  } info;

} vgx_VGXServerResponse_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __vgxserver_response_unfilled_content_buffer( const vgx_VGXServerResponse_t *response ) {
  int64_t sz_response_payload_data = iStreamBuffer.Size( response->buffers.content ) - response->content_offset;
  return response->content_length - sz_response_payload_data;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union u_x_vgx_partial_ident {
  QWORD _bits;
  struct {
    BYTE defined;
    struct {
      BYTE width;
      BYTE height;
      BYTE depth;
    } matrix;
    struct {
      BYTE partition;
      BYTE replica;
      BYTE channel;
      BYTE primary;
    } position;
  };
} x_vgx_partial_ident;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerClient_t {
  // [Q1.1.1]
  int id;

  // [Q1.1.2]
  union {
    DWORD _bits;
    struct {
      // [Q1.1.2.1]
      uint8_t direct;

      // [Q1.1.2.2]
      uint8_t plugin;

      // [Q1.1.2.3]
      // True when a matrix request should only be sent to primary replicas
      uint8_t primary;

      // [Q1.1.2.4]
      // True when voluntarily stopped handling already received data where further progress can be made
      uint8_t yielded;
    };
  } flags;

  // [Q1.2]
  //
  vgx_URI_t *URI;

  // [Q1.3-4]
  VGX_LLIST_CHAIN_STRUCT( struct s_vgx_VGXServerClient_t );

  // [Q1.5]
  // IO Loop's nanosecond timestamp when first aware of a new request
  int64_t io_t0_ns;
  
  struct {
    // [Q1.6]
    struct s_vgx_VGXServerDispatcherStreamSet_t *streams;

    struct {
      // [Q1.7]
      struct s_vgx_VGXServerDispatcherChannel_t *head;

      // [Q1.8]
      struct s_vgx_VGXServerDispatcherChannel_t *tail;
    } channels;
  } dispatcher;

  // ---------------------

  // [Q2]
  vgx_VGXServerRequest_t request; 

  // ---------------------

  // [Q3]
  vgx_VGXServerResponse_t response;

  // ---------------------

  // [Q4.1]
  x_vgx_partial_ident partial_ident;

  // [Q4.2-3]
  struct {
    x_vgx_partial__level level;
  } dispatch_metas;

  // [Q4.4]
  // Server start timebase
  int64_t tbase_ns;

  // [Q4.5]
  char *p_x_vgx_backlog_word;

  // [Q4.6]
  struct {
    BYTE main_server;
    BYTE __rsv_4_6_2;
    BYTE __rsv_4_6_3;
    BYTE __rsv_4_6_4;
    uint16_t port_base;
    uint16_t port_offset;
  } env;

  // [Q4.7]
  // IO Loop's nanosecond timestamp for most recently completed request
  int64_t io_t1_ns;

  // [Q4.8]
  QWORD __rsv_4_8;


} vgx_VGXServerClient_t;


#define CLIENT_DESTROY_HEADERS_CAPSULE( Client )  DESTROY_HEADERS_CAPSULE( &(Client)->request.headers->capsule )


#define CLIENT_IS_DIRECT( Client )              (Client)->flags.direct

#define CLIENT_HAS_PLUGIN( Client )             ((Client)->flags.plugin != 0)
#define CLIENT_HAS_PRE_PROCESSOR( Client )      (((Client)->flags.plugin & VGX_SERVER_PLUGIN_PHASE__PRE) != 0 )
#define CLIENT_HAS_EXEC_PROCESSOR( Client )     (((Client)->flags.plugin & VGX_SERVER_PLUGIN_PHASE__EXEC) != 0 )
#define CLIENT_HAS_POST_PROCESSOR( Client )     (((Client)->flags.plugin & VGX_SERVER_PLUGIN_PHASE__POST) != 0 )
#define CLIENT_HAS_REQUEST_PROCESSOR( Client )  (((Client)->flags.plugin & (VGX_SERVER_PLUGIN_PHASE__PRE | VGX_SERVER_PLUGIN_PHASE__EXEC)) != 0 )
#define CLIENT_HAS_ANY_PROCESSOR( Client )      (((Client)->flags.plugin & (VGX_SERVER_PLUGIN_PHASE__PRE | VGX_SERVER_PLUGIN_PHASE__EXEC | VGX_SERVER_PLUGIN_PHASE__POST)) != 0 )

#define CLIENT_YIELD( Server, Client )  \
do {                                    \
  if( !(Client)->flags.yielded ) {      \
    (Client)->flags.yielded = 1;        \
    (Server)->pool.clients.n_yielded++; \
  }                                     \
} WHILE_ZERO

#define CLIENT_BLOCK( Server, Client )  \
do {                                    \
  if( (Client)->flags.yielded ) {       \
    (Client)->flags.yielded = 0;        \
    (Server)->pool.clients.n_yielded--; \
  }                                     \
} WHILE_ZERO


#define CLIENT_YIELDED( Client )  ((Client)->flags.yielded != 0)
#define CLIENT_BLOCKED( Client )  ((Client)->flags.yielded == 0)


#define CLIENT_SET_PRIMARY_ONLY( Client )   ((Client)->flags.primary = 1)
#define CLIENT_USE_PRIMARY_ONLY( Client )   ((Client)->flags.primary != 0)
#define CLIENT_USE_ANY_REPLICA( Client )    ((Client)->flags.primary == 0)



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerClientPool_t {
  struct {
    vgx_VGXServerClient_t *head;
    vgx_VGXServerClient_t *tail;
  } iolist;
  vgx_VGXServerClient_t *clients;
  int capacity;
  short n_yielded;
  short __rsv2;
} vgx_VGXServerClientPool_t;




/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherStreamSet_t {
  vgx_VGXServerRequest_t _request;
  vgx_VGXServerRequest_t *prequest;
  struct {
    int len;
    int __rsv;
    x_vgx_partial__header *headers;
    vgx_VGXServerResponse_t **list;
  } responses;
} vgx_VGXServerDispatcherStreamSet_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherStreamSets_t {
  int sz;
  int __rsv;
  vgx_VGXServerDispatcherStreamSet_t *list;
} vgx_VGXServerDispatcherStreamSets_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherChannel_t {
  // -------------------------------------------------------------------
  struct {
    // [Q1.1.1.1]
    int8_t channel;
    // [Q1.1.1.2]
    BYTE replica;
    // [Q1.1.1.3]
    BYTE partition;
    // [Q1.1.1.4]
    BYTE __rsv_1_1_1_4;
  } id;

  // [Q1.1.2]
  vgx_VGXServerChannelState state;

  // [Q1.2]
  // Socket connected to server
  CXSOCKET socket;

  // [Q1.3-5]
  struct {
    // [Q1.3]
    // Client request buffer read pointer, i.e. start of remaining data to be written to socket...
    const char *read;

    // [Q1.4]
    // ...bounded by pointer to end of buffer
    const char *end;

    // [Q1.5]
    // Number of requests sent since channel connect
    int64_t counter;

    // [Q1.6.1]
    int cost;

    // [Q1.6.1]
    DWORD __rsv_1_6_2;
  } request;

  // [Q1.7-8]
  VGX_LLIST_CHAIN_STRUCT( struct s_vgx_VGXServerDispatcherChannel_t );


  // -------------------------------------------------------------------


  // -------------------------------------------------------------------
  // [Q2.1]
  vgx_VGXServerResponse_t *response;

  // [Q2.2]
  int64_t __rsv_2_2;

  // [Q2.3.1]
  union {
    struct {
      BYTE partial;
      volatile BYTE busy;
      volatile BYTE connected_MCS;
      BYTE yielded;
    };
  } flag;

  // [Q2.3.2.1]
  short remain_ident;

  // [Q2.3.2.2]
  short sz_ident;
  
  // [Q2.4]
  char *ident_request;

  // [Q2.5]
  volatile int64_t t0_ns;

  // [Q2.6-8]
  // Backreferences to parent structures
  struct  {
    struct {
      // [Q2.6]
      vgx_VGXServerClient_t *client;
    } dynamic;
    struct {
      // [Q2.7]
      struct s_vgx_VGXServerDispatcherReplica_t *replica;
      // [Q2.8]
      struct s_vgx_VGXServerDispatcherPartition_t *partition;
    } permanent;
  } parent;

  // -------------------------------------------------------------------

} vgx_VGXServerDispatcherChannel_t;




#define CHANNEL_YIELD( Server, Channel )  \
do {                                      \
  if( !(Channel)->flag.yielded ) {        \
    (Channel)->flag.yielded = 1;          \
    (Server)->matrix.n_ch_yielded++;      \
  }                                       \
} WHILE_ZERO

#define CHANNEL_BLOCK( Server, Channel )  \
do {                                      \
  if( (Channel)->flag.yielded ) {         \
    (Channel)->flag.yielded = 0;          \
    (Server)->matrix.n_ch_yielded--;      \
  }                                       \
} WHILE_ZERO


#define CHANNEL_YIELDED( Channel )  ((Channel)->flag.yielded != 0)
#define CHANNEL_BLOCKED( Channel )  ((Channel)->flag.yielded == 0)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __channel_in_client_chain( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return (channel->state & __VGXSERVER_CHANNEL_STATE__INCHAIN_MASK) != 0;
}
#define CHANNEL_IN_CHAIN( Channel ) __channel_in_client_chain( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __channel_io_outbound( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return (channel->state & __VGXSERVER_CHANNEL_STATE__SEND_MASK) != 0;
}
#define CHANNEL_OUTBOUND( Channel ) __channel_io_outbound( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __channel_io_inbound( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return (channel->state & __VGXSERVER_CHANNEL_STATE__RECV_MASK) != 0;
}
#define CHANNEL_INBOUND( Channel ) __channel_io_inbound( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __channel_io_inflight( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return channel->state > VGXSERVER_CHANNEL_STATE__ASSIGNED && channel->state < VGXSERVER_CHANNEL_STATE__COMPLETE;
}
#define CHANNEL_INFLIGHT( Channel ) __channel_io_inflight( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerChannelState __vgxserver_channel_state__update( vgx_VGXServerDispatcherChannel_t *channel, vgx_VGXServerChannelState state ) {
  return channel->state = state;
}
#define CHANNEL_UPDATE_STATE( Channel, State ) __vgxserver_channel_state__update( Channel, State )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerChannelState __vgxserver_channel_state__set_error( vgx_VGXServerDispatcherChannel_t *channel ) {
  return channel->state |= __VGXSERVER_CHANNEL_STATE__ERROR_MASK;
}
#define CHANNEL_SET_ERROR( Channel ) __vgxserver_channel_state__set_error( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __vgxserver_channel_is_partial( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return channel->flag.partial;
}
#define CHANNEL_IS_PARTIAL( Channel ) __vgxserver_channel_is_partial( Channel )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static vgx_VGXServerChannelState __vgxserver_channel_state__get_state_noerror( const vgx_VGXServerDispatcherChannel_t *channel ) {
  return channel->state & __VGXSERVER_CHANNEL_STATE__NOERROR_MASK;
}
#define CHANNEL_STATE_NOERROR( Channel ) __vgxserver_channel_state__get_state_noerror( Channel )



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherReplica_t {

  // [Q1.1]
  struct {
    // [Q1.1.1.1]
    BYTE replica;

    // [Q1.1.1.2]
    BYTE partition;

    // [Q1.1.1.3]
    BYTE __rsv_1_1_1_3;

    // [Q1.1.1.4]
    BYTE __rsv_1_1_1_4;
  } id;

  // [Q1.1.2]
  DWORD __rsv_1_1_2;

  // [Q1.2.1]
  struct {
    // [Q1.2.1.1]
    // Dynamically updated cost of this replica
    short cost;
    // [Q1.2.1.2]
    // Replica static priority. Lower number is higher priority
    union {
      short mix;
      struct {
        char base;
        char deboost;
      };
    } priority;
  } resource;

  // [Q1.2.2.1]
  union {
    BYTE _bits;
    struct {
      volatile uint8_t defunct_raised   : 1;
      volatile uint8_t defunct_caught   : 1;
      volatile uint8_t initial_attempt  : 1;
      volatile uint8_t __rsv_1_2_2_1_4  : 1;
      volatile uint8_t __rsv_1_2_2_1_5  : 1;
      volatile uint8_t tmp_deboost      : 1;
      volatile uint8_t __rsv_1_2_2_1_7  : 1;
      volatile uint8_t __rsv_1_2_2_1_8  : 1;
    };
  } flags_MCS;

  // [Q1.2.2.2]
  int8_t depth;

  // [Q1.2.2.3]
  BYTE primary;

  // [Q1.2.2.4]
  int8_t __rsv_1_2_2_4;

  // [Q1.3]
  // URI for channels to clone when activated, providing host/port
  vgx_URI_t *remote;

  // [Q1.4]
  // Replica address info.
  // May be NULL if replica has not been able to resolve host/service yet
  struct addrinfo *addrinfo_MCS;

  // [Q1.5]
  QWORD __rsv_1_5;

  // [Q1.6 - 8]
  // Static pool of the full set of permanently connected channels
  struct {
    // [Q1.6]
    // Channel object array (the allocated channel objects)
    vgx_VGXServerDispatcherChannel_t *data;

    // [Q1.7]
    // Channel stack (NULL terminated)
    vgx_VGXServerDispatcherChannel_t **stack;

    // [Q1.8]
    // Next available channel stack pointer
    vgx_VGXServerDispatcherChannel_t **idle;
  } channel_pool;

} vgx_VGXServerDispatcherReplica_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __replica__exhausted( const vgx_VGXServerDispatcherReplica_t *replica ) {
  return *replica->channel_pool.idle == NULL;
}
#define REPLICA_EXHAUSTED( Replica ) __replica__exhausted( Replica )



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherPartition_t {

  // [Q1.1.1]
  struct {
    // [Q1.1.1.1]
    BYTE partition;

    // [Q1.1.1.2]
    BYTE __rsv_1_1_1_2;

    // [Q1.1.1.3]
    BYTE __rsv_1_1_1_3;

    // [Q1.1.1.2]
    BYTE __rsv_1_1_1_4;
  } id;

  // [Q1.1.2]
  union {
    DWORD _bits;
    struct {
      BYTE partial;
      BYTE __rsv_1_2_2_2;
      BYTE __rsv_1_2_2_3;
      BYTE __rsv_1_2_2_4;
    };
  } flag;

  // [Q1.2-3]
  struct {
    // [Q1.2.1.1]
    // Number of replica objects replica array
    BYTE height;

    // [Q1.2.1.2]
    BYTE __rsv_1_2_1_2;

    // [Q1.2.1.3]
    BYTE __rsv_1_2_1_3;

    // [Q1.2.1.4]
    BYTE __rsv_1_2_1_4;

    // [Q1.2.2]
    int __rsv_1_2_2;

    // [Q1.3]
    // Array of replica objects
    vgx_VGXServerDispatcherReplica_t *list;
  } replica;

  // [Q1.4]
  QWORD __rsv_1_4;

} vgx_VGXServerDispatcherPartition_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherMatrix_t {

  // -------------------------------------------------------------------
  // [Q1.1.1]
  union {
    DWORD _bits;
    struct {
      BYTE enabled;
      BYTE allow_incomplete;
      BYTE __rsv_1_1_1_3;
      BYTE __rsv_1_1_1_4;
    };
  } flags;

  // [Q1.1.2]
  int n_ch_yielded;

  // [Q1.2-3]
  // Dispatcher partition objects
  struct {
    // [Q1.2.1.1]
    // Number of partition objects in partition array
    BYTE width;

    // [Q1.2.1.2]
    BYTE __rsv_1_2_1_2;

    // [Q1.2.1.3]
    BYTE __rsv_1_2_1_3;

    // [Q1.2.1.4]
    BYTE __rsv_1_2_1_4;

    // [Q1.2.2.1]
    volatile uint16_t nopen_channels_MCS;

    // [Q1.2.2.1]
    volatile uint16_t nmax_channels_MCS;

    // [Q1.3]
    // Array of partition objects
    vgx_VGXServerDispatcherPartition_t *list;
  } partition;

  // [Q1.4-5]
  // List of clients currently engaged in dispatcher I/O activity
  struct {
    // [Q1.4]
    struct s_vgx_VGXServerClient_t *head;

    // [Q1.5]
    struct s_vgx_VGXServerClient_t *tail;
  } active;

  // [Q1.6-8]
  // Pool of data buffer sets to be
  // allocated on demand to clients
  struct {
    // [Q1.6]
    // Allocated array of buffer set objects
    vgx_VGXServerDispatcherStreamSets_t *sets;

    // [Q1.7]
    // Buffer set stack (NULL terminated)
    vgx_VGXServerDispatcherStreamSet_t **stack;

    // [Q1.8]
    // Next available buffer set stack pointer
    vgx_VGXServerDispatcherStreamSet_t **idle;
  } stream_set_pool;

  // -------------------------------------------------------------------
  // [Q2]
  CS_LOCK lock;

  // -------------------------------------------------------------------
  // [Q3.1]
  struct s_vgx_VGXServerDispatcherAsyncTask_t *asynctask;

  // [Q3.2]
  //CQwordQueue_t *queue_MCS;
  CQwordQueue_t *backlog;

  // [Q3.3.1]
  ATOMIC_VOLATILE_i32( backlog_sz_atomic );

  // [Q3.3.2]
  ATOMIC_VOLATILE_i32( backlog_count_atomic );

  // [Q3.4]
  QWORD __rsv_3_4;

  // [Q3.5]
  QWORD __rsv_3_5;

  // [Q3.6]
  QWORD __rsv_3_6;

  // [Q3.7]
  QWORD __rsv_3_7;

  // [Q3.8]
  QWORD __rsv_3_8;

  // -------------------------------------------------------------------


} vgx_VGXServerDispatcherMatrix_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static bool __vgxserver_dispatcher_matrix__enabled( const vgx_VGXServerDispatcherMatrix_t *matrix ) {
  return matrix->flags.enabled;
}
#define DISPATCHER_MATRIX_ENABLED( Server ) __vgxserver_dispatcher_matrix__enabled( &(Server)->matrix )



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static bool __vgxserver_dispatcher_matrix__multi_partial( const vgx_VGXServerDispatcherMatrix_t *matrix ) {
  return matrix->partition.width > 1;
}
#define DISPATCHER_MATRIX_MULTI_PARTIAL( Matrix ) __vgxserver_dispatcher_matrix__multi_partial( Matrix )



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherConfigReplica_t {
  // [Q1.1]
  // Replica uri
  vgx_URI_t *uri;

  // [Q1.2]
  QWORD __rsv_1_2;

  // [Q1.3.1.1]
  // Replica port
  uint16_t port;

  // [Q1.3.1.2]
  uint16_t __rsv_1_3_1_2;

  // [Q1.3.2]
  // Replica settings
  union {
    DWORD bits;
    struct {
      int8_t channels;
      int8_t priority;
      int8_t __rsv8;
      struct {
        BYTE readable : 1;
        BYTE writable : 1;
        BYTE __r3 : 1;
        BYTE __r4 : 1;
        BYTE __r5 : 1;
        BYTE __r6 : 1;
        BYTE __r7 : 1;
        BYTE __r8 : 1;
      } flag;
    };
  } settings;

  // [Q1.4]
  QWORD __rsv_1_4;

} vgx_VGXServerDispatcherConfigReplica_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherConfigPartition_t {

  // Number of replicas in partition
  BYTE height;

  // True if partition is a partial, False if partition represents all data
  bool partial;

  // List of replica configurations
  vgx_VGXServerDispatcherConfigReplica_t *replicas;


} vgx_VGXServerDispatcherConfigPartition_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherConfig_t {

  struct {
    // Number of partitions in matrix
    BYTE width;

    // Number of replicas in shards
    BYTE height;
  } shape;

  // Best-effort response when defunct partition(s)
  bool allow_incomplete;

  // List of partition configurations
  vgx_VGXServerDispatcherConfigPartition_t *partitions;

} vgx_VGXServerDispatcherConfig_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerFrontConfig_t {

  struct {
    // 
    uint16_t base;
    //
    uint16_t offset;
  } port;

  //
  CString_t *CSTR__ip;

  //
  CString_t *CSTR__prefix;

} vgx_VGXServerFrontConfig_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef int (*f_vgx_ServicePluginCall)( const char *plugin_name, bool post, vgx_URIQueryParameters_t *params, vgx_VGXServerRequest_t *request, vgx_VGXServerResponse_t *response, CString_t **CSTR__error );



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerConfig_t {
  vgx_VGXServerFrontConfig_t *front;
  vgx_VGXServerDispatcherConfig_t *dispatcher;
  f_vgx_ServicePluginCall executor_pluginf;
} vgx_VGXServerConfig_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerExecutorCompletion_t {
  // -------------------------------------------------------------------
  // [Q1]
  CS_LOCK lock;

  // -------------------------------------------------------------------
  // [Q2.1]
  CQwordQueue_t *queue;

  // [Q2.2.1]
  ATOMIC_VOLATILE_i32 length_atomic;

  // [Q2.2.2]
  ATOMIC_VOLATILE_i32( poll_blocked_atomic );

  // [Q2.3]
  vgx_URI_t *signal;

  // [Q2.4]
  vgx_URI_t *monitor;

  // [Q2.5]
  int64_t completion_count;

  // [Q2.6]
  int64_t n_blocked_poll_signals;

  // [Q2.7.1]
  int lock_init;

  // [Q2.7.2] eventfd (linux only)
  int efd;

  // [Q2.8]
  QWORD __rsv_2_8;

} vgx_VGXServerExecutorCompletion_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatchQueue_t {
  // -------------------------------------------------------------------
  // [Q1]
  CS_LOCK lock;

  // -------------------------------------------------------------------
  // [Q2.1-6]
  CS_COND wake;

  // [Q2.7]
  CQwordQueue_t *queue;

  // [Q2.8.1
  ATOMIC_VOLATILE_i32 length_atomic;

  // [Q2.8.2]
  ATOMIC_VOLATILE_i32 n_waiting_atomic;


  // -------------------------------------------------------------------
  // [Q3.1]
  int64_t ts_last_collect;

  // [Q3.2.1]
  union {
    DWORD bits;
    struct {
      int8_t __rsv_3_2_1_1;
      int8_t __rsv_3_2_1_2;
      int8_t __rsv_3_2_1_3;
      struct {
        uint8_t d_lock : 1;
        uint8_t d_cond : 1;
        uint8_t _rsv3  : 1;
        uint8_t _rsv4  : 1;
        uint8_t _rsv5  : 1;
        uint8_t _rsv6  : 1;
        uint8_t _rsv7  : 1;
        uint8_t _rsv8  : 1;
      } init;
    };
  } flag;

  // [Q3.2.2]
  DWORD __rsv_3_2_2;

  // [Q3.3]
  QWORD __rsv_3_3;

  // [Q3.4]
  QWORD __rsv_3_4;

  // [Q3.5]
  QWORD __rsv_3_5;

  // [Q3.6]
  QWORD __rsv_3_6;

  // [Q3.7]
  QWORD __rsv_3_7;

  // [Q3.8]
  QWORD __rsv_3_8;

} vgx_VGXServerDispatchQueue_t;



#define DISPATCH_QUEUE_COUNT 4
#define DISPATCH_QUEUE_BYPASS 5


/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerWorkDispatch_t {

  // -------------------------------------------------------------------
  // [Q1/2/3] [Q4/5/6] [Q7/8/9] [Q10/11/12]
  vgx_VGXServerDispatchQueue_t Q[4];

  // -------------------------------------------------------------------
  // [Q13 Q14]
  vgx_VGXServerExecutorCompletion_t completion;

  // -------------------------------------------------------------------
  // [Q15.1]
  int64_t n_total;

  // [Q15.2.1]
  int n_current;

  // [Q15.2.2]
  int ready;

  // [Q15.3]
  QWORD __rsv_15_3;
  
  // [Q15.4]
  QWORD __rsv_15_4;
  
  // [Q15.5]
  QWORD __rsv_15_5;
  
  // [Q15.6]
  QWORD __rsv_15_6;
  
  // [Q15.7]
  QWORD __rsv_15_7;

  // [Q15.8]
  QWORD __rsv_15_8;

  // -------------------------------------------------------------------
  // [Q16]
  QWORD __rsv_16[8];

} vgx_VGXServerWorkDispatch_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerExecutor_t {

  // [Q1.1]
  comlib_task_t *TASK;

  // [Q1.2]
  struct s_vgx_VGXServer_t *server;

  // [Q1.3.1]
  const int8_t id;

  // [Q1.3.2]
  uint8_t idle_TCS;

  // [Q1.3.3]
  uint8_t max_sleep;

  // [Q1.3.4]
  uint8_t priority;

  // [Q1.3.2]
  union {
    DWORD bits;
    struct {
      int16_t __rsv16;
      uint8_t n_performed;
      struct {
        uint8_t __rsv_0 : 1;
        uint8_t __rsv_1 : 1;
        uint8_t __rsv_2 : 1;
        uint8_t __rsv_3 : 1;
        uint8_t __rsv_4 : 1;
        uint8_t __rsv_5 : 1;
        uint8_t __rsv_6 : 1;
        uint8_t __rsv_7 : 1;
      };
    };
  } flags;

  // [Q1.4]
  int64_t last_alive_ts;

  // [Q1.5]
  // OWNED BY SERVER THREAD
  int64_t __snapshot_ts;

  // [Q1.6]
  // MAIN - EXECUTOR reference
  int64_t ns_offset;

  // [Q1.7]
  vgx_VGXServerDispatchQueue_t *jobQ;

  // [Q1.8]
  ATOMIC_VOLATILE_i64 count_atomic;

  // --------------------------


} vgx_VGXServerExecutor_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerExecutorPool_t {
  int sz;
  int __rsv_2;
  vgx_VGXServerExecutor_t **executors;
} vgx_VGXServerExecutorPool_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherAsyncSubtask_t {
  int64_t interval;
  int64_t next;
  struct s_vgx_VGXServer_t *server;
  int (*func)( struct s_vgx_VGXServer_t *server );
} vgx_VGXServerDispatcherAsyncSubtask_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherAsyncTask_t {

  // [Q1.1]
  comlib_task_t *TASK;

  // [Q1.2]
  struct s_vgx_VGXServer_t *server;

  // [Q1.3]
  vgx_VGXServerDispatcherAsyncSubtask_t **subtasks;

  // --------------------------


} vgx_VGXServerDispatcherAsyncTask_t;





#define REQUEST_DURATION_SAMPLE_BUCKETS   80
#define REQUEST_DURATION_SAMPLE_BASE      0.0001
#define REQUEST_DURATION_SAMPLE_FPSHIFT   50

typedef struct s_vgx_VGXServerChannelInspect_t {
  // Channel number
  int8_t channel;
  // Channel replica
  BYTE replica;
  // Channel partition
  BYTE partition;
  // Channel state
  vgx_VGXServerChannelState state;
  // Matrix socket
  CXSOCKET socket;
  // Cost
  int cost;
  // Number of requests since channel connect
  int64_t n_requests;
  // Number of bytes sent since channel connect
  int64_t n_bytes_sent;
  // Number of bytes received since channel connect
  int64_t n_bytes_recv;

} vgx_VGXServerChannelInspect_t;



typedef struct s_vgx_VGXServerClientInspect_t {
  // Client ID
  int id;
  // Front URI
  vgx_URI_t *uri;
  // Front socket
  CXSOCKET socket;
  // Request t0 (timestamp of most recent request start)
  int64_t io_t0_ns;
  // Request t1 (timestamp of most recent request completion)
  int64_t io_t1_ns;
  // Time since most recent request was initiated
  int64_t delta_t_ns;
  // Front request
  struct {
    // HTTP request state
    vgx_VGXServerClientState state;
    // HTTP request method
    HTTPRequestMethod method;
    // [Q1.3.1.2.1]
    int8_t executor_id;
    // [Q1.3.1.2.2]
    int8_t replica_affinity;
    // Target partial (used by dispatcher matrix when positive)
    int8_t target_partial;
    // HTTP request path
    char *path;
    // HTTP request headers
    struct {
      // Number of headers
      int sz;
      // Pre-parsed Content-Length: <n>
      int64_t content_length;
      // Request parameter digest
      objectid_t signature;
      // Unique request serial number
      int64_t sn;
      // BORROWED reference to client's headers raw data (be careful)
      const char *data;
      const char *_end;
    } headers;
    // Content type
    vgx_MediaType content_type;
    // Accept type
    vgx_MediaType accept_type;
  } request; 
  // Position in matrix
  x_vgx_partial_ident partial_ident;
  // Partial level
  x_vgx_partial__level partial_level;
  // Number of back-end matrix channels
  int n_channels;
  // List of back-end matrix channel inspect objects
  vgx_VGXServerChannelInspect_t *channels;

} vgx_VGXServerClientInspect_t;



typedef struct s_vgx_VGXServerMatrixInspect_t {
  // Number of clients dispatched to handle back-end matrix
  int n_clients;
  // List of back-end matrix client inspect objects
  vgx_VGXServerClientInspect_t *clients;

} vgx_VGXServerMatrixInspect_t;



typedef struct s_vgx_VGXServerPerfCounters_t {
  // [Q1.1]
  int64_t server_t0;

  // [Q1.2]
  int64_t server_uptime_ns;

  // [Q1.3]
  int64_t bytes_in;

  // [Q1.4]
  int64_t bytes_out;

  // [Q1.5]
  int64_t connected_clients; 

  // [Q1.6]
  int64_t total_clients; 

  // [Q1.7.1]
  int sz_dispatch;

  // [Q1.7.2]
  int sz_working;

  // [Q1.8]
  int64_t request_count_plugin;

  // [Q2.1]
  int64_t request_count_total;

  // [Q2.2.1]
  int n_executors;

  // [Q2.2.2]
  int service_in;

  // [Q2.3]
  double duration_total;

  // [Q2.4]
  double average_duration_long;

  // [Q2.5]
  double average_rate_long;

  // [Q2.6]
  double average_duration_short;

  // [Q2.7]
  double average_rate_short;
   
  // [Q2.8]
  const struct s_vgx_VGXServerPerfCounters_t *readonly_snapshot;

  // Q[3,4,5,6,7]
  int __duration_sample_buckets[ REQUEST_DURATION_SAMPLE_BUCKETS ];
  // Q[8,9,10,11,12]
  float duration_short_buckets[ REQUEST_DURATION_SAMPLE_BUCKETS ];
  // Q[13,14,15,16,17,18,19,20,21,22]
  int64_t duration_long_buckets[ REQUEST_DURATION_SAMPLE_BUCKETS ];

  // Q[23,24,25,26]
  uint16_t __http_400_527[ 128 ];

  // [Q27.1-2]
  struct {
    int64_t http;
    int64_t service;
  } error_count;

  // [Q27.3]
  double error_rate;

  // [Q27.4]
  vgx_VGXServerMatrixInspect_t *inspect;

  // [Q27.5-8]
  struct {
    // [Q27.5]
    QWORD __rsv_27_5;
    
    // [Q27.6]
    QWORD __rsv_27_6;
    
    // [Q27.7]
    QWORD __rsv_27_7;
    
    // [Q27.8]
    QWORD __rsv_27_8;
  } matrix;
  

} vgx_VGXServerPerfCounters_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerFrontIOReady_t {
  vgx_VGXServerClient_t *client;
  struct pollfd *pfd;
} vgx_VGXServerFrontIOReady_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServerDispatcherIOReady_t {
  vgx_VGXServerDispatcherChannel_t *channel;
  struct pollfd *pfd;
} vgx_VGXServerDispatcherIOReady_t;



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_VGXServer_t {

  // -------------------------------------------------------------------
  // [Q1.1]
  // Server main loop
  comlib_task_t *TASK;

  // [Q1.2]
  // Various control
  union {
    QWORD _bits;
    struct {
      // [Q1.2.1.1]
      volatile int8_t serving_public;
      // [Q1.2.1.2]
      volatile int8_t serving_any;
      // [Q1.2.1.3]
      volatile int8_t __rsv_1_2_1_3;
      // [Q1.2.1.4]
      volatile int8_t __rsv_1_2_1_4;

      // [Q1.2.2.1]
      volatile int8_t polling_CCS;
      // [Q1.2.2.2]
      volatile int8_t public_service_in_TCS;
      // [Q1.2.2.3]
      volatile int8_t any_service_in_TCS;
      // [Q1.2.2.4]
      struct {
        volatile uint8_t change_pending    : 1;
        volatile uint8_t snapshot_request  : 1;
        volatile uint8_t reset_counters    : 1;
        volatile uint8_t stop_requested    : 1;
        volatile uint8_t inspect_request   : 1;
        volatile uint8_t _rsv_1_2_2_5_6    : 1;
        volatile uint8_t suspended         : 1;
        volatile uint8_t suspend_request   : 1;
      } flag_TCS;
    };
  } control;

  // [Q1.3]
  // Parent graph
  struct s_vgx_Graph_t *sysgraph;

  // [Q1.4-8]
  // Pre-allocated pools
  struct {
    // [Q1.4-7]
    // Client pool
    vgx_VGXServerClientPool_t clients;

    // [Q1.8]
    // Executor pool
    vgx_VGXServerExecutorPool_t *executors;
  } pool;
  // -------------------------------------------------------------------


  // -------------------------------------------------------------------
  // [Q2.1-4]
  // Server I/O
  struct {
    // [Q2.1]
    // Socket input buffer
    char *buffer;

    // [Q2.2]
    // List of file descriptors for poll()
    struct pollfd *pollfd_list;

    struct {
      // [Q2.3]
      // Front clients and channels with I/O ready sockets
      vgx_VGXServerFrontIOReady_t *front;

      // [Q2.4]
      // Dispatcher channels with I/O ready sockets
      vgx_VGXServerDispatcherIOReady_t *dispatcher;
    } ready;
  } io;

  // [Q2.5-8]
  // Counters and metrics
  struct {
    // [Q2.5]
    // Performance counters
    vgx_VGXServerPerfCounters_t *perf;

    // [Q2.6]
    // Maximum memory usage of process since start
    int64_t mem_max_process_use;

    // [Q2.7]
    // (not used)
    vgx_StringList_t *client_uris_snapshot;

    // [Q2.8]
    // Various small counters
    struct {
      // [Q2.8.1]
      uint8_t mem_max_use_pct;
      // [Q2.8.2]
      int8_t __rsv_2_8_2;
      // [Q2.8.3]
      int8_t __rsv_2_8_3;
      // [Q2.8.4]
      int8_t __rsv_2_8_4;
      // [Q2.8.5]
      int8_t __rsv_2_8_5;
      // [Q2.8.6]
      int8_t __rsv_2_8_6;
      // [Q2.8.7]
      int8_t __rsv_2_8_7;
      // [Q2.8.8]
      int8_t __rsv_2_8_8;
    } byte;
  } counters;
  // -------------------------------------------------------------------


  // -------------------------------------------------------------------
  // [Q3.1]
  // Bound URI
  vgx_URI_t *Listen;
  
  // [Q3.2-4]
  // Configuration
  struct {
    // [Q3.2]
    // Server configuration
    vgx_VGXServerConfig_t *cf_server;

    // [Q3.3]
    // Dynamically assigned friendly name of service
    CString_t *CSTR__service_name;

    // [Q3.4]
    struct {
      int8_t is_main;
      int8_t __rsv_3_5_2;
      int8_t __rsv_3_5_3;
      int8_t __rsv_3_5_4;
      int8_t __rsv_3_5_5;
      int8_t __rsv_3_5_6;
      int8_t __rsv_3_5_7;
      int8_t __rsv_3_5_8;
    } flag;
  } config;

  // [Q3.5]
  // Request serial number
  int64_t req_sn;

  // [Q3.6]
  // Nanosecond tick offset
  int64_t tbase_ns;

  // [Q3.7-8]
  // Endpoint resources
  struct {
    // [Q3.7]
    // Plugin execution entrypoint
    f_vgx_ServicePluginCall pluginf;

    // [Q3.8]
    // Pages, scripts, images, etc.
    framehash_t *artifacts;

  } resource;
  // -------------------------------------------------------------------


  // -------------------------------------------------------------------
  // [Q4-19]
  //
  vgx_VGXServerWorkDispatch_t dispatch;
  // -------------------------------------------------------------------


  // -------------------------------------------------------------------
  // [Q20 21 22]
  //
  vgx_VGXServerDispatcherMatrix_t matrix;
  // -------------------------------------------------------------------

} vgx_VGXServer_t;



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_vgx_IVGXServer_t {

  struct {
    int (*Create)( const char *prefix );
    void (*Destroy)( void );
  } Artifacts;

  struct {
    int (*Init)( void );
    void (*Clear)( void );
    struct {
      int (*Init)( void );
      void (*Clear)( void );
      int (*Register)( const char *plugin_name, vgx_server_plugin_phase phase );
      int (*Unregister)( const char *plugin_name );
      uint8_t (*IsRegistered)( const char *plugin_name );
    } Plugin;
  } Resource;

  struct {
    int (*StartNew)( struct s_vgx_Graph_t *SYSTEM, const char *ip, uint16_t port, const char *prefix, bool service_in, f_vgx_ServicePluginCall pluginf, vgx_VGXServerDispatcherConfig_t **cf_dispatcher );
    int (*StopDelete)( struct s_vgx_Graph_t *SYSTEM );
    int (*Restart)( struct s_vgx_Graph_t *SYSTEM, CString_t **CSTR__error );
    int (*GetPort)( const struct s_vgx_Graph_t *SYSTEM );
    const char * (*GetAdminIP)( const struct s_vgx_Graph_t *SYSTEM );
    int (*GetAdminPort)( const struct s_vgx_Graph_t *SYSTEM );
    const char * (*GetPrefix)( const struct s_vgx_Graph_t *SYSTEM );
    int (*SetName)( struct s_vgx_Graph_t *SYSTEM, const char *name );
    CString_t * (*GetName)( struct s_vgx_Graph_t *SYSTEM );
    vgx_StringList_t * (*GetAllClientURIs)( struct s_vgx_Graph_t *SYSTEM, int timeout_ms );
    int (*In)( struct s_vgx_Graph_t *SYSTEM );
    int (*Out)( struct s_vgx_Graph_t *SYSTEM );
  } Service;

  struct {
    int (*Get)( struct s_vgx_Graph_t *SYSTEM, vgx_VGXServerPerfCounters_t *counters, int timeout_ms );
    int (*Reset)( struct s_vgx_Graph_t *SYSTEM );
    int (*GetLatencyPercentile)( const vgx_VGXServerPerfCounters_t *counters, float pctX, double *pctX_short, double *pctX_long );
    vgx_VGXServerMatrixInspect_t * (*GetMatrixInspect)( struct s_vgx_Graph_t *SYSTEM, int port_offset, int timeout_ms );
  } Counters;

  struct {
    vgx_VGXServerRequest_t * (*New)( HTTPRequestMethod method, const char *path );
    void (*Delete)( vgx_VGXServerRequest_t **request );
    int (*AddHeader)( vgx_VGXServerRequest_t *request, const char *field, const char *value );
    int64_t (*AddContent)( vgx_VGXServerRequest_t *request, const char *data, int64_t sz );
  } Request;

  struct {
    vgx_VGXServerResponse_t * (*New)( const char *label );
    void (*Delete)( vgx_VGXServerResponse_t **response );
    int (*PrepareBody)( vgx_VGXServerResponse_t *response );
    int (*PrepareBodyError)( vgx_VGXServerResponse_t *response, CString_t *CSTR__error );
  } Response;

  struct {
    int (*SendAll)( vgx_URI_t *URI, vgx_VGXServerRequest_t *request, int timeout_ms );
    int (*ReceiveAll)( vgx_URI_t *URI, vgx_VGXServerResponse_t *response, int timeout_ms );
  } Util;

  struct {
    // Config
    vgx_VGXServerConfig_t * (*New)( void );
    void (*Delete)( vgx_VGXServerConfig_t **config );
    vgx_VGXServerConfig_t * (*Clone)( vgx_VGXServer_t *server );
    void (*SetFront)( vgx_VGXServerConfig_t *config, vgx_VGXServerFrontConfig_t **cf_front );
    void (*SetDispatcher)( vgx_VGXServerConfig_t *config, vgx_VGXServerDispatcherConfig_t **cf_dispatcher );
    const vgx_VGXServerConfig_t * (*Get)( vgx_VGXServer_t *server, uint16_t *rport, int *rwidth, int *rheight, bool *rallow_incomplete );
    void (*SetExecutorPluginEntrypoint)( vgx_VGXServerConfig_t *config, f_vgx_ServicePluginCall pluginf );
    
    // Front Config
    struct {
      vgx_VGXServerFrontConfig_t * (*New)( const char *ip, uint16_t port, uint16_t offset, const char *prefix );
      void (*Delete)( vgx_VGXServerFrontConfig_t **config );
    } Front;

    // Dispatcher Config
    struct {
      vgx_VGXServerDispatcherConfig_t * (*New)( int width, int height );
      void (*Delete)( vgx_VGXServerDispatcherConfig_t **config );
      int (*SetReplicaAddress)( vgx_VGXServerDispatcherConfig_t *config, int partition_id, int replica_id, const char *host, int port, CString_t **CSTR__error );
      int (*SetReplicaAccess)( vgx_VGXServerDispatcherConfig_t *config, int replica_id, bool writable );
      int (*SetReplicaChannels)( vgx_VGXServerDispatcherConfig_t *config, int replica_id, int channels );
      int (*SetReplicaPriority)( vgx_VGXServerDispatcherConfig_t *config, int replica_id, int priority );
      bool (*Verify)( const vgx_VGXServerDispatcherConfig_t *config, CString_t **CSTR__error );
      int (*Channels)( const vgx_VGXServerDispatcherConfig_t *config );
      void (*Dump)( const vgx_VGXServerDispatcherConfig_t *config );
    } Dispatcher;
  } Config;

  struct {
    struct {
      int (*Init)( struct s_vgx_VGXServer_t *server, CString_t **CSTR__error );
      void (*Clear)( struct s_vgx_VGXServer_t *server );
      int (*Width)( const struct s_vgx_VGXServer_t *server );
      int (*BacklogSize)( struct s_vgx_VGXServer_t *server );
      int (*BacklogCount)( struct s_vgx_VGXServer_t *server );
    } Matrix;
  } Dispatcher;

} vgx_IVGXServer_t;





#endif
