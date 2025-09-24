/*
###################################################
#
# File:   _vgx_serialization.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VGX_SERIALIZATION_H
#define _VGX_VGX_SERIALIZATION_H

#include "_vgx.h"


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const QWORD g_BEGIN_FILE_DELIM[] = {
  0x1111111111111111ULL,
  0x1111CCCCCCCC1111ULL,
  0x1111CCCCCCCC1111ULL,
  0x1111111111111111ULL,
  0xf39daf3a2eb8c063ULL,
  0x32676f8d90e82356ULL,
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL
};

static const QWORD g_END_FILE_DELIM[] = {
  0xFFFFFFFFFFFFFFFFULL,
  0xFFFFFFFFFFFFFFFFULL,
  0x809bfeeb4ce26bfcULL,
  0xcde72807b24f3a83ULL,
  0x0000000000000000ULL,
  0x0000CCCCCCCC0000ULL,
  0x0000CCCCCCCC0000ULL,
  0x0000000000000000ULL
};

static const QWORD g_BEGIN_SECTION_DELIM[] = {
  0x1111111111111111ULL,
  0x0000BBBBBBBB0000ULL,
  0x1111111111111111ULL,
  0xFFFFFFFFFFFFFFFFULL
};

static const QWORD g_END_SECTION_DELIM[] = {
  0xFFFFFFFFFFFFFFFFULL,
  0x0000000000000000ULL,
  0x0000EEEEEEEE0000ULL,
  0x0000000000000000ULL
};

static const QWORD g_KEY_FOLLOWS[] = {
  0x0000111100000000ULL
};

static const QWORD g_VALUE_FOLLOWS[] = {
  0x0000222200000000ULL
};

static const QWORD g_END_KEY_VALUE[] = {
  0x0000EEEE00000000ULL
};


#define __OUTPUT  output
#define __INPUT   input
#define __NQWORDS nqwords



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_graph_state_t {
  // BEGIN
  struct {
    // CL1
    QWORD _delim[qwsizeof(g_BEGIN_FILE_DELIM)];
  } begin;

  // GRAPH
  struct {
    // CL2
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t order;      // Number of vertices in graph
    int64_t size;       // Number of arcs in graph
    int64_t opcount;    // Graph's last operation number
    int64_t n_ops;      // Operations since last persist
    // CL 3,4,5
    char name[VGX_MAX_GRAPH_NAME+1];     // Graph name
    // CL 6
    char version[64];   // VGX version
    // CL 7
    int64_t readonly;   // Graph state is readonly
    int64_t n_tmx;      // Number of vertices with expiration (self or arc(s))
    QWORD local_only;   // Nonzero when graph operations should not be streamed when attached
    QWORD __rsv_7_4;
    objectid_t tx_id_out;   // Last transaction ID produced
    int64_t tx_serial_out;  // Last transaction serial number produced
    int64_t tx_count_out;   // Number of out transactions
    // CL 8
    objectid_t tx_id_in;    // Last transaction ID consumed
    int64_t tx_serial_in;   // Last transaction serial number consumed
    int64_t tx_count_in;    // Number of in transactions
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } graph;

  // VERTEX TYPE
  struct {
    // CL 9
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t ntype;      // Number of vertex types in use
    QWORD __rsv0;
    QWORD __rsv1;
    QWORD __rsv2;
    // (256) CL 10 - 265
    struct {
      QWORD typehash;   // Hash of full type name
      QWORD typeenc;    // Type enumeration code
      int64_t order;    // Number of vertices of this type
      QWORD __rsv3;
      char prefix[32];  // Type name prefix (truncated)
    } entry[VERTEX_TYPE_ENUMERATION_MAX_ENTRIES];
    // CL 266
    QWORD __rsv4;
    QWORD __rsv5;
    QWORD __rsv6;
    QWORD __rsv7;
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } vertex_type;
  
  // RELATIONSHIP
  struct {
    // CL 267
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t nrel;       // Number of relationships in use
    QWORD __rsv0;
    QWORD __rsv1;
    QWORD __rsv2;
    // (15872) CL 268 - 16139
    struct {
      QWORD relhash;    // Hash of full relationship
      QWORD relenc;     // Relationship enumeration code
      int64_t size;     // Number of relationships of this type (TBD)
      QWORD __rsv3;
      char prefix[32];  // Relationship prefix (truncated)
    } entry[__VGX_PREDICATOR_REL_VALID_RANGE_SIZE];
    // CL 16140
    QWORD __rsv4;
    QWORD __rsv5;
    QWORD __rsv6;
    QWORD __rsv7;
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } relationship;

  // VERTEX PROPERTY
  struct {
    // CL 16141
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t nkey;       // Number of unique property keys in use
    int64_t nstrval;    // Number of unique property string values in use
    int64_t nprop;      // Total number of properties for all vertices
    int64_t nstrings;   // Total number of strings
    // CL 16142
    QWORD __rsv2;
    QWORD __rsv3;
    QWORD __rsv4;
    QWORD __rsv5;
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } vertex_property;

  // VECTOR
  struct {
    // CL 16143
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t ndim;       // Number of unique vector dimensions in use
    QWORD __rsv1;
    int64_t nvectors;   // Total number of vector for all vertices
    QWORD __rsv3;
    // CL 16144
    QWORD __rsv4;
    QWORD __rsv5;
    QWORD __rsv6;
    QWORD __rsv7;
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } vector;

  // TIME (all in milliseconds since 1970)
  struct {
    // CL 16145
    QWORD _start[qwsizeof(g_BEGIN_SECTION_DELIM)];
    int64_t graph_t0;   // Timestamp when first graph instance was created
    int64_t TIC_t0;     // Timestamp when current graph instance was started
    int64_t graph_up;   // Time current graph instance has been alive
    QWORD __rsv0;
    // CL 16146
    int64_t persist_t0; // Timestamp when serialization started
    int64_t persist_t1; // Timestamp when serialization completed
    int64_t persist_n;  // Number of times persisted
    int64_t persist_t;  // Total time spent persisting
    QWORD _end[qwsizeof(g_END_SECTION_DELIM)];
  } time;

  // *rsv*
  struct {
    // CL 16147 - 16383
    QWORD __pad[ 8*237 ];
  } __rsv;

  // END
  struct {
    // CL 16384
    QWORD _delim[qwsizeof(g_END_FILE_DELIM)];
  } end;

  // -----------
  // Exactly 1MB
  // -----------

} graph_state_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define WRITE_OR_THROW( QwordArray, Count, OpRef )                                  \
  do {                                                                              \
    int64_t __n = CALLABLE( __OUTPUT )->WriteNolock( __OUTPUT, QwordArray, Count ); \
    if( __n != Count ) {                                                            \
      __NQWORDS = -1;                                                               \
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, OpRef );                                   \
    }                                                                               \
    else {                                                                          \
      __NQWORDS += __n;                                                             \
    }                                                                               \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define READ_OR_THROW( QwordArray, Count, OpRef )                                     \
  do {                                                                                \
    int64_t __n = CALLABLE( __INPUT )->ReadNolock( __INPUT, (void**)&(QwordArray), Count );   \
    if( __n != Count ) {                                                              \
      __NQWORDS = -1;                                                                 \
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, OpRef );                                     \
    }                                                                                 \
    else {                                                                            \
      __NQWORDS += __n;                                                               \
    }                                                                                 \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define EVAL_OR_THROW( FuncThatEvalsToInt64, OpRef )  \
  do {                                                \
    int64_t __n = FuncThatEvalsToInt64;               \
    if( __n < 0 ) {                                   \
      THROW_ERROR( CXLIB_ERR_GENERAL, OpRef );        \
    }                                                 \
    else {                                            \
      __NQWORDS += __n;                               \
    }                                                 \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define WRITE_OR_RETURN( QwordArray, Count )                                        \
  do {                                                                              \
    int64_t __n = CALLABLE( __OUTPUT )->WriteNolock( __OUTPUT, QwordArray, Count ); \
    if( __n != Count ) {                                                            \
      return -1;                                                                    \
    }                                                                               \
    else {                                                                          \
      __NQWORDS += __n;                                                             \
    }                                                                               \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define READ_OR_RETURN( QwordArray, Count )                                           \
  do {                                                                                \
    int64_t __n = CALLABLE( __INPUT )->ReadNolock( __INPUT, (void**)&(QwordArray), Count );   \
    if( __n != Count ) {                                                              \
      return -1;                                                                      \
    }                                                                                 \
    else {                                                                            \
      __NQWORDS += __n;                                                               \
    }                                                                                 \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define EXPECT_OR_THROW( ExpectQwordArray, ErrCode )                              \
  do {                                                                            \
    QWORD __expect[ qwsizeof( ExpectQwordArray ) ];                               \
    QWORD *pdelim = __expect;                                                     \
    READ_OR_THROW( pdelim, qwsizeof(__expect), ErrCode );                         \
    if( !__qwarray_match( __expect, ExpectQwordArray, qwsizeof(__expect) ) ) {    \
      THROW_ERROR( CXLIB_ERR_CORRUPTION, ErrCode);                                \
    }                                                                             \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define EXPECT_OR_RETURN( ExpectQwordArray, ErrCode )                             \
  do {                                                                            \
    QWORD __expect[ qwsizeof( ExpectQwordArray ) ];                               \
    QWORD *pdelim = __expect;                                                     \
    READ_OR_RETURN( pdelim, qwsizeof(__expect) );                                 \
    if( !__qwarray_match( __expect, ExpectQwordArray, qwsizeof(__expect) ) ) {    \
      return -1;                                                                  \
    }                                                                             \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static bool __qwarray_match( const QWORD a[], const QWORD b[], size_t sz ) {
  const QWORD *pa = a;
  const QWORD *pb = b;
  for( size_t i=0; i<sz; i++ ) {
    if( *pa++ != *pb++ ) {
      return false;
    }
  }
  return true;
}



typedef struct s_vgx_ISerialization_t {
  int64_t (*WriteString)( const CString_t *string, CQwordQueue_t *OUTPUT );
  int64_t (*WriteDigestedString)( const CString_t *string, CQwordQueue_t *OUTPUT );
  int64_t (*WriteKeyValue)( const CString_t *key, const CString_t *value, CQwordQueue_t *OUTPUT );
  int64_t (*WriteBeginFile)( const CString_t *filename, CQwordQueue_t *OUTPUT );
  int64_t (*WriteEndFile)( CQwordQueue_t *OUTPUT );
  int64_t (*WriteBeginSection)( const CString_t *section_name, CQwordQueue_t *OUTPUT );
  int64_t (*WriteBeginSectionFormat)( CQwordQueue_t *OUTPUT, const char *format, ... );
  int64_t (*WriteEndSection)( CQwordQueue_t *OUTPUT );

  int64_t (*ReadString)( CString_t **string, const CString_t *expect, CQwordQueue_t *INPUT );
  int64_t (*ReadDigestedString)( CString_t **string, const CString_t *expect, CQwordQueue_t *INPUT );
  int64_t (*ReadKeyValue)( CString_t **key, CString_t **value, CQwordQueue_t *INPUT );
  int64_t (*ExpectBeginFile)( const CString_t *filename, CQwordQueue_t *INPUT );
  int64_t (*ExpectEndFile)( CQwordQueue_t *INPUT );
  int64_t (*ExpectBeginSection)( const CString_t *section_name, CQwordQueue_t *INPUT );
  int64_t (*ExpectBeginSectionFormat)( CQwordQueue_t *INPUT, const char *format, ... );
  int64_t (*ExpectEndSection)( CQwordQueue_t *INPUT );

  void (*PrintGraphCounts_ROG)( vgx_Graph_t *self, const char *message );
  int64_t (*BulkSerialize)( vgx_Graph_t *self, vgx_ExecutionTimingBudget_t *timing_budget, bool force, bool remote, CString_t **CSTR__error );
  graph_state_t * (*LoadState)( vgx_Graph_t *self );

} vgx_ISerialization_t;



DLL_HIDDEN extern vgx_ISerialization_t iSerialization;







#endif


