/*
###################################################
#
# File:   cxmalloc.h
# Author: Stian Lysne
#
#
###################################################
*/

#ifndef CXMALLOC_INCLUDED_H
#define CXMALLOC_INCLUDED_H


#include "comlib.h"

struct s_cxmalloc_family_t;
struct s_cxmalloc_object_processing_context_t;


/*******************************************************************//**
 * cxmalloc_metaflex_element_t
 *
 ***********************************************************************
 */
typedef union u_cxmalloc_metaflex_element_t {
  uint64_t bits;        // G1:  64 bits
  uint64_t integer;     // G2:  64-bit unsigned int
  double real;          // G3:  double precision float
  void *pointer;        // G4:  pointer
  char bytes[8];        // G5:  8 bytes
  struct {
    int32_t _i32;       // G6-1/2:  32-bit signed int
    float   _f32;       // G6-2/2:  single precision float
  };
  struct {
    int32_t _i32_0;     // G7-1/2:  32-bit signed int 1/2
    int32_t _i32_1;     // G7-2/2:  32-bit signed int 2/2
  };
  struct {
    float   _f32_0;     // G8-1/2:  single precision float 1/2
    float   _f32_1;     // G8-2/2:  single precision float 2/2
  };
  struct {
    float   _f32_x;     // G9-1/3:  single precision float
    int16_t _i16_a;     // G9-2/3:  16-bit signed int 1/2
    int16_t _i16_b;     // G9-3/3:  16-bit signed int 2/2
  };
  struct {
    int16_t _i16_0;     // G10-1/4: 16-bit signed int 1/4
    int16_t _i16_1;     // G10-2/4: 16-bit signed int 2/4
    int16_t _i16_2;     // G10-3/4: 16-bit signed int 3/4
    int16_t _i16_3;     // G10-4/4: 16-bit signed int 4/4
  };
} cxmalloc_metaflex_element_t;


typedef union u_cxmalloc_metaflex_t {
  __m128i M;
  aptr_t aptr;
  struct {
    union u_cxmalloc_metaflex_element_t M1;
    union u_cxmalloc_metaflex_element_t M2;
  };
} cxmalloc_metaflex_t;



/*******************************************************************//**
 * cxmalloc_header_t
 ***********************************************************************
 */
ALIGNED_TYPE( struct, 32 ) s_cxmalloc_header_t {
  cxmalloc_metaflex_t metaflex;
  __m128i __internal;
} cxmalloc_header_t;



typedef struct s_cxmalloc_object_tap_t {
  QWORD *line_meta;
  QWORD *line_obj;
  QWORD *line_array;
} cxmalloc_object_tap_t;


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
struct s_cxmalloc_line_serialization_context_t;
typedef int (*f_cxmalloc_line_serializer)( struct s_cxmalloc_line_serialization_context_t *context );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_cxmalloc_line_serialization_context_t {
  f_cxmalloc_line_serializer serialize;
  void *linehead;
  cxmalloc_object_tap_t tapout;
  int64_t line_qwords;
  CQwordQueue_t *out_ext;
  int64_t ext_offset;
  QWORD *__buffer;
#ifdef CXMALLOC_CONSISTENCY_CHECK
  FILE *objdump;
#endif
} cxmalloc_line_serialization_context_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
struct s_cxmalloc_line_deserialization_context_t;
typedef int (*f_cxmalloc_line_deserializer)( struct s_cxmalloc_line_deserialization_context_t *context );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_cxmalloc_line_deserialization_context_t {
  f_cxmalloc_line_deserializer deserialize;
  void *linehead;
  cxmalloc_object_tap_t tapin;
  int64_t line_qwords;
  CQwordQueue_t *in_ext;
  int64_t ext_offset;
  QWORD *__buffer;
  struct s_cxmalloc_family_t *family;
  void *auxiliary[8];
} cxmalloc_line_deserialization_context_t;



typedef uint8_t cxmalloc_object_class_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef union u_cxmalloc_handle_t {
  QWORD qword;
  struct {
    union {
      struct {
        cxmalloc_object_class_t objclass; /* extra custom user info (NOT USED BY ALLOCATOR) */
        uint8_t __na1[3];
      };
      struct {
        uint32_t __na2   : 8;    
        uint32_t offset : 24;   /* line offset */
      };
    };
    uint16_t bidx;            /* block index */
    uint16_t aidx;            /* allocator index */
  };
  struct {
    uint64_t __na3      : 8;  /* [-------N] */
    uint64_t allocdata  : 56; /* [axbxooo-] */
  };
} cxmalloc_handle_t;



/*******************************************************************//**
 * cxmalloc_descriptor_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_descriptor_t {
  struct {
    cxmalloc_metaflex_t initval;    /* [1] */
    uint32_t __rsv1[3];
    uint32_t serialized_sz;         /* [2] */
  } meta;
  struct {
    uint32_t sz;                    /* [3] number of extra object bytes to allocate outside of the array for custom object data */
    uint32_t serialized_sz;         /* [4] */
  } obj;
  struct {
    uint32_t sz;                    /* [5] number of bytes per allocation unit for this allocator family */
    uint32_t serialized_sz;         /* [6] */
  } unit;
  f_cxmalloc_line_serializer serialize_line;      /* [7] */
  f_cxmalloc_line_deserializer deserialize_line;  /* [8] */
  struct {
    size_t block_sz;                /* [9] byte limit for data blocks in any allocator in the family */
    uint32_t line_limit;            /* [10] hint that this is the largest sized lines we expect to be handled without resorting to malloc */
    unsigned int subdue;            /* [11] controls allocator size stepping */
    int allow_oversized;            /* [12] set to non-zero if one-off malloc() is allowed for oversized allocation requests */
    int max_allocators;             /* [13] upper limit on the number of allocators that will be created (unless 'line_limit' requires more allocators) */
  } parameter;
  struct {
    const CString_t *CSTR__path;    /* [14] family base directory name - full path */
    QWORD _rsv;
  } persist;
  QWORD __rsv3[3];
  struct {
    void *obj[8];                   /* [15] list of associated objects needed for deserialization  */
  } auxiliary;
} cxmalloc_descriptor_t;




/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef int64_t (*f_cxmalloc_object_processor)( struct s_cxmalloc_object_processing_context_t * const context, comlib_object_t *obj );



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
typedef struct s_cxmalloc_object_processing_context_t {
  f_cxmalloc_object_processor process_object;
  
  object_class_t object_class;

  int n_allocators_processed;
  int n_blocks_processed;
  int64_t n_objects_processed;

  int n_allocators_active;
  int n_blocks_active;
  int64_t n_objects_active;

  void *filter;
  void *input;
  void *output;

  bool completed;
  bool error;
  int errcode;

} cxmalloc_object_processing_context_t;




/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_cxmalloc_family_constructor_args_t {
  const cxmalloc_descriptor_t *family_descriptor;
} cxmalloc_family_constructor_args_t;



/*******************************************************************//**
 * f_get_object_identifier
 * Suitable function to use for extracting an object identifier
 ***********************************************************************
 */
typedef const char * (*f_get_object_identifier)( const comlib_object_t *obj );




/*******************************************************************//**
 * cxmalloc_vtable_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_family_vtable_t {
  /* common methods */
  COMLIB_VTABLE_HEAD

  /* Allocate */
  void *            (*New)(                   struct s_cxmalloc_family_t *family, uint32_t sz );

  /* Line array methods */
  void *            (*Renew)(                 struct s_cxmalloc_family_t *family, void *line );
  int64_t           (*Own)(                   struct s_cxmalloc_family_t *family, void *line );
  int64_t           (*Discard)(               struct s_cxmalloc_family_t *family, void *line );
  int64_t           (*RefCount)(              struct s_cxmalloc_family_t *family, const void *line );
  uint32_t          (*PrevLength)(            struct s_cxmalloc_family_t *family, const void *line );
  uint32_t          (*NextLength)(            struct s_cxmalloc_family_t *family, const void *line );
  uint32_t          (*LengthOf)(              struct s_cxmalloc_family_t *family, const void *line );
  uint16_t          (*IndexOf)(               struct s_cxmalloc_family_t *family, const void *line );
  void *            (*ObjectFromArray)(       struct s_cxmalloc_family_t *family, const void *line );
  cxmalloc_handle_t (*ArrayAsHandle)(         struct s_cxmalloc_family_t *family, const void *line );
  void *            (*HandleAsArray)(         struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );
  cxmalloc_metaflex_t * (*Meta)(              struct s_cxmalloc_family_t *family, const void *line );
  
  /* Object methods */
  void *            (*RenewObject)(           struct s_cxmalloc_family_t *family, void *obj );
  int64_t           (*OwnObjectNolock)(       struct s_cxmalloc_family_t *family, void *obj );
  int64_t           (*OwnObject)(             struct s_cxmalloc_family_t *family, void *obj );
  int64_t           (*OwnObjectByHandle)(     struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );
  int64_t           (*DiscardObjectNolock)(   struct s_cxmalloc_family_t *family, void *obj );
  int64_t           (*DiscardObject)(         struct s_cxmalloc_family_t *family, void *obj );
  int64_t           (*DiscardObjectByHandle)( struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );
  int64_t           (*RefCountObjectNolock)(  struct s_cxmalloc_family_t *family, const void *obj );
  int64_t           (*RefCountObject)(        struct s_cxmalloc_family_t *family, const void *obj );
  int64_t           (*RefCountObjectByHandle)(struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );
  void *            (*ArrayFromObject)(       struct s_cxmalloc_family_t *family, const void *obj );
  cxmalloc_handle_t (*ObjectAsHandle)(        struct s_cxmalloc_family_t *family, const void *obj );
  void *            (*HandleAsObjectNolock)(  struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );
  void *            (*HandleAsObjectSafe)(    struct s_cxmalloc_family_t *family, cxmalloc_handle_t handle );

  /* Family methods */
  uint32_t          (*MinLength)(             struct s_cxmalloc_family_t *family );
  uint32_t          (*MaxLength)(             struct s_cxmalloc_family_t *family );
  uint16_t          (*SizeBounds)(            struct s_cxmalloc_family_t *family, uint32_t sz, uint32_t *low, uint32_t *high );
  void              (*LazyDiscards)(          struct s_cxmalloc_family_t *family, int use_lazy_discards );
  int               (*Check)(                 struct s_cxmalloc_family_t *family );
  comlib_object_t * (*GetObjectAtAddress)(    struct s_cxmalloc_family_t *family, QWORD address );
  comlib_object_t * (*FindObjectByObid)(      struct s_cxmalloc_family_t *family, objectid_t obid );
  comlib_object_t * (*GetObjectByOffset)(     struct s_cxmalloc_family_t *family, int64_t *poffset );
  int64_t           (*Sweep)(                 struct s_cxmalloc_family_t *family, f_get_object_identifier get_object_identifier );
  int               (*Size)(                  struct s_cxmalloc_family_t *family );
  int64_t           (*Bytes)(                 struct s_cxmalloc_family_t *family );
  histogram_t *     (*Histogram)(             struct s_cxmalloc_family_t *family );
  int64_t           (*Active)(                struct s_cxmalloc_family_t *family );
  double            (*Utilization)(           struct s_cxmalloc_family_t *family );

  /* Readonly */
  int               (*SetReadonly)(           struct s_cxmalloc_family_t *family ); 
  int               (*IsReadonly)(            struct s_cxmalloc_family_t *family ); 
  int               (*ClearReadonly)(         struct s_cxmalloc_family_t *family ); 

  /* Serialization (non-standard interface, separate from the COMLIB serialization protocol) */
  int64_t           (*BulkSerialize)(         struct s_cxmalloc_family_t *family, bool force );
  int64_t           (*RestoreObjects)(        struct s_cxmalloc_family_t *family );
  int64_t           (*ProcessObjects)(        struct s_cxmalloc_family_t *family, cxmalloc_object_processing_context_t *context );

} cxmalloc_family_vtable_t;



/*******************************************************************//**
 * cxmalloc_family_t
 ***********************************************************************
 */
typedef struct s_cxmalloc_family_t {
  // ------------------------------------------------
  /* [Q1.1/2] */
  COMLIB_OBJECT_HEAD( cxmalloc_family_vtable_t )
  /* [Q1.3/4] id */
  objectid_t obid;
  /* [Q1.5] family descriptor */
  cxmalloc_descriptor_t *descriptor;
  /* [Q1.6.1] unit_sz = 2 ** unit_sz_order */
  unsigned int unit_sz_order;
  /* [Q1.6.2] number of extra object bytes to allocate outside of the array for custom object data */
  unsigned int object_bytes;
  /* [Q1.7.1] total number or header bytes */
  unsigned int header_bytes;
  /* [Q1.7.2] */
  unsigned int subdue;
  /* [Q1.8.1] number of allocators in family */
  int size;
  /* [Q1.8.2] largest line size in family */
  uint32_t max_length;
  // ------------------------------------------------
  /* [Q2] family mutex */
  CS_LOCK lock;
  // ------------------------------------------------
  /* [Q3.1] table of allocators of different line sizes in this family */
  struct s_cxmalloc_allocator_t **allocators;
  /* [Q3.2.1] smallest line size in family */
  uint32_t min_length;
  /* [Q3.2.2] flags */
  struct {
    char lazy_discards; /* [12a] normally false, set to 1 to speed up destruction of family */
    char ready;         /* [12b] true (1) when family is constructed, 0 otherwise */
    uint8_t __rsv_3_2_2_3;
    uint8_t __rsv_3_2_2_4;
  } flag;
  /* [Q3.3.1] */
  int readonly_cnt; /* readonly when >0, writable when 0 */
  /* [Q3.3.2] */
  DWORD __rsv_3_3_2;
  /* [Q3.4]*/
  QWORD __rsv_3_4;
  /* [Q3.5]*/
  QWORD __rsv_3_5;
  /* [Q3.6]*/
  QWORD __rsv_3_6;
  /* [Q3.7]*/
  QWORD __rsv_3_7;
  /* [Q3.8]*/
  QWORD __rsv_3_8;
 
} cxmalloc_family_t;




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define CXMALLOC_HEADER_TEMPLATE( ATYPE, UTYPE, ARRAY )                 \
                                                                        \
typedef struct s_##ATYPE##_interface_t {                                \
  /* for the static allocator instance only */                          \
  int           (*Init)( void );                                        \
  void          (*Clear)( void );                                       \
  /* Allocate */                                                        \
  UTYPE *       (*New)( uint32_t sz );                                  \
  /* Line array methods */                                              \
  UTYPE *       (*Renew)( UTYPE *ARRAY );                               \
  int64_t       (*Own)( UTYPE *ARRAY );                                 \
  int64_t       (*Discard)( UTYPE *ARRAY );                             \
  int64_t       (*RefCount)( const UTYPE *ARRAY );                      \
  uint32_t      (*PrevLength)( const UTYPE *ARRAY );                    \
  uint32_t      (*NextLength)( const UTYPE *ARRAY );                    \
  uint32_t      (*LengthOf)( const UTYPE *ARRAY );                      \
  uint16_t      (*IndexOf)( const UTYPE *ARRAY );                       \
  void *        (*ObjectFromArray)( const UTYPE *ARRAY );               \
  cxmalloc_handle_t (*ArrayAsHandle)( const UTYPE *ARRAY );             \
  void *        (*HandleAsArray)( cxmalloc_handle_t handle );           \
  cxmalloc_metaflex_t * (*Meta)( const UTYPE *ARRAY );                  \
  /* Object methods */                                                  \
  void *        (*RenewObject)( void *obj );                            \
  int64_t       (*OwnObjectNolock)( void *obj );                        \
  int64_t       (*OwnObject)( void *obj );                              \
  int64_t       (*OwnObjectByHandle)( cxmalloc_handle_t handle );       \
  int64_t       (*DiscardObjectNolock)( void *obj );                    \
  int64_t       (*DiscardObject)( void *obj );                          \
  int64_t       (*DiscardObjectByHandle)( cxmalloc_handle_t handle );   \
  int64_t       (*RefCountObjectNolock)( const void *obj );             \
  int64_t       (*RefCountObject)( const void *obj );                   \
  int64_t       (*RefCountObjectByHandle)( cxmalloc_handle_t handle );  \
  void *        (*ArrayFromObject)( const void *obj );                  \
  cxmalloc_handle_t (*ObjectAsHandle)( const void *obj );               \
  void *        (*HandleAsObjectNolock)( cxmalloc_handle_t handle );    \
  void *        (*HandleAsObjectSafe)( cxmalloc_handle_t handle );      \
  /* Family methods */                                                  \
  uint32_t      (*MinLength)( void );                                   \
  uint32_t      (*MaxLength)( void );                                   \
  uint16_t      (*SizeBounds)( uint32_t sz, uint32_t *low, uint32_t *high ); \
  void          (*LazyDiscards)( int use_lazy_discards );               \
  int           (*Check)( void );                                       \
  comlib_object_t * (*GetObjectAtAddress)( QWORD address );             \
  comlib_object_t * (*FindObjectByObid)( objectid_t obid );             \
  comlib_object_t * (*GetObjectByOffset)( int64_t *poffset );           \
  int64_t       (*Sweep)( f_get_object_identifier get_object_identifier ); \
  int           (*Size)( void );                                        \
  int64_t       (*Bytes)( void );                                       \
  histogram_t * (*Histogram)( void );                                   \
  int64_t       (*Active)( void );                                      \
  double        (*Utilization)( void );                                 \
  /* Readonly */                                                        \
  int           (*SetReadonly)( void );                                 \
  int           (*IsReadonly)( void );                                  \
  int           (*ClearReadonly)( void );                               \
  /* Serialization */                                                   \
  int64_t       (*BulkSerialize)( bool force );                         \
  int64_t       (*RestoreObjects)( void );                              \
  int64_t       (*ProcessObjects)( cxmalloc_object_processing_context_t *context );  \
  /* Descriptor */                                                      \
  const cxmalloc_descriptor_t *descriptor;                              \
} ATYPE##_interface_t;                                                  \
                                                                        \
                                                                        \
DLL_EXPORT extern const ATYPE##_interface_t * ATYPE##Interface( void );        \
DLL_EXPORT extern cxmalloc_family_t * ATYPE##Singleton( void );                \


#define CXMALLOC_META_FROM_OBJECT( ObjectPtr )         ((cxmalloc_metaflex_t*)((char*)(ObjectPtr)-sizeof(__m256i)))




void cxmalloc_family_RegisterClass( void );
void cxmalloc_family_UnregisterClass( void );

#ifdef __cplusplus
extern "C" {
#endif

DLL_EXPORT extern int cxmalloc_INIT(void);
DLL_EXPORT extern void cxmalloc_DESTROY(void);

DLL_EXPORT extern const char * cxmalloc_version( bool ext );

#ifdef __cplusplus
}
#endif

#endif




