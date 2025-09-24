/*
###################################################
#
# File:   objectmodel.h
#
###################################################
*/
#ifndef COMLIB_OBJECTMODEL_H
#define COMLIB_OBJECTMODEL_H


#include "cxlib.h"
#include "classdefs.h"
#include "moduledefs.h"

struct s_comlib_object_t;
struct s_CStringQueue_t;
struct s_CDwordQueue_t;
struct s_CQwordQueue_t;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef void * p_constructor_args_t;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int (*f_object_comparator_t)( const struct s_comlib_object_t *self, const void *idptr );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef void * (*f_object_identifier_t)( const struct s_comlib_object_t *self );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int64_t (*f_object_serializer_t)( const struct s_comlib_object_t *self, struct s_CQwordQueue_t *out_queue );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_t * (*f_object_deserializer_t)( struct s_comlib_object_t *container, struct s_CQwordQueue_t *in_queue );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_t * (*f_object_constructor_t)( const void *identifier, const p_constructor_args_t args );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef void (*f_object_destructor_t)( struct s_comlib_object_t *self );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_CStringQueue_t * (*f_object_representer_t)( const struct s_comlib_object_t *self, struct s_CStringQueue_t *output );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef void * (*f_object_allocate_t)( struct s_comlib_object_t *allocator, size_t elements );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef int64_t (*f_object_deallocate_t)( struct s_comlib_object_t *allocator, void *obj );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef uint16_t (*f_allocator_size_bounds_t)( struct s_comlib_object_t *allocator, uint32_t sz, uint32_t *low, uint32_t *high );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_object_allocator_context_t {
  struct s_comlib_object_t *allocator;
  uint32_t max_elems;
  f_object_allocate_t allocfunc;
  f_object_deallocate_t deallocfunc;
  f_allocator_size_bounds_t bounds;
} object_allocator_context_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_t * (*f_object_allocator_t)( const struct s_comlib_object_t *self );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_VTABLE_HEAD                                        \
  f_object_comparator_t vm_cmpid;           /* vm_cmpid       */  \
  f_object_identifier_t vm_getid;           /* vm_getid       */  \
  f_object_serializer_t vm_serialize;       /* vm_serialize   */  \
  f_object_deserializer_t vm_deserialize;   /* vm_deserialize */  \
  f_object_constructor_t vm_construct;      /* vm_construct   */  \
  f_object_destructor_t vm_destroy;         /* vm_destroy     */  \
  f_object_representer_t vm_represent;      /* vm_represent   */  \
  f_object_allocator_t vm_allocator;        /* vm_allocator   */



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_vtable_t {
  COMLIB_VTABLE_HEAD
} comlib_object_vtable_t;



typedef enum e_object_idtype_t {
  OBJECT_IDENTIFIED_BY_NULL,
  OBJECT_IDENTIFIED_BY_NONE,
  OBJECT_IDENTIFIED_BY_SHORTID,
  OBJECT_IDENTIFIED_BY_QWORD,
  OBJECT_IDENTIFIED_BY_ADDRESS,
  OBJECT_IDENTIFIED_BY_OBJECTID,
  OBJECT_IDENTIFIED_BY_HIGHID,
  OBJECT_IDENTIFIED_BY_SHORTSTRING,
  OBJECT_IDENTIFIED_BY_LONGSTRING
} object_idtype_t;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_typeslot_t {
  comlib_object_vtable_t *vtable; // QWORD
  size_t obsize;                  // QWORD
  object_basetype_t basetype;     // DWORD
  object_idtype_t idtype;         // DWORD
  int16_t idstrlen;               // WORD
  uint8_t flags;                  // BYTE
} comlib_object_typeslot_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define _COMLIB_OBJECT_BASETYPE_BITS 8
#define _COMLIB_OBJECT_FLAGS_BITS 8
#define _COMLIB_OBJECT_CLASS_BITS  16
#define _COMLIB_OBJECT_IDTYPE_BITS 16
#define _COMLIB_OBJECT_IDSTRLEN_BITS 16
typedef union u_comlib_object_typeinfo_t {
  QWORD qword;
  struct {
    uint8_t tp_basetype;
    uint8_t tp_flags;
    uint8_t tp_class;
    uint8_t __tp_rsv;
    uint16_t tp_idtype;
    uint16_t tp_idstrlen;
  };
} comlib_object_typeinfo_t;

//                                  tp_basetype ---------------||
//                                  tp_flags ----------------||
//                                  tp_class --------------||
//                                  __tp_rsv ------------||
//                                  tp_idtype -------||||
//                                  tp_idlen ----||||
//                                               ....::::__..::..
#define _COMLIB_OBJECT_TYPEINFO_NOFLAGS_MASK   0xFFFFFFFF00FF00FFULL




/*******************************************************************//**
 * 
 * All objects within this entire framework must include this header as
 * its first element. A set of shared attributes will then be available
 * for all kinds of objects so we can perform a minimum set of standard
 * operations on their instances.
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_HEAD( VTableType )  \
  union {                                 \
    void *object;                         \
    VTableType *vtable;                   \
  };                                      \
  comlib_object_typeinfo_t typeinfo;

typedef struct s_comlib_object_head_t {
  COMLIB_OBJECT_HEAD( comlib_object_vtable_t )
} comlib_object_head_t;


#define COMLIB_OBJECT( ObjectPtr )                                  ((comlib_object_t*)(ObjectPtr))
#define COMLIB_CONST_OBJECT( ObjectPtr )                            ((const comlib_object_t*)(ObjectPtr))


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_GETID( ObjectPtr )                      ((ObjectPtr)->vtable->vm_getid( COMLIB_OBJECT(ObjectPtr) ))
#define GETID                                                 COMLIB_OBJECT_GETID


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_CMPID( ObjectPtr, ObidPtr )             ((ObjectPtr)->vtable->vm_cmpid( COMLIB_OBJECT(ObjectPtr), ObidPtr ))
#define CMPID                                                 COMLIB_OBJECT_CMPID


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_TYPEINFO( ObjectPtr )                   ((comlib_object_t*)(ObjectPtr))->typeinfo
#define COMLIB_OBJECT_TYPEINFO_QWORD( ObjectPtr )             COMLIB_OBJECT_TYPEINFO( ObjectPtr ).qword
#define TYPEINFO                                              COMLIB_OBJECT_TYPEINFO


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_SERIALIZABLE( ObjectPtr )              ((ObjectPtr)->vtable->vm_serialize != NULL)
#define COMLIB_OBJECT_SERIALIZE( ObjectPtr, OutputQueuePtr ) ((ObjectPtr)->vtable->vm_serialize( COMLIB_OBJECT(ObjectPtr), OutputQueuePtr ))
#define SERIALIZE                                            COMLIB_OBJECT_SERIALIZE



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_DESERIALIZABLE( ObjectPtr )              ((ObjectPtr)->vtable->vm_deserialize != NULL)
#define COMLIB_OBJECT_DESERIALIZE( ObjectPtr, InputQueuePtr )  ((ObjectPtr)->vtable->vm_deserialize( COMLIB_OBJECT(ObjectPtr), InputQueuePtr ))
#define DESERIALIZE                                            COMLIB_OBJECT_DESERIALIZE



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_CLASS_VTABLE( ClassName )                            COMLIB_GetClassVTable( CLASS_##ClassName )
#define COMLIB_CLASS_CONSTRUCTOR( ClassName )                       COMLIB_CLASS_VTABLE( ClassName )->vm_construct
#define COMLIB_OBJECT_NEW( ClassName, IdentifierPtr, ArgsPtr )      (ClassName*)COMLIB_CLASS_CONSTRUCTOR(ClassName)( IdentifierPtr, ArgsPtr )
#define NEW                                                         COMLIB_OBJECT_NEW
#define COMLIB_OBJECT_NEW_DEFAULT( ClassName )                      COMLIB_OBJECT_NEW( ClassName, NULL, NULL )
#define X_COMLIB_OBJECT_NEW_DEFAULT( ClassName )                    COMLIB_OBJECT_NEW_DEFAULT( ClassName )
#define COMLIB_OBJECT_INIT( ClassName, Instance, IdentifierPtr )    COMLIB_InitializeObject( COMLIB_OBJECT(Instance), IdentifierPtr, CLASS_##ClassName )
#define X_COMLIB_OBJECT_INIT( ClassName, Instance, IdentifierPtr )  COMLIB_OBJECT_INIT( ClassName, Instance, IdentifierPtr )
#define COMLIB_OBJECT_ZERO( ClassName, Instance )                   COMLIB_ZeroObject( COMLIB_OBJECT(Instance), CLASS_##ClassName )
#define COMLIB_OBJECT_CLONE( ClassName, Instance )                  (ClassName*)COMLIB_CloneObject( COMLIB_OBJECT(Instance), sizeof( ClassName ) )
#define CALLABLE( ObjectPtr )                                       (ObjectPtr)->vtable
#define CALL( ObjectPtr, Method, ... )                              (ObjectPtr)->vtable->Method( ObjectPtr, ##__VA_ARGS__ )



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_DESTRUCTIBLE( ObjectPtr )               ((ObjectPtr)->vtable->vm_destroy != NULL)
#define COMLIB_OBJECT_DESTROY( ObjectPtr )                    (ObjectPtr)->vtable->vm_destroy( COMLIB_OBJECT(ObjectPtr) )
#define DESTROY                                               COMLIB_OBJECT_DESTROY


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_REPR( ObjectPtr, OutputQueue )          (ObjectPtr)->vtable->vm_represent( COMLIB_CONST_OBJECT(ObjectPtr), OutputQueue )
#define REPR                                                  COMLIB_OBJECT_REPR


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_OBJECT_PRINT( ObjectPtr )                      PrintObject( COMLIB_CONST_OBJECT(ObjectPtr) )
#define PRINT                                                 COMLIB_OBJECT_PRINT


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define COMLIB_QWORD_AS_OBJECT_BASETYPE( QWord )              (object_basetype_t)((comlib_object_typeinfo_t*)&(QWord))->tp_basetype
#define COMLIB_QWORD_AS_OBJECT_FLAGS( QWord )                 (object_flags_t)((comlib_object_typeinfo_t*)&(QWord))->tp_flags
#define COMLIB_QWORD_AS_OBJECT_CLASS( QWord )                 (object_class_t)((comlib_object_typeinfo_t*)&(QWord))->tp_class
#define COMLIB_QWORD_AS_OBJECT_IDTYPE( QWord )                (object_idtype_t)((comlib_object_typeinfo_t*)&(QWord))->tp_idtype
#define COMLIB_QWORD_AS_OBJECT_IDSTRLEN( QWord )              (int16_t)((comlib_object_typeinfo_t*)&(QWord))->tp_idstrlen

#define COMLIB_CLASS_CODE( ClassName )                        (CLASS_##ClassName)
#define COMLIB_CLASS_TYPEINFO( ClassName )                    COMLIB_GetClassTypeInfo( CLASS_##ClassName ) 

#define COMLIB_TYPEINFO_QWORD_MATCH( TQ1, TQ2 )               ( ( ( (TQ1) ^ (TQ2) ) & _COMLIB_OBJECT_TYPEINFO_NOFLAGS_MASK ) == 0 )
#define COMLIB_TYPEINFO_MATCH( TypeInfo1, TypeInfo2 )         COMLIB_TYPEINFO_QWORD_MATCH( (TypeInfo1).qword, (TypeInfo2).qword )

#define COMLIB_CLASS_TYPEMATCH( ClassName, TypeInfo )         COMLIB_TYPEINFO_MATCH( COMLIB_CLASS_TYPEINFO( ClassName ), TypeInfo )
#define COMLIB_OBJECT_TYPEMATCH( ObjectPtr, TypeInfo )        COMLIB_TYPEINFO_MATCH( COMLIB_OBJECT_TYPEINFO( ObjectPtr ), TypeInfo )
#define COMLIB_OBJECT_ISINSTANCE( ObjectPtr, ClassName )      COMLIB_TYPEINFO_MATCH( COMLIB_OBJECT_TYPEINFO( ObjectPtr ), COMLIB_CLASS_TYPEINFO( ClassName ) )

#define COMLIB_CLASS( ClassName )                             COMLIB_CLASS_TYPEINFO( ClassName ).tp_class
#define COMLIB_OBJECT_CLASS_CODE( ObjectPtr )                 ((ObjectPtr)->typeinfo.tp_class)
#define COMLIB_OBJECT_CLASSMATCH( ObjectPtr, ObjectClass )    ((ObjectPtr)->typeinfo.tp_class == ObjectClass)


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_comlib_object_t {
  COMLIB_OBJECT_HEAD( comlib_object_vtable_t )
} comlib_object_t;




#include "comlibsequence/comlibsequence_defs.h"

typedef CDwordList_t * Unicode;
typedef CDwordList_vtable_t * IUnicode;
#define NewUnicode() COMLIB_OBJECT_NEW_DEFAULT( CDwordList_t )



DLL_COMLIB_PUBLIC extern int64_t COMLIB_length_utf8( const BYTE *utf8_text, int64_t sz, int64_t *rsz, int64_t *errpos );
DLL_COMLIB_PUBLIC extern bool COMLIB_check_utf8( const BYTE *utf8_text, int64_t *errpos );
DLL_COMLIB_PUBLIC extern int64_t COMLIB_copy_utf8( const BYTE *utf8_text, char *output, int64_t *errpos );
DLL_COMLIB_PUBLIC extern int64_t COMLIB_offset_utf8( const BYTE *utf8_text, int64_t utf8_len, int64_t index, int64_t *rcp, int64_t *errpos );
DLL_COMLIB_PUBLIC extern Unicode COMLIB_decode_utf8( const BYTE *utf8_text, int64_t *errpos );
DLL_COMLIB_PUBLIC extern BYTE * COMLIB_encode_utf8( Unicode unicode, int64_t *len, int64_t *errpos );



void COMLIB_InitializeObjectModel( void );
void COMLIB_DestroyObjectModel( void );




#define COMLIB_REGISTER_CLASS( ClassName, BaseType, VTablePtr, IdentifiedBy, IdLength )                       \
do {                                                                                                          \
  size_t class_bytes = sizeof(ClassName);                                                                     \
  comlib_object_vtable_t *vtable = (comlib_object_vtable_t*)(VTablePtr);                                      \
  if( COMLIB_RegisterClass( CLASS_##ClassName, BaseType, class_bytes, vtable, IdentifiedBy, IdLength ) != 0 ) {  \
    FATAL( 0, "Failed to register class: %s", #ClassName );                                                   \
  }                                                                                                           \
  else {                                                                                                      \
    VERBOSE( 0, "Registered class: %s (sz=%llu)", #ClassName, class_bytes );                                  \
  }                                                                                                           \
} WHILE_ZERO

#define COMLIB_UNREGISTER_CLASS( ClassName )                  \
do {                                                          \
  if( COMLIB_UnregisterClass( CLASS_##ClassName ) != 0 )         \
    FATAL( 0, "Failed to unregister class: %s", #ClassName ); \
  else                                                        \
    VERBOSE( 0, "Unregistered class: %s", #ClassName );       \
} WHILE_ZERO

#define X_COMLIB_REGISTER_CLASS( ClassName, BaseType, VTablePtr, IdentifiedBy, IdLength ) COMLIB_REGISTER_CLASS( ClassName, BaseType, VTablePtr, IdentifiedBy, IdLength )
#define X_COMLIB_UNREGISTER_CLASS( ClassName )  COMLIB_UNREGISTER_CLASS( ClassName )

#ifdef __cplusplus
extern "C" {
#endif

DLL_COMLIB_PUBLIC extern int COMLIB_DefaultComparator( const comlib_object_t *self, const void *idptr );
DLL_COMLIB_PUBLIC extern void * COMLIB_DefaultIdentifier( const comlib_object_t *self );
DLL_COMLIB_PUBLIC extern int64_t COMLIB_DefaultSerializer( const comlib_object_t *self, struct s_CQwordQueue_t *out_queue );
DLL_COMLIB_PUBLIC extern comlib_object_t * COMLIB_DefaultDeserializer( const comlib_object_t *container, struct s_CQwordQueue_t *in_queue );
DLL_COMLIB_PUBLIC extern struct s_CStringQueue_t * COMLIB_DefaultRepresenter( const comlib_object_t *self, struct s_CStringQueue_t *output );

DLL_COMLIB_PUBLIC extern comlib_object_t * COMLIB_DeserializeObject( comlib_object_t *self, comlib_object_typeinfo_t *ptypeinfo, struct s_CQwordQueue_t *in_queue );

DLL_COMLIB_PUBLIC extern void COMLIB_DefaultNotImplemented( void *_, ... );

DLL_COMLIB_PUBLIC extern int COMLIB_RegisterClass( object_class_t obclass, object_basetype_t basetype, size_t obsize, comlib_object_vtable_t *vtable, object_idtype_t idtype, int16_t idstrlen );
DLL_COMLIB_PUBLIC extern int COMLIB_UnregisterClass( object_class_t obclass );
DLL_COMLIB_PUBLIC extern comlib_object_typeslot_t * COMLIB_GetClassTypeSlot( object_class_t obclass );
DLL_COMLIB_PUBLIC extern comlib_object_typeinfo_t COMLIB_GetClassTypeInfo( object_class_t obclass );
DLL_COMLIB_PUBLIC extern comlib_object_vtable_t * COMLIB_GetClassVTable( object_class_t obclass );
DLL_COMLIB_PUBLIC extern object_basetype_t COMLIB_GetClassBaseType( object_class_t obclass );
DLL_COMLIB_PUBLIC extern object_idtype_t COMLIB_GetClassIdType( object_class_t obclass );
DLL_COMLIB_PUBLIC extern comlib_object_t * COMLIB_InitializeObject( comlib_object_t *obj, const void *identifier, object_class_t obclass );
DLL_COMLIB_PUBLIC extern comlib_object_t * COMLIB_CloneObject( comlib_object_t *obj, size_t obj_bytes );
DLL_COMLIB_PUBLIC extern comlib_object_t * COMLIB_ObjectSetIdentifier( comlib_object_t *obj, const void *identifier );
DLL_COMLIB_PUBLIC extern void COMLIB_ZeroObject( comlib_object_t *obj, object_class_t obclass );

DLL_COMLIB_PUBLIC extern CStringQueue_t * COMLIB_GetObjectOutput( void );
DLL_COMLIB_PUBLIC extern void COMLIB_DumpObjectOutput( void );
DLL_COMLIB_PUBLIC extern void PrintObject( const comlib_object_t *obj );






#ifdef __cplusplus
}
#endif


#endif
