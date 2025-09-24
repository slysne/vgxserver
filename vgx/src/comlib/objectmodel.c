/*
 * messages.c
 *
 *
*/

#include "_comlib.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );


#define MAP_BITS 8
#define MAP_SIZE (1<<MAP_BITS)

#if MAP_BITS > _COMLIB_OBJECT_BASETYPE_BITS
#error
#endif


static comlib_object_typeslot_t g_typemap[MAP_SIZE] = {0};
static int g_objectmodel_initialized = 0;
static CStringQueue_t *g_queue = NULL;


DLL_EXPORT object_classname_t OBJECT_CLASSNAME[ COMLIB_MAX_CLASS ];



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __set_classnames( void ) {
#define POPULATE_CLASSDEF( ClassName )                          \
  do {                                                          \
    object_class_t ccode = COMLIB_CLASS_CODE( ClassName );      \
    OBJECT_CLASSNAME[ ccode ].typecode = ccode;                 \
    strcpy( OBJECT_CLASSNAME[ ccode ].classname, #ClassName );  \
  } WHILE_ZERO

  // Initialize array
  memset( OBJECT_CLASSNAME, 0, sizeof( OBJECT_CLASSNAME ) );
  for( int i=0; i<COMLIB_MAX_CLASS; i++ ) {
    OBJECT_CLASSNAME[ i ].typecode = i;
    strcpy( OBJECT_CLASSNAME[ i ].classname, "?" );
  }
  
  // Set the known class names
  POPULATE_CLASSDEF( NONE );
  POPULATE_CLASSDEF( DemoClass_t );
  POPULATE_CLASSDEF( cxmalloc_family_t );
  POPULATE_CLASSDEF( CTokenizer_t );
  POPULATE_CLASSDEF( CString_t );
  POPULATE_CLASSDEF( Text_t );

  POPULATE_CLASSDEF( ComlibLinearSequence_t );

  POPULATE_CLASSDEF( CStringQueue_t );
  POPULATE_CLASSDEF( CByteQueue_t );
  POPULATE_CLASSDEF( CWordQueue_t );
  POPULATE_CLASSDEF( CDwordQueue_t );
  POPULATE_CLASSDEF( CQwordQueue_t );
  POPULATE_CLASSDEF( Cm128iQueue_t );
  POPULATE_CLASSDEF( Cm256iQueue_t );
  POPULATE_CLASSDEF( Cm512iQueue_t );
  POPULATE_CLASSDEF( CtptrQueue_t );
  POPULATE_CLASSDEF( Cx2tptrQueue_t );
  POPULATE_CLASSDEF( CaptrQueue_t );

  POPULATE_CLASSDEF( CStringBuffer_t );
  POPULATE_CLASSDEF( CByteBuffer_t );
  POPULATE_CLASSDEF( CWordBuffer_t );
  POPULATE_CLASSDEF( CDwordBuffer_t );
  POPULATE_CLASSDEF( CQwordBuffer_t );
  POPULATE_CLASSDEF( Cm128iBuffer_t );
  POPULATE_CLASSDEF( Cm256iBuffer_t );
  POPULATE_CLASSDEF( Cm512iBuffer_t );
  POPULATE_CLASSDEF( CtptrBuffer_t );
  POPULATE_CLASSDEF( Cx2tptrBuffer_t );
  POPULATE_CLASSDEF( CaptrBuffer_t );

  POPULATE_CLASSDEF( CByteHeap_t );
  POPULATE_CLASSDEF( CWordHeap_t );
  POPULATE_CLASSDEF( CDwordHeap_t );
  POPULATE_CLASSDEF( CQwordHeap_t );
  POPULATE_CLASSDEF( Cm128iHeap_t );
  POPULATE_CLASSDEF( Cm256iHeap_t );
  POPULATE_CLASSDEF( Cm512iHeap_t );
  POPULATE_CLASSDEF( CtptrHeap_t );
  POPULATE_CLASSDEF( Cx2tptrHeap_t );
  POPULATE_CLASSDEF( CaptrHeap_t );

  POPULATE_CLASSDEF( CByteList_t );
  POPULATE_CLASSDEF( CWordList_t );
  POPULATE_CLASSDEF( CDwordList_t );
  POPULATE_CLASSDEF( CQwordList_t );
  POPULATE_CLASSDEF( Cm128iList_t );
  POPULATE_CLASSDEF( Cm256iList_t );
  POPULATE_CLASSDEF( Cm512iList_t );
  POPULATE_CLASSDEF( CtptrList_t );
  POPULATE_CLASSDEF( Cx2tptrList_t );
  POPULATE_CLASSDEF( CaptrList_t );

  POPULATE_CLASSDEF( framehash_t );
  POPULATE_CLASSDEF( FramehashTestObject_t );
  POPULATE_CLASSDEF( PyFramehashObjectWrapper_t );

  POPULATE_CLASSDEF( vgx_Vector_t );
  POPULATE_CLASSDEF( vgx_Fingerprinter_t );
  POPULATE_CLASSDEF( vgx_Similarity_t );
  POPULATE_CLASSDEF( vgx_Graph_t );
  POPULATE_CLASSDEF( vgx_Vertex_t );
  POPULATE_CLASSDEF( vgx_IndexEntry_t );
  POPULATE_CLASSDEF( vgx_ArrayVector_t );
  POPULATE_CLASSDEF( vgx_FeatureVector_t );

  POPULATE_CLASSDEF( vgx_Evaluator_t );
  POPULATE_CLASSDEF( vgx_BaseQuery_t );
  POPULATE_CLASSDEF( vgx_AdjacencyQuery_t );
  POPULATE_CLASSDEF( vgx_NeighborhoodQuery_t );
  POPULATE_CLASSDEF( vgx_GlobalQuery_t );
  POPULATE_CLASSDEF( vgx_AggregatorQuery_t );
  POPULATE_CLASSDEF( INVALID );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void COMLIB_InitializeObjectModel( void ) {
  // TODO: more?  refactor?

  if( !g_objectmodel_initialized ) {
    CStringQueue_constructor_args_t qargs = { .element_capacity = ARCH_PAGE_SIZE, .comparator=NULL };
    if( g_queue != NULL ) {
      COMLIB_OBJECT_DESTROY( g_queue );
    }

    __set_classnames();

    g_objectmodel_initialized = 1;

    g_queue = COMLIB_OBJECT_NEW( CStringQueue_t, NULL, &qargs );
    if( g_queue == NULL ) {
      FATAL( 0, "Internal queue creation failed during object model initialization" );
    }
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void COMLIB_DestroyObjectModel( void ) {
  if( g_objectmodel_initialized ) {
    memset( OBJECT_CLASSNAME, 0, sizeof( OBJECT_CLASSNAME ) );
    if( g_queue ) {
      COMLIB_OBJECT_DESTROY( g_queue );
      g_queue = NULL;
    }
    g_objectmodel_initialized = 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CStringQueue_t * COMLIB_GetObjectOutput( void ) {
  return g_queue;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_DefaultComparator( const comlib_object_t *self, const void *idptr ) {
  QWORD a,b;
  void * pid = COMLIB_OBJECT_GETID( self );
  int c;
  switch( self->typeinfo.tp_idtype ) {
  case OBJECT_IDENTIFIED_BY_NONE:
    return 0;
  case OBJECT_IDENTIFIED_BY_SHORTID:
    /* FALLTHRU */
  case OBJECT_IDENTIFIED_BY_QWORD:
    a = *((QWORD*)pid);
    b = *((QWORD*)idptr);
    return a == b ? 0 : (a > b) - (a < b);
  case OBJECT_IDENTIFIED_BY_ADDRESS:
    return (pid > idptr) - (pid < idptr);
  case OBJECT_IDENTIFIED_BY_OBJECTID:
    return idcmp( (objectid_t*)pid, (objectid_t*)idptr );
  case OBJECT_IDENTIFIED_BY_HIGHID:
    a = ((objectid_t*)pid)->H;
    b = ((objectid_t*)idptr)->H;
    return a == b ? 0 : (a > b) - (a < b);
  case OBJECT_IDENTIFIED_BY_SHORTSTRING:
    return strncmp( (char*)pid, (char*)idptr, 16 );
  case OBJECT_IDENTIFIED_BY_LONGSTRING:
    // first, quick prefix checks before we dereference longstring
    if( (c = strncmp( ((longstring_t*)pid)->prefix, (char*)idptr, 8 )) != 0 ) return c;
    // full longstring check
    return strncmp( ((longstring_t*)pid)->string, (char*)idptr, OBJECTID_LONGSTRING_MAX );
  default:
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void * COMLIB_DefaultIdentifier( const comlib_object_t *self ) {
  switch( self->typeinfo.tp_idtype ) {
  case OBJECT_IDENTIFIED_BY_NONE:
    return NULL;
  case OBJECT_IDENTIFIED_BY_ADDRESS:
    return (comlib_object_t*)self;
  default:
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CStringQueue_t * COMLIB_DefaultRepresenter( const comlib_object_t *self, CStringQueue_t *output ) {
  int64_t (*Format)( CStringQueue_t *q, const char *fmt, ... ) = CALLABLE(output)->Format;
  char buffer[33];
#define PUT( FormatString, ... ) Format( output, FormatString, ##__VA_ARGS__ )
  PUT( "<object at 0x%llx> basetype=0x%02x flags=0x%02x class=0x%02x idtype=0x%04x idstrlen=%u ", self,
    self->typeinfo.tp_basetype, self->typeinfo.tp_flags, self->typeinfo.tp_class, self->typeinfo.tp_idtype, self->typeinfo.tp_idstrlen );
  void *pid = COMLIB_OBJECT_GETID( self );
  if( pid ) {
    switch( self->typeinfo.tp_idtype ) {
      case OBJECT_IDENTIFIED_BY_NONE:
        PUT( "id<NULL>" ); break;
      case OBJECT_IDENTIFIED_BY_SHORTID:
        PUT( "id<SHORTID>=%llx", *((QWORD*)pid) ); break;
      case OBJECT_IDENTIFIED_BY_QWORD:
        PUT( "id<QWORD>=%llu", *((QWORD*)pid) ); break;
      case OBJECT_IDENTIFIED_BY_ADDRESS:
        PUT( "id<ADDRESS>=0x%llx", pid ); break;
      case OBJECT_IDENTIFIED_BY_OBJECTID:
        PUT( "id<OBJECTID>=%s", idtostr( buffer, (objectid_t*)pid ) ); break;
      case OBJECT_IDENTIFIED_BY_HIGHID:
        PUT( "id<HIGHID>=%llx", *((QWORD*)pid) ); break;
      case OBJECT_IDENTIFIED_BY_SHORTSTRING:
        PUT( "id<SHORTSTRING>=%s", strncpy( buffer, (const char*)pid, self->typeinfo.tp_idstrlen ) ); break;
      case OBJECT_IDENTIFIED_BY_LONGSTRING: /* pid will point to an objectid_t instance! */
        PUT( "id<LONGSTRING>=%s", ((const longstring_t*)pid)->string ); break;
      default:
        PUT( "id<UNKNOWN>" ); break;
    }
  }
  else {
    PUT( "id<NULL>" );
  }
  return output;
#undef PUT
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_EXPORT int64_t COMLIB_DefaultSerializer( const comlib_object_t *self, CQwordQueue_t *out_queue ) {
  // TODO: something?
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_EXPORT comlib_object_t * COMLIB_DefaultDeserializer( const comlib_object_t *container, CQwordQueue_t *in_queue ) {
  // TODO: something?
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_t * COMLIB_DeserializeObject( comlib_object_t *self, comlib_object_typeinfo_t *ptypeinfo, CQwordQueue_t *in_queue ) {
  comlib_object_vtable_t *vtable;
  
  if( self ) {
    vtable = CALLABLE( self );
  }
  else {
    comlib_object_typeinfo_t typeinfo;
    if( !ptypeinfo ) {
      QWORD *pqword = &typeinfo.qword;
      ptypeinfo = &typeinfo;
      if( CALLABLE( in_queue )->ReadNolock( in_queue, (void**)&pqword, 1 ) != 1 ) {
        CALLABLE( in_queue )->UnreadNolock( in_queue, -1 );
        REASON( 0, "Failed to read data from input queue" );
        return NULL;
      }
    }
    if( (vtable = COMLIB_GetClassVTable( ptypeinfo->tp_class )) == NULL ) {
      uint8_t code = ptypeinfo->tp_class;
      const char *classname = OBJECT_CLASSNAME[ code ].classname;
      REASON( 0, "Vtable not defined for class %u (%s)", code, classname );
      return NULL;
    }
  }

  return vtable->vm_deserialize( self, in_queue );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_EXPORT void COMLIB_DefaultNotImplemented( void *_, ... ) {
  FATAL( 0, "Unsupported method call (program error - cannot continue)" );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_RegisterClass( object_class_t obclass, object_basetype_t basetype, size_t obsize, comlib_object_vtable_t *vtable, object_idtype_t idtype, int16_t idstrlen ) {
  comlib_object_typeslot_t *slot = NULL;
  int idx = (int)obclass;
  if( idx >> MAP_BITS ) {
    return -1; // obclass out of range
  }
  slot = g_typemap + idx;
  if( (slot->vtable && slot->vtable != vtable) || 
      (slot->obsize != 0 && slot->obsize != obsize ) ||
      (slot->basetype != CXLIB_OBTYPE_NOTYPE && slot->basetype != basetype ) || 
      (slot->idtype != OBJECT_IDENTIFIED_BY_NULL && slot->idtype != idtype ) )
  {
    pmesg(4, "class registry conflict: obclass=%d basetype=%d idtype=%d\n", obclass, basetype, idtype);
    return -1;  // type slot already occupied by a different type
  }
  slot->vtable = vtable;
  slot->obsize = obsize;
  slot->basetype = basetype;
  slot->idtype = idtype;
  slot->idstrlen = idstrlen;
  slot->flags = 0;
  
  // VTABLE DEFAULTS
  if( slot->vtable->vm_cmpid == NULL ) {
    slot->vtable->vm_cmpid = (f_object_comparator_t)COMLIB_DefaultComparator;
  }
  if( slot->vtable->vm_getid == NULL ) {
    slot->vtable->vm_getid = (f_object_identifier_t)COMLIB_DefaultIdentifier;
  }
  if( slot->vtable->vm_serialize == NULL ) {
    slot->vtable->vm_serialize = (f_object_serializer_t)COMLIB_DefaultSerializer;
  }
  if( slot->vtable->vm_deserialize == NULL ) {
    slot->vtable->vm_deserialize = (f_object_deserializer_t)COMLIB_DefaultDeserializer;
  }
  if( slot->vtable->vm_construct == NULL ) {
    pmesg(4, "constructor not defined for class %x\n", obclass );
    return -1;  // type slot already occupied by a different type
  }
  if( slot->vtable->vm_destroy == NULL ) {
    pmesg(4, "destructor not defined for class %x\n", obclass );
    return -1;  // type slot already occupied by a different type
  }
  if( slot->vtable->vm_represent == NULL ) {
    slot->vtable->vm_represent = (f_object_representer_t)COMLIB_DefaultRepresenter;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_typeslot_t * COMLIB_GetClassTypeSlot( object_class_t obclass ) {
  int idx = (int)obclass;
  if( idx >> MAP_BITS ) {
    return NULL; // obclass out of range
  }
  return g_typemap + idx;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_typeinfo_t COMLIB_GetClassTypeInfo( object_class_t obclass ) {
  comlib_object_typeinfo_t typeinfo;
  comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
  if( slot ) {
    typeinfo.tp_basetype = (uint8_t)slot->basetype;
    typeinfo.tp_flags = (uint8_t)slot->flags;
    typeinfo.tp_class = (uint8_t)obclass;
    typeinfo.tp_idtype = (uint16_t)slot->idtype;
    typeinfo.tp_idstrlen = (uint16_t)slot->idstrlen;
  }
  else {
    typeinfo.tp_basetype = CXLIB_OBTYPE_NOTYPE;
    typeinfo.tp_flags = 0;
    typeinfo.tp_class = CLASS_NONE;
    typeinfo.tp_idtype = OBJECT_IDENTIFIED_BY_NULL;
    typeinfo.tp_idstrlen = 0;
  }
  return typeinfo;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_vtable_t * COMLIB_GetClassVTable( object_class_t obclass ) {
  if( g_objectmodel_initialized ) {
    comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
    if( slot ) {
      return slot->vtable;
    }
    else {
      return NULL;
    }
  }
  else {
    printf( "Object model not initialized. Did you forget to call comlib_INIT() ?\n" );
    FATAL( 0xFFF, "Stop." );
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT object_basetype_t COMLIB_GetClassBaseType( object_class_t obclass ) {
  comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
  if( slot ) {
    return slot->basetype;
  }
  else {
    return CXLIB_OBTYPE_NOTYPE;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT object_idtype_t COMLIB_GetClassIdType( object_class_t obclass ) {
  comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
  if( slot ) {
    return slot->idtype;
  }
  else {
    return OBJECT_IDENTIFIED_BY_NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_UnregisterClass( object_class_t obclass ) {
  comlib_object_typeslot_t *slot = NULL;
  int idx = (int)obclass;
  if( idx >> MAP_BITS ) {
    return -1; // obclass out of range
  }
  slot = g_typemap + idx;
  slot->basetype = CXLIB_OBTYPE_NOTYPE;
  slot->flags = 0;
  slot->vtable = NULL;
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_t * COMLIB_InitializeObject( comlib_object_t *obj, const void *identifier, object_class_t obclass ) {

  // Get the type data for registered type
  comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
  if( slot == NULL ) {
    return NULL;
  }

  // Hook up the type's vtable to object
  obj->vtable = slot->vtable;
  obj->typeinfo = COMLIB_GetClassTypeInfo( obclass );

  // Populate object ID from supplied identifier
  if( identifier ) {
    obj = COMLIB_ObjectSetIdentifier( obj, identifier );
  }

  // Mark as initialized
  obj->typeinfo.tp_flags = 0;
  object_flags_t flags = { .is_initialized=1 };
  obj->typeinfo.tp_flags |= flags.bits;

  return obj;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __destroy_clone( comlib_object_t *obj ) {
  ALIGNED_FREE( obj );
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_t * COMLIB_CloneObject( comlib_object_t *obj, size_t obj_bytes ) {
  comlib_object_t *clone;
  if( CALIGNED_BYTES( clone, obj_bytes ) == NULL ) {
    return NULL;
  }
  memcpy( clone, obj, obj_bytes );

  // Mark as cloned
  object_flags_t flags = { .is_clone=1 };
  obj->typeinfo.tp_flags |= flags.bits;

  // Use special clone destructor
  obj->vtable->vm_destroy = __destroy_clone;

  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_object_t * COMLIB_ObjectSetIdentifier( comlib_object_t *obj, const void *identifier ) {
  void * pid = COMLIB_OBJECT_GETID( obj );
  switch( obj->typeinfo.tp_idtype ) {
  case OBJECT_IDENTIFIED_BY_NONE:
    return NULL;  // can't supply identifier for objects that don't have identifiers
  case OBJECT_IDENTIFIED_BY_SHORTID:
    /* FALLTHRU */
  case OBJECT_IDENTIFIED_BY_QWORD:
    *((QWORD*)pid) = *((QWORD*)identifier);
    return obj;
  case OBJECT_IDENTIFIED_BY_ADDRESS:
    if( pid == identifier )
      return obj;
    else
      return NULL; // wrong ID supplied
  case OBJECT_IDENTIFIED_BY_OBJECTID:
    idcpy( (objectid_t*)pid, (const objectid_t*)identifier );
    return obj;
  case OBJECT_IDENTIFIED_BY_HIGHID:
    ((objectid_t*)pid)->H = *((QWORD*)identifier);
    ((longstring_t*)pid)->string = NULL; // the other half. TODO: why do we mix H and longstring in this case ?
    return obj;
  case OBJECT_IDENTIFIED_BY_SHORTSTRING:
    strncpy( (char*)pid, (const char*)identifier, obj->typeinfo.tp_idstrlen );
    return obj;
  case OBJECT_IDENTIFIED_BY_LONGSTRING: /* pid will point to an objectid_t instance! */
    objectid_longstring_from_string( (objectid_t*)pid, identifier );
    return obj;
  default:
    return NULL; // unknown id type
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_ZeroObject( comlib_object_t *obj, object_class_t obclass ) {
  // Get the type data for registered type
  comlib_object_typeslot_t *slot = COMLIB_GetClassTypeSlot( obclass );
  if( slot && obj ) {
    char *mem = (char*)obj;
    mem += sizeof(comlib_object_head_t);
    size_t bytes = slot->obsize - sizeof(comlib_object_head_t);
    memset( mem, 0, bytes );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_DumpObjectOutput( void ) {
  char *buffer = NULL;
  // TODO: add lock, since there are several calls going on below and could result in funny looking output.
  CALLABLE( g_queue )->NulTerm( g_queue );
  CALLABLE( g_queue )->Read( g_queue, (void**)&buffer, -1 );
  if( buffer ) {
    printf( "%s", buffer );
    ALIGNED_FREE( buffer );
  }
  else {
    printf( "<error>\n" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void PrintObject( const comlib_object_t *obj ) {
  if( obj ) {
    CALLABLE( COMLIB_OBJECT_REPR( obj, g_queue ) )->Write( g_queue, "\n", 1 );
  }
  else {
    CALLABLE( g_queue )->Write( g_queue, "NULL\n", 5 );
  }
  COMLIB_DumpObjectOutput();
}



