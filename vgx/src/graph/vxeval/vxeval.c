/*######################################################################
 *#
 *# vxeval.c
 *#
 *#
 *######################################################################
 */




#include "_vxeval.h"


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void   (*f_ecld_pi8)( vgx_Evaluator_t *self ) = NULL;
DLL_HIDDEN void   (*f_ssq_pi8)( vgx_Evaluator_t *self ) = NULL;
DLL_HIDDEN void   (*f_rsqrtssq_pi8)( vgx_Evaluator_t *self ) = NULL;
DLL_HIDDEN void   (*f_dp_pi8)( vgx_Evaluator_t *self ) = NULL;
DLL_HIDDEN void   (*f_cos_pi8)( vgx_Evaluator_t *self ) = NULL;
DLL_HIDDEN double (*vxeval_bytearray_distance)( const BYTE *A, const BYTE *B, float fA, float fB, int len ) = NULL;
DLL_HIDDEN double (*vxeval_bytearray_sum_squares)( const BYTE *A, int len ) = NULL;
DLL_HIDDEN double (*vxeval_bytearray_rsqrt_ssq)( const BYTE *A, int len ) = NULL;
DLL_HIDDEN double (*vxeval_bytearray_dot_product)( const BYTE *A, const BYTE *B, int len ) = NULL;
DLL_HIDDEN double (*vxeval_bytearray_cosine)( const BYTE *A, const BYTE *B, int len ) = NULL;




#include "modules/eval.h"
#include "rpndef/_rpndef.h"




/******************************************************************************
 *
 ******************************************************************************
 */
static vgx_AllocatedVertex_t _aV_dummy = {0};
static vgx_Vertex_t *g_dummy = NULL;
static const size_t sz_id_prefix = sizeof( vgx_VertexIdentifierPrefix_t ) - 1;




/*******************************************************************//**
 *
 ***********************************************************************
 */
static int _vxeval__initialize( void );
static int _vxeval__destroy( void );
static int _vxeval__create_evaluators( vgx_Graph_t *graph );
static void _vxeval__destroy_evaluators( vgx_Graph_t *graph );
static vgx_Evaluator_t * _vxeval__new_evaluator( vgx_Graph_t *graph, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error );
static void _vxeval__discard_evaluator( vgx_Evaluator_t **evaluator );
static int _vxeval__is_positive( const vgx_EvalStackItem_t *item );
static int64_t _vxeval__get_integer( const vgx_EvalStackItem_t *item );
static double _vxeval__get_real( const vgx_EvalStackItem_t *item );
static vgx_EvalStackItem_t _vxeval__get_nan( void );
static vgx_ExpressEvalStack_t * _vxeval__new_keyval_stack_from_properties_CS( vgx_Graph_t *graph, vgx_SelectProperties_t *properties );
static vgx_ExpressEvalStack_t * _vxeval__clone_keyval_stack_CS( vgx_Evaluator_t *evaluator_CS, const vgx_Vertex_t *tail_RO, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id );
static void _vxeval__discard_stack( vgx_ExpressEvalStack_t **stack );
static void _vxeval__discard_stack_CS( vgx_ExpressEvalStack_t **stack_CS );
static vgx_ExpressEvalMemory_t * _vxeval__new_memory( int order );
static int _vxeval__own_memory( vgx_ExpressEvalMemory_t *mem );
static vgx_ExpressEvalMemory_t * _vxeval__clone_memory( vgx_ExpressEvalMemory_t *other );
static void _vxeval__discard_memory( vgx_ExpressEvalMemory_t **mem );
static int _vxeval__store_cstring( vgx_Evaluator_t *self, const CString_t *CSTR__str );
static int64_t _vxeval__clear_cstrings( vgx_ExpressEvalMemory_t *memory );
static int _vxeval__store_vector( vgx_Evaluator_t *self, const vgx_Vector_t *vector );
static int64_t _vxeval__clear_vectors( vgx_ExpressEvalMemory_t *memory );
static int _vxeval__local_auto_scope_object( vgx_Evaluator_t *self, vgx_EvalStackItem_t *item, bool delete_on_fail );
static void _vxeval__clear_local_scope( vgx_Evaluator_t *self );
static void _vxeval__delete_local_scope( vgx_Evaluator_t *self );


static int64_t _vxeval__clear_dwset( vgx_ExpressEvalMemory_t *memory );

static vgx_StringList_t * _vxeval__get_rpn_definitions( void );




/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IEvaluator_t iEvaluator = {
  .Initialize           = _vxeval__initialize,
  .Destroy              = _vxeval__destroy,
  .CreateEvaluators     = _vxeval__create_evaluators,
  .DestroyEvaluators    = _vxeval__destroy_evaluators,
  .NewEvaluator         = _vxeval__new_evaluator,
  .DiscardEvaluator     = _vxeval__discard_evaluator,
  .IsPositive           = _vxeval__is_positive,
  .GetInteger           = _vxeval__get_integer,
  .GetReal              = _vxeval__get_real,
  .GetNaN               = _vxeval__get_nan,
  .NewKeyValStack_CS    = _vxeval__new_keyval_stack_from_properties_CS,
  .CloneKeyValStack_CS  = _vxeval__clone_keyval_stack_CS,
  .DiscardStack         = _vxeval__discard_stack,
  .DiscardStack_CS      = _vxeval__discard_stack_CS,
  .NewMemory            = _vxeval__new_memory,
  .OwnMemory            = _vxeval__own_memory,
  .CloneMemory          = _vxeval__clone_memory,
  .DiscardMemory        = _vxeval__discard_memory,
  .StoreCString         = _vxeval__store_cstring,
  .ClearCStrings        = _vxeval__clear_cstrings,
  .StoreVector          = _vxeval__store_vector,
  .ClearVectors         = _vxeval__clear_vectors,
  .LocalAutoScopeObject = _vxeval__local_auto_scope_object,
  .ClearLocalScope      = _vxeval__clear_local_scope,
  .DeleteLocalScope     = _vxeval__delete_local_scope,
  .ClearDWordSet        = _vxeval__clear_dwset,
  .GetRpnDefinitions    = _vxeval__get_rpn_definitions
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int                    Evaluator__cmpid( const vgx_Evaluator_t *self, const void *idptr );
static objectid_t *           Evaluator__getid( vgx_Evaluator_t *self );
static int64_t                Evaluator__serialize( vgx_Evaluator_t *self, CQwordQueue_t *output );
static vgx_Evaluator_t *      Evaluator__deserialize( comlib_object_t *container, CQwordQueue_t *input );
static vgx_Evaluator_t *      Evaluator__constructor( const void *identifier, vgx_Evaluator_constructor_args_t *args );
static void                   __destructor_CS( vgx_Evaluator_t *self_CS );
static void                   Evaluator__destructor( vgx_Evaluator_t *self );
static CStringQueue_t *       Evaluator__represent( vgx_Evaluator_t *self, CStringQueue_t *output );
static int                    Evaluator__reset( vgx_Evaluator_t *self );
static void                   Evaluator__set_context( vgx_Evaluator_t *self, const vgx_Vertex_t *tail, const vgx_ArcHead_t *arc, vgx_Vector_t *vector, double rankscore );
static void                   Evaluator__set_default_prop( vgx_Evaluator_t *self, vgx_EvalStackItem_t *default_prop );
static void                   Evaluator__own_memory( vgx_Evaluator_t *self, vgx_ExpressEvalMemory_t *memory );
static void                   Evaluator__set_vector( vgx_Evaluator_t *self, vgx_Vector_t *vector );
static void                   Evaluator__set_collector( vgx_Evaluator_t *self, vgx_BaseCollector_context_t *collector );
static void                   Evaluator__set_timing_budget( vgx_Evaluator_t *self, vgx_ExecutionTimingBudget_t *timing_budget );
static vgx_EvalStackItem_t *  Evaluator__eval( vgx_Evaluator_t *self );
static vgx_EvalStackItem_t *  Evaluator__eval_vertex( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex );
static vgx_EvalStackItem_t *  Evaluator__eval_arc( vgx_Evaluator_t *self, vgx_LockableArc_t *next );
static int                    Evaluator__n_this_deref( const vgx_Evaluator_t *self );
static int                    Evaluator__n_head_deref( const vgx_Evaluator_t *self );
static int                    Evaluator__n_traversals( const vgx_Evaluator_t *self );
static int                    Evaluator__n_this_next_access( const vgx_Evaluator_t *self );
static int                    Evaluator__n_lookbacks( const vgx_Evaluator_t *self );
static int                    Evaluator__n_identifiers( const vgx_Evaluator_t *self );
static bool                   Evaluator__has_cull( const vgx_Evaluator_t *self );
static int                    Evaluator__n_synarc_ops( const vgx_Evaluator_t *self );
static int                    Evaluator__n_wreg_ops( const vgx_Evaluator_t *self );
static int64_t                Evaluator__get_wreg_ncall( const vgx_Evaluator_t *self );
static int                    Evaluator__clear_wreg( vgx_Evaluator_t *self );
static void                   Evaluator__clear_mcull_heap_array( vgx_Evaluator_t *self );
static vgx_StackItemType_t    Evaluator__value_type( vgx_Evaluator_t *self );
static vgx_Evaluator_t *      Evaluator__clone( vgx_Evaluator_t *self, vgx_Vector_t *vector );
static vgx_Evaluator_t *      Evaluator__own( vgx_Evaluator_t *self );
static void                   Evaluator__discard( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static vgx_Evaluator_vtable_t Evaluator_Methods = {
  /* common comlib_object_vtable_t interface */
  .vm_cmpid         = (f_object_comparator_t)Evaluator__cmpid,
  .vm_getid         = (f_object_identifier_t)Evaluator__getid,
  .vm_serialize     = (f_object_serializer_t)Evaluator__serialize,
  .vm_deserialize   = (f_object_deserializer_t)Evaluator__deserialize,
  .vm_construct     = (f_object_constructor_t)Evaluator__constructor,
  .vm_destroy       = (f_object_destructor_t)Evaluator__destructor,
  .vm_represent     = (f_object_representer_t)Evaluator__represent,
  .vm_allocator     = NULL,
  /* Evaluator interface */
  .Reset            = Evaluator__reset,
  .SetContext       = Evaluator__set_context,
  .SetDefaultProp   = Evaluator__set_default_prop,
  .OwnMemory        = Evaluator__own_memory,
  .SetVector        = Evaluator__set_vector,
  .SetCollector     = Evaluator__set_collector,
  .SetTimingBudget  = Evaluator__set_timing_budget,
  .Eval             = Evaluator__eval,
  .EvalVertex       = Evaluator__eval_vertex,
  .EvalArc          = Evaluator__eval_arc,
  .PrevDeref        = Evaluator__n_lookbacks,
  .ThisDeref        = Evaluator__n_this_deref,
  .HeadDeref        = Evaluator__n_head_deref,
  .Traversals       = Evaluator__n_traversals,
  .ThisNextAccess   = Evaluator__n_this_next_access,
  .Identifiers      = Evaluator__n_identifiers,
  .HasCull          = Evaluator__has_cull,
  .SynArcOps        = Evaluator__n_synarc_ops,
  .WRegOps          = Evaluator__n_wreg_ops,
  .ClearWReg        = Evaluator__clear_wreg,
  .GetWRegNCall     = Evaluator__get_wreg_ncall,
  .ClearMCull       = Evaluator__clear_mcull_heap_array,
  .ValueType        = Evaluator__value_type,
  .Clone            = Evaluator__clone,
  .Own              = Evaluator__own,
  .Discard          = Evaluator__discard
};



/******************************************************************************
 *
 ******************************************************************************
 */
static void __Evaluator_deconstruct( vgx_Evaluator_t *self );



/******************************************************************************
 *
 ******************************************************************************
 */
static void              __reset_runtime_stack( vgx_Evaluator_t *self );
static vgx_EvalStackItem_t * __evaluator__run( vgx_Evaluator_t *self );



/******************************************************************************
 *
 ******************************************************************************
 */
static const CString_t *  __smart_own_cstring_CS( vgx_Graph_t *graph, const CString_t *CSTR__str );
static void               __smart_discard_cstring_CS( CString_t *CSTR__str );
static void               __initialize_dummy_vertex( void );



/******************************************************************************
 *
 ******************************************************************************
 */
static int __create_work_registers( vgx_Evaluator_t *self );
static void __destroy_work_registers( vgx_Evaluator_t *self );
static void __prepare_work_registers( vgx_Evaluator_t *self );


static int __create_mcull_heap_array( vgx_Evaluator_t *self );
static void __destroy_mcull_heap_array( vgx_Evaluator_t *self );


static int __create_runtime_eval_stack( vgx_Evaluator_t *self, vgx_Graph_t *graph );
static void __destroy_runtime_eval_stack( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Evaluator_RegisterClass( void ) {
  // Do some basic sanity checking for critical types
  ASSERT_TYPE_SIZE( vgx_EvalStackItem_t,            16 );
  ASSERT_TYPE_SIZE( vgx_ExpressEvalOperation_t,   24 );
  COMLIB_REGISTER_CLASS( vgx_Evaluator_t, CXLIB_OBTYPE_PROCESSOR, &Evaluator_Methods, OBJECT_IDENTIFIED_BY_OBJECTID, -1 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void vgx_Evaluator_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( vgx_Vertex_t );
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int _vxeval__initialize( void ) {
  int ret = 0;
  XTRY {
    __initialize_dummy_vertex();

    // Parser
    if( _vxeval_parser__initialize() < 0 ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x121 );
    }

    f_ecld_pi8 = __eval_scalar_ecld_pi8;
    f_ssq_pi8 = __eval_scalar_ssq_pi8;
    f_rsqrtssq_pi8 = __eval_scalar_rsqrtssq_pi8;
    f_dp_pi8 = __eval_scalar_dp_pi8;
    f_cos_pi8 = __eval_scalar_cos_pi8;
    vxeval_bytearray_distance = __scalar_ecld_pi8;
    vxeval_bytearray_sum_squares = __scalar_ssq_pi8;
    vxeval_bytearray_rsqrt_ssq = __scalar_rsqrtssq_pi8;
    vxeval_bytearray_dot_product = __scalar_dp_pi8;
    vxeval_bytearray_cosine = __scalar_cos_pi8;

#if defined CXPLAT_ARCH_HASFMA
    int fma_feature = iVGXProfile.CPU.HasFeatureFMA();
    if( fma_feature ) {
      int avx_version = iVGXProfile.CPU.GetAVXVersion();
      if( avx_version == 512 ) {
        f_ecld_pi8 = __eval_avx512_ecld_pi8;
        f_ssq_pi8 = __eval_avx512_ssq_pi8;
        f_rsqrtssq_pi8 = __eval_avx512_rsqrtssq_pi8;
        f_dp_pi8 = __eval_avx512_dp_pi8;
        f_cos_pi8 = __eval_avx512_cos_pi8;
        vxeval_bytearray_distance = __avx512_ecld_pi8;
        vxeval_bytearray_sum_squares = __avx512_ssq_pi8;
        vxeval_bytearray_rsqrt_ssq = __avx512_rsqrtssq_pi8;
        vxeval_bytearray_dot_product = __avx512_dp_pi8;
        vxeval_bytearray_cosine = __avx512_cos_pi8;
      }
      else if( fma_feature && avx_version == 2 ) {
        f_ecld_pi8 = __eval_avx2_ecld_pi8;
        f_ssq_pi8 = __eval_avx2_ssq_pi8;
        f_rsqrtssq_pi8 = __eval_avx2_rsqrtssq_pi8;
        f_dp_pi8 = __eval_avx2_dp_pi8;
        f_cos_pi8 = __eval_avx2_cos_pi8;
        vxeval_bytearray_distance = __avx2_ecld_pi8;
        vxeval_bytearray_sum_squares = __avx2_ssq_pi8;
        vxeval_bytearray_rsqrt_ssq = __avx2_rsqrtssq_pi8;
        vxeval_bytearray_dot_product = __avx2_dp_pi8;
        vxeval_bytearray_cosine = __avx2_cos_pi8;
      }
    }
#elif defined CXPLAT_ARCH_ARM64
    
    f_ecld_pi8 = __eval_neon_ecld_pi8;
    f_ssq_pi8 = __eval_neon_ssq_pi8;
    f_rsqrtssq_pi8 = __eval_neon_rsqrtssq_pi8;
    f_dp_pi8 = __eval_neon_dp_pi8;
    f_cos_pi8 = __eval_neon_cos_pi8;
    vxeval_bytearray_distance = __neon_ecld_pi8;
    vxeval_bytearray_sum_squares = __neon_ssq_pi8;
    vxeval_bytearray_rsqrt_ssq = __neon_rsqrtssq_pi8;
    vxeval_bytearray_dot_product = __neon_dp_pi8;
    vxeval_bytearray_cosine = __neon_cos_pi8;


#endif

  }
  XCATCH( errcode ) {
    _vxeval__destroy();
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
static int _vxeval__destroy( void ) {
  // Destroy the parser
  _vxeval_parser__destroy();

  // Undefine the dummy vertex
  memset( &_aV_dummy, 0, sizeof( vgx_AllocatedVertex_t ) );
  g_dummy = NULL;

  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int _vxeval__create_evaluators( vgx_Graph_t *graph ) {
  int ret = 0;
  CString_t *CSTR__str = NULL;

  XTRY {
    // Destroy previous map if it exists
    iMapping.DeleteMap( &graph->evaluators );

    // User expressions
    if( (CSTR__str = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "user expressions (%s)", CStringValue( CALLABLE( graph )->Name( graph ) ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x131 );
    }

    // New map
    vgx_mapping_spec_t map_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_128BIT | (unsigned)VGX_MAPPING_OPTIMIZE_NORMAL;
    if( (graph->evaluators = iMapping.NewMap( NULL, CSTR__str, MAPPING_SIZE_UNLIMITED, 0, map_spec, CLASS_NONE )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x132 );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__str );
  }

  return ret;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void _vxeval__destroy_evaluators( vgx_Graph_t *graph ) {
  iMapping.DeleteMap( &graph->evaluators );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Evaluator_t * _vxeval__new_evaluator( vgx_Graph_t *graph, const char *expression, vgx_Vector_t *vector, CString_t **CSTR__error ) {
  vgx_Evaluator_constructor_args_t args = {
    .parent       = graph,
    .expression   = expression,
    .vector       = vector,
    .CSTR__error  = CSTR__error
  };
  vgx_Evaluator_t *evaluator = COMLIB_OBJECT_NEW( vgx_Evaluator_t, NULL, &args );
  return evaluator;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void _vxeval__discard_evaluator( vgx_Evaluator_t **evaluator ) {
  if( evaluator && *evaluator ) {
    CALLABLE( *evaluator )->Discard( *evaluator );
    *evaluator = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int _vxeval__is_positive( const vgx_EvalStackItem_t *item ) {
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    return item->integer > 0;
  case STACK_ITEM_TYPE_REAL:
    return item->real > 0.0;
  case STACK_ITEM_TYPE_VERTEX:
  case STACK_ITEM_TYPE_CSTRING:
  case STACK_ITEM_TYPE_VECTOR:
  case STACK_ITEM_TYPE_BITVECTOR:
  case STACK_ITEM_TYPE_VERTEXID:
    return item->bits != 0;
  case STACK_ITEM_TYPE_KEYVAL:
    return vgx_cstring_array_map_val( &item->bits ) > 0.0;
  default:
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t _vxeval__get_integer( const vgx_EvalStackItem_t *item ) {
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    return item->integer;
  case STACK_ITEM_TYPE_REAL:
    if( fabs(item->real) < (double)LLONG_MAX ) {
      return (int64_t)item->real;
    }
    else if( item->real > 0.0 ) {
      return LLONG_MAX;
    }
    else {
      return LLONG_MIN;
    }
  case STACK_ITEM_TYPE_VERTEX:
    return (int64_t)item->vertex;
  case STACK_ITEM_TYPE_CSTRING:
    return (int64_t)CStringReinterpretAsQWORD( item->CSTR__str );
  case STACK_ITEM_TYPE_VECTOR:
    return (int64_t)item->vector;
  case STACK_ITEM_TYPE_BITVECTOR:
    return (int64_t)item->bits;
  case STACK_ITEM_TYPE_KEYVAL:
    return vgx_cstring_array_map_key( &item->bits );
  case STACK_ITEM_TYPE_VERTEXID:
    return (int64_t)item->vertexid;
  default:
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static double _vxeval__get_real( const vgx_EvalStackItem_t *item ) {
  switch( item->type ) {
  case STACK_ITEM_TYPE_INTEGER:
    return (double)item->integer;
  case STACK_ITEM_TYPE_REAL:
    if( fabs(item->real) < INFINITY ) {
      return item->real;
    }
    else if( item->real > 0.0 ) {
      return DBL_MAX;
    }
    else {
      return -DBL_MAX;
    }
  case STACK_ITEM_TYPE_VERTEX:
    return (double)(uintptr_t)item->vertex;
  case STACK_ITEM_TYPE_CSTRING:
    return (double)CStringReinterpretAsQWORD( item->CSTR__str );
  case STACK_ITEM_TYPE_VECTOR:
    return (double)(uintptr_t)item->vector;
  case STACK_ITEM_TYPE_BITVECTOR:
    return (double)item->bits;
  case STACK_ITEM_TYPE_KEYVAL:
    return vgx_cstring_array_map_val( &item->bits );
  case STACK_ITEM_TYPE_VERTEXID:
    return (double)(uintptr_t)item->vertexid;
  default:
    return 0.0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t _vxeval__get_nan( void ) {
  vgx_EvalStackItem_t nan;
  SET_NAN( &nan );
  return nan;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static const CString_t * __smart_own_cstring_CS( vgx_Graph_t *graph, const CString_t *CSTR__str ) {
  if( CSTR__str == NULL ) {
    return NULL;
  }
  // String is a literal - we don't need additional ownership since the evaluator owns the literal
  // and the evaluator is assumed to survive any usage of the string
  if( IS_STRING_LITERAL( CSTR__str ) ) {
    return CSTR__str;
  }
  // String uses allocator and can be incref'ed
  else if( IS_STRING_OWNABLE( CSTR__str ) && CSTR__str->allocator_context ) {
    _cxmalloc_object_incref_nolock( (CString_t*)CSTR__str );
    return CSTR__str;
  }
  // String is dynamic or malloc'ed and must be cloned
  else {
    if( CStringLength( CSTR__str ) > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
      return CStringClone( CSTR__str );
    }
    else {
      return CStringCloneAlloc( CSTR__str, graph->ephemeral_string_allocator_context );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __smart_discard_cstring_CS( CString_t *CSTR__str ) {
  // If string is a literal we don't have additional ownership,
  // but otherwise we perform a decref.
  if( !IS_STRING_LITERAL( CSTR__str ) ) {
    if( CSTR__str->allocator_context ) {
      cxmalloc_family_t *alloc = (cxmalloc_family_t*)CSTR__str->allocator_context->allocator;
      CALLABLE( alloc )->DiscardObjectNolock( alloc, CSTR__str );
    }
    else {
      CStringDelete( CSTR__str );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ExpressEvalStack_t * _vxeval__new_keyval_stack_from_properties_CS( vgx_Graph_t *graph, vgx_SelectProperties_t *properties ) {
  vgx_ExpressEvalStack_t *stack = calloc( 1, sizeof( vgx_ExpressEvalStack_t ) );
  if( stack ) {
    int64_t nprop = properties->len;
    stack->sz = 1 + 2*(int)nprop + 1; // 1 initial dummy slot, two slots per property (key+value), 1 terminator slot
    stack->eval_depth.max = 1; // don't care
    stack->eval_depth.run = 1; // don't care
    if( CALIGNED_ARRAY( stack->data, vgx_EvalStackItem_t, stack->sz ) != NULL ) {
      vgx_EvalStackItem_t *dest = stack->data;
      
      // Set initial dummy slot
      SET_NONE( dest );

      // Set slots from properties
      vgx_VertexProperty_t *src = properties->properties; 
      for( int64_t n=0; n<nprop; n++ ) {
        // KEY
        ++dest;
        if( (dest->CSTR__str = __smart_own_cstring_CS( graph, src->key )) == NULL ) {
          dest++->type = STACK_ITEM_TYPE_NONE; // null key
          dest->type = STACK_ITEM_TYPE_NONE;   // null value
          ++src;
          continue;
        }
        dest->type = STACK_ITEM_TYPE_CSTRING;
        // VALUE
        ++dest;
        switch( src->val.type ) {
        case  VGX_VALUE_TYPE_BOOLEAN:
          dest->integer = 1; // TODO: reconcile this with the fact that the property value is actually 0 for htis type!
          dest->type = STACK_ITEM_TYPE_INTEGER;
          break;
        case VGX_VALUE_TYPE_INTEGER:
          dest->integer = src->val.data.simple.integer;
          dest->type = STACK_ITEM_TYPE_INTEGER;
          break;
        case VGX_VALUE_TYPE_REAL:
          dest->real = src->val.data.simple.real;
          dest->type = STACK_ITEM_TYPE_REAL;
          break;
        case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
        case VGX_VALUE_TYPE_CSTRING:
          if( (dest->CSTR__str = __smart_own_cstring_CS( graph, src->val.data.simple.CSTR__string )) != NULL ) {
            dest->type = STACK_ITEM_TYPE_CSTRING;
          }
          else {
            dest->type = STACK_ITEM_TYPE_INTEGER; // error but ok when we cast to int
          }
          break;
        default:
          dest->integer = -1;
          dest->type = STACK_ITEM_TYPE_INTEGER;
        }
        // Next property
        ++src;
      }

      // Set terminator slot
      ++dest;
      EvalStackItemSetTerminator( dest );

      // Set parent
      stack->parent.graph = graph;
      stack->parent.properties = properties;
      stack->parent.evaluator = NULL;
    }
    else {
      free( stack );
      stack = NULL;
    }
  }
  return stack;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ExpressEvalStack_t * _vxeval__clone_keyval_stack_CS( vgx_Evaluator_t *evaluator_CS, const vgx_Vertex_t *tail_RO, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id ) {
  vgx_ExpressEvalStack_t *clone = calloc( 1, sizeof( vgx_ExpressEvalStack_t ) );
  if( clone ) {
    vgx_ExpressEvalStack_t *orig = &evaluator_CS->rpn_program.stack;
    clone->eval_depth = orig->eval_depth;
    clone->sz = orig->sz;
    if( CALIGNED_ARRAY( clone->data, vgx_EvalStackItem_t, orig->sz ) != NULL ) {
      vgx_Graph_t *graph = evaluator_CS->graph;
      vgx_EvalStackItem_t *dest = clone->data;
      vgx_EvalStackItem_t *src = orig->data;
      vgx_VertexIdentifier_t *ident;
      const char _key[] = {0}, *key = _key;
      char obidbuf[33];
      // Copy initial dummy slot
      dest->type = src->type;
      dest++->bits = src++->bits;

      // Copy key-val stack until first empty slot and ensure object ownership as needed
      while( !EvalStackItemIsTerminator( src ) ) {

        // Ignore empty slot
        if( src->type != STACK_ITEM_TYPE_CSTRING ) {
          ++src;
          ++src;
          continue;
        }

        //
        // KEY
        //
        if( (dest->CSTR__str = __smart_own_cstring_CS( graph, src->CSTR__str )) != NULL ) {
          key = CStringValue( dest->CSTR__str );
          dest->type = STACK_ITEM_TYPE_CSTRING;
        }
        else {
          key = _key;
          dest->type = STACK_ITEM_TYPE_INTEGER; // rare error, but convert data to int so nothing crashes later
        }

        ++dest;
        ++src;

        //
        // VALUE
        //
        switch( (dest->type = src->type) ) {
        // Own cstring instance as needed
        case STACK_ITEM_TYPE_CSTRING:
          if( (dest->CSTR__str = __smart_own_cstring_CS( graph, src->CSTR__str )) == NULL ) {
            dest->type = STACK_ITEM_TYPE_INTEGER; // rare error, but convert data to int so nothing crashes later
          }
          break;
        // Internalid - the only safe thing is to create a string unfortunately
        case STACK_ITEM_TYPE_VERTEX:
          if( (dest->CSTR__str = CStringNewAlloc( idtostr( obidbuf, __vertex_internalid( src->vertex ) ), graph->ephemeral_string_allocator_context )) != NULL ) {
            dest->type = STACK_ITEM_TYPE_CSTRING;
          }
          else {
            dest->type = STACK_ITEM_TYPE_INTEGER; // rare error, but convert data to int so nothing crashes later
          }
          break;
        // Use provided id buffer or convert vertex ID to cstring
        case STACK_ITEM_TYPE_VERTEXID:
          // Tail ID
          if( src->vertexid == &tail_RO->identifier ) {
            ident = ptail_id;
          }
          // Head ID
          else {
            ident = phead_id;
          }
          // Buffer provided, populate if empty or no action if already populated
          if( ident ) {
            // Id not set, copy from stack item into buffer
            if( ident->idprefix.data[0] == '\0' ) {
              // Copy the prefix
              memcpy( (void*)ident->idprefix.data, src->vertexid->idprefix.data, sizeof( vgx_VertexIdentifierPrefix_t ) );
              // Long ID
              ident->CSTR__idstr = src->vertexid->CSTR__idstr;
            }
            // Long ID, own another reference
            if( ident->CSTR__idstr != NULL ) {
              ident->CSTR__idstr = (CString_t*)__smart_own_cstring_CS( graph, ident->CSTR__idstr ); // null on error, but ok since prefix will be used instead
            }
            // Stack item now points to buffer
            dest->vertexid = ident;
          }
          // No buffer, convert to cstring
          else {
            if( src->vertexid->CSTR__idstr ) {
              dest->CSTR__str = __smart_own_cstring_CS( graph, src->vertexid->CSTR__idstr );
            }
            else {
              dest->CSTR__str = CStringNewAlloc( src->vertexid->idprefix.data, graph->ephemeral_string_allocator_context );
            }
            if( dest->CSTR__str ) {
              dest->type = STACK_ITEM_TYPE_CSTRING;
            }
            else {
              dest->type = STACK_ITEM_TYPE_INTEGER; // rare error, but convert data to int so nothing crashes later
            }
          }
          break;
        // Convert internal vector to external
        case STACK_ITEM_TYPE_VECTOR:
          if( src->vector ) {
            if( (dest->vector = CALLABLE( graph->similarity )->ExternalizeVector( graph->similarity, (vgx_Vector_t*)src->vector, true )) == NULL ) {
              dest->type = STACK_ITEM_TYPE_INTEGER; // rare error, but convert data to int so nothing crashes later
            }
          }
          else {
            dest->vector = ivectorobject.Null( graph->similarity );
          }
          break;
        // Anything else remains as is
        default:
          dest->bits = src->bits;
        }
        ++dest;
        ++src;
      }

      // Terminator
      EvalStackItemSetTerminator( dest );

      CALLABLE( evaluator_CS )->Own( evaluator_CS );
      clone->parent.graph = evaluator_CS->graph;
      clone->parent.properties = NULL;
      clone->parent.evaluator = evaluator_CS;
    }
    else {
      free( clone );
      clone = NULL;
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxeval__discard_stack_CS( vgx_ExpressEvalStack_t **stack_CS ) {
  if( stack_CS && *stack_CS ) {
    if( (*stack_CS)->data ) {
      vgx_EvalStackItem_t *cursor = (*stack_CS)->data;
      vgx_EvalStackItem_t *end = cursor + (*stack_CS)->sz;
      // Skip initial dummy
      ++cursor;
      while( cursor < end && !EvalStackItemIsTerminator( cursor ) ) {
        // Ignore empty slot
        if( cursor->type != STACK_ITEM_TYPE_CSTRING ) {
          ++cursor;
          ++cursor;
          continue;
        }

        // Discard key
        __smart_discard_cstring_CS( (CString_t*)cursor->CSTR__str );

        // Value is next
        ++cursor;
        switch( cursor->type ) {
        case STACK_ITEM_TYPE_CSTRING:
          __smart_discard_cstring_CS( (CString_t*)cursor->CSTR__str );
          break;
        case STACK_ITEM_TYPE_VERTEXID:
          if( cursor->vertexid->CSTR__idstr ) {
            __smart_discard_cstring_CS( (CString_t*)cursor->vertexid->CSTR__idstr );
          }
          break;
        case STACK_ITEM_TYPE_VECTOR:
          if( cursor->vector ) {
            CALLABLE( cursor->vector )->Decref( (vgx_Vector_t*)cursor->vector );
          }
          break;
        default:
          break;
        }

        // Next pair
        ++cursor;
      }
      // Should now point to terminator
      if( !EvalStackItemIsTerminator( cursor ) ) {
        REASON( 0x007, "Invalid stack terminator in %s", __FUNCTION__ );
      }
      ALIGNED_FREE( (*stack_CS)->data );
    }
    vgx_Graph_t *graph = (*stack_CS)->parent.graph;
    vgx_Evaluator_t *ev = (*stack_CS)->parent.evaluator;
    if( ev ) {
      __destructor_CS( ev );
    }
    vgx_SelectProperties_t *prop = (*stack_CS)->parent.properties;
    if( prop ) {
      _vxvertex_property__free_select_properties_CS( graph, &prop );
    }
    free( *stack_CS );
    *stack_CS = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxeval__discard_stack( vgx_ExpressEvalStack_t **stack ) {
  if( stack && *stack ) {
    if( (*stack)->data ) {
      ALIGNED_FREE( (*stack)->data );
    }
    vgx_Graph_t *graph = (*stack)->parent.graph;
    vgx_Evaluator_t *ev = (*stack)->parent.evaluator;
    if( ev ) {
      CALLABLE( ev )->Discard( ev );
    }
    vgx_SelectProperties_t *prop = (*stack)->parent.properties;
    if( prop ) {
      iVertexProperty.FreeSelectProperties( graph, &prop );
    }
    free( *stack );
    *stack = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CQwordList_t * __new_cstringrefs( vgx_ExpressEvalMemory_t *self ) {
  CQwordList_constructor_args_t args = {
    .element_capacity = (1LL << self->order),
    .comparator = NULL
  };
  return  COMLIB_OBJECT_NEW( CQwordList_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CQwordList_t * __clone_cstringrefs( CQwordList_t *cstringrefs ) {
  CQwordList_t *clone;
  CQwordList_constructor_args_t args = {
    .element_capacity = CALLABLE( cstringrefs )->Capacity( cstringrefs ),
    .comparator = NULL
  };
  if( (clone = COMLIB_OBJECT_NEW( CQwordList_t, NULL, &args )) == NULL ) {
    return NULL;
  }
  QWORD *cursor = CALLABLE( cstringrefs )->Cursor( cstringrefs, 0 );
  QWORD *end = cursor + ComlibSequenceLength( cstringrefs );
  CQwordList_vtable_t *iList = CALLABLE( clone );
  cxmalloc_family_t *family = NULL;
  if( cursor < end ) {
    QWORD addr = *cursor;
    CString_t *CSTR__str = (CString_t*)addr;
    family = (cxmalloc_family_t*)CSTR__str->allocator_context->allocator;
    SYNCHRONIZE_ON( family->lock ) {
      while( cursor < end ) {
        addr = *cursor++;
        if( iList->Append( clone, &addr ) == 1 ) { 
          CSTR__str = (CString_t*)addr;
          _cxmalloc_object_incref_nolock( CSTR__str );
        }
      }
    } RELEASE;
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __delete_cstringrefs( CQwordList_t **cstringrefs ) {
  int64_t sz = 0;
  if( cstringrefs && *cstringrefs ) {
    CQwordList_t *list = *cstringrefs;
    CQwordList_vtable_t *iList = CALLABLE( list );
    QWORD *cursor = iList->Cursor( list, 0 );
    sz = ComlibSequenceLength( list );
    QWORD *end = cursor + sz;
    cxmalloc_family_t *family = NULL;
    if( cursor < end ) {
      QWORD addr = *cursor;
      CString_t *CSTR__str = (CString_t*)addr;
      family = (cxmalloc_family_t*)CSTR__str->allocator_context->allocator;
      cxmalloc_family_vtable_t *iFamily = CALLABLE( family );
      SYNCHRONIZE_ON( family->lock ) {
        while( cursor < end ) {
          addr = *cursor++;
          CSTR__str = (CString_t*)addr;
          iFamily->DiscardObjectNolock( family, CSTR__str );
        }
      } RELEASE;
    }
    COMLIB_OBJECT_DESTROY( *cstringrefs );
    *cstringrefs = NULL;
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CQwordList_t * __new_vectorrefs( vgx_ExpressEvalMemory_t *self ) {
  CQwordList_constructor_args_t args = {
    .element_capacity = (1LL << self->order),
    .comparator = NULL
  };
  return  COMLIB_OBJECT_NEW( CQwordList_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CQwordList_t * __clone_vectorrefs( CQwordList_t *vectorrefs ) {
  CQwordList_t *clone;
  CQwordList_constructor_args_t args = {
    .element_capacity = CALLABLE( vectorrefs )->Capacity( vectorrefs ),
    .comparator = NULL
  };
  if( (clone = COMLIB_OBJECT_NEW( CQwordList_t, NULL, &args )) == NULL ) {
    return NULL;
  }
  QWORD *cursor = CALLABLE( vectorrefs )->Cursor( vectorrefs, 0 );
  QWORD *end = cursor + ComlibSequenceLength( vectorrefs );
  CQwordList_vtable_t *iList = CALLABLE( clone );
  while( cursor < end ) {
    QWORD addr = *cursor++;
    if( iList->Append( clone, &addr ) == 1 ) { 
      vgx_Vector_t *vector = (vgx_Vector_t*)addr;
      CALLABLE( vector )->Incref( vector );
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __delete_vectorrefs( CQwordList_t **vectorrefs ) {
  int64_t sz = 0;
  if( vectorrefs && *vectorrefs ) {
    CQwordList_t *list = *vectorrefs;
    CQwordList_vtable_t *iList = CALLABLE( list );
    QWORD *cursor = iList->Cursor( list, 0 );
    sz = ComlibSequenceLength( list );
    QWORD *end = cursor + sz;
    while( cursor < end ) {
      QWORD addr = *cursor++;
      vgx_Vector_t *vector = (vgx_Vector_t*)addr;
      CALLABLE( vector )->Decref( vector );
    }
    COMLIB_OBJECT_DESTROY( *vectorrefs );
    *vectorrefs = NULL;
  }
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ExpressEvalMemory_t * _vxeval__new_memory( int order ) {
  // Default 8 slots
  if( order < 0 ) {
    order = VGX_EXPRESS_EVAL_MEMORY_OSTATIC;
  }
  else if( order >= __ARCH_ADDRESS_BITS ) {
    return NULL;
  }

  vgx_ExpressEvalMemory_t *mem = calloc( 1, sizeof( vgx_ExpressEvalMemory_t ) );
  if( mem ) {
    mem->order = order;
    size_t sz = 1ULL << mem->order;
    mem->mask = sz - 1;
    // Default use static memory
    if( order == VGX_EXPRESS_EVAL_MEMORY_OSTATIC ) {
      mem->data = mem->__data;
    }
    // Larger, allocate
    else if( TALIGNED_ARRAY( mem->data, vgx_EvalStackItem_t, sz ) == NULL ) {
      free( mem );
      return NULL;
    }
    // Initialze
    vgx_EvalStackItem_t *cursor = mem->data;
    vgx_EvalStackItem_t *end = cursor + sz;
    do {
      cursor->type = STACK_ITEM_TYPE_INTEGER;
      cursor->integer = 0;
    } while( ++cursor < end );
    // One owner
    mem->refc = 1;
    // Stack pointer
    mem->sp = __EXPRESS_EVAL_MEM_SPTOP;

    // CString reference list defaults to empty, allocate as needed.
    mem->cstringref = NULL;

    // Integer set defaults to empty, allocate as needed.
    mem->dwset.slots = NULL;
    mem->dwset.mask = 0;
    mem->dwset.sz = 0;

  }
  return mem;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxeval__own_memory( vgx_ExpressEvalMemory_t *mem ) {
  return ++(mem->refc);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_ExpressEvalMemory_t * _vxeval__clone_memory( vgx_ExpressEvalMemory_t *other ) {
  vgx_ExpressEvalMemory_t *clone = NULL;

  XTRY {
    if( (clone = calloc( 1, sizeof( vgx_ExpressEvalMemory_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    clone->order = other->order;
    clone->mask = other->mask;
    clone->sp = other->sp;
    
    size_t sz = clone->mask + 1;
    // Default use static memory
    if( other->data == other->__data ) {
      clone->data = clone->__data;
    }
    // Larger, allocate
    else if( TALIGNED_ARRAY( clone->data, vgx_EvalStackItem_t, sz ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }
    // Initialze
    vgx_EvalStackItem_t *src = other->data;
    vgx_EvalStackItem_t *dest = clone->data;
    vgx_EvalStackItem_t *end = src + sz;
    do {
      dest->type = src->type;
      dest->bits = src->bits;
      ++dest;
    } while( ++src < end );
    
    if( other->cstringref != NULL ) {
      if( (clone->cstringref = __clone_cstringrefs( other->cstringref )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
    }

    if( other->vectorref != NULL ) {
      if( (clone->vectorref = __clone_vectorrefs( other->vectorref )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
    }

    // Integer set is NOT cloned
    clone->dwset.slots = NULL;
    clone->dwset.mask = 0;
    clone->dwset.sz = 0;

    // One owner
    clone->refc = 1;

  }
  XCATCH( errcode ) {
    if( clone ) {
      __delete_cstringrefs( &clone->cstringref );
      __delete_vectorrefs( &clone->vectorref );
      free( clone );
    }
  }
  XFINALLY {
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxeval__discard_memory( vgx_ExpressEvalMemory_t **mem ) {
  if( mem && *mem ) {
    if( --((*mem)->refc) == 0 ) {
      if( (*mem)->data != NULL && (*mem)->data != (*mem)->__data ) {
        ALIGNED_FREE( (*mem)->data );
      }

      __delete_cstringrefs( &(*mem)->cstringref );
      __delete_vectorrefs( &(*mem)->vectorref );

      _vxeval__clear_dwset( *mem );

      free( *mem );
    }
    *mem = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __store_cstring( vgx_Evaluator_t *self, const CString_t *CSTR__str ) {
  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  if( mem->cstringref == NULL ) {
    if( (mem->cstringref = __new_cstringrefs( mem )) == NULL ) {
      return -1;
    }
  }

  // Add ref
  QWORD addr = (QWORD)CSTR__str;
  if( CALLABLE( mem->cstringref )->Append( mem->cstringref, &addr ) == 1 ) {
    return 0;
  }
    
  return -1;

}



/*******************************************************************//**
 * Own another reference to (or clone) the original string, then store it.
 * 
 *
 * Return 0 on success, -1 on error
 ***********************************************************************
 */
static int _vxeval__store_cstring( vgx_Evaluator_t *self, const CString_t *CSTR__str ) {
  CString_t *CSTR__stored = OwnOrCloneEphemeralCString( self->graph, CSTR__str );
  if( CSTR__stored ) {
    if( __store_cstring( self, CSTR__stored ) == 0 ) {
      return 0;
    }
    iString.Discard( &CSTR__stored );
  }
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxeval__clear_cstrings( vgx_ExpressEvalMemory_t *memory ) {
  return __delete_cstringrefs( &memory->cstringref );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __store_vector( vgx_Evaluator_t *self, const vgx_Vector_t *vector ) {

  vgx_ExpressEvalMemory_t *mem = self->context.memory;
  if( mem->vectorref == NULL ) {
    if( (mem->vectorref = __new_vectorrefs( mem )) == NULL ) {
      return -1;
    }
  }

  // Add ref
  QWORD addr = (QWORD)vector;
  if( CALLABLE( mem->vectorref )->Append( mem->vectorref, &addr ) == 1 ) {
    return 0;
  }
    
  return -1;

}



/*******************************************************************//**
 * Own another reference to (or clone) the original vector, then store it.
 * 
 *
 * Return 0 on success, -1 on error
 ***********************************************************************
 */
static int _vxeval__store_vector( vgx_Evaluator_t *self, const vgx_Vector_t *vector ) {
  if( CALLABLE( vector )->Context( vector )->simobj != self->graph->similarity ) {
    return -1;
  }

  if( __store_vector( self, vector ) != 0 ) {
    return -1;
  }

  CALLABLE( vector )->Incref( (vgx_Vector_t*)vector );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxeval__clear_vectors( vgx_ExpressEvalMemory_t *memory ) {
  return __delete_vectorrefs( &memory->vectorref );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_EvalStackItem_t * __new_local_scope( vgx_Evaluator_t *self, int sz ) {
  vgx_ExpressEvalContext_t *context = &self->context;
  vgx_EvalStackItem_t *slots = calloc( sz, sizeof( vgx_EvalStackItem_t ) );
  if( slots ) {
    context->local_scope.sz = sz;
  }
  return slots;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __expand_local_scope( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalContext_t *context = &self->context;
  // Initial scope slots
  if( context->local_scope.objects == NULL ) {
    if( (context->local_scope.objects = __new_local_scope( self, 4 )) == NULL ) {
      return -1;
    }
    context->local_scope.idx = 0;
  }
  // Expand
  else {
    int old_size = context->local_scope.sz;
    vgx_EvalStackItem_t *new_slots = __new_local_scope( self, 2 * old_size );
    if( new_slots == NULL ) {
      return -1;
    }
    memcpy( new_slots, context->local_scope.objects, old_size * sizeof( vgx_EvalStackItem_t ) );
    context->local_scope.objects = new_slots;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __delete_auto_scope_object( vgx_EvalStackItem_t *item ) {
  switch( item->type ) {
  case STACK_ITEM_TYPE_CSTRING:
    iString.Discard( (CString_t**)&item->CSTR__str );
    return;
  case STACK_ITEM_TYPE_VECTOR:
    CALLABLE( item->vector )->Decref( (vgx_Vector_t*)item->vector );
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxeval__local_auto_scope_object( vgx_Evaluator_t *self, vgx_EvalStackItem_t *item, bool delete_on_fail ) {
  int ret = 0;
  vgx_ExpressEvalContext_t *context = &self->context;
  XTRY {
    // No local scope, create new
    if( context->local_scope.objects == NULL ) {
      if( __expand_local_scope( self ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }
    }

    // No more space in local scope, expand
    if( context->local_scope.idx >= context->local_scope.sz ) {
      if( __expand_local_scope( self ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }
    }

    if( context->local_scope.objects == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // Item goes to this slot
    vgx_EvalStackItem_t *slot = context->local_scope.objects + context->local_scope.idx++;
    *slot = *item;
    ret = 1;
  }
  XCATCH( errcode ) {
    ret = -1;
    if( delete_on_fail ) {
      __delete_auto_scope_object( item );
    }
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
static void _vxeval__clear_local_scope( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalContext_t *context = &self->context;
  if( context->local_scope.objects ) {
    vgx_EvalStackItem_t *cursor = context->local_scope.objects;
    vgx_EvalStackItem_t *end = context->local_scope.objects + context->local_scope.idx;
    while( cursor < end ) {
      __delete_auto_scope_object( cursor++ );
    }
  }
  context->local_scope.idx = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxeval__delete_local_scope( vgx_Evaluator_t *self ) {
  vgx_ExpressEvalContext_t *context = &self->context;
  _vxeval__clear_local_scope( self );
  free( context->local_scope.objects );
  context->local_scope.sz = 0;
  context->local_scope.idx = 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxeval__clear_dwset( vgx_ExpressEvalMemory_t *memory ) {
  int64_t sz = memory->dwset.sz;
  if( memory->dwset.slots ) {
    ALIGNED_FREE( memory->dwset.slots );
    memory->dwset.slots = NULL;
  }
  memory->dwset.mask = 0;
  memory->dwset.sz = 0;
  return sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __create_work_registers( vgx_Evaluator_t *self ) {
  int ret = 0;
  // Allocate work registers
  XTRY {
    self->context.wreg.sz = self->rpn_program.n_wreg;
    if( self->context.wreg.sz > 1 ) {
      CALIGNED_ARRAY_THROWS( self->context.wreg.data, vgx_EvalStackItem_t, self->context.wreg.sz, 0x000 );
      memset( self->context.wreg.data, 0, sizeof( vgx_EvalStackItem_t ) * self->context.wreg.sz );
      self->context.wreg.cursor = self->context.wreg.data;
    }
    else {
      self->context.wreg.cursor = &self->context.wreg.single;
    }
    self->context.wreg.ncall = 0;
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
static void __destroy_work_registers( vgx_Evaluator_t *self ) {
  if( self->context.wreg.data ) {
    ALIGNED_FREE( self->context.wreg.data );
    self->context.wreg.data = NULL;
  }
  memset( &self->context.wreg, 0, sizeof( vgx_ExpressEvalWorkRegisters_t ) );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __prepare_work_registers( vgx_Evaluator_t *self ) {
  if( self->context.wreg.data ) {
    self->context.wreg.cursor = self->context.wreg.data;
  }
  else {
    self->context.wreg.cursor = &self->context.wreg.single;
  }
  self->context.wreg.ncall++;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __create_mcull_heap_array( vgx_Evaluator_t *self ) {
  if( self->rpn_program.cull > 0 ) {
    if( CALIGNED_ARRAY( self->context.cullheap, vgx_ArcHeadHeapItem_t, self->rpn_program.cull ) == NULL ) {
      return -1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __destroy_mcull_heap_array( vgx_Evaluator_t *self ) {
  if( self->context.cullheap ) {
    ALIGNED_FREE( self->context.cullheap );
    self->context.cullheap = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __create_runtime_eval_stack( vgx_Evaluator_t *self, vgx_Graph_t *graph ) {
  int ret = 0;
  // Allocate runtime evaluation stack
  XTRY {
    self->rpn_program.stack.sz = 1 + self->rpn_program.stack.eval_depth.max + 1; // include dummy slots at beginning and end
    CALIGNED_ARRAY_THROWS( self->rpn_program.stack.data, vgx_EvalStackItem_t, self->rpn_program.stack.sz, 0x000 );
    self->sp = self->rpn_program.stack.data;
    vgx_EvalStackItem_t *init_cursor = self->rpn_program.stack.data;
    vgx_EvalStackItem_t *init_end = init_cursor + self->rpn_program.stack.sz;
    while( init_cursor < init_end ) {
      init_cursor->type = STACK_ITEM_TYPE_INIT;
      init_cursor++->bits = 0;
    }
    self->rpn_program.stack.parent.graph = graph;
    self->rpn_program.stack.parent.properties = NULL;
    self->rpn_program.stack.parent.evaluator = self;
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
static void __destroy_runtime_eval_stack( vgx_Evaluator_t *self ) {
  if( self->rpn_program.stack.data ) {
    ALIGNED_FREE( self->rpn_program.stack.data );
    self->rpn_program.stack.data = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_StringList_t * _vxeval__get_rpn_definitions( void ) {
#define CCta  (__OP_CLS_MASK | __OP_TYP_MASK | __OP_ARG_MASK)
  __rpn_operation **ops = __rpn_definitions;
  __rpn_operation **cursor = ops;
  int64_t n = 0;
  __rpn_operation *op;
  while( (op = *cursor++) != NULL ) {
    if( op->surface.token ) {
      ++n;
    }
  }
  vgx_StringList_t *definitions = iString.List.New( NULL, n );
  if( definitions ) {
    char buf[1024];
    cursor = ops;
    int64_t i = 0;
    while( (op = *cursor++) != NULL ) {
      if( op->surface.token ) {
        char *p = buf;
        const char *token = op->surface.token;
        __rpn_op_type tp = op->type;
        int amin = 0;
        int amax = -1;
        int a = tp & __OP_ARG_MASK;
        if( a <= 5 ) {
          amin = amax = a;
        }
        else if( a == 0xF ) {
          __variadic_arg_counts( op, &amin, &amax );
        }

        if( (tp & __OP_REF_MASK) != 0 ) {
          int ref = tp & __OP_REF_MASK;
          p = write_chars( p, "[" );
          if( (ref & __OP_CLS_HEAD_DEREF) != 0 ) {
            p = write_chars( p, "DEREF " );
          }
          if( (ref & __OP_CLS_TRAVERSE) != 0 ) {
            p = write_chars( p, "TRAVERSE " );
          }
          if( (ref & __OP_CLS_TAIL_DEREF) != 0 ) {
            p = write_chars( p, "LOOKBACK " );
          }
          --p;
          *p++ = ']';
        }

        switch( (tp & __OP_CLS_MASK) ) {
        case __OP_CLS_INFIX:
          p = write_chars( p, "INFIX (" );
          switch( (tp & CCta) ) {
          case OP_UNARY_INFIX:
            p = write_chars( p, "unary" );
            break;
          case OP_BINARY_INFIX:
            p = write_chars( p, "binary" );
            break;
          case OP_BINARY_CMP_INFIX:
            p = write_chars( p, "binary comparison" );
            break;
          case OP_TERNARY_COND_INFIX:
            p = write_chars( p, "ternary conditional" );
            break;
          case OP_TERNARY_COLON_INFIX:
            p = write_chars( p, "ternary colon" );
            break;
          case OP_BITWISE_INFIX:
            p = write_chars( p, "bitwise" );
            break;
          }
          p = write_chars( p, "): " );
          p = write_chars( p, token );
          break;
        // Prefix ops
        case __OP_CLS_PREFIX:
          // Function call
          if( op->precedence == OPP_CALL ) {
            // some_func(a,b[,c[,d]])
            p = write_chars( p, "FUNCTION: " );
            p = write_chars( p, token );
            *p++ = '(';
            char aname = 'a';
            // required args
            for( int r=0; r<amin; r++ ) {
              if( aname != 'a' ) {
                *p++ = ',';
              }
              *p++ = ' ';
              *p++ = aname++;
            }
            // optional args
            if( amax > amin ) {
              if( amax == INT_MAX ) {
                p = write_chars( p, " [...] " );
              }
              else {
                for( int v=amin; v<amax; v++ ) {
                  *p++ = '[';
                  if( aname != 'a' ) {
                    *p++ = ',';
                  }
                  *p++ = ' ';
                  *p++ = aname++;
                }
                for( int v=amin; v<amax; v++ ) {
                  *p++ = ']';
                }
              }
            }
            if( amax > 0 ) {
              *p++ = ' ';
            }
            *p++ = ')';
          }
          // Non-call prefix
          else {
            p = write_chars( p, "PREFIX: " );
            p = write_chars( p, token );
          }
          break;
        case __OP_CLS_GROUP:
          p = write_chars( p, "GROUPING: " );
          p = write_chars( p, token );
          break;
        case __OP_CLS_OBJECT:
          p = write_chars( p, "OBJECT: " );
          p = write_chars( p, token );
          break;
        case __OP_CLS_SUBSCRIPT:
          p = write_chars( p, "SUBSCRIPT: " );
          p = write_chars( p, token );
          break;
        case __OP_CLS_ASSIGN:
          p = write_chars( p, "ASSIGNMENT: " );
          p = write_chars( p, token );
          break;
        case __OP_CLS_SYMBOLIC:
          p = write_chars( p, "SYMBOLIC (" );
          switch( (tp & CCta) ) {
            case (OP_TAIL_ATTR_OPERAND & CCta):
            case (OP_THIS_ATTR_OPERAND & CCta):
            case (OP_ARC_ATTR_OPERAND & CCta):
            case (OP_HEAD_ATTR_OPERAND & CCta):
              p = write_chars( p, "attribute" );
              break;
            case (OP_TAIL_TYPE_OPERAND & CCta):
            case (OP_THIS_TYPE_OPERAND & CCta):
            case (OP_HEAD_TYPE_OPERAND & CCta):
              p = write_chars( p, "type" );
              break;
            case (OP_TAIL_REL_OPERAND & CCta):
            case (OP_ARC_REL_OPERAND & CCta):
              p = write_chars( p, "relationship" );
              break;
            case (OP_ARC_DIR_OPERAND & CCta):
              p = write_chars( p, "direction" );
              break;
            case (OP_INTEGER_OPERAND & CCta):
              p = write_chars( p, "integer" );
              break;
            case (OP_REAL_OPERAND & CCta):
              p = write_chars( p, "real" );
              break;
            case (OP_ARC_DIR_ENUM_OPERAND & CCta):
            case (OP_ARC_MOD_ENUM_OPERAND & CCta):
              p = write_chars( p, "enum" );
              break;
          }
          p = write_chars( p, "): " );
          p = write_chars( p, token );
          break;
        case __OP_CLS_LITERAL:
          p = write_chars( p, "LITERAL (" );
          switch( (tp & CCta) ) {
          case OP_NONE_LITERAL:
            p = write_chars( p, "none" );
            break;
          case OP_INTEGER_LITERAL:
            p = write_chars( p, "integer" );
            break;
          case OP_REAL_LITERAL:
            p = write_chars( p, "real" );
            break;
          case OP_ADDRESS_LITERAL:
            p = write_chars( p, "address" );
            break;
          }
          p = write_chars( p, "): " );
          p = write_chars( p, token );
          break;
        default:
          p = write_chars( p, "DEBUG: " );
          p = write_chars( p, token );
        }
        write_term( p );

        iString.List.SetItem( definitions, i, buf );
        ++i;
      }
    }
  }
  return definitions;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int Evaluator__cmpid( const vgx_Evaluator_t *self, const void *idptr ) {
  return idcmp( Evaluator__getid( (vgx_Evaluator_t*)self), (const objectid_t*)idptr );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static objectid_t * Evaluator__getid( vgx_Evaluator_t *self ) {
  return &self->id;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t Evaluator__serialize( vgx_Evaluator_t *self, CQwordQueue_t *output ) {
  return 0; // not implemented
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Evaluator_t * Evaluator__deserialize( comlib_object_t *container, CQwordQueue_t *input ) {
  return NULL; // not implemented
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_Evaluator_t * Evaluator__constructor( void const *__ign_identifier, vgx_Evaluator_constructor_args_t *args ) {
  vgx_Evaluator_t *self = NULL;
  vgx_Graph_t *graph = args->parent;
  if( graph == NULL ) {
    return NULL;
  }
  framehash_t *E = graph->evaluators;
  objectid_t obid = {0};

  if( args->expression == NULL  ) {
    return NULL;
  }

  // Try to look up an existing definition, then clone and return it
  if( _vxeval_parser__get_evaluator( graph, args->expression, args->vector, &self ) < 0 ) {
    return NULL;
  }
  if( self ) {
    return self;
  }
  
  // No existing definition, create new expression
  XTRY {

    // Allcate evaluator object
    if( (self = calloc( 1, sizeof( vgx_Evaluator_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x151 );
    }

    // Initialize object (empty ID for now)
    if( COMLIB_OBJECT_INIT( vgx_Evaluator_t, self, &obid ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x152 );
    }
    ATOMIC_ASSIGN_i64( &self->_refc_atomic, 1 );

    // Set parent graph
    self->graph = graph;

    // Set current graph info
    GRAPH_LOCK( graph ) {
      self->current.t0 = _vgx_graph_inception( graph );
      self->current.tnow = _vgx_graph_milliseconds( graph ) / 1000.0;
      self->current.order = GraphOrder( graph );
      self->current.size = GraphSize( graph );
      self->current.op = iOperation.GetId_LCK( &graph->operation );
      if( (self->current.vector = args->vector) != NULL ) {
        CALLABLE( self->current.vector )->Incref( self->current.vector );
      }
      else {
        self->current.vector = ivectorobject.Null( graph->similarity );
      }
    } GRAPH_RELEASE;

    // Parse infix expression or load from previous assignment
    int parse_code = _vxeval_parser__create_rpn_from_infix( &self->rpn_program, graph, args->expression, args->CSTR__error );
    // Error
    if( parse_code < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x153 );
    }
    // New rpn created and we want to store the expression in map for future lookup
    else if( parse_code > 0 && self->rpn_program.CSTR__assigned ) {
      // Update object id from expression id
      objectid_t *pid = Evaluator__getid( self );
      pid->H = pid->L = CStringHash64( self->rpn_program.CSTR__assigned );

      framehash_valuetype_t tp_ins = CELL_VALUE_TYPE_NULL;
      GRAPH_LOCK( graph ) {
        // Remove previous definition if one exists
        if( CALLABLE( E )->HasObj128Nolock( E, pid ) ) {
          CALLABLE( E )->DelObj128Nolock( E, pid );
        }
        // Insert the new expression
        tp_ins = CALLABLE( E )->SetObj128Nolock( E, pid, COMLIB_OBJECT( self ) );
      } GRAPH_RELEASE;
      if( tp_ins != CELL_VALUE_TYPE_OBJECT128 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x154 );
      }
      // Map now owns a reference to the evaluator object
      ATOMIC_INCREMENT_i64( &self->_refc_atomic );
    }

    // Allocate work registers
    if( __create_work_registers( self ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x155 );
    }

    // Allocate mcull heap memory if needed
    if( __create_mcull_heap_array( self ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x156 );
    }

    // Allocate runtime evaluation stack
    if( __create_runtime_eval_stack( self, graph ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x157 );
    }

    self->cache.CSTR__tmp_prop = NULL;

    // Ready
    if( Evaluator__reset( self ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x158 );
    }
  }
  XCATCH( errcode ) {
    if( self ) {
      COMLIB_OBJECT_DESTROY( self );
      self = NULL;
    }
  }
  XFINALLY {
  }

  return self;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __destructor_CS( vgx_Evaluator_t *self_CS ) {
  if( ATOMIC_DECREMENT_i64( &self_CS->_refc_atomic ) == 0 ) {
    __Evaluator_deconstruct( self_CS );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__destructor( vgx_Evaluator_t *self ) {
  GRAPH_LOCK( self->graph ) {
    __destructor_CS( self );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CStringQueue_t * Evaluator__represent( vgx_Evaluator_t *self, CStringQueue_t *output ) {
#define PUT( FormatString, ... ) CALLABLE(output)->Format( output, FormatString, ##__VA_ARGS__ )
  COMLIB_DefaultRepresenter( (const comlib_object_t*)self, output );
  PUT( "\n" );

  const char *name = self->rpn_program.CSTR__assigned ? CStringValue( self->rpn_program.CSTR__assigned ) : NULL;
  const char *expr = self->rpn_program.CSTR__expression ? CStringValue( self->rpn_program.CSTR__expression ) : NULL;

  PUT( "<vgx_Evaluator_t at %p (%s)>\n", self, name );
  PUT( "EXPRESSION  : %s\n", expr );
  PUT( "STACK_DEPTH : %d\n", self->rpn_program.stack.eval_depth.max );
  PUT( "OPERATIONS  : %d\n", self->rpn_program.length );
  PUT( "PASSTHRU    : %d\n", self->rpn_program.n_passthru );

  PUT( "\n\n" );

  return output;
#undef PUT

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __clear_property_cache( vgx_ExpressEvalPropertyCache_t *pc ) {
  SET_NONE( &pc->item );
  pc->keyhash = 0;
  pc->vertex = NULL;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __clear_eval_caches( vgx_ExpressEvalCache_t *ec ) {
  __clear_property_cache( &ec->TAIL );
  __clear_property_cache( &ec->VERTEX );
  __clear_property_cache( &ec->HEAD );

  ec->relationship.CSTR__rel = NULL;
  ec->relationship.relhash = 0;
  ec->relationship.rel = VGX_PREDICATOR_REL_NONE;

  ec->vertextype.CSTR__type = NULL;
  ec->vertextype.typehash = 0;
  ec->vertextype.vtx = VERTEX_TYPE_ENUMERATION_NONE;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__reset( vgx_Evaluator_t *self ) {
  // Reset caches
  __clear_eval_caches( &self->cache );

  // -------------
  // Reset context
  // -------------
  self->context.TAIL = g_dummy;
  self->context.arrive = VGX_PREDICATOR_NONE;
  self->context.VERTEX = g_dummy;
  self->context.exit = VGX_PREDICATOR_NONE;
  self->context.HEAD = g_dummy;

  // rankscore
  self->context.rankscore = 0.0;

  // default_prop
  Evaluator__set_default_prop( self, NULL );

  // memory
  _vxeval__discard_memory( &self->context.memory );
  if( (self->context.memory = _vxeval__new_memory( -1 )) == NULL ) {
    return -1;
  }

  // wreg
  Evaluator__clear_wreg( self );

  // cullheap
  Evaluator__clear_mcull_heap_array( self );

  // larc
  self->context.larc = NULL;

  // collector
  self->context.collector = NULL;
  self->context.collector_type = VGX_COLLECTOR_TYPE_NONE;

  // timing_budget
  self->context.timing_budget = NULL;

  // threadid
  self->context.threadid = GET_CURRENT_THREAD_ID();

  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__set_context( vgx_Evaluator_t *self, const vgx_Vertex_t *tail, const vgx_ArcHead_t *arc, vgx_Vector_t *vector, double rankscore ) {
  // Reset tail property cache
  self->cache.TAIL.vertex = NULL;

  self->context.VERTEX = self->context.TAIL = tail;
  self->context.rankscore = rankscore;
  if( arc ) {
    self->context.exit = self->context.arrive = arc->predicator;
    self->context.HEAD = arc->vertex;
  }
  if( vector ) {
    CALLABLE( self )->SetVector( self, vector );
  }

  self->context.larc = NULL;
}



/*******************************************************************//**
 * 
 * NOTE: STEALS OBJECTS WITHIN STACK ITEM!
 ***********************************************************************
 */
static void Evaluator__set_default_prop( vgx_Evaluator_t *self, vgx_EvalStackItem_t *default_prop ) {
  // Clean up previous default if needed
  switch( self->context.default_prop.type ) {
  case STACK_ITEM_TYPE_CSTRING:
    CStringDelete( self->context.default_prop.CSTR__str );
    break;
  case STACK_ITEM_TYPE_VECTOR:
    if( self->context.default_prop.vector ) {
      CALLABLE( self->context.default_prop.vector )->Decref( (vgx_Vector_t*)self->context.default_prop.vector );
    }
    break;
  default:
    break;
  }

  if( default_prop ) {
    self->context.default_prop.type = default_prop->type;
    self->context.default_prop.bits = default_prop->bits;
    // Steal
    SET_NONE( default_prop );
  }
  else {
    SET_NONE( &self->context.default_prop );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__own_memory( vgx_Evaluator_t *self, vgx_ExpressEvalMemory_t *memory ) {
  _vxeval__discard_memory( &self->context.memory );
  self->context.memory = memory;
  memory->refc++;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__set_vector( vgx_Evaluator_t *self, vgx_Vector_t *vector ) {
  // Discard any previous vector
  if( self->current.vector ) {
    CALLABLE( self->current.vector )->Decref( self->current.vector );
  }
  // Own new vector if any
  if( (self->current.vector = vector) != NULL ) {
    CALLABLE( self->current.vector )->Incref( self->current.vector );
  }
  else {
    self->current.vector = ivectorobject.Null( self->graph->similarity );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__set_collector( vgx_Evaluator_t *self, vgx_BaseCollector_context_t *collector ) {
  if( (self->context.collector = collector) != NULL ) {
    self->context.collector_type = collector->type;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__set_timing_budget( vgx_Evaluator_t *self, vgx_ExecutionTimingBudget_t *timing_budget ) {
  self->context.timing_budget = timing_budget;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static vgx_EvalStackItem_t * Evaluator__eval( vgx_Evaluator_t *self ) {
  return __evaluator__run( self );
}



/*******************************************************************//**
 * 
 * Evaluate current vertex.
 * This assumes the context has been set to non-NULL values for:
 *   - tail vertex
 *   - prev arc
 * It further assumes the evaluator will NOT access:
 *   - next arc
 *   - head vertex
 ***********************************************************************
 */
static vgx_EvalStackItem_t * Evaluator__eval_vertex( vgx_Evaluator_t *self, const vgx_Vertex_t *vertex ) {

  // Assign default values to head if traversals are attempted
  if( self->rpn_program.deref.arc != 0 ) {
    self->context.exit = VGX_PREDICATOR_NONE;
    self->context.HEAD = vertex;
  }

  // Reset vertex property cache
  self->cache.VERTEX.vertex = NULL;

  // Set vertex to evaluate
  self->context.VERTEX = vertex;

  return __evaluator__run( self );
}



/*******************************************************************//**
 * 
 * Evaluate next arc.
 * This assumes the context has been set to non-NULL values for:
 *   - tail vertex
 *   - prev arc
 *   - this vertex
 ***********************************************************************
 */
static vgx_EvalStackItem_t * Evaluator__eval_arc( vgx_Evaluator_t *self, vgx_LockableArc_t *next ) {
  vgx_EvalStackItem_t *ret;
  // Reset head property cache
  self->cache.HEAD.vertex = NULL;

  // Set the arc
  self->context.VERTEX = next->tail;
  self->context.exit = next->head.predicator;
  self->context.HEAD = next->head.vertex;

  // Store the larc in case of collection
  self->context.larc = next;

  // Execute program
  ret = __evaluator__run( self );

  return ret;
}



/*******************************************************************//*
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_this_deref( const vgx_Evaluator_t *self ) {
  return self->rpn_program.deref.this;
}



/*******************************************************************//*
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_head_deref( const vgx_Evaluator_t *self ) {
  return self->rpn_program.deref.head;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_traversals( const vgx_Evaluator_t *self ) {
  return self->rpn_program.deref.arc;
}



/*******************************************************************//*
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_this_next_access( const vgx_Evaluator_t *self ) {
  return self->rpn_program.deref.this + self->rpn_program.deref.head + self->rpn_program.deref.arc;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_lookbacks( const vgx_Evaluator_t *self ) {
  return self->rpn_program.deref.tail;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_identifiers( const vgx_Evaluator_t *self ) {
  return self->rpn_program.identifiers;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static bool Evaluator__has_cull( const vgx_Evaluator_t *self ) {
  return self->rpn_program.cull;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_synarc_ops( const vgx_Evaluator_t *self ) {
  return self->rpn_program.synarc_ops;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__n_wreg_ops( const vgx_Evaluator_t *self ) {
  return self->rpn_program.n_wreg;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int Evaluator__clear_wreg( vgx_Evaluator_t *self ) {
  if( self->context.wreg.sz > 0 ) {
    if( self->context.wreg.data ) {
      memset( self->context.wreg.data, 0, sizeof(vgx_EvalStackItem_t) * self->context.wreg.sz );
    }
    self->context.wreg.single.meta = 0;
    self->context.wreg.single.bits = 0;
  }
  self->context.wreg.ncall = 0;
  return self->context.wreg.sz;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t Evaluator__get_wreg_ncall( const vgx_Evaluator_t *self ) {
  return self->context.wreg.ncall;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__clear_mcull_heap_array( vgx_Evaluator_t *self ) {
  if( self->context.cullheap ) {
    vgx_ArcHeadHeapItem_t *init_cursor = self->context.cullheap;
    vgx_ArcHeadHeapItem_t *init_end = init_cursor + self->rpn_program.cull;
    for( ; init_cursor < init_end; ++init_cursor ) {
      init_cursor->vertex = NULL;
      init_cursor->score = 0.0;
    }
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_StackItemType_t Evaluator__value_type( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = GET_PITEM( self );
  return item->type;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Evaluator_t * Evaluator__clone( vgx_Evaluator_t *self, vgx_Vector_t *vector ) {

  vgx_Graph_t *graph = self->graph;
  vgx_Evaluator_t *clone = calloc( 1, sizeof( vgx_Evaluator_t ) );

  if( clone ) {
    GRAPH_LOCK( graph ) {
      XTRY {
        // Initialize object
        COMLIB_OBJECT_INIT( vgx_Evaluator_t, clone, &self->id );
        ATOMIC_ASSIGN_i64( &clone->_refc_atomic, 1 );
        
        // Graph state
        clone->graph = graph;
        clone->current.t0 = _vgx_graph_inception( graph );
        clone->current.tnow = _vgx_graph_milliseconds( graph ) / 1000.0;
        clone->current.order = GraphOrder( graph );
        clone->current.size = GraphSize( graph );
        clone->current.op = iOperation.GetId_LCK( &graph->operation );
        
        // Override vector if supplied, otherwise inherit
        if( vector ) {
          clone->current.vector = vector;
        }
        else {
          clone->current.vector = self->current.vector;
        }
        if( clone->current.vector ) {
          CALLABLE( clone->current.vector )->Incref( clone->current.vector );
        }
        else {
          clone->current.vector = ivectorobject.Null( graph->similarity );
        }

        // Cache is zero because calloc above

        // Allocate Operations
        vgx_ExpressEvalProgram_t *orig = &self->rpn_program;
        clone->rpn_program.parser._sz = orig->parser._sz;
        clone->rpn_program.length = orig->length;
        clone->rpn_program.n_passthru = orig->n_passthru;
        clone->rpn_program.deref.tail = orig->deref.tail;
        clone->rpn_program.deref.this = orig->deref.this;
        clone->rpn_program.deref.head = orig->deref.head;
        clone->rpn_program.deref.arc = orig->deref.arc;
        clone->rpn_program.identifiers = orig->identifiers;
        clone->rpn_program.cull = orig->cull;
        clone->rpn_program.synarc_ops = orig->synarc_ops;
        clone->rpn_program.n_wreg = orig->n_wreg;
        int opcount = orig->length + orig->n_passthru;
        CALIGNED_ARRAY_THROWS( clone->rpn_program.operations, vgx_ExpressEvalOperation_t, opcount + 1LL, 0x001 );
        clone->rpn_program.parser._cursor = clone->rpn_program.operations;
        vgx_ExpressEvalOperation_t *dest = clone->rpn_program.operations;
        vgx_ExpressEvalOperation_t *src = orig->operations;
        for( int i=0; i<=opcount; i++ ) { // Includes end NULL terminator!
          *dest++ = *src++;
        }
        
        if( (clone->rpn_program.CSTR__assigned = orig->CSTR__assigned) != NULL ) {
          icstringobject.IncrefNolock( clone->rpn_program.CSTR__assigned );
        }

        // Clone strings
        if( orig->strings ) {
          vgx_ExpressEvalString_t *str_src = orig->strings;
          vgx_ExpressEvalString_t **str_dest = &clone->rpn_program.strings;
          while( str_src ) {
            vgx_ExpressEvalString_t *node = *str_dest = calloc( 1, sizeof( vgx_ExpressEvalString_t ) );
            if( node == NULL ) {
              THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
            }
            str_dest = &node->next;
            node->CSTR__literal = str_src->CSTR__literal;
            icstringobject.IncrefNolock( node->CSTR__literal );
            str_src = str_src->next;
          }
        }

        // Allocate work registers
        if( __create_work_registers( clone ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }

        // Allocate mcull heap memory if needed
        if( __create_mcull_heap_array( clone ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }

        // Allocate Evaluation Stack
        clone->rpn_program.stack.eval_depth = orig->stack.eval_depth;
        if( __create_runtime_eval_stack( clone, graph ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
        }

        // Expression
        if( (clone->rpn_program.CSTR__expression = self->rpn_program.CSTR__expression) != NULL ) {
          icstringobject.IncrefNolock( clone->rpn_program.CSTR__expression );
        }

        clone->cache.CSTR__tmp_prop = NULL;

        // Ready
        if( Evaluator__reset( clone ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
        }

      }
      XCATCH( errcode ) {
        COMLIB_OBJECT_DESTROY( clone );
        clone = NULL;
      }
      XFINALLY {
      }
    } GRAPH_RELEASE;
  }

  return clone;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Evaluator_t * Evaluator__own( vgx_Evaluator_t *self ) {
  ATOMIC_INCREMENT_i64( &self->_refc_atomic );
  return self;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void Evaluator__discard( vgx_Evaluator_t *self ) {
  Evaluator__destructor( self );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static void __Evaluator_deconstruct( vgx_Evaluator_t *self ) {
  // Stack
  __destroy_runtime_eval_stack( self );
  // MCull heap array
  __destroy_mcull_heap_array( self );
  // Work Registers
  __destroy_work_registers( self );
  // Local Scope
  iEvaluator.DeleteLocalScope( self );
  // Cache temp
  iString.Discard( &self->cache.CSTR__tmp_prop );
  // Program
  if( self->rpn_program.operations ) {
    ALIGNED_FREE( self->rpn_program.operations );
  }
  // Info
  if( self->rpn_program.debug.info ) {
    COMLIB_OBJECT_DESTROY( self->rpn_program.debug.info );
  }
  // Name
  if( self->rpn_program.CSTR__assigned ) {
    icstringobject.DecrefNolock( self->rpn_program.CSTR__assigned );
  }
  // Expression
  if( self->rpn_program.CSTR__expression ) {
    icstringobject.DecrefNolock( self->rpn_program.CSTR__expression );
  }
  // Vector
  if( self->current.vector ) {
    CALLABLE( self->current.vector )->Decref( self->current.vector );
  }
  // Memory
  _vxeval__discard_memory( &self->context.memory );
  // String literals
  vgx_ExpressEvalString_t *cursor = self->rpn_program.strings;
  while( cursor ) {
    vgx_ExpressEvalString_t *node = cursor;
    iString.Discard( &node->CSTR__literal );
    cursor = node->next;
    free( node ); 
  }
  // Default property
  Evaluator__set_default_prop( self, NULL );
  // Free object
  free( self );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __reset_runtime_stack( vgx_Evaluator_t *self ) {
  self->sp = self->rpn_program.stack.data;
  SET_NONE( self->sp );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_EvalStackItem_t * __evaluator__run( vgx_Evaluator_t *self ) {
  // Working registers
  __prepare_work_registers( self );

  // Reset stack
  __reset_runtime_stack( self );

  // Reset local scope
  if( self->context.local_scope.objects ) {
    iEvaluator.ClearLocalScope( self );
  }

  // Reset operation cursor
  self->op = self->rpn_program.operations;

  // Run evaluation
  f_evaluator f = self->op->func;
  while( f ) {
    f( self );
    f = (++self->op)->func;
  }


  // Return top of stack after completion
  return GET_PITEM( self );
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __initialize_dummy_vertex( void ) {
  memset( &_aV_dummy, 0, sizeof( vgx_AllocatedVertex_t ) );
  vgx_Vertex_t *V = (vgx_Vertex_t*)&_aV_dummy.object;
  V->vtable = (vgx_Vertex_vtable_t*)COMLIB_CLASS_VTABLE( vgx_Vertex_t );
  V->typeinfo = COMLIB_CLASS_TYPEINFO( vgx_Vertex_t );
  V->descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_INVALID;
  V->descriptor.property.bits = 0;
  iOperation.InitId( &V->operation, 1 );     // clean after init
  V->rank = vgx_Rank_INIT();
  iarcvector.SetNoArc( &V->outarcs );
  iarcvector.SetNoArc( &V->inarcs );
  V->TMX.vertex_ts = 0;
  V->TMX.arc_ts = 0;
  V->TMC = 0;
  V->TMM = 0;
  idset( COMLIB_OBJECT_GETID( V ), 1, 1 );
  _cxmalloc_linehead_from_object( V )->data.refc = 1;
  _cxmalloc_linehead_from_object( V )->data.aidx = 2;
  _cxmalloc_linehead_from_object( V )->data.bidx = 0;
  _cxmalloc_linehead_from_object( V )->data.offset = 0;
  _cxmalloc_linehead_from_object( V )->data.size = 1;
  g_dummy = V;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxeval.h"


test_descriptor_t _vgx_vxeval_tests[] = {
  { "VGX Expression Evaluator Tests",     __utest_vxeval },
  {NULL}
};
#endif
