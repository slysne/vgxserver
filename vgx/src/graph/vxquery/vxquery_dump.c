/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxquery_dump.c
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

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




static __THREAD void const ** gt_current_stack = NULL;
static __THREAD void const ** gt_current_stack_last = NULL;
static __THREAD void const ** gt_current_SP = NULL;




/**************************************************************************//**
 * __delete_stack
 *
 ******************************************************************************
 */
static void __delete_stack( void ) {
  if( gt_current_stack ) {
    free( (void*)gt_current_stack );
    gt_current_stack = NULL;
  }
  gt_current_stack_last = NULL;
  gt_current_SP = NULL;
}



static const void ** __get_new_stack( void ) {
#define __STACK_SIZE 128
  __delete_stack();
  if( (gt_current_stack = calloc( __STACK_SIZE+1, sizeof(void*) )) != NULL ) {
    gt_current_stack_last = gt_current_stack + __STACK_SIZE;
  }
  gt_current_SP = gt_current_stack;
  return gt_current_SP;
}




/**************************************************************************//**
 * __stack_push
 *
 ******************************************************************************
 */
static void __stack_push( const void *obj ) {
  if( gt_current_stack ) {
    if( gt_current_SP < gt_current_stack_last ) {
      *(++gt_current_SP) = obj;
    }
  }
}



static const void * __stack_pop( void ) {
  if( gt_current_stack ) {
    if( gt_current_SP > gt_current_stack ) {
      return *gt_current_SP--;
    }
  }
  return NULL;
}




/**************************************************************************//**
 * __stack_contains
 *
 ******************************************************************************
 */
static int __stack_contains( const void *obj ) {
  if( gt_current_stack ) {
    const void **cursor = gt_current_stack;
    if( gt_current_SP < gt_current_stack_last ) {
      while( (cursor <= gt_current_SP) ) {
        if( *cursor++ == obj ) {
          return 1;
        }
      }
    }
    // stack full, automatic hit to terminate recursion (avoid infinite loop)
    else {
      printf( "!!!STACK FULL!!!\n" );
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __WRITE( int indent, CStringQueue_t *queue, const char *data, int64_t size ) {
  CStringQueue_vtable_t *iQ = CALLABLE( queue );
  for( int i=0; i<indent; i++ ) {
    iQ->AppendNolock( queue, " " );
  }
  int64_t n = iQ->WriteNolock( queue, data, size );
  return n + indent;
}



static __THREAD CStringQueue_t * gt_current_output = NULL;
static __THREAD vgx_Graph_t * gt_current_graph = NULL;





#define SET_CURRENT_OUTPUT( Output )  gt_current_output = (Output)
#define SET_CURRENT_GRAPH( Graph )  gt_current_graph = (Graph)

#define WRITE_CHARS( Indent, Chars )  __WRITE( Indent, gt_current_output, (Chars), -1 )

#define NEWLINE( Indent ) __WRITE( Indent, gt_current_output, "\n", 1 )

#define WRITELINE_CHARS( Indent, Chars )  WRITE_CHARS( Indent, Chars ); NEWLINE( Indent )


#define WRITE_CSTRING( Indent, CString )  __WRITE( Indent, gt_current_output, CStringValue( CString ), CStringLength( CString ) )

#define WRITELINE_CSTRING( Indent, CString )  WRITE_CSTRING( Indent, CString ); NEWLINE( Indent )


#define WRITE_FORMAT( Indent, Format, ... )     \
do {                                            \
  CString_t *CSTR__str;                                                   \
  if( (CSTR__str = CStringNewFormat( Format, ##__VA_ARGS__ )) != NULL ) { \
    WRITE_CSTRING( Indent, CSTR__str );                                   \
    CStringDelete( CSTR__str );                                           \
    CSTR__str = NULL;                                                     \
  }                                         \
} WHILE_ZERO                                \


#define WRITELINE_FORMAT( Indent, Format, ... )   \
do {                                              \
  CString_t *CSTR__str;                                                   \
  if( (CSTR__str = CStringNewFormat( Format, ##__VA_ARGS__ )) != NULL ) { \
    WRITELINE_CSTRING( Indent, CSTR__str );                               \
    CStringDelete( CSTR__str );                                           \
    CSTR__str = NULL;                                                     \
  }                                               \
} WHILE_ZERO   


#define WRITE_KEYCSTR( Indent, Key, CString )   \
do {                                            \
  const char *str = (CString) != NULL ? CStringValue( CString ) : "";   \
  WRITE_FORMAT( Indent, "%s%s", Key, str );                             \
} WHILE_ZERO


#define WRITELINE_KEYCSTR( Indent, Key, CString )   WRITE_KEYCSTR( Indent, Key, CString );  \
                                                    __WRITE( Indent, gt_current_output, "\n", 1 );


#define INDENT( Recursion ) (8 * (Recursion))



#define BEGIN_RECURSIVE_OBJECT( Level, Type, Alias, Obj )   \
  do {                                        \
    int __level__ = Level;                    \
    const Type * const Alias = (Type*)Obj;    \
    const char *__type__ = #Type;             \
    if( Alias == NULL ) {                     \
      WRITELINE_CHARS( 0, "NULL" );           \
    }                                         \
    else {                                    \
      WRITELINE_FORMAT( 0, "{ %llp (%s)", Alias, __type__ );  \
      if( __stack_contains( Alias ) ) {       \
        WRITELINE_CHARS( INDENT( __level__ ), " <RECURSIVE REFERENCE!>" ); \
      }                                       \
      else {                                  \
        __stack_push( Alias );                \
        do /* { code  */


#define END_RECURSIVE_OBJECT                \
        /* } */ WHILE_ZERO;                 \
        __stack_pop();                      \
        WRITELINE_CHARS( INDENT( __level__ - 1 ) , "}" ); \
      }                                     \
    }                                       \
  }                                         \
  WHILE_ZERO





static void __dump_vertex_condition( const vgx_VertexCondition_t * const vertex_condition, int recursion );
static void __dump_degree_condition( const vgx_DegreeCondition_t * const degree_condition, int recursion );
static void __dump_similarity_condition( const vgx_SimilarityCondition_t * const similarity_condition, int recursion );
static void __dump_timestamp_condition( const vgx_TimestampCondition_t * const timestamp_condition, int recursion );
static void __dump_property_condition_set( const vgx_PropertyConditionSet_t * const property_condition_set, int recursion );
static void __dump_recursive_condition( const vgx_RecursiveCondition_t * const recursive_condition, int recursion );
static void __dump_arc_condition_set( const vgx_ArcConditionSet_t * const arc_condition_set, int recursion );
static void __dump_arc_condition( const vgx_ArcCondition_t * const arc_condition, int recursion );
static void __dump_ranking_condition( const vgx_RankingCondition_t * const ranking_condition, int recursion );
static void __dump_evaluator( const vgx_Evaluator_t * const evaluator, int recursion );
static void __dump_symbol( const uintptr_t address, int recursion );


static void __dump_collector_mode( const vgx_collector_mode_t mode, int recursion );
static void __dump_collector( const vgx_BaseCollector_context_t * const collector, int recursion );
static void __dump_similarity( const vgx_Similarity_t * const similarity, int recursion );
static void __dump_ranking_context( const vgx_ranking_context_t * const ranking_context, int recursion );
static void __dump_sortspec( const vgx_sortspec_t sortspec, int recursion );
static void __dump_vertex( const vgx_Vertex_t * const vertex, int recursion );
static void __dump_archead( const vgx_ArcHead_t * const archead, int recursion );
static void __dump_predicator( const vgx_predicator_t predicator, int recursion );
static void __dump_predicator_eph( const vgx_predicator_eph_t eph, int recursion );
static void __dump_predicator_mod( const vgx_predicator_mod_t mod, int recursion );
static void __dump_predicator_rel( const vgx_predicator_rel_t rel, int recursion );
static void __dump_predicator_val( const vgx_predicator_val_t val, int recursion );
static void __dump_value_comparison( const vgx_value_comparison value_comparison, int recursion );
static void __dump_value_condition( const vgx_value_condition_t value_condition, int recursion );
static void __dump_value( const vgx_value_t value, int recursion );
static void __dump_value_type( const vgx_value_type_t value_type, int recursion );
static void __dump_simple_value( const vgx_simple_value_t simple_value, const vgx_value_type_t value_type, int recursion );
static void __dump_neighborhood_probe( const vgx_neighborhood_probe_t * const neighborhood_probe, int recursion );
static void __dump_recursive_probe( const vgx_recursive_probe_t * const recursive_probe, int recursion );
static void __dump_vertex_probe( const vgx_vertex_probe_t * const vertex_probe, int recursion );
static void __dump_vertex_probe_spec( const vgx_vertex_probe_spec spec, int recursion );
static void __dump_vertex_manifestation( const vgx_VertexStateContext_man_t man, int recursion );
static void __dump_vertex_identifiers( const vgx_StringList_t *list, int recursion );
static void __dump_vertex_type( const vgx_vertex_type_t vtype, int recursion );
static void __dump_degree_probe( const vgx_degree_probe_t * const degree_probe, int recursion );
static void __dump_timestamp_probe( const vgx_timestamp_probe_t * const timestamp_probe, int recursion );
static void __dump_similarity_probe( const vgx_similarity_probe_t * const similarity_probe, int recursion );
static void __dump_property_probe( const vgx_property_probe_t * const property_probe, int recursion );
static void __dump_vertex_property( const vgx_VertexProperty_t * const vertex_property, int recursion );
static void __dump_vector( const vgx_Vector_t * const vector, int recursion );
static void __dump_timing_budget( const vgx_ExecutionTimingBudget_t * const timing_budget, int recursion );
static void __dump_timing_budget_flags( const vgx_ExecutionTimingBudgetFlags_t * const timing_budget_flags, int recursion );
static void __dump_arcfilter_context( const vgx_virtual_ArcFilter_context_t * const arcfilter, int recursion );
static void __dump_vertexfilter_context( const vgx_VertexFilter_context_t * const vertexfilter, int recursion );

static void __dump_logic( const vgx_boolean_logic logic, int recursion );
static void __dump_arcdir( const vgx_arc_direction arcdir, int recursion );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CStringQueue_t * _vxquery_dump__query( const vgx_BaseQuery_t *base, CStringQueue_t *output ) {

  SET_CURRENT_OUTPUT( output );
  SET_CURRENT_GRAPH( NULL );

  if( __get_new_stack() == NULL ) {
    return NULL;
  }


  vgx_AdjacencyQuery_t *adjacency = NULL;
  vgx_GlobalQuery_t *global = NULL;
  vgx_AggregatorQuery_t *aggregator = NULL;
  vgx_NeighborhoodQuery_t *neighborhood = NULL;

  static const char *sADJACENCY     = "vgx_AdjacencyQuery_t";
  static const char *sNEIGHBORHOOD  = "vgx_NeighborhoodQuery_t";
  static const char *sGLOBAL        = "vgx_GlobalQuery_t";
  static const char *sAGGREGATOR    = "vgx_AggregatorQuery_t";

  const char *sQUERY = "???";

  WRITELINE_CHARS(      0, "================================" );
  switch( base->type ) {
  case VGX_QUERY_TYPE_ADJACENCY:
    sQUERY = sADJACENCY;
    break;
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    sQUERY = sNEIGHBORHOOD;
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    sQUERY = sGLOBAL;
    break;   
  case VGX_QUERY_TYPE_AGGREGATOR:
    sQUERY = sAGGREGATOR;
    break;
  default:
    break;
  }
  WRITELINE_FORMAT(     0, "%s @ %llp", sQUERY, base );
  WRITELINE_CHARS(      0, "================================" );
  // Base portion
  WRITELINE_CHARS(      0, "__base__" );
  WRITELINE_FORMAT(     0, ".type               : 0x%02X", base->type );
  WRITE_CHARS(          0, ".timing_budget      : " );
  __dump_timing_budget( &base->timing_budget, 1 );
  WRITELINE_KEYCSTR(    0, ".error              : ", base->CSTR__error );
  WRITELINE_KEYCSTR(    0, ".pre_filter         : ", base->CSTR__pre_filter );
  WRITELINE_KEYCSTR(    0, ".vertex_filter      : ", base->CSTR__vertex_filter );
  WRITELINE_KEYCSTR(    0, ".post_filter        : ", base->CSTR__post_filter );
  WRITE_CHARS(          0, ".vertex_condition   : " );
  __dump_vertex_condition( base->vertex_condition, 1 );
  WRITE_CHARS(          0, ".ranking_condition  : " );
  __dump_ranking_condition( base->ranking_condition, 1 );
  WRITELINE_FORMAT(     0, ".evaluator_memory   : %d (@ %llp)", base->evaluator_memory ? (1 << base->evaluator_memory->order) : 0, base->evaluator_memory );
  WRITELINE_FORMAT(     0, ".search_result      : %lld (@ %llp)", base->search_result ? base->search_result->list_length : 0, base->search_result  );
  WRITELINE_FORMAT(     0, ".parent_opid        : %lld", base->parent_opid );

  // Adjacency or Global portion
  switch( base->type ) {
  case VGX_QUERY_TYPE_ADJACENCY:
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
  case VGX_QUERY_TYPE_AGGREGATOR:
    adjacency = (vgx_AdjacencyQuery_t*)base;
    WRITELINE_CHARS(    0, "__adjacency__" );
    WRITELINE_KEYCSTR(  0, ".anchor_id          : ", adjacency->CSTR__anchor_id );
    WRITELINE_FORMAT(   0, ".access_reason      : 0x%03X", adjacency->access_reason );
    WRITE_CHARS(        0, ".arc_condition_set  : " );
    __dump_arc_condition_set( adjacency->arc_condition_set, 1 );
    WRITELINE_FORMAT(   0, ".n_arcs             : %lld", adjacency->n_arcs );
    WRITELINE_FORMAT(   0, ".n_vertices         : %lld", adjacency->n_vertices );
    WRITELINE_FORMAT(   0, ".is_safe_multilock  : %d", adjacency->is_safe_multilock );
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    global = (vgx_GlobalQuery_t*)base;
    WRITELINE_CHARS(    0, "__global__" );
    WRITELINE_KEYCSTR(  0, ".vertex_id          : ", global->CSTR__vertex_id );
    WRITELINE_FORMAT(   0, ".n_items            : %lld", global->n_items );
    WRITE_CHARS(        0, ".collector_mode     : " );
    __dump_collector_mode( global->collector_mode, 1 );
    break;
  default:
    break;
  }

  // Result
  vgx_ResponseAttrFastMask *presult_fieldmask = NULL;
  vgx_Evaluator_t **presult_selector = NULL;
  int *presult_offset = NULL;
  int64_t *presult_hits = NULL;
  vgx_BaseCollector_context_t **presult_result = NULL;
  char *p = (char*)base;
  switch( base->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    presult_fieldmask = (vgx_ResponseAttrFastMask*)(p + offsetof( vgx_NeighborhoodQuery_t, fieldmask ));
    presult_selector = (vgx_Evaluator_t**)(p + offsetof( vgx_NeighborhoodQuery_t, selector ));
    presult_offset = (int*)(p + offsetof( vgx_NeighborhoodQuery_t, offset ));
    presult_hits = (int64_t*)(p + offsetof( vgx_NeighborhoodQuery_t, hits ));
    presult_result = (vgx_BaseCollector_context_t**)(p + offsetof( vgx_NeighborhoodQuery_t, collector ));
    break;
  case VGX_QUERY_TYPE_GLOBAL:
    presult_fieldmask = (vgx_ResponseAttrFastMask*)(p + offsetof( vgx_GlobalQuery_t, fieldmask ));
    presult_selector = (vgx_Evaluator_t**)(p + offsetof( vgx_GlobalQuery_t, selector ));
    presult_offset = (int*)(p + offsetof( vgx_GlobalQuery_t, offset ));
    presult_hits = (int64_t*)(p + offsetof( vgx_GlobalQuery_t, hits ));
    presult_result = (vgx_BaseCollector_context_t**)(p + offsetof( vgx_GlobalQuery_t, collector ));
    break;
  default:
    break;
  }
  switch( base->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
  case VGX_QUERY_TYPE_GLOBAL:
    WRITELINE_CHARS(    0, "__result__" );
    WRITELINE_FORMAT(   0, ".fieldmask          : 0x%08X", *presult_fieldmask );
    WRITE_CHARS(        0, ".selector           : " );
    __dump_evaluator( *presult_selector, 1 );
    WRITELINE_FORMAT(   0, ".offset             : %d", *presult_offset );
    WRITELINE_FORMAT(   0, ".hits               : %lld", *presult_hits );
    WRITELINE_FORMAT(   0, ".result             : " );
    __dump_collector( *presult_result, 1 );
  default:
    break;
  }

  // 
  switch( base->type ) {
  case VGX_QUERY_TYPE_NEIGHBORHOOD:
    neighborhood = (vgx_NeighborhoodQuery_t*)base;
    WRITELINE_CHARS(    0, "__neighborhood__" );
    WRITE_CHARS(        0, ".collect_arc_condition_set : " );
    __dump_arc_condition_set( neighborhood->collect_arc_condition_set, 1 );
    WRITE_CHARS(        0, ".collector_mode            : " );
    __dump_collector_mode( neighborhood->collector_mode, 1 );
    break;
  case VGX_QUERY_TYPE_AGGREGATOR:
    aggregator = (vgx_AggregatorQuery_t*)base;
    WRITELINE_CHARS(    0, "__aggregator__" );
    WRITELINE_FORMAT(   0, ".fields                    : %llp" );
    if( aggregator->fields ) {
      WRITELINE_FORMAT( 0, "        .predval.int       : %lld", aggregator->fields->predval->integer );
      WRITELINE_FORMAT( 0, "        .predval.real      : %#g",  aggregator->fields->predval->real );
      WRITELINE_FORMAT( 0, "        .degree            : %lld", *aggregator->fields->degree );
      WRITELINE_FORMAT( 0, "        .indegree          : %lld", *aggregator->fields->indegree );
      WRITELINE_FORMAT( 0, "        .outdegree         : %lld", *aggregator->fields->outdegree );
    }
    WRITE_CHARS(        0, ".collect_arc_condition_set : " );
    __dump_arc_condition_set( aggregator->collect_arc_condition_set, 1 );
    break;
  default:
    break;
  }

  WRITELINE_CHARS( 0, "================================" );

  __delete_stack();

  return output;


}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxquery_dump__search_context( vgx_Graph_t *graph, const vgx_base_search_context_t *base ) {
  SET_CURRENT_OUTPUT( COMLIB_GetObjectOutput() );
  if( graph ) {
    SET_CURRENT_GRAPH( graph );
  }

  if( __get_new_stack() == NULL ) {
    return;
  }

  vgx_adjacency_search_context_t *adjacency = NULL;
  vgx_global_search_context_t *global = NULL;
  vgx_aggregator_search_context_t *aggregator = NULL;
  vgx_neighborhood_search_context_t *neighborhood = NULL;

  static const char *sADJACENCY     = "vgx_adjacency_search_context_t";
  static const char *sNEIGHBORHOOD  = "vgx_neighborhood_search_context_t";
  static const char *sGLOBAL        = "vgx_global_search_context_t";
  static const char *sAGGREGATOR    = "vgx_aggregator_search_context_t";

  const char *sSEARCH = "???";

  WRITELINE_CHARS(      0, "================================" );
  switch( base->type ) {
  case VGX_SEARCH_TYPE_ADJACENCY:
    sSEARCH = sADJACENCY;
    break;
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
    sSEARCH = sNEIGHBORHOOD;
    break;
  case VGX_SEARCH_TYPE_GLOBAL:
    sSEARCH = sGLOBAL;
    break;   
  case VGX_SEARCH_TYPE_AGGREGATOR:
    sSEARCH = sAGGREGATOR;
    break;
  default:
    break;
  }
  WRITELINE_FORMAT(     0, "%s @ %llp", sSEARCH, base );
  WRITELINE_CHARS(      0, "================================" );
  // Base portion
  WRITELINE_CHARS(      0, "__base__" );
  WRITELINE_FORMAT(     0, ".type               : 0x%02X", base->type );
  WRITE_CHARS(          0, ".timing_budget      : " );
  __dump_timing_budget( base->timing_budget, 1 );
  WRITELINE_KEYCSTR(    0, ".error              : ", base->CSTR__error );
  WRITE_CHARS(          0, ".simcontext         : " );
  __dump_similarity( base->simcontext, 1 );
  WRITE_CHARS(          0, ".pre_evaluator      : " );
  __dump_evaluator( base->pre_evaluator, 1 );
  WRITE_CHARS(          0, ".vertex_evaluator   : " );
  __dump_evaluator( base->vertex_evaluator, 1 );
  WRITE_CHARS(          0, ".post_evaluator     : " );
  __dump_evaluator( base->post_evaluator, 1 );
  WRITE_CHARS(          0, ".ranking_context    : " );
  __dump_ranking_context( base->ranking_context, 1 );

  // Adjacency or Global portion
  switch( base->type ) {
  case VGX_SEARCH_TYPE_ADJACENCY:
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
  case VGX_SEARCH_TYPE_AGGREGATOR:
    adjacency = (vgx_adjacency_search_context_t*)base;
    WRITELINE_CHARS(    0, "__adjacency__" );
    WRITELINE_CHARS(    0, ".anchor             : " );
    if( adjacency->anchor ) {
      __dump_vertex( adjacency->anchor, 1 );
    }
    WRITELINE_FORMAT(   0, ".n_arcs             : %lld", adjacency->n_arcs );
    WRITELINE_FORMAT(   0, ".n_vertices         : %lld", adjacency->n_vertices );
    WRITELINE_FORMAT(   0, ".counts_are_deep    : %d", adjacency->counts_are_deep );
    WRITE_CHARS(        0, ".probe              : " );
    __dump_neighborhood_probe( adjacency->probe, 1 );
    break;
  case VGX_SEARCH_TYPE_GLOBAL:
    global = (vgx_global_search_context_t*)base;
    WRITELINE_CHARS(    0, "__global__" );
    WRITELINE_FORMAT(   0, ".n_items            : %lld", global->n_items );
    WRITELINE_FORMAT(   0, ".counts_are_deep    : %d", global->counts_are_deep );
    WRITE_CHARS(        0, ".probe              : " );
    __dump_vertex_probe( global->probe, 1 );
    break;
  default:
    break;
  }

  // Result
  int *presult_offset = NULL;
  int64_t *presult_hits = NULL;
  vgx_BaseCollector_context_t **presult_result = NULL;
  char *p = (char*)base;
  switch( base->type ) {
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
    presult_offset = (int*)(p + offsetof( vgx_neighborhood_search_context_t, offset ));
    presult_hits = (int64_t*)(p + offsetof( vgx_neighborhood_search_context_t, hits ));
    presult_result = (vgx_BaseCollector_context_t**)(p + offsetof( vgx_neighborhood_search_context_t, result ));
    break;
  case VGX_SEARCH_TYPE_GLOBAL:
    presult_offset = (int*)(p + offsetof( vgx_global_search_context_t, offset ));
    presult_hits = (int64_t*)(p + offsetof( vgx_global_search_context_t, hits ));
    presult_result = (vgx_BaseCollector_context_t**)(p + offsetof( vgx_global_search_context_t, result ));
    break;
  default:
    break;
  }
  switch( base->type ) {
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
  case VGX_SEARCH_TYPE_GLOBAL:
    WRITELINE_CHARS(    0, "__result__" );
    WRITELINE_FORMAT(   0, ".offset             : %d", *presult_offset );
    WRITELINE_FORMAT(   0, ".hits               : %lld", *presult_hits );
    WRITE_CHARS(        0, ".result             : " );
    __dump_collector( *presult_result, 1 );
  default:
    break;
  }

  // Extra
  switch( base->type ) {
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
    neighborhood = (vgx_neighborhood_search_context_t*)base;
    WRITELINE_CHARS(    0, "__neighborhood__" );
    WRITE_CHARS(        0, ".collector_mode     : " );
    __dump_collector_mode( neighborhood->collector_mode, 1 );
    WRITE_CHARS(        0, ".collector          : " );
    __dump_collector( neighborhood->collector, 1 );  // ??? REDUNDANT ??? Same as the .result attribute ???
    break;
  case VGX_SEARCH_TYPE_AGGREGATOR:
    aggregator = (vgx_aggregator_search_context_t*)base;
    WRITELINE_CHARS(    0, "__aggregator__" );
    WRITELINE_FORMAT(   0, ".fields                    : %llp" );
    if( aggregator->fields ) {
      WRITELINE_FORMAT( 0, "        .predval.int       : %lld", aggregator->fields->predval->integer );
      WRITELINE_FORMAT( 0, "        .predval.real      : %#g",  aggregator->fields->predval->real );
      WRITELINE_FORMAT( 0, "        .degree            : %lld", *aggregator->fields->degree );
      WRITELINE_FORMAT( 0, "        .indegree          : %lld", *aggregator->fields->indegree );
      WRITELINE_FORMAT( 0, "        .outdegree         : %lld", *aggregator->fields->outdegree );
    }
    WRITE_CHARS(        0, ".collector_mode     : " );
    __dump_collector_mode( aggregator->collector_mode, 1 );
    WRITE_CHARS(        0, ".collector          : " );
    __dump_collector( aggregator->collector, 1 );
    break;
  case VGX_SEARCH_TYPE_GLOBAL:
    global = (vgx_global_search_context_t*)base;
    WRITELINE_CHARS(    0, "__global__" );
    WRITE_CHARS(        0, ".collector_mode     : " );
    __dump_collector_mode( global->collector.mode, 1 );
    WRITE_CHARS(        0, ".collector          : " );
    __dump_collector( global->collector.base, 1 );
    break;
  default:
    break;
  }

  WRITELINE_CHARS( 0, "================================" );
  
  COMLIB_DumpObjectOutput();

  __delete_stack();

}






/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vertex_condition( const vgx_VertexCondition_t * const vertex_condition, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_VertexCondition_t, VC, vertex_condition ) {
    WRITELINE_FORMAT(   indent,  ".positive    : %d",   VC->positive );
    WRITE_CHARS(        indent,  ".spec        : " );
    __dump_vertex_probe_spec( VC->spec, next );
    WRITE_CHARS(        indent,  ".man         : " );
    __dump_vertex_manifestation( VC->manifestation, next );
    WRITELINE_KEYCSTR(  indent,  ".vertex_type : ",     VC->CSTR__vertex_type );
    WRITELINE_FORMAT(   indent,  ".degree      : %lld", VC->degree );
    WRITELINE_FORMAT(   indent,  ".indegree    : %lld", VC->indegree );
    WRITELINE_FORMAT(   indent,  ".outdegree   : %lld", VC->outdegree );
    WRITE_CHARS(        indent,  ".id          : " );
    __dump_vertex_identifiers( VC->CSTR__idlist, next );
    WRITE_CHARS(        indent,  ".advanced.local_evaluator.filter  : " );
    __dump_evaluator( VC->advanced.local_evaluator.filter, next );
    WRITE_CHARS(        indent,  ".advanced.local_evaluator.post    : " );
    __dump_evaluator( VC->advanced.local_evaluator.post, next );
    WRITE_CHARS(        indent,  ".advanced.degree_condition        : " );
    __dump_degree_condition( VC->advanced.degree_condition, next );
    WRITE_CHARS(        indent,  ".advanced.similarity_condition    : " );
    __dump_similarity_condition( VC->advanced.similarity_condition, next );
    WRITE_CHARS(        indent,  ".advanced.timestamp_condition     : " );
    __dump_timestamp_condition( VC->advanced.timestamp_condition, next );
    WRITE_CHARS(        indent,  ".advanced.property_condition_set  : " );
    __dump_property_condition_set( VC->advanced.property_condition_set, next );
    WRITE_CHARS(        indent,  ".advanced.recursive.conditional   : " );
    __dump_recursive_condition( &VC->advanced.recursive.conditional, next );
    WRITE_CHARS(        indent,  ".advanced.recursive.traversing    : " );
    __dump_recursive_condition( &VC->advanced.recursive.traversing, next );
    WRITE_CHARS(        indent,  ".advanced.recursive.collect_condition_set : " );
    __dump_arc_condition_set( VC->advanced.recursive.collect_condition_set, next );
    WRITE_CHARS(        indent,  ".advanced.recursive.collector_mode : " );
    __dump_collector_mode( VC->advanced.recursive.collector_mode, next );
    WRITELINE_KEYCSTR(  indent,  ".error       : ", VC->CSTR__error );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_degree_condition( const vgx_DegreeCondition_t * const degree_condition, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_DegreeCondition_t, DC, degree_condition ) {
    WRITELINE_CHARS(  indent, "<TODO>" );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_similarity_condition( const vgx_SimilarityCondition_t * const similarity_condition, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_SimilarityCondition_t, SC, similarity_condition ) {
    WRITELINE_CHARS(  indent, "<TODO>" );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_timestamp_condition( const vgx_TimestampCondition_t * const timestamp_condition, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_TimestampCondition_t, TC, timestamp_condition ) {
    WRITELINE_CHARS(  indent, "<TODO>" );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_property_condition_set( const vgx_PropertyConditionSet_t * const property_condition_set, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_PropertyConditionSet_t, PCS, property_condition_set ) {
    WRITELINE_CHARS(  indent, "<TODO>" );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_recursive_condition( const vgx_RecursiveCondition_t * const recursive_condition, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_RecursiveCondition_t, RC, recursive_condition ) {
    WRITE_CHARS(        indent, ".evaluator         : " );
    __dump_evaluator( RC->evaluator, next );
    WRITE_CHARS(        indent, ".vertex_condition  : " );
    __dump_vertex_condition( RC->vertex_condition, next );
    WRITE_CHARS(        indent, ".arc_condition_set : " );
    __dump_arc_condition_set( RC->arc_condition_set, next );
    WRITELINE_FORMAT(   indent, ".override.enable   : %d", (int)RC->override.enable );
    WRITELINE_FORMAT(   indent, ".override.match    : %d", (int)RC->override.match );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_arc_condition_set( const vgx_ArcConditionSet_t * const arc_condition_set, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_ArcConditionSet_t, ACS, arc_condition_set ) {
    WRITELINE_FORMAT(   indent, ".accept      : %d",   ACS->accept );
    WRITE_CHARS(        indent, ".logic       : " );
    __dump_logic( ACS->logic, next );
    WRITE_CHARS(        indent, ".arcdir      : " );
    __dump_arcdir( ACS->arcdir, next );
    vgx_ArcCondition_t ** AC = ACS->set;
    WRITE_CHARS(        indent, ".set         : " );
    if( AC ) {
      while( *AC ) {
        __dump_arc_condition( *AC, next );
        AC++;
      }
    }
    else {
      WRITELINE_CHARS( 0, "NULL" );
    }
    WRITELINE_KEYCSTR(  indent, ".error       : ", ACS->CSTR__error );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_arc_condition( const vgx_ArcCondition_t * const arc_condition, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_ArcCondition_t, AC, arc_condition ) {
    WRITELINE_FORMAT(   indent, ".positive      : %d", AC->positive );
    WRITELINE_KEYCSTR(  indent, ".relationship  : ", AC->CSTR__relationship );
    WRITE_CHARS(        indent, ".modifier      : " );
    __dump_predicator_mod( AC->modifier, next );
    WRITE_CHARS(        indent, ".vcomp         : " );
    __dump_value_comparison( AC->vcomp, next );
    WRITE_CHARS(        indent, ".value1         : " );
    __dump_predicator_val( AC->value1, next );
    WRITE_CHARS(        indent, ".value2         : " );
    __dump_predicator_val( AC->value2, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_ranking_condition( const vgx_RankingCondition_t * const ranking_condition, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_RankingCondition_t, RC, ranking_condition ) {
    WRITE_CHARS(        indent, ".sortspec                : " );
    __dump_sortspec( RC->sortspec, next );
    WRITE_CHARS(        indent, ".modifier                : " );
    __dump_predicator_mod( RC->modifier, next );
    WRITE_CHARS(        indent, ".vector                  : " );
    __dump_vector( RC->vector, next );
    WRITELINE_KEYCSTR(  indent, ".expression              : ", RC->CSTR__expression );
    WRITE_CHARS(        indent, ".aggregate_condition_set : " );
    __dump_arc_condition_set( RC->aggregate_condition_set, next );
    WRITELINE_FORMAT(   indent, ".aggregate_deephits      : %lld", RC->aggregate_deephits );
    WRITELINE_KEYCSTR(  indent, ".error                   : ", RC->CSTR__error );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_evaluator( const vgx_Evaluator_t * const evaluator, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_Evaluator_t, E, evaluator ) {
    const char *expr = E->rpn_program.CSTR__assigned ? CStringValue( E->rpn_program.CSTR__assigned ) : "?";
    WRITELINE_CHARS(  indent, expr );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_symbol( const uintptr_t address, int recursion ) {
  if( address ) {
    WRITELINE_FORMAT( 0, "%p (%s)", address, cxlib_get_symbol_name( address ).value );
  }
  else {
    WRITELINE_CHARS( 0, "NULL" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_collector_mode( const vgx_collector_mode_t mode, int recursion ) {
  switch( _vgx_collector_mode_collect( mode ) ) {
  case VGX_COLLECTOR_MODE_COLLECT_ARCS:
    WRITE_CHARS( 0, "arcs " );
    break;
  case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
    WRITE_CHARS( 0, "vertices " );
    break;
  case VGX_COLLECTOR_MODE_COLLECT_AGGREGATE:
    WRITE_CHARS( 0, "aggregate " );
    break;
  default:
    WRITE_CHARS( 0, "none " );
    switch( _vgx_collector_mode_type( mode ) ) {
    case VGX_COLLECTOR_MODE_NONE_CONTINUE:
      WRITE_CHARS( 0, "continue " );
      break;
    case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
      WRITE_CHARS( 0, "stop at first " );
      break;
    default:
      break;
    }
    break;
  }
  if( _vgx_collector_mode_is_deep_collect( mode ) ) {
    WRITE_CHARS( 0, "deep " );
  }

  WRITELINE_FORMAT( 0, "(0x%04X)", mode );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_collector( const vgx_BaseCollector_context_t * const collector, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_BaseCollector_context_t, C, collector ) {
    int is_sorted = 0;
    int is_list = 0;
    WRITE_CHARS(      indent, ".type       : " );

    // Type
    switch( C->type & __VGX_COLLECTOR_TYPE_MASK ) {
    case __VGX_COLLECTOR_INDIVIDUAL:
      WRITE_CHARS( 0, "item " );
      break;
    case __VGX_COLLECTOR_NEIGHBORHOOD_SUMMARY:
      WRITE_CHARS( 0, "neighborhood summary " );
      break;
    case __VGX_COLLECTOR_GLOBAL_SUMMARY:
      WRITE_CHARS( 0, "global summary " );
      break;
    }

    // Sorted
    switch( C->type & __VGX_COLLECTOR_SORT_MASK ) {
    case __VGX_COLLECTOR_SORTED:
      WRITE_CHARS( 0, "sorted " );
      is_sorted = 1;
      break;
    case __VGX_COLLECTOR_UNSORTED:
      WRITE_CHARS( 0, "unsorted " );
      break;
    }

    // Item
    switch( C->type & __VGX_COLLECTOR_ITEM_MASK ) {
    case __VGX_COLLECTOR_ARC:
      WRITE_CHARS( 0, "arc " );
      break;
    case __VGX_COLLECTOR_VERTEX:
      WRITE_CHARS( 0, "vertex " );
      break;
    }

    // Container
    switch( C->type & __VGX_COLLECTOR_CONTAINER_MASK ) {
    case __VGX_COLLECTOR_LIST:
      WRITELINE_CHARS( 0, "list" );
      is_list = 1;
      break;
    case __VGX_COLLECTOR_AGGREGATION:
      WRITELINE_CHARS( 0, "aggregation" );
      break;
    default:
      NEWLINE( 0 );
    }

    int64_t length = 0;
    if( is_sorted && is_list ) {
      Cm256iHeap_t *heap = C->container.sequence.heap;
      length = CALLABLE( heap )->Length( heap );
    }
    else if( !is_sorted && is_list ) {
      Cm256iList_t *list = C->container.sequence.list;
      length = CALLABLE( list )->Length( list );
    }

    WRITELINE_FORMAT( indent, ".length     : %lld", length );

  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_similarity( const vgx_Similarity_t * const similarity, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_Similarity_t, S, similarity ) {
    WRITELINE_FORMAT( indent, "<TODO>" );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_ranking_context( const vgx_ranking_context_t * const ranking_context, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_ranking_context_t, RC, ranking_context ) {
    WRITE_CHARS(      indent, ".sortspec            : " );
    __dump_sortspec( RC->sortspec, next );
    WRITE_CHARS(      indent, ".modifier            : " );
    __dump_predicator_mod( RC->modifier, next );
    WRITELINE_FORMAT( indent, ".graph               : %llp", RC->graph );
    WRITELINE_FORMAT( indent, ".readonly_graph      : %d", RC->readonly_graph );
    WRITE_CHARS(      indent, ".simcontext          : " );
    __dump_similarity( RC->simcontext, next );
    WRITE_CHARS(      indent, ".vector              : " );
    __dump_vector( RC->vector, next );
    WRITELINE_FORMAT( indent, ".fingerprint         : 0x%016X", RC->fingerprint );
    WRITE_CHARS(      indent, ".evaluator           : " );
    __dump_evaluator( RC->evaluator, next );
    WRITE_CHARS(      indent, ".timing_budget       : " );
    __dump_timing_budget( RC->timing_budget, next );
    WRITE_CHARS(      indent, ".postfilter_context  : " );
    __dump_arcfilter_context( RC->postfilter_context, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_sortspec( const vgx_sortspec_t sortspec, int recursion ) {
  WRITELINE_FORMAT( 0, "%s (0x%04X)",  _vgx_sortspec_as_string( sortspec ), sortspec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vertex( const vgx_Vertex_t * const vertex, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_Vertex_t, V, vertex ) {
    if( !COMLIB_OBJECT_CLASSMATCH( V, COMLIB_CLASS_CODE( vgx_Vertex_t ) ) || __vertex_is_defunct( V ) ) {
      WRITELINE_CHARS(  0, "<defunct>" );
    }
    else {
      const char *id = V->identifier.CSTR__idstr ? CStringValue( V->identifier.CSTR__idstr ) : V->identifier.idprefix.data;
      WRITELINE_FORMAT( indent, "%s", id );
    }
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_archead( const vgx_ArcHead_t * const archead, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_ArcHead_t, H, archead ) {
    WRITE_CHARS(      indent, ".vertex     : " );
    __dump_vertex( H->vertex, next );
    WRITE_CHARS(      indent, ".predicator : " );
    __dump_predicator( H->predicator, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_predicator( const vgx_predicator_t predicator, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  const vgx_predicator_t P = predicator;
  WRITELINE_FORMAT(      0, "0x%016X", P.data );
  WRITE_CHARS(      indent, ".eph  : " );
  __dump_predicator_eph( P.eph, next );
  WRITE_CHARS(      indent, ".mod  : " );
  __dump_predicator_mod( P.mod, next );
  WRITE_CHARS(      indent, ".rel  : " );
  __dump_predicator_rel( P.rel, next );
  WRITE_CHARS(      indent, ".val  : " );
  __dump_predicator_val( P.val, next );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_predicator_eph( const vgx_predicator_eph_t eph, int recursion ) {
  WRITELINE_FORMAT( 0, "%d 0x%1X %d", eph.neg, eph.type, eph.value );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_predicator_mod( const vgx_predicator_mod_t mod, int recursion ) {
  WRITELINE_FORMAT( 0, "%s (0x%02X)", _vgx_modifier_as_string( mod ), mod.bits );
}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_predicator_rel( const vgx_predicator_rel_t rel, int recursion ) {
  const CString_t *CSTR__rel = NULL;
  if( gt_current_graph ) {
    CSTR__rel = iEnumerator_OPEN.Relationship.Decode( gt_current_graph, rel.enc );
  }
  if( CSTR__rel ) {
    WRITELINE_FORMAT( 0, "%s (0x%04X)", CStringValue( CSTR__rel ), rel.enc );
  }
  else {
    WRITELINE_FORMAT( 0, "0x%04X", rel.enc );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_predicator_val( const vgx_predicator_val_t val, int recursion ) {
  WRITELINE_FORMAT( 0, "%d(s) %u(u) %f(f)", val.integer, val.uinteger, val.real );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_value_comparison( const vgx_value_comparison value_comparison, int recursion ) {
  WRITELINE_FORMAT( 0, "(0x%02X)", _vgx_vcomp_as_string( value_comparison ), value_comparison );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_value_condition( const vgx_value_condition_t value_condition, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  const vgx_value_condition_t C = value_condition;
  NEWLINE( 0 );
  WRITE_CHARS(      indent, ".value1 : " );
  __dump_value( C.value1, next );
  WRITE_CHARS(      indent, ".value2 : " );
  __dump_value( C.value2, next );
  WRITE_CHARS(      indent, ".vcomp  : " );
  __dump_value_comparison( C.vcomp, next );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_value( const vgx_value_t value, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  const vgx_value_t V = value;
  NEWLINE( 0 );
  WRITE_CHARS(      indent, ".type        : " );
  __dump_value_type( V.type, next );
  WRITELINE_FORMAT( indent, ".data.bits   : 0x%016X", V.data.bits );
  WRITE_CHARS(      indent, ".data.simple : " );
  __dump_simple_value( V.data.simple, V.type, next );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_value_type( const vgx_value_type_t value_type, int recursion ) {
  switch( value_type ) {
  case VGX_VALUE_TYPE_NULL:
    WRITELINE_CHARS( 0, "NULL" );
    return;
  case VGX_VALUE_TYPE_BOOLEAN:
    WRITELINE_CHARS( 0, "BOOLEAN" );
    return;
  case VGX_VALUE_TYPE_INTEGER:
    WRITELINE_CHARS( 0, "INTEGER" );
    return;
  case VGX_VALUE_TYPE_REAL:
    WRITELINE_CHARS( 0, "REAL" );
    return;
  case VGX_VALUE_TYPE_QWORD:
    WRITELINE_CHARS( 0, "QWORD" );
    return;
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
    WRITELINE_CHARS( 0, "ENUMERATED_CSTRING" );
    return;
  case VGX_VALUE_TYPE_CSTRING:
    WRITELINE_CHARS( 0, "CSTRING" );
    return;
  case VGX_VALUE_TYPE_BORROWED_STRING:
    WRITELINE_CHARS( 0, "BORROWED_STRING" );
    return;
  case VGX_VALUE_TYPE_STRING:
    WRITELINE_CHARS( 0, "STRING" );
    return;
  case VGX_VALUE_TYPE_POINTER:
    WRITELINE_CHARS( 0, "POINTER" );
    return;
  default:
    WRITELINE_CHARS( 0, "???" );
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_simple_value( const vgx_simple_value_t simple_value, const vgx_value_type_t value_type, int recursion ) {
  const vgx_simple_value_t S = simple_value;
  switch( value_type ) {
  case VGX_VALUE_TYPE_NULL:
    WRITELINE_CHARS( 0, "0" );
    return;
  case VGX_VALUE_TYPE_BOOLEAN:
  case VGX_VALUE_TYPE_INTEGER:
    WRITELINE_FORMAT( 0, "%d", S.integer );
    return;
  case VGX_VALUE_TYPE_REAL:
    WRITELINE_FORMAT( 0, "%f", S.real );
    return;
  case VGX_VALUE_TYPE_QWORD:
    WRITELINE_FORMAT( 0, "%llu", S.qword );
    return;
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
  case VGX_VALUE_TYPE_CSTRING:
    WRITELINE_FORMAT( 0, "%s", S.CSTR__string ? CStringValue( S.CSTR__string ) : "<error>" );
    return;
  case VGX_VALUE_TYPE_BORROWED_STRING:
  case VGX_VALUE_TYPE_STRING:
    WRITELINE_FORMAT( 0, "%s", S.string );
    return;
  case VGX_VALUE_TYPE_POINTER:
    WRITELINE_FORMAT( 0, "%llp", S.pointer );
    return;
  default:
    WRITELINE_FORMAT( 0, "???" );
    return;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_neighborhood_probe( const vgx_neighborhood_probe_t * const neighborhood_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_neighborhood_probe_t, P, neighborhood_probe ) {
    WRITELINE_FORMAT( indent, ".graph                   : %llp", P->graph );
    WRITE_CHARS(      indent, ".conditional             : " );
    __dump_recursive_probe( &P->conditional, next );
    WRITE_CHARS(      indent, ".traversing              : " );
    __dump_recursive_probe( &P->traversing, next );
    WRITELINE_FORMAT( indent, ".distance                : %d", P->distance );
    WRITELINE_FORMAT( indent, ".readonly_graph          : %d", P->readonly_graph );
    WRITE_CHARS(      indent, ".collector_mode          : " );
    __dump_collector_mode( P->collector_mode, next );
    WRITE_CHARS(      indent, ".current_tail_RO         : " );
    __dump_vertex( P->current_tail_RO, next );
    WRITE_CHARS(      indent, ".common_collector        : " );
    __dump_collector( P->common_collector, next );
    WRITE_CHARS(      indent, ".pre_evaluator           : " );
    __dump_evaluator( P->pre_evaluator, next );
    WRITE_CHARS(      indent, ".post_evaluator          : " );
    __dump_evaluator( P->post_evaluator, next );
    WRITE_CHARS(      indent, ".collect_filter_context  : " );
    __dump_arcfilter_context( P->collect_filter_context, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_recursive_probe( const vgx_recursive_probe_t * const recursive_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_recursive_probe_t, P, recursive_probe ) {
    WRITE_CHARS(      indent, ".arcfilter         : " );
    __dump_arcfilter_context( P->arcfilter, next );
    WRITE_CHARS(      indent, ".vertex_probe      : " );
    __dump_vertex_probe( P->vertex_probe, next );
    WRITE_CHARS(      indent, ".arcdir            : " );
    __dump_arcdir( P->arcdir, next );
    WRITE_CHARS(      indent, ".evaluator         : " );
    __dump_evaluator( P->evaluator, next );
    WRITELINE_FORMAT( indent, ".override.enable   : %d", (int)P->override.enable );
    WRITELINE_FORMAT( indent, ".override.match    : %d", (int)P->override.match );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vertex_probe( const vgx_vertex_probe_t * const vertex_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_vertex_probe_t, P, vertex_probe ) {
    WRITE_CHARS(      indent, ".spec                  : " );
    __dump_vertex_probe_spec( P->spec, next );
    WRITE_CHARS(      indent, ".manifestation         : " );
    __dump_vertex_manifestation( P->manifestation, next );
    WRITE_CHARS(      indent, ".timing_budget         : " );
    __dump_timing_budget( P->timing_budget, next );
    WRITE_CHARS(      indent, ".vertexfilter_context  : " );
    __dump_vertexfilter_context( P->vertexfilter_context, next );
    WRITELINE_FORMAT( indent, ".distance              : %d", P->distance );
    WRITE_CHARS(      indent, ".vertex_type           : " );
    __dump_vertex_type( P->vertex_type, next );
    WRITELINE_FORMAT( indent, ".degree                : %lld", P->degree );
    WRITELINE_FORMAT( indent, ".indegree              : %lld", P->indegree );
    WRITELINE_FORMAT( indent, ".outdegree             : %lld", P->outdegree );
    WRITE_CHARS(      indent, ".id                    : " );
    __dump_vertex_identifiers( P->CSTR__idlist, next );
    WRITE_CHARS(      indent, ".advanced.local_evaluator.filter  : " );
    __dump_evaluator( P->advanced.local_evaluator.filter, next );
    WRITE_CHARS(      indent, ".advanced.local_evaluator.post    : " );
    __dump_evaluator( P->advanced.local_evaluator.post, next );
    WRITE_CHARS(      indent, ".advanced.degree_probe            : " );
    __dump_degree_probe( P->advanced.degree_probe, next );
    WRITE_CHARS(      indent, ".advanced.timestamp_probe         : " );
    __dump_timestamp_probe( P->advanced.timestamp_probe, next );
    WRITE_CHARS(      indent, ".advanced.similarity_probe        : " );
    __dump_similarity_probe( P->advanced.similarity_probe, next );
    WRITE_CHARS(      indent, ".advanced.property_probe          : " );
    __dump_property_probe( P->advanced.property_probe, next );
    WRITE_CHARS(      indent, ".advanced.next.neighborhood_probe : " );
    __dump_neighborhood_probe( P->advanced.next.neighborhood_probe, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_vertex_probe_spec( const vgx_vertex_probe_spec spec, int recursion ) {
  char *sspec = _vgx_probe_spec_as_new_string( spec );
  WRITELINE_FORMAT( 0, "%s (0x%08X)", sspec, spec );
  free( sspec );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_vertex_manifestation( const vgx_VertexStateContext_man_t man, int recursion ) {
  WRITELINE_FORMAT( 0,  "%s", _vgx_manifestation_as_string( man ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_vertex_identifiers( const vgx_StringList_t *list, int recursion ) {
  int indent = INDENT( recursion );
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_StringList_t, L, list ) {
    for( int64_t i=0; i<iString.List.Size( L ); i++ ) {
      const CString_t *CSTR__id = iString.List.GetItem( (vgx_StringList_t*)L, i );
      const char *str = CStringValue( CSTR__id );
      WRITELINE_FORMAT( indent, ".id[%3lld]             : %s", str );
    }
  } END_RECURSIVE_OBJECT;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_vertex_type( const vgx_vertex_type_t vtype, int recursion ) {
  const CString_t *CSTR__vtype = NULL;
  if( gt_current_graph ) {
    CSTR__vtype = iEnumerator_OPEN.VertexType.Decode( gt_current_graph, vtype );
  }
  if( CSTR__vtype ) {
    WRITELINE_FORMAT( 0, "%s (0x%02X)", CStringValue( CSTR__vtype ), vtype );
  }
  else {
    WRITELINE_FORMAT( 0, "0x%02X", vtype );
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_degree_probe( const vgx_degree_probe_t * const degree_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_degree_probe_t, P, degree_probe ) {
    WRITE_CHARS(      indent, ".collector           : " );
    __dump_collector( (vgx_BaseCollector_context_t*)P->collector, next );
    WRITE_CHARS(      indent, ".neighborhood_probe  : " );
    __dump_neighborhood_probe( P->neighborhood_probe, next );
    WRITE_CHARS(      indent, ".value_condition     : " );
    __dump_value_condition( P->value_condition, next );
    WRITELINE_FORMAT( indent, ".current_value       : %lld", P->current_value );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_timestamp_probe( const vgx_timestamp_probe_t * const timestamp_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_timestamp_probe_t, P, timestamp_probe ) {
    WRITELINE_FORMAT( indent, ".positive    : %d", P->positive );
    WRITE_CHARS(      indent, ".tmc_valcond : " );
    __dump_value_condition( P->tmc_valcond, next );
    WRITE_CHARS(      indent, ".tmm_valcond : " );
    __dump_value_condition( P->tmm_valcond, next );
    WRITE_CHARS(      indent, ".tmx_valcond : " );
    __dump_value_condition( P->tmx_valcond, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_similarity_probe( const vgx_similarity_probe_t * const similarity_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_similarity_probe_t, P, similarity_probe ) {
    WRITELINE_FORMAT( indent, ".positive    : %d", P->positive );
    WRITE_CHARS(      indent, ".simcontext  : " );
    __dump_similarity( P->simcontext, next );
    WRITE_CHARS(      indent, ".probevector : " );
    __dump_vector( P->probevector, next );
    WRITELINE_FORMAT( indent, ".fingerprint : 0x%016X", P->fingerprint );
    WRITE_CHARS(      indent, ".simscore    : " );
    __dump_value_condition( P->simscore, next );
    WRITE_CHARS(      indent, ".hamdist     : " );
    __dump_value_condition( P->hamdist, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_property_probe( const vgx_property_probe_t * const property_probe, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_property_probe_t, P, property_probe ) {
    WRITELINE_FORMAT( indent, ".positive    : %d", P->positive_match );
    WRITELINE_FORMAT( indent, ".len    : %lld", P->len );
    WRITE_CHARS(      indent, ".condition_list : " );
    const vgx_VertexProperty_t *cursor = P->condition_list;
    do {
      __dump_vertex_property( cursor, next );
    } while( cursor && (++cursor)->key );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vertex_property( const vgx_VertexProperty_t * const vertex_property, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_VertexProperty_t, VP, vertex_property ) {
    WRITELINE_KEYCSTR(  indent, ".key       : ", VP->key );
    WRITELINE_FORMAT(   indent, ".keyhash   : 0x%016X", VP->keyhash );
    WRITE_CHARS(        indent, ".val       : " );
    __dump_value( VP->val, next );
    WRITE_CHARS(        indent, ".condition : " );
    __dump_value_condition( VP->condition, next );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vector( const vgx_Vector_t * const vector, int recursion ) {
  int indent = INDENT( recursion );
  const vgx_Vector_t * const V = vector;
  char buf[16] = {0};
  if( V ) {
    WRITELINE_FORMAT(      0, "%llp (vgx_Vector_t)", V );
    WRITELINE_FORMAT( indent, ".metas               : 0x%08X", V->metas.qword );
    WRITELINE_FORMAT( indent, ".metas.flags         : %s", uint8_to_bin( buf, V->metas.flags.bits ) );
    WRITELINE_FORMAT( indent, ".metas.type          : %d", V->metas.type );
    WRITELINE_FORMAT( indent, ".metas.vlen          : %d", V->metas.vlen );
    if( V->metas.flags.ecl ) {
      WRITELINE_FORMAT( indent, ".metas.scalar.factor : %f", V->metas.scalar.factor );
    }
    else {
      WRITELINE_FORMAT( indent, ".metas.scalar.norm   : %f", V->metas.scalar.norm );
    }
  }
  else {
    WRITELINE_CHARS(  0, "NULL" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_timing_budget( const vgx_ExecutionTimingBudget_t * const timing_budget, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  const vgx_ExecutionTimingBudget_t * const TB = timing_budget;
  if( TB ) {
    WRITELINE_FORMAT(      0, "%llp (vgx_ExecutionTimingBudget_t)", TB );
    if( TB->timeout_ms == 0 ) {
      WRITELINE_CHARS(  indent, "(non-blocking)" );
    }
    else {
      WRITELINE_FORMAT( indent, ".t_remain_ms   : %lld", TB->t_remain_ms );
      WRITELINE_FORMAT( indent, ".t0_ms         : %lld", TB->t0_ms );
      WRITELINE_FORMAT( indent, ".tt_ms         : %lld", TB->tt_ms );
      WRITELINE_FORMAT( indent, ".timout_ms     : %d", TB->timeout_ms );
      WRITE_CHARS(      indent, ".flags         : " );
      __dump_timing_budget_flags( &TB->flags, next );
      WRITELINE_FORMAT( indent, ".resource      : %p", TB->resource );
      WRITELINE_FORMAT( indent, ".reason        : 0x%03X", TB->reason );
    }
  }
  else {
    WRITELINE_CHARS(  0, "NULL" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_timing_budget_flags( const vgx_ExecutionTimingBudgetFlags_t * const timing_budget_flags, int recursion ) {
  int indent = INDENT( recursion );
  const vgx_ExecutionTimingBudgetFlags_t * const F = timing_budget_flags;
  if( F ) {
    WRITELINE_FORMAT(      0, "%llp (vgx_ExecutionTimingBudgetFlags_t)", F );
    WRITELINE_FORMAT( indent, ".is_halted         : %d", (int)F->is_halted );
    WRITELINE_FORMAT( indent, ".is_blocking       : %d", (int)F->is_blocking );
    WRITELINE_FORMAT( indent, ".is_infinite       : %d", (int)F->is_infinite );
    WRITELINE_FORMAT( indent, ".resource_blocked  : %d", (int)F->resource_blocked );
  }
  else {
    WRITELINE_CHARS(  0, "NULL" );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_arcfilter_context( const vgx_virtual_ArcFilter_context_t * const arcfilter, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_GenericArcFilter_context_t, AF, arcfilter ) {
    WRITELINE_FORMAT( indent, ".type                          : 0x%04X", AF->type );
    WRITELINE_FORMAT( indent, ".positive_match                : %d", AF->positive_match );
    WRITELINE_FORMAT( indent, ".arcfilter_locked_head_access  : %d", AF->arcfilter_locked_head_access );
    WRITELINE_FORMAT( indent, ".eval_synarc                   : %d", AF->eval_synarc );
    WRITE_CHARS(      indent, ".traversing_evaluator          : " );
    __dump_evaluator( AF->traversing_evaluator, next );
    WRITE_CHARS(      indent, ".filter                        : " );
    __dump_symbol( (uintptr_t)AF->filter, next );
    WRITE_CHARS(      indent, ".timing_budget                 : " );
    __dump_timing_budget( AF->timing_budget, next );
    WRITE_CHARS(      indent, ".previous_context              : " );
    __dump_arcfilter_context( AF->previous_context, next );
    WRITE_CHARS(      indent, ".current_tail                  : " );
    __dump_vertex( AF->current_tail, next );
    WRITE_CHARS(      indent, ".current_head                  : " );
    __dump_archead( AF->current_head, next );
    WRITE_CHARS(      indent, ".superfilter                   : " );
    __dump_arcfilter_context( AF->superfilter, next );
    WRITELINE_CHARS(  indent, "__generic__" );
    WRITE_CHARS(      indent, ".terminal.current              : " );
    __dump_vertex( AF->terminal.current, next );
    WRITELINE_FORMAT( indent, ".terminal.list                 : %llp", AF->terminal.list );
    WRITE_CHARS(      indent, ".terminal.logic                : " );
    __dump_logic( AF->terminal.logic, next );
    WRITE_CHARS(      indent, ".arcfilter_callback            : " );
    __dump_symbol( (uintptr_t)AF->arcfilter_callback, next );
    WRITE_CHARS(      indent, ".logic                         : " );
    __dump_logic( AF->logic, next );
    WRITE_CHARS(      indent, ".pred_condition1               : " );
    __dump_predicator( AF->pred_condition1, next );
    WRITE_CHARS(      indent, ".pred_condition2               : " );
    __dump_predicator( AF->pred_condition2, next );
    WRITE_CHARS(      indent, ".vertex_probe                  : " );
    __dump_vertex_probe( AF->vertex_probe, next );
    WRITE_CHARS(      indent, ".culleval                      : " );
    __dump_evaluator( AF->culleval, next );
    WRITELINE_FORMAT( indent, ".locked_cull                   : %d", AF->locked_cull );
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __dump_vertexfilter_context( const vgx_VertexFilter_context_t * const vertexfilter, int recursion ) {
  int indent = INDENT( recursion );
  int next = recursion + 1;
  BEGIN_RECURSIVE_OBJECT( recursion, vgx_VertexFilter_context_t, VF, vertexfilter ) {
    WRITELINE_FORMAT(   indent, ".type                  : 0x%04X", VF->type );
    WRITELINE_FORMAT(   indent, ".positive_match        : %d", VF->positive_match );
    WRITE_CHARS(        indent, ".local_evaluator.pre   : " );
    __dump_evaluator( VF->local_evaluator.pre, next );
    WRITE_CHARS(        indent, ".local_evaluator.main  : " );
    __dump_evaluator( VF->local_evaluator.main, next );
    WRITE_CHARS(        indent, ".local_evaluator.post  : " );
    __dump_evaluator( VF->local_evaluator.post, next );
    WRITELINE_FORMAT(   indent, ".current_thread        : %d", VF->current_thread );
    WRITE_CHARS(        indent, ".filter                : " );
    __dump_symbol( (uintptr_t)VF->filter, next );
    WRITE_CHARS(        indent, ".timing_budget         : " );
    __dump_timing_budget( VF->timing_budget, next );
    switch( VF->type ) {
    case VGX_VERTEX_FILTER_TYPE_EVALUATOR:
    case VGX_VERTEX_FILTER_TYPE_GENERIC:
      WRITELINE_CHARS(  indent, "__generic__" );
      WRITE_CHARS(      indent, ".vertex_probe          : " );
      __dump_vertex_probe( ((vgx_GenericVertexFilter_context_t*)VF)->vertex_probe, next );
      break;
    default:
      break;
    }
  } END_RECURSIVE_OBJECT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_logic( const vgx_boolean_logic logic, int recursion ) {
  WRITELINE_FORMAT( 0, "%s (0x%X)", _vgx_logic_as_string( logic ), logic );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static void __dump_arcdir( const vgx_arc_direction arcdir, int recursion ) {
  WRITELINE_FORMAT( 0, "%s (0x%X)", _vgx_arcdir_as_string( arcdir ), arcdir );
}
