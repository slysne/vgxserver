/*######################################################################
 *#
 *# vxquery_probe.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxarcvector.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

static int __fallback_similarity_anchor_vector( vgx_Graph_t *self, vgx_VertexCondition_t *vertex_condition, vgx_neighborhood_probe_t *probe, vgx_Vertex_t *vertex_RO, CString_t **CSTR__error );

static void __delete_ranking_context( vgx_ranking_context_t **ranking_context );
static int __configure_new_ranking_context_from_condition( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, vgx_Similarity_t *simcontext_clone, vgx_ranking_context_t **ranking_context, CString_t **CSTR__error );

static void __clear_base_search_context( vgx_base_search_context_t *search );
static int __configure_base_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, vgx_base_search_context_t *search );

static void __delete_vertex_property_probe( vgx_property_probe_t **property_probe );
static vgx_property_probe_t * __new_vertex_property_probe_from_condition_set( vgx_Graph_t *self, const vgx_PropertyConditionSet_t *condition_set );

static void __delete_vertex_timestamp_probe( vgx_timestamp_probe_t **timestamp_probe );
static vgx_timestamp_probe_t * __new_vertex_timestamp_probe_from_condition( const vgx_TimestampCondition_t *condition );

static void __delete_vertex_similarity_probe( vgx_similarity_probe_t **similarity_probe );
static vgx_similarity_probe_t * __new_vertex_similarity_probe_from_condition( const vgx_SimilarityCondition_t *condition, vgx_Similarity_t *simcontext_borrowed );

static void __delete_vertex_degree_probe( vgx_degree_probe_t **degree_probe );
static vgx_degree_probe_t * __new_vertex_degree_probe_from_condition( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, const vgx_DegreeCondition_t *condition );

static void __delete_vertex_probe( vgx_vertex_probe_t **probe );
static int __configure_new_vertex_probe_from_condition( vgx_Graph_t *self,
                                                        bool readonly_graph,
                                                        vgx_BaseQuery_t *query,
                                                        vgx_VertexCondition_t *vertex_condition,
                                                        vgx_BaseCollector_context_t *collector,
                                                        vgx_Similarity_t *simcontext_borrowed,
                                                        int distance,
                                                        vgx_vertex_probe_t **vertex_probe );

static void __delete_neighborhood_probe( vgx_neighborhood_probe_t **probe );

static int __configure_new_neighborhood_probe(  vgx_Graph_t *self,
                                                bool readonly_graph,
                                                vgx_Vertex_t *anchor_RO,
                                                vgx_BaseQuery_t *query, 
                                                vgx_Evaluator_t *pre_evaluator,
                                                vgx_Evaluator_t *post_evaluator,
                                                const vgx_RecursiveCondition_t *conditional,
                                                const vgx_RecursiveCondition_t *traversing,
                                                vgx_collector_mode_t collector_mode,
                                                const vgx_ArcConditionSet_t *collect_arc_condition_set,
                                                vgx_BaseCollector_context_t *collector,
                                                vgx_Similarity_t *simcontext_borrowed,
                                                CString_t **CSTR__error,
                                                int distance,
                                                vgx_neighborhood_probe_t **neighborhood_probe );


static void __clear_adjacency_search_context( vgx_adjacency_search_context_t *search );
static vgx_adjacency_search_context_t * __clone_adjacency_search_context( vgx_Graph_t *self, vgx_adjacency_search_context_t *other );
static int __configure_adjacency_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AdjacencyQuery_t *query, vgx_adjacency_search_context_t *search );

static void __clear_neighborhood_search_context( vgx_neighborhood_search_context_t *search );
static vgx_neighborhood_search_context_t * __clone_neighborhood_search_context( vgx_Graph_t *self, vgx_neighborhood_search_context_t *other );
static int __configure_neighborhood_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *vertex_RO, vgx_NeighborhoodQuery_t *query, vgx_neighborhood_search_context_t *search );

static void __clear_aggregator_search_context( vgx_aggregator_search_context_t *search );
static vgx_aggregator_search_context_t * __clone_aggregator_search_context( vgx_Graph_t *self, vgx_aggregator_search_context_t *other );
static int __configure_aggregator_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AggregatorQuery_t *query, vgx_aggregator_search_context_t *search );

static void __clear_global_search_context( vgx_global_search_context_t *search );
static vgx_global_search_context_t * __clone_global_search_context( vgx_Graph_t *self, vgx_global_search_context_t *other );


static vgx_adjacency_search_context_t * _vxquery_probe__new_adjacency_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AdjacencyQuery_t *query );
static vgx_neighborhood_search_context_t * _vxquery_probe__new_neighborhood_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *vertex_RO, vgx_NeighborhoodQuery_t *query );
static vgx_aggregator_search_context_t * _vxquery_probe__new_aggregator_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AggregatorQuery_t *query );
static vgx_global_search_context_t * _vxquery_probe__new_global_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_GlobalQuery_t *query );
static void _vxquery_probe__delete_search_context( vgx_base_search_context_t **search );
static vgx_base_search_context_t * _vxquery_probe__clone_search_context( vgx_Graph_t *self, const vgx_base_search_context_t *other );


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN IGraphProbe_t iGraphProbe = {
  .NewAdjacencySearch       = _vxquery_probe__new_adjacency_search_context,
  .NewNeighborhoodSearch    = _vxquery_probe__new_neighborhood_search_context,
  .NewAggregatorSearch      = _vxquery_probe__new_aggregator_search_context,
  .NewGlobalSearch          = _vxquery_probe__new_global_search_context,
  .DeleteSearch             = _vxquery_probe__delete_search_context,
  .CloneSearch              = _vxquery_probe__clone_search_context
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __fallback_similarity_anchor_vector( vgx_Graph_t *self, vgx_VertexCondition_t *vertex_condition, vgx_neighborhood_probe_t *probe, vgx_Vertex_t *vertex_RO, CString_t **CSTR__error ) {
  // For similarity search, if a NULL-vector was specified we try to use the anchor's vector instead
  //
  // TODO: traversing or conditional probe ???
  //
  if( probe && probe->traversing.vertex_probe && probe->traversing.vertex_probe->advanced.similarity_probe && vertex_RO ) {
    vgx_Vector_t *vector = probe->traversing.vertex_probe->advanced.similarity_probe->probevector;
    // Root anchor's probe vector is a NULL-vector
    if( CALLABLE( vector )->IsNull( vector ) ) {
      // Root anchor has no vector, search is not defined for this probe
      if( vertex_RO->vector == NULL ) {
        CString_t *CSTR__id = NewEphemeralCString( self, CALLABLE( vertex_RO )->IDString( vertex_RO ) );
        __set_error_string_from_reason( CSTR__error, CSTR__id, VGX_ACCESS_REASON_NO_VERTEX_VECTOR );
        return -1;
      }
      else {
        // Discard the NULL-vector
        CALLABLE( vector )->Decref( vector );
        // Own and use the anchor's vector instead
        CALLABLE( vertex_RO->vector )->Incref( vertex_RO->vector );
        probe->traversing.vertex_probe->advanced.similarity_probe->probevector = vertex_RO->vector;
        // Replace the vector in the vertex condition as well
        if( vertex_condition && vertex_condition->advanced.similarity_condition ) {
          vgx_Vector_t *original_vector = vertex_condition->advanced.similarity_condition->probevector;
          if( original_vector ) {
            // Discard the vector in the original condition
            CALLABLE( original_vector )->Decref( original_vector );
            // Own and use the anchor's vector in the original condition as well
            CALLABLE( vertex_RO->vector )->Incref( vertex_RO->vector );
            vertex_condition->advanced.similarity_condition->probevector = vertex_RO->vector;
          }
        }
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_ranking_context( vgx_ranking_context_t **ranking_context ) {
  if( *ranking_context ) {
    if( (*ranking_context)->vector ) {
      CALLABLE( (*ranking_context)->vector )->Decref( (*ranking_context)->vector );
    }
    
    // Delete the evaluation context
    if( (*ranking_context)->evaluator ) {
      COMLIB_OBJECT_DESTROY( (*ranking_context)->evaluator );
      (*ranking_context)->evaluator = NULL;
    }

    // Delete any aggregate condition filter we may have
    iArcFilter.Delete( &(*ranking_context)->postfilter_context );

    free( *ranking_context );
    *ranking_context = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __configure_new_ranking_context_from_condition( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, vgx_Similarity_t *simcontext_clone, vgx_ranking_context_t **ranking_context, CString_t **CSTR__error ) {
  vgx_ExecutionTimingBudget_t *timing_budget = &query->timing_budget;
  vgx_RankingCondition_t *ranking_condition = query->ranking_condition;
  if( ranking_condition == NULL ) {
    return -1;
  }

  // 1. Allocate the ranking context
  if( (*ranking_context = (vgx_ranking_context_t*)calloc( 1, sizeof( vgx_ranking_context_t ) )) == NULL ) {
    return -1;
  }

  // 2. Copy the query sortspec into our new ranking context
  (*ranking_context)->sortspec = ranking_condition->sortspec;

  // 3. Copy the query predicator modifier into our new ranking context
  (*ranking_context)->modifier = ranking_condition->modifier;

  // 4. Set the graph
  (*ranking_context)->graph = self;
  (*ranking_context)->readonly_graph = readonly_graph;

  // 5. Use the shared similarity context clone (shared with filter)
  (*ranking_context)->simcontext = simcontext_clone;

  // 6. Own another reference to the query's vector, if any
  vgx_Vector_t *vector = ranking_condition->vector;
  if( ((*ranking_context)->vector = vector) != NULL ) {
    CALLABLE( vector )->Incref( vector );
    // Use the fingerprint from the query vector
    (*ranking_context)->fingerprint = vector->fp;
  }
  else {
    // No vector, so fingerprint is 0
    (*ranking_context)->fingerprint = 0;
  }

  // 7. Composite ranking formula
  if( ranking_condition->CSTR__expression ) {
    const char *expression = CStringValue( ranking_condition->CSTR__expression );
    vgx_Evaluator_t *evaluator;
    if( (evaluator = (*ranking_context)->evaluator = iEvaluator.NewEvaluator( self, expression, ranking_condition->vector, CSTR__error )) == NULL ) {
      // error
      __delete_ranking_context( ranking_context );
      return -1;
    }
    if( query->evaluator_memory == NULL ) {
      if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
        return -1;
      }
    }
    CALLABLE( evaluator )->OwnMemory( evaluator, query->evaluator_memory );
  }

  // 8. Ranker timing budget
  (*ranking_context)->timing_budget = timing_budget;

  // 9. Aggregate condition
  if( ranking_condition->aggregate_condition_set ) {

    // Create the aggregate condition filter
    // TODO ------------- ADD advanced filter to the aggregate condition ---------------------------------------------------v
    if( ((*ranking_context)->postfilter_context = iArcFilter.New( self, readonly_graph, ranking_condition->aggregate_condition_set, NULL, NULL, timing_budget )) == NULL ) {
      // error
      __delete_ranking_context( ranking_context );
      return -1;
    }

    // TODO:
    // DEAL WITH CONFIGURING THE NEW ARCFILTER FOR THINGS LIKE DYNAMIC COMPARISON -- DOES THAT EVEN MEAN ANYTHING WITH A POSTFILTER?? DON'T THINK IT DOES...

  }


  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_base_search_context( vgx_base_search_context_t *search ) {
  if( search ) {
    // Discard any error string
    iString.Discard( &search->CSTR__error );

    // Discard the similarity clone
    if( search->simcontext ) {
      COMLIB_OBJECT_DESTROY( search->simcontext );
      search->simcontext = NULL;
    }

    // Discard the advanced filters
    iEvaluator.DiscardEvaluator( &search->pre_evaluator );
    iEvaluator.DiscardEvaluator( &search->vertex_evaluator );
    iEvaluator.DiscardEvaluator( &search->post_evaluator );

    // Delete the ranking context
    __delete_ranking_context( &search->ranking_context );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Evaluator_t * __new_evaluator( vgx_Graph_t *self, vgx_BaseQuery_t *query, const char *expression ) {
  vgx_Evaluator_t *evaluator = NULL;
  vgx_Vector_t *vector = NULL;
  // Use vector from ranking condition if supplied
  if( query->ranking_condition && query->ranking_condition->vector ) {
    vector = query->ranking_condition->vector;
  }
  // Fallback to vertex similarity probe vector
  else if( query->vertex_condition && query->vertex_condition->advanced.similarity_condition ) {
    vector = query->vertex_condition->advanced.similarity_condition->probevector;
  }
  if( (evaluator = iEvaluator.NewEvaluator( self, expression, vector, &query->CSTR__error )) == NULL ) {
    return NULL;
  }
  if( query->evaluator_memory == NULL ) {
    if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
      iEvaluator.DiscardEvaluator( &evaluator );
      return NULL;
    }
  }
  CALLABLE( evaluator )->OwnMemory( evaluator, query->evaluator_memory );
  return evaluator;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_Evaluator_t * __prepare_evaluator( vgx_Evaluator_t *E, vgx_BaseCollector_context_t *C, vgx_ExpressEvalMemory_t *M, vgx_ExecutionTimingBudget_t *TB ) {
  if( E ) {
    vgx_Evaluator_vtable_t *iEval = CALLABLE( E );
    iEval->Own( E );
    iEval->SetCollector( E, C );
    iEval->OwnMemory( E, M );
    iEval->SetTimingBudget( E, TB );
  }
  return E;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_Evaluator_t * __own_evaluator( vgx_Evaluator_t *E, vgx_ExecutionTimingBudget_t *TB ) {
  if( E ) {
    vgx_Evaluator_vtable_t *iEval = CALLABLE( E );
    iEval->Own( E );
    iEval->SetTimingBudget( E, TB );
  }
  return E;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __require_simcontext( vgx_VertexCondition_t *condition ) {
  if( condition == NULL ) {
    return 0;
  }
  else if( condition->advanced.similarity_condition ) {
    return 1;
  }
  else {
    return __require_simcontext( condition->advanced.recursive.conditional.vertex_condition ) ||
          __require_simcontext( condition->advanced.recursive.traversing.vertex_condition );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __configure_base_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, vgx_base_search_context_t *search ) {
  int retcode = 0;

  XTRY {
    // 1. Use shared timing budget from query
    search->timing_budget = &query->timing_budget;

    // 2. Initialize error string to empty
    search->CSTR__error = NULL;

    // 3. Advanced filters
    // PRE
    vgx_Evaluator_t *E;
    if( query->CSTR__pre_filter ) {
      if( (E = search->pre_evaluator = __new_evaluator( self, query, CStringValue( query->CSTR__pre_filter ))) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x601 );
      }
      if( CALLABLE( E )->Traversals( E ) ) {
        __set_error_string( &query->CSTR__error, "traversal not allowed in pre-filter" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x602 );
      }
      if( CALLABLE( E )->HasCull( E ) ) {
        __set_error_string( &query->CSTR__error, "mcull() not allowed in pre-filter" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x603 );
      }
    }
    // MAIN
    if( query->CSTR__vertex_filter ) {
      if( (search->vertex_evaluator = __new_evaluator( self, query, CStringValue( query->CSTR__vertex_filter ))) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x604 );
      }
    }
    // POST
    if( query->CSTR__post_filter ) {
      if( (E = search->post_evaluator = __new_evaluator( self, query, CStringValue( query->CSTR__post_filter ))) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x607 );
      }
      if( CALLABLE( E )->Traversals( E ) ) {
        __set_error_string( &query->CSTR__error, "traversal not allowed in post-filter" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x608 );
      }
      if( CALLABLE( E )->HasCull( E ) ) {
        __set_error_string( &query->CSTR__error, "mcull() not allowed in post-filter" );
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x608 );
      }
    }

    // 4. Clone the graph simcontext if needed
    // We either have a similarity filter condition or a similarity condition.
    int require_simcontext = (query->ranking_condition && _vgx_sortby( query->ranking_condition->sortspec ) == VGX_SORTBY_SIMSCORE );
    if( !require_simcontext ) {
      require_simcontext = __require_simcontext( query->vertex_condition );
    }
    if( require_simcontext ) {
      // Clone the graph's similarity context so we can use it independenty during query execution.
      if( (search->simcontext = CALLABLE(self->similarity)->Clone( self->similarity )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x606 );
      }
      // Make sure the cached values are cleared and invalidated! (Otherwise mysterious sorting errors will happen)
      CALLABLE(search->simcontext)->Clear( search->simcontext );
    }
    else {
      // No need for similarity for this query
      search->simcontext = NULL;
    }

    // 5. Create and configure a new ranking context
    if( query->ranking_condition ) {
      if( __configure_new_ranking_context_from_condition( self, readonly_graph, query, search->simcontext, &search->ranking_context, &query->CSTR__error ) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x607 );
      }
    }
    else {
      search->ranking_context = NULL;
    }

  }
  XCATCH( errcode ) {
    __clear_base_search_context( search );
    retcode = -1;
  }
  XFINALLY {}

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_degree_probe( vgx_degree_probe_t **degree_probe ) {
  if( degree_probe && *degree_probe ) {
    __delete_neighborhood_probe( &(*degree_probe)->neighborhood_probe );
    iGraphCollector.DeleteCollector( (vgx_BaseCollector_context_t**)&(*degree_probe)->collector );
    iVertexProperty.ClearValueCondition( &(*degree_probe)->value_condition );
    free( *degree_probe );
    *degree_probe = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_degree_probe_t * __new_vertex_degree_probe_from_condition( vgx_Graph_t *self, bool readonly_graph, vgx_BaseQuery_t *query, const vgx_DegreeCondition_t *condition ) {
  vgx_degree_probe_t *degree_probe = NULL;
  XTRY {
    if( (degree_probe = calloc( 1, sizeof( vgx_degree_probe_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x611 );
    }

    if( iVertexProperty.CloneValueConditionInto( &degree_probe->value_condition, &condition->value_condition ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x612 );
    }

    if( (degree_probe->collector = iGraphCollector.NewArcCollector( self, NULL, query, NULL )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x613 );
    }

    // Configure the degree condition's neighborhood probe (borrows collector reference!)
    vgx_BaseCollector_context_t *base_collector = (vgx_BaseCollector_context_t*)degree_probe->collector;
    const vgx_RecursiveCondition_t recursive_condition = {
      .evaluator         = NULL,
      .vertex_condition  = NULL,
      .arc_condition_set = condition->arc_condition_set,
      .override          = {
          .enable           = false,
          .match            = VGX_ARC_FILTER_MATCH_MISS
      }
    };
    const vgx_RecursiveCondition_t *conditional = &recursive_condition;
    const vgx_RecursiveCondition_t *traversing = &recursive_condition;
    if( __configure_new_neighborhood_probe( self, readonly_graph, NULL, query, NULL, NULL, conditional, traversing, VGX_COLLECTOR_MODE_COLLECT_ARCS, NULL, base_collector, NULL, NULL, 1, &degree_probe->neighborhood_probe ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x614 );
    }

    // Reset current value
    degree_probe->current_value = 0;

  }
  XCATCH( errcode ) {
    __delete_vertex_degree_probe( &degree_probe );
  }
  XFINALLY {
  }
  return degree_probe;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_timestamp_probe( vgx_timestamp_probe_t **timestamp_probe ) {
  if( timestamp_probe && *timestamp_probe ) {
    free( *timestamp_probe );
    *timestamp_probe = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_timestamp_probe_t * __new_vertex_timestamp_probe_from_condition( const vgx_TimestampCondition_t *condition ) {
  vgx_timestamp_probe_t *timestamp_probe = NULL;
  if( (timestamp_probe = calloc( 1, sizeof( vgx_timestamp_probe_t ) )) != NULL ) {
    timestamp_probe->positive = condition->positive;
    iVertexProperty.CloneValueConditionInto( &timestamp_probe->tmc_valcond, &condition->tmc_valcond );
    iVertexProperty.CloneValueConditionInto( &timestamp_probe->tmm_valcond, &condition->tmm_valcond );
    iVertexProperty.CloneValueConditionInto( &timestamp_probe->tmx_valcond, &condition->tmx_valcond );
  }
  return timestamp_probe;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_similarity_probe( vgx_similarity_probe_t **similarity_probe ) {
  if( similarity_probe && *similarity_probe ) {
    vgx_Vector_t *vector = (*similarity_probe)->probevector;
    if( vector ) {
      CALLABLE( vector )->Decref( vector );
    }
    free( *similarity_probe );
    *similarity_probe = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_similarity_probe_t * __new_vertex_similarity_probe_from_condition( const vgx_SimilarityCondition_t *condition, vgx_Similarity_t *simcontext_borrowed ) {
  vgx_similarity_probe_t *similarity_probe = NULL;
  if( (similarity_probe = calloc( 1, sizeof( vgx_similarity_probe_t ) )) != NULL ) {
    // Set the match sign
    similarity_probe->positive = condition->positive;

    // Set the shared context that was handed to us from the search context higher up
    similarity_probe->simcontext = simcontext_borrowed;

    // Own another reference to the vector and set the fingerprint
    if( (similarity_probe->probevector = condition->probevector) != NULL ) {
      CALLABLE( similarity_probe->probevector )->Incref( similarity_probe->probevector );
      similarity_probe->fingerprint = similarity_probe->probevector->fp;
    }
    else {
      similarity_probe->fingerprint = 0;
    }

    // Set the similarity score condition
    similarity_probe->simscore = condition->simval_condition;

    // Set the hamming distance condition
    similarity_probe->hamdist = condition->hamval_condition;
  }
  return similarity_probe;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_property_probe( vgx_property_probe_t **property_probe ) {
  if( property_probe && *property_probe ) {
    vgx_VertexProperty_t *cursor = (*property_probe)->condition_list;
    if( cursor ) {
      int64_t len = (*property_probe)->len;
      for( int64_t vx=0; vx<len; vx++ ) {
        iVertexProperty.Clear( cursor++ );
      }
      free( (*property_probe)->condition_list );
    }
    free( *property_probe );
    *property_probe = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_property_probe_t * __new_vertex_property_probe_from_condition_set( vgx_Graph_t *self, const vgx_PropertyConditionSet_t *condition_set ) {
  vgx_property_probe_t *probe = NULL;

  XTRY {
    if( (probe = calloc( 1, sizeof( vgx_property_probe_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x621 );
    }
    CQwordList_t *list = condition_set->__data;
    int64_t len = condition_set->Length( list );
    if( (probe->condition_list = calloc( len, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x622 );
    }
    probe->len = len;
    for( int64_t px=0; px<len; px++ ) {
      vgx_VertexProperty_t *prop;
      if( condition_set->Get( list, px, &prop ) != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x623 );
      }

      if( iVertexProperty.CloneInto( &probe->condition_list[ px ], prop ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x624 );
      }

      // Pre-compute the hash of the key in the probe so we don't have to do it for each vertex
      probe->condition_list[ px ].keyhash = iEnumerator_OPEN.Property.Key.GetEnum( self, probe->condition_list[ px ].key );

    }
    probe->positive_match = condition_set->positive;
  }
  XCATCH( errcode ) {
    __delete_vertex_property_probe( &probe );
  }
  XFINALLY {
  }

  return probe;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_vertex_probe( vgx_vertex_probe_t **vertex_probe ) {
  if( *vertex_probe ) {
    vgx_vertex_probe_t *VP = *vertex_probe;

    // Delete the vertex filter
    iVertexFilter.Delete( &VP->vertexfilter_context );

    // Delete property probe
    __delete_vertex_property_probe( &VP->advanced.property_probe );

    // Delete similarity probe
    __delete_vertex_similarity_probe( &VP->advanced.similarity_probe );

    // Delete timestamp probe
    __delete_vertex_timestamp_probe( &VP->advanced.timestamp_probe );

    // Delete degree probe
    __delete_vertex_degree_probe( &VP->advanced.degree_probe );

    // Delete neighbor's neighbor probe
    __delete_neighborhood_probe( &VP->advanced.next.neighborhood_probe );

    // Discard any filter evaluator we may own
    iEvaluator.DiscardEvaluator( &VP->advanced.local_evaluator.filter );
    iEvaluator.DiscardEvaluator( &VP->advanced.local_evaluator.post );

    // Discard the ID probe value if any
    iString.List.Discard( &VP->CSTR__idlist );

    // Free the vertex probe and set to NULL
    free( *vertex_probe );
    *vertex_probe = NULL;
  }
}



/*******************************************************************//**
 *
 *
 * NOTE: vertex_condition may have deep collect flag modified here.
 *
 * Returns : 1 = configured ok
 *           0 = early termination
 *          -1 = error
 ***********************************************************************
 */
static int __configure_new_vertex_probe_from_condition( vgx_Graph_t *self,
                                                        bool readonly_graph,
                                                        vgx_BaseQuery_t *query,
                                                        vgx_VertexCondition_t *vertex_condition,
                                                        vgx_BaseCollector_context_t *collector,
                                                        vgx_Similarity_t *simcontext_borrowed,
                                                        int distance,
                                                        vgx_vertex_probe_t **vertex_probe )
{
  int ret = 1;

  // No conditions, return empty probe
  if( vertex_condition == NULL ) {
    *vertex_probe = NULL;
  }
  // We have vertex conditions, now set up the probe
  else {
    vgx_ExecutionTimingBudget_t *timing_budget = &query->timing_budget;
    vgx_vertex_probe_spec spec = vertex_condition->spec;
    vgx_VertexStateContext_man_t manifestation = vertex_condition->manifestation;
    // Vertex conditions are equivalent to a full wildcard, so again no conditions and return empty probe
    if( _vgx_vertex_condition_full_wildcard( spec, manifestation ) && vertex_condition->positive ) {
      *vertex_probe = NULL;
    }
    // We have non-wildcard vertex conditions
    else {
      vgx_vertex_probe_t *VP = NULL;
      XTRY {

        // 1. Allocate the new vertex probe object
        if( (VP = *vertex_probe = (vgx_vertex_probe_t*)calloc( 1, sizeof( vgx_vertex_probe_t ) )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x631 );
        }

        // ----------------------
        // 2. Copy the probe spec
        // ----------------------
        VP->spec = spec;

        // --------------------------------
        // 3. Copy the manifestation filter
        // --------------------------------
        VP->manifestation = manifestation;

        // ---------------------------
        // 4. Use shared timing budget
        // ---------------------------
        VP->timing_budget = timing_budget;

        // -------------------------------
        // 5. Set the distance from anchor
        // -------------------------------
        VP->distance = distance;

        // -----------------------------------------------------------------------------------------------------------------------------
        // 6. Check standard vertex conditions indicated by the query's probe spec and set the corresponding probe values and thresholds
        // -----------------------------------------------------------------------------------------------------------------------------

        // vertex type filter?
        if( _vgx_vertex_condition_has_vertextype( spec ) ) {
          const char *type_str = NULL;
          if( vertex_condition->CSTR__vertex_type ) {
            type_str = CStringValue( vertex_condition->CSTR__vertex_type );
          }
          // Enumeration supplied directly
          if( type_str && *type_str == '#' ) {
            int64_t tn;
            if( strtoint64( type_str+1, &tn ) < 0 || tn < 0 || tn > VERTEX_TYPE_ENUMERATION_MAX_ENTRIES || !__vertex_type_enumeration_valid( VP->vertex_type = (vgx_vertex_type_t)tn ) ) {
              __format_error_string( &vertex_condition->CSTR__error, "invalid type enumeration: %s", type_str );
              THROW_SILENT( CXLIB_ERR_API, 0x632 );
            }
          }
          // Translate from string to enumeration
          else {
            VP->vertex_type = (vgx_vertex_type_t)iEnumerator_OPEN.VertexType.GetEnum( self, vertex_condition->CSTR__vertex_type );
            // Vertex type does not exist, early termination
            if( VP->vertex_type == VERTEX_TYPE_ENUMERATION_NONEXIST ) {
              ret = 0; // early termination, TODO: What about negative condition, then we should not terminate ???
            }
            // Bad type name
            else if( !__vertex_type_enumeration_valid( VP->vertex_type ) ) {
              __format_error_string( &vertex_condition->CSTR__error, "invalid vertex type: %s (0x%02x)", type_str ? type_str : "(null)", VP->vertex_type );
              THROW_SILENT( CXLIB_ERR_API, 0x633 );
            }
          }
        }

        // degree filter?
        if( _vgx_vertex_condition_has_degree( spec ) ) {
          VP->degree = vertex_condition->degree;
        }

        // indegree filter?
        if( _vgx_vertex_condition_has_indegree( spec ) ) {
          VP->indegree = vertex_condition->indegree;
        }

        // outdegree filter?
        if( _vgx_vertex_condition_has_outdegree( spec ) ) {
          VP->outdegree = vertex_condition->outdegree;
        }

        // id filter?
        if( _vgx_vertex_condition_has_id( spec ) ) {
          if( (VP->CSTR__idlist = iString.List.Clone( vertex_condition->CSTR__idlist )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x635 );
          }
        }

        // ----------------------------------------------------------------------------------------
        // 7. Check and configure advanced vertex conditions as indicated by the query's probe spec
        // ----------------------------------------------------------------------------------------
        // advanced filter?
        if( _vgx_vertex_condition_has_advanced( spec ) ) {

          // LOCAL FILTER EXPRESSIONS
          if( vertex_condition->advanced.local_evaluator.filter || vertex_condition->advanced.local_evaluator.post ) {
            if( query->evaluator_memory == NULL ) {
              if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
                THROW_ERROR( CXLIB_ERR_MEMORY, 0x636 );
              }
            }
            VP->advanced.local_evaluator.filter = __prepare_evaluator( vertex_condition->advanced.local_evaluator.filter, collector, query->evaluator_memory, timing_budget );
            VP->advanced.local_evaluator.post = __prepare_evaluator( vertex_condition->advanced.local_evaluator.post, collector, query->evaluator_memory, timing_budget );
          }

          // DEGREE conditions(s)
          if( vertex_condition->advanced.degree_condition != NULL ) {
            if( (VP->advanced.degree_probe = __new_vertex_degree_probe_from_condition( self, readonly_graph, query, vertex_condition->advanced.degree_condition )) == NULL ) {
              THROW_SILENT( CXLIB_ERR_GENERAL, 0x637 );
            }
          }

          // TIME condition(s)
          if( vertex_condition->advanced.timestamp_condition != NULL ) {
            if( (VP->advanced.timestamp_probe = __new_vertex_timestamp_probe_from_condition( vertex_condition->advanced.timestamp_condition )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x638 );
            }
          }

          // SIMILARITY conditions(s)
          if( vertex_condition->advanced.similarity_condition != NULL ) {
            if( (VP->advanced.similarity_probe = __new_vertex_similarity_probe_from_condition( vertex_condition->advanced.similarity_condition, simcontext_borrowed )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x639 );
            }
          }

          // PROPERTY condition(s)
          if( vertex_condition->advanced.property_condition_set ) {
            if( (VP->advanced.property_probe = __new_vertex_property_probe_from_condition_set( self, vertex_condition->advanced.property_condition_set )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x63A );
            }
          }

          // Recursive neighborhood probe
          if( _vgx_vertex_condition_is_recursive( vertex_condition ) == true ) {
            // Configure a new (recursive) neighborhood probe for the neighbor's neighbor
            int next_distance = distance + 1;
            if( next_distance > VGX_PREDICATOR_EPH_DISTANCE_MAX ) {
              __format_error_string( &vertex_condition->CSTR__error, "probe recursion limit reached: %d", next_distance );
              THROW_SILENT( CXLIB_ERR_API, 0x63B );
            }
            else {
              vgx_collector_mode_t collect_next_mode = collector ? vertex_condition->advanced.recursive.collector_mode : VGX_COLLECTOR_MODE_NONE_CONTINUE;
              const vgx_RecursiveCondition_t *next_conditional = &vertex_condition->advanced.recursive.conditional;
              const vgx_RecursiveCondition_t *next_traversing = &vertex_condition->advanced.recursive.traversing;

              int conf = __configure_new_neighborhood_probe(  self,
                                                              readonly_graph,
                                                              NULL, // <- anchor is initialized to NULL for extended (recursive) neighborhood search: Will be populated dynamically during execution!
                                                              query,
                                                              NULL, // pre
                                                              NULL, // post
                                                              next_conditional,
                                                              next_traversing,
                                                              collect_next_mode,
                                                              vertex_condition->advanced.recursive.collect_condition_set,
                                                              collector,
                                                              simcontext_borrowed,
                                                              &((vgx_VertexCondition_t*)vertex_condition)->CSTR__error,
                                                              next_distance,
                                                              &VP->advanced.next.neighborhood_probe
                                                           );
              // Configuration ok
              if( conf > 0 ) { 
                // Mark as deep collect if we're collecting or inherit deep collect if collection is happening in extended neighborhoods beyond this one
                if( _vgx_collector_mode_collect( collect_next_mode ) 
                      ||
                    ( next_traversing->vertex_condition && _vgx_collector_mode_is_deep_collect( next_traversing->vertex_condition->advanced.recursive.collector_mode ) )
                  )
                {
                  vertex_condition->advanced.recursive.collector_mode |= VGX_COLLECTOR_MODE_DEEP_COLLECT;
                  VP->advanced.next.neighborhood_probe->collector_mode |= VGX_COLLECTOR_MODE_DEEP_COLLECT;
                }
              }
              // Early termination (no need to run query)
              else if( conf == 0 ) {
                ret = 0;
              }
              // Error
              else {
                THROW_SILENT( CXLIB_ERR_GENERAL, 0x63C );
              }
            }

          }
        }

        // -----------------------------------------------------------------
        // 8. Create and configure new VERTEX filter based vertex conditions
        // -----------------------------------------------------------------
        if( (VP->vertexfilter_context = iVertexFilter.New( VP, timing_budget )) == NULL ) {
          THROW_SILENT( CXLIB_ERR_GENERAL, 0x63D );
        }
        (*vertex_probe)->vertexfilter_context->positive_match = vertex_condition->positive;
      }
      XCATCH( errcode ) {
        __delete_vertex_probe( vertex_probe );
        ret = -1;
      }
      XFINALLY {

      }
    }
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __delete_neighborhood_probe( vgx_neighborhood_probe_t **probe ) {
  if( *probe ) {
    vgx_neighborhood_probe_t *P = *probe;

    // Reset conditional and traversing probes
    int shared_arcfilter_instance = P->conditional.arcfilter == P->traversing.arcfilter;
    int shared_vertex_probe_instance = P->conditional.vertex_probe == P->traversing.vertex_probe;

    // Delete arc filter(s)
    iArcFilter.Delete( &P->conditional.arcfilter );
    if( shared_arcfilter_instance ) {
      P->traversing.arcfilter = NULL;
    }
    else {
      iArcFilter.Delete( &P->traversing.arcfilter );
    }

    // Delete vertex probe(s)
    __delete_vertex_probe( &P->conditional.vertex_probe );
    if( shared_vertex_probe_instance ) {
      P->traversing.vertex_probe = NULL;
    }
    else {
      __delete_vertex_probe( &P->traversing.vertex_probe );
    }

    // Discard evaluators (refcounted)
    iEvaluator.DiscardEvaluator( &P->conditional.evaluator );
    iEvaluator.DiscardEvaluator( &P->traversing.evaluator );

    // Delete collector filter
    iArcFilter.Delete( &P->collect_filter_context );
    // Discard any pre/post evaluators we may own
    iEvaluator.DiscardEvaluator( &P->pre_evaluator );
    iEvaluator.DiscardEvaluator( &P->post_evaluator );
    // Free the probe and set to NULL
    free( P );
    *probe = NULL;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __propagate_condition_errors( const vgx_RecursiveCondition_t *condition, CString_t **CSTR__error ) {

  // Steal and propagate any arc condition error that may have occurred
  if( condition->arc_condition_set && condition->arc_condition_set->CSTR__error ) {
    // Propagate error up if not already set
    if( CSTR__error && *CSTR__error == NULL ) {
      *CSTR__error = condition->arc_condition_set->CSTR__error;
      ((vgx_ArcConditionSet_t*)condition->arc_condition_set)->CSTR__error = NULL; // steal despite const
    }
    // Clear the (unpropagated) error - this error is masked
    else {
      iString.Discard( (CString_t**)&(condition->arc_condition_set->CSTR__error) );
    }
  }

  // Steal and propagate any vertex condition error that may have occurred
  if( condition->vertex_condition && condition->vertex_condition->CSTR__error ) {
    // Propagate error up if not already set
    if( CSTR__error && *CSTR__error == NULL ) {
      *CSTR__error = condition->vertex_condition->CSTR__error;
      ((vgx_VertexCondition_t*)condition->vertex_condition)->CSTR__error = NULL; // steal despite const
    }
    // Clear the (unpropagated) error - this error is masked
    else {
      iString.Discard( (CString_t**)&(condition->vertex_condition->CSTR__error) );
    }
  }
}



/*******************************************************************//**
 *
 * Returns : 1 = configured ok 
 *           0 = early termination
 *          -1 = error
 ***********************************************************************
 */
static int __configure_new_neighborhood_probe(  vgx_Graph_t *self,
                                                bool readonly_graph,
                                                vgx_Vertex_t *anchor_RO,
                                                vgx_BaseQuery_t *query, 
                                                vgx_Evaluator_t *pre_evaluator,
                                                vgx_Evaluator_t *post_evaluator,
                                                const vgx_RecursiveCondition_t *conditional,
                                                const vgx_RecursiveCondition_t *traversing,
                                                vgx_collector_mode_t collector_mode,
                                                const vgx_ArcConditionSet_t *collect_arc_condition_set,
                                                vgx_BaseCollector_context_t *collector,
                                                vgx_Similarity_t *simcontext_borrowed,
                                                CString_t **CSTR__error,
                                                int distance,
                                                vgx_neighborhood_probe_t **neighborhood_probe )
{
  int retcode = 1;
  XTRY {
    vgx_ExecutionTimingBudget_t *timing_budget = &query->timing_budget;
    vgx_neighborhood_probe_t *probe;

    // True if conditional and traversing conditions are different.
    int is_forked_path = conditional != traversing;

    // 1. Allocate the new probe object
    if( (probe = *neighborhood_probe = (vgx_neighborhood_probe_t*)calloc( 1, sizeof( vgx_neighborhood_probe_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x641 );
    }

    // Evaluators
    if( pre_evaluator || post_evaluator || conditional->evaluator || traversing->evaluator ) {
      if( query->evaluator_memory == NULL ) {
        if( (query->evaluator_memory = iEvaluator.NewMemory( -1 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x642 );
        }
      }
      probe->pre_evaluator = __prepare_evaluator( pre_evaluator, collector, query->evaluator_memory, timing_budget );
      probe->post_evaluator = __prepare_evaluator( post_evaluator, collector, query->evaluator_memory, timing_budget );
      probe->conditional.evaluator = __prepare_evaluator( conditional->evaluator, collector, query->evaluator_memory, timing_budget );
      if( is_forked_path ) {
        probe->traversing.evaluator = __prepare_evaluator( traversing->evaluator, collector, query->evaluator_memory, timing_budget );
      }
      else {
        probe->traversing.evaluator = __own_evaluator( conditional->evaluator, timing_budget );
      }
    }

    probe->graph = self;
    probe->distance = distance;                 // Path length from query anchor to node being probed
    probe->readonly_graph = readonly_graph;     // Readonly graph flag
    probe->collector_mode = collector_mode;     // none, arc or vertex
    probe->current_tail_RO = anchor_RO;         // query anchor

    // 2. Reference the collector context, if collection is to be performed at this neighborhood level
    probe->common_collector = collector;        // WARNING: The collector instance is owned and managed by the top neighborhood search context!

    // 3.
    // -----------------------------------
    // Vertex Probe(s) (may be RECURSIVE!)
    // -----------------------------------

    //  #.  Forked  Cond  Trav   |  CProbe  TProbe
    //  -------------------------+------------------
    //  1.     0      0     0    |    0       0
    //  2.     0      0     1    |   (impossible)
    //  3.     0      1     0    |   (impossible)
    //  4.     0      1     1    |    1  ->   INH
    //  5.     1      0     0    |    0       0
    //  6.     1      0     1    |    INH <-  1
    //  7.     1      1     0    |    1  ->   INH
    //  8.     1      1     1    |    1       1
    //

    int cond_proceed = 1;
    int trav_proceed = 1;

    // CONDITIONAL vertex probe (make if conditional condition exists) [4 7 8]
    if( conditional->vertex_condition ) {
      if( (cond_proceed = __configure_new_vertex_probe_from_condition( self, readonly_graph, query, conditional->vertex_condition, collector, simcontext_borrowed, distance, &probe->conditional.vertex_probe )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x643 );
      }
    }

    // TRAVERSING vertex probe (only make if forked path and traversing condition exists) [6 8]
    if( is_forked_path && traversing->vertex_condition ) {
      if( (trav_proceed = __configure_new_vertex_probe_from_condition( self, readonly_graph, query, traversing->vertex_condition, collector, simcontext_borrowed, distance, &probe->traversing.vertex_probe )) < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x644 );
      }
    }



    // 4.
    // -------------
    // Arc Filter(s)
    // -------------

    //  #.  Forked  Cond  Trav   |  CFilter TFilter
    //  -------------------------+------------------
    //  1.     0      0     0    |    FILT -> INH
    //  -.     0      0     1    |   (impossible)
    //  -.     0      1     0    |   (impossible)
    //  4.     0      1     1    |    FILT -> INH
    //  5.     1      0     0    |    FILT -> INH
    //  6.     1      0     1    |    INH <-  FILT
    //  7.     1      1     0    |    FILT -> INH
    //  8.     1      1     1    |    FILT    FILT
    //

    int has_cfilter = conditional->arc_condition_set || conditional->evaluator || conditional->vertex_condition;
    int has_tfilter = traversing->arc_condition_set || traversing->evaluator || traversing->vertex_condition;
    probe->conditional.arcdir = conditional->arc_condition_set ? conditional->arc_condition_set->arcdir : VGX_ARCDIR_ANY;
    probe->traversing.arcdir = traversing->arc_condition_set ? traversing->arc_condition_set->arcdir : VGX_ARCDIR_ANY;

    probe->conditional.override.enable = conditional->override.enable;
    probe->conditional.override.match = conditional->override.match;
    probe->traversing.override.enable = traversing->override.enable;
    probe->traversing.override.match = traversing->override.match;

    // CONDITIONAL arc filter [1 4 5 7 8], i.e not 6
    // _____ 
    // _           _
    // C * T = C + T
    //
    if( has_cfilter || !has_tfilter ) {
      if( (probe->conditional.arcfilter = iArcFilter.New( self, readonly_graph, conditional->arc_condition_set, probe->conditional.vertex_probe, probe->conditional.evaluator, timing_budget )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x645 );
      }
      // Set the filter's current tail vertex (will be NULL for all but the first neighborhood, i.e. recursive traversal MUST UPDATE THIS FOR EACH RECURSIVE NEIGHBORHOOD!
      probe->conditional.arcfilter->current_tail = probe->current_tail_RO;
    }

    // TRAVERSING arc filter [6 8]
    if( is_forked_path && has_tfilter ) {
      if( (probe->traversing.arcfilter = iArcFilter.New( self, readonly_graph, traversing->arc_condition_set, probe->traversing.vertex_probe, probe->traversing.evaluator, timing_budget )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x647 );
      }
      // Set the filter's current tail vertex (will be NULL for all but the first neighborhood, i.e. recursive traversal MUST UPDATE THIS FOR EACH RECURSIVE NEIGHBORHOOD!
      probe->traversing.arcfilter->current_tail = probe->current_tail_RO;

      // CONDITIONAL inherits traversing if not given [6]
      if( !has_cfilter ) {
        probe->conditional.arcfilter = probe->traversing.arcfilter;
        probe->conditional.arcdir = probe->traversing.arcdir;
        probe->conditional.override = probe->traversing.override;
      }
    }
    // TRAVERSING inherits conditional in all other cases [1 4 5 7]
    else {
      probe->traversing.arcfilter = probe->conditional.arcfilter;
      probe->traversing.arcdir = probe->conditional.arcdir;
      probe->traversing.override = probe->conditional.override;
    }

    // 5. Create collector filter for this neighborhood level
    // TODO ---------------------------- ADD advanced filter to collector filter ----------------v
    if( (probe->collect_filter_context = iArcFilter.New( self, readonly_graph, collect_arc_condition_set, NULL, NULL, timing_budget )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x647 );
    }
    // If we had empty specification for collect filter, have it follow traversing arcfilter
    if( collect_arc_condition_set == NULL ) {
      probe->collect_filter_context->superfilter = probe->traversing.arcfilter;
    }

    // 8. Finally verify vertex probe(s) are not early termination
    if( cond_proceed == 0 || trav_proceed == 0 ) {
      retcode = 0; // early termination
    }

  }
  XCATCH( errcode ) {
    __propagate_condition_errors( conditional, CSTR__error );
    __propagate_condition_errors( traversing, CSTR__error );

    // If error not set, set default error
    if( CSTR__error && *CSTR__error == NULL ) {
      __set_error_string( CSTR__error, "probe error" );
    }

    __delete_neighborhood_probe( neighborhood_probe );
    retcode = -1;
  }
  XFINALLY {
  }

  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_adjacency_search_context( vgx_adjacency_search_context_t *search ) {
  if( search ) {
    // Reset counters
    search->counts_are_deep = false;
    search->n_neighbors = 0;
    search->n_arcs = 0;
    // Delete the neighborhood probe
    __delete_neighborhood_probe( &search->probe );
    // Clear the base context
    __clear_base_search_context( (vgx_base_search_context_t*)search );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_adjacency_search_context_t * __clone_adjacency_search_context( vgx_Graph_t *self, vgx_adjacency_search_context_t *other ) {
  // TODO
  return NULL;
}



/*******************************************************************//**
 *
 * Returns : 1 = configured ok 
 *           0 = early termination
 *          -1 = error
 ***********************************************************************
 */
static int __configure_adjacency_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AdjacencyQuery_t *query, vgx_adjacency_search_context_t *search ) {
  int retcode = 0;
  XTRY {
    // 1. BASE: Configure the adjacency context's base portion
    if( __configure_base_search_context( self, readonly_graph, (vgx_BaseQuery_t*)query, (vgx_base_search_context_t*)search ) != 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x651 );
    }

    // 2. Set the adjacency anchor
    search->anchor = anchor_RO;
    
    // 3. Reset the arc and neighbor counters
    search->n_arcs = 0;
    search->n_neighbors = 0;
    search->counts_are_deep = false;
 
    // 4. Create and configure a new neighborhood probe
    const vgx_RecursiveCondition_t recursive_condition = {
      .evaluator         = search->vertex_evaluator,
      .vertex_condition  = query->vertex_condition,
      .arc_condition_set = query->arc_condition_set,
      .override          = {
          .enable           = false,
          .match            = VGX_ARC_FILTER_MATCH_MISS
      }
    };
    const vgx_RecursiveCondition_t *conditional = &recursive_condition; 
    const vgx_RecursiveCondition_t *traversing = &recursive_condition;
    if( (retcode = __configure_new_neighborhood_probe( self, readonly_graph, search->anchor, (vgx_BaseQuery_t*)query, search->pre_evaluator, search->post_evaluator, conditional, traversing, VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST, NULL, NULL, search->simcontext, &query->CSTR__error, 1, &search->probe )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x652 );
    }
  }
  XCATCH( errcode ) {
    __clear_adjacency_search_context( search );
    retcode = -1;
  }
  XFINALLY {}
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_adjacency_search_context_t * _vxquery_probe__new_adjacency_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AdjacencyQuery_t *query ) {
  // Set up internal adjacency probe from external query 
  vgx_adjacency_search_context_t *search = NULL;
  
  XTRY {

    // 1. Allocate adjacency search context
    if( (search = (vgx_adjacency_search_context_t*)calloc( 1, sizeof( vgx_adjacency_search_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x661 );
    }

    // 2. Configure the adjacency context
    search->type = VGX_SEARCH_TYPE_ADJACENCY;
    search->debug = query->debug;
    int configured = __configure_adjacency_search_context( self, readonly_graph, anchor_RO, query, search );
    if( configured < 0 ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x662 );
    }
    else if( configured == 0 ) {
      // TODO: Implement early termination. Probe will still run its course but unnecessarily so since it will never produce hits.
    }

    if( __fallback_similarity_anchor_vector( self, query->vertex_condition, search->probe, anchor_RO, &query->CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_MEMORY, 0x664 );
    }

    // Debug
    if( query->debug & VGX_QUERY_DEBUG_SEARCH_PRE ) {
      _vxquery_dump__search_context( self, (vgx_base_search_context_t*)search );
    }

  }
  XCATCH( errcode ) {
    iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
  }
  XFINALLY {}

  return search;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_neighborhood_search_context( vgx_neighborhood_search_context_t *search ) {
  if( search ) {
    // Delete the collector, if any
    iGraphCollector.DeleteCollector( &search->collector );
    search->collector_mode = VGX_COLLECTOR_MODE_NONE_CONTINUE;
    // Delete the result, if any
    if( search->result ) {
      iGraphCollector.DeleteCollector( &search->result );
    }
    search->hits = 0;
    search->offset = 0;
    // Delete the neighborhood probe
    __delete_neighborhood_probe( &search->probe );
    // Reset counts
    search->counts_are_deep = false;
    search->n_neighbors = 0;
    search->n_arcs = 0;
    // Clear the base context
    __clear_base_search_context( (vgx_base_search_context_t*)search );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_neighborhood_search_context_t * __clone_neighborhood_search_context( vgx_Graph_t *self, vgx_neighborhood_search_context_t *other ) {
  // TODO
  return NULL;
}



/*******************************************************************//**
 *
 * Returns : 1 = configured ok 
 *           0 = early termination
 *          -1 = error
 ***********************************************************************
 */
static int __configure_neighborhood_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *vertex_RO, vgx_NeighborhoodQuery_t *query, vgx_neighborhood_search_context_t *search ) {
  int retcode = 1;
  XTRY {
    // 1.
    // -- BASE --
    // Configure the neighborhood context's base portion
    if( __configure_base_search_context( self, readonly_graph, (vgx_BaseQuery_t*)query, (vgx_base_search_context_t*)search ) != 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x671 );
    }

    // 2.
    // -- SPECIALIZATION --
    // Create the collector instance
    // First get the count parameters. NOTE: If requested hits < 0 it will automatically set the hits to the total number of neighbor vertices
    // in the immediate neighborhood (both directions). This is probably not what we want when collecting across multi-neighborhood hops. We have
    // no way of knowing how large the total result set will be until all traversal is complete, but we don't want to run the same query twice
    // just to get the total counts first. TODO: Figure out something sensible here.
    vgx_collect_counts_t counts = {0};
    if( vertex_RO ) {
      counts = iGraphTraverse.GetNeighborhoodCollectableCounts( self, readonly_graph, vertex_RO, VGX_ARCDIR_ANY, query->offset, query->hits );
      if( counts.n_collect < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x672 );
      }
    }

    // 
    if( query->ranking_condition != NULL && query->ranking_condition->aggregate_deephits > 0 ) {
      if( counts.n_collect < query->ranking_condition->aggregate_deephits ) {
        counts.n_collect = query->ranking_condition->aggregate_deephits;
      }
    }

    // SL 20180809
    // TODO: We don't have support in the probe for conditional collection yet.
    // This means anything except the negative wildcard for collection will be
    // interpreted as a positive wildcard, so collect everything that matches
    // the search filter.
    // In order to support conditional collection we have to add support for this
    // in the arcvector module. At the moment we collect only items that pass
    // the search filter (if collection is turned on). We need to expand arcfilter
    // traversal by allowing collection of different arcs in a multiple arc from
    // from the arc that matches the filter. This is not trivial.

    search->collector_mode = query->collector_mode;
    switch( _vgx_collector_mode_type( search->collector_mode ) ) {
    // ARCS
    case VGX_COLLECTOR_MODE_COLLECT_ARCS:
      if( (search->collector = (vgx_BaseCollector_context_t*)iGraphCollector.NewArcCollector( self, search->ranking_context, (vgx_BaseQuery_t*)query, &counts )) == NULL ) {
        __set_error_string( &query->CSTR__error, "failed to create arc collector" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x673 );
      }
      break;
    // VERTICES
    case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
      if( (search->collector = (vgx_BaseCollector_context_t*)iGraphCollector.NewVertexCollector( self, search->ranking_context, (vgx_BaseQuery_t*)query, &counts )) == NULL ) {
        __set_error_string( &query->CSTR__error, "failed to create vertex collector" );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x674 );
      }
      break;
    default:
      __set_error_string( &query->CSTR__error, "invalid collector mode for neighborhood search context" );
      THROW_ERROR( CXLIB_ERR_API, 0x675 );
    }

    // 3.
    // -- ADJACENCY --
    // Set the anchor vertex
    search->anchor = vertex_RO;
    // Reset the arc and neighbor counters
    search->n_arcs = 0;
    search->n_neighbors = 0;
    search->counts_are_deep = false;

    // This is the probe to be configured
    vgx_neighborhood_probe_t **probe = &search->probe;

    // Check the collect condition for negative wildcard (i.e. don't collect anything)
    vgx_collector_mode_t immediate_collector_mode;
    if(    query->collect_arc_condition_set
        && query->collect_arc_condition_set->accept == false
        && (query->collect_arc_condition_set->set == NULL || iArcCondition.IsWild( *query->collect_arc_condition_set->set ))
      )
    {
      immediate_collector_mode = VGX_COLLECTOR_MODE_NONE_CONTINUE;
    } 
    else {
      immediate_collector_mode = search->collector_mode;
    }

    // Create and configure a new neighborhood probe
    const vgx_RecursiveCondition_t recursive_condition = {
      .evaluator         = search->vertex_evaluator,
      .vertex_condition  = query->vertex_condition,
      .arc_condition_set = query->arc_condition_set,
      .override          = {
          .enable           = false,
          .match            = VGX_ARC_FILTER_MATCH_MISS,
      }
    };
    const vgx_RecursiveCondition_t *conditional = &recursive_condition;
    const vgx_RecursiveCondition_t *traversing = &recursive_condition;

    if( (retcode = __configure_new_neighborhood_probe( self, readonly_graph, search->anchor, (vgx_BaseQuery_t*)query,search->pre_evaluator, search->post_evaluator, conditional, traversing, immediate_collector_mode, query->collect_arc_condition_set, search->collector, search->simcontext, &query->CSTR__error, 1, probe ) ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x676 );
    }

    // 4.
    // -- RESULT --
    search->offset = counts.offset;
    search->hits = counts.hits;
    search->result = NULL; // <= TODO: Should we just create the return list here?

  }
  XCATCH( errcode ) {
    __clear_neighborhood_search_context( search );
    retcode = -1;
  }
  XFINALLY {}
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_neighborhood_search_context_t * _vxquery_probe__new_neighborhood_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *vertex_RO, vgx_NeighborhoodQuery_t *query ) {
  // Set up internal neighborhood probe from external query 
  vgx_neighborhood_search_context_t *search = NULL;
  
  XTRY {
    // TEMPORARY ASSERTION THAT ANCHOR IS NOT NULL
    ASSERTION( vertex_RO != NULL, "CODE ERROR: ANCHOR VERTEX MUST NOT BE NULL!" );

    // 1. Allocate neighborhood search context
    if( (search = (vgx_neighborhood_search_context_t*)calloc( 1, sizeof( vgx_neighborhood_search_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x681 );
    }

    // 2. Configure the neighborhood context
    search->type = VGX_SEARCH_TYPE_NEIGHBORHOOD;
    search->debug = query->debug;
    int configured = __configure_neighborhood_search_context( self, readonly_graph, vertex_RO, query, search );
    if( configured < 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x682 );
    }
    else if( configured == 0 ) {
      // TODO: Implement early termination. Probe will still run its course but unnecessarily so since it will never produce hits.
    }

    if( __fallback_similarity_anchor_vector( self, query->vertex_condition, search->probe, vertex_RO, &query->CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_MEMORY, 0x684 );
    }

    // Debug
    if( query->debug & VGX_QUERY_DEBUG_SEARCH_PRE ) {
      _vxquery_dump__search_context( self, (vgx_base_search_context_t*)search );
    }

  }
  XCATCH( errcode ) {
    iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
  }
  XFINALLY {}

  return search;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_aggregator_search_context( vgx_aggregator_search_context_t *search ) {
  if( search ) {
    // Reset field data
    if( search->fields ) {
      memset( &search->fields->data, 0, sizeof( vgx_aggregator_field_data_t ) );
    }
    // Delete the aggregator collector
    iGraphAggregator.DeleteAggregator( &search->collector );
    // Reset counters
    search->counts_are_deep = false;
    search->n_neighbors = 0;
    search->n_arcs = 0;
    // Delete the neighborhood probe
    __delete_neighborhood_probe( &search->probe );
    // Clear the base context
    __clear_base_search_context( (vgx_base_search_context_t*)search );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_aggregator_search_context_t * __clone_aggregator_search_context( vgx_Graph_t *self, vgx_aggregator_search_context_t *other ) {
  // TODO
  return NULL;
}



/*******************************************************************//**
 *
 * Returns : 1 = configured ok 
 *           0 = early termination
 *          -1 = error
 ***********************************************************************
 */
static int __configure_aggregator_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AggregatorQuery_t *query, vgx_aggregator_search_context_t *search ) {
  int retcode = 1;
  vgx_ExecutionTimingBudget_t *timing_budget = &query->timing_budget;
  XTRY {
    // 1. BASE: Configure the aggregator context's base portion
    if( __configure_base_search_context( self, readonly_graph, (vgx_BaseQuery_t*)query, (vgx_base_search_context_t*)search ) != 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x691 );
    }

    // 2. Set the anchor vertex
    search->anchor = anchor_RO;

    // 3. Reset the arc and neighbor counters
    search->n_arcs = 0;
    search->n_neighbors = 0;
    search->counts_are_deep = false;

    // 4. Create the aggregator collector
    search->fields = query->fields;
    if( (search->collector = (vgx_BaseCollector_context_t*)iGraphAggregator.NewNeighborhoodAggregator( self, search->fields, timing_budget )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x692 );
    }

    // 5. Check the collect condition for negative wildcard (i.e. don't collect anything)
    vgx_collector_mode_t immediate_collector_mode;
    if(    query->collect_arc_condition_set
        && query->collect_arc_condition_set->accept == false
        && (query->collect_arc_condition_set->set == NULL || iArcCondition.IsWild( *query->collect_arc_condition_set->set ))
      )
    {
      immediate_collector_mode = VGX_COLLECTOR_MODE_NONE_CONTINUE;
    } 
    else {
      immediate_collector_mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
    }

    // 6. Create and configure a new neighborhood probe
    const vgx_RecursiveCondition_t recursive_condition = {
      .evaluator         = search->vertex_evaluator,
      .vertex_condition  = query->vertex_condition,
      .arc_condition_set = query->arc_condition_set,
      .override          = {
          .enable           = false,
          .match            = VGX_ARC_FILTER_MATCH_MISS
      }
    };
    const vgx_RecursiveCondition_t *conditional = &recursive_condition;
    const vgx_RecursiveCondition_t *traversing = &recursive_condition;

    if( (retcode = __configure_new_neighborhood_probe( self, readonly_graph, search->anchor, (vgx_BaseQuery_t*)query, search->pre_evaluator, search->post_evaluator, conditional, traversing, immediate_collector_mode, query->collect_arc_condition_set, search->collector, search->simcontext, &query->CSTR__error, 1, &search->probe )) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x693 );
    }

  }
  XCATCH( errcode ) {
    __clear_aggregator_search_context( search );
    retcode = -1;
  }
  XFINALLY {}
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_aggregator_search_context_t * _vxquery_probe__new_aggregator_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_Vertex_t *anchor_RO, vgx_AggregatorQuery_t *query ) {
  // Set up internal aggregator probe from external query 
  vgx_aggregator_search_context_t *search = NULL;
  
  XTRY {
    // TEMPORARY ASSERTION THAT ANCHOR IS NOT NULL
    ASSERTION( anchor_RO != NULL, "CODE ERROR: ANCHOR VERTEX MUST NOT BE NULL!" );

    // 1. Allocate a new aggregator search context object
    if( (search = (vgx_aggregator_search_context_t*)calloc( 1, sizeof( vgx_aggregator_search_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x6A1 );
    }

    // 2. Configure the aggregator search context
    search->type = VGX_SEARCH_TYPE_AGGREGATOR;
    search->debug = query->debug;
    int configured = __configure_aggregator_search_context( self, readonly_graph, anchor_RO, query, search );
    if( configured < 0 ) {
      THROW_SILENT( CXLIB_ERR_MEMORY, 0x6A2 );
    }
    else if( configured == 0 ) {
      // TODO: Implement early termination. Probe will still run its course but unnecessarily so since it will never produce hits.
    }

    if( __fallback_similarity_anchor_vector( self, query->vertex_condition, search->probe, anchor_RO, &query->CSTR__error ) < 0 ) {
      THROW_SILENT( CXLIB_ERR_MEMORY, 0x6A3 );
    }

    // Debug
    if( query->debug & VGX_QUERY_DEBUG_SEARCH_PRE ) {
      _vxquery_dump__search_context( self, (vgx_base_search_context_t*)search );
    }


    // FUTURE TODO: Make it so we can aggregate beyond immediate neighborhood. We can filter, but we can't count stuff in the extended
    // neighborhood yet. This should be possible by passing on the fields data to the neighborhood probe just like we do with the
    // collector for the neighborhood search. Just to lazy to do this right now.

  }
  XCATCH( errcode ) {
    iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
  }
  XFINALLY {}

  return search;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __clear_global_search_context( vgx_global_search_context_t *search ) {

  // Delete the collector
  iGraphCollector.DeleteCollector( &search->collector.base );
  
  // Delete the result
  if( search->result ) {
    iGraphCollector.DeleteCollector( &search->result );
  }

  // Delete the vertex probe
  __delete_vertex_probe( &search->probe );

  // Clear the base search context
  __clear_base_search_context( (vgx_base_search_context_t*)search );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static vgx_global_search_context_t * __clone_global_search_context( vgx_Graph_t *self, vgx_global_search_context_t *other ) {
  // TODO
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_global_search_context_t * _vxquery_probe__new_global_search_context( vgx_Graph_t *self, bool readonly_graph, vgx_GlobalQuery_t *query ) {
  // Set up internal global probe from external query 
  vgx_global_search_context_t *search = NULL;
  
  XTRY {
    // -- ALLOCATE --
    // Allocate a new global search context object
    if( (search = (vgx_global_search_context_t*)calloc( 1, sizeof( vgx_global_search_context_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x6B1 );
    }

    // -- BASE --
    // Configure base search context
    search->type = VGX_SEARCH_TYPE_GLOBAL;
    search->debug = query->debug;
    if( __configure_base_search_context( self, readonly_graph, (vgx_BaseQuery_t*)query, (vgx_base_search_context_t*)search ) != 0 ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x6B2 );
    }

    // -- SPECIALIZATION --
    //
    //
    vgx_collect_counts_t counts = iGraphTraverse.GetGlobalCollectableCounts( self, query );
    vgx_collector_mode_t collector_mode = query->collector_mode;
    // Early termination
    if( counts.n_collect == 0 && (search->ranking_context == NULL || search->ranking_context->sortspec == VGX_SORTBY_NONE ) ) {
      collector_mode = VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST;
    }
    // Error
    else if( counts.n_collect < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x6B5 );
    }

    switch( collector_mode ) {
    case VGX_COLLECTOR_MODE_COLLECT_ARCS:
      search->collector.mode = VGX_COLLECTOR_MODE_COLLECT_ARCS;
      if( (search->collector.arc = iGraphCollector.NewArcCollector( self, search->ranking_context, (vgx_BaseQuery_t*)query, &counts )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x6B6 );
      }
      break;
    case VGX_COLLECTOR_MODE_COLLECT_VERTICES:
      search->collector.mode = VGX_COLLECTOR_MODE_COLLECT_VERTICES;
      if( (search->collector.vertex = iGraphCollector.NewVertexCollector( self, search->ranking_context, (vgx_BaseQuery_t*)query, &counts )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x6B7 );
      }
      break;
    case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
      search->collector.mode = VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST;
      if( (search->collector.vertex = iGraphCollector.NewVertexCollector( self, NULL, (vgx_BaseQuery_t*)query, &counts )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x6B8 );
      }
      break;
    default:
      break;
    }

    // -- GLOBAL --
    // Reset counters
    search->n_items = 0;
    search->counts_are_deep = false;

    // Create and configure a new vertex probe from the vertex query condition
    int configured = 0;
    configured = __configure_new_vertex_probe_from_condition( self, readonly_graph, (vgx_BaseQuery_t*)query, query->vertex_condition, search->collector.base, search->simcontext, 0, &search->probe );
    if( configured < 0 ) {
      __transfer_error_string( &query->CSTR__error, &query->vertex_condition->CSTR__error );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x6B9 );
    }
    else if( configured == 0 ) {
      // TODO: Implement early termination. Probe will still run its course but unnecessarily so since it will never produce hits.
    }

    vgx_Evaluator_t *ev;
    if( search->probe ) {
      if( (ev = search->probe->advanced.local_evaluator.filter) != NULL && CALLABLE( ev )->Traversals( ev ) ) {
        __set_error_string( &query->CSTR__error, "traversal not allowed in local filter" );
        THROW_SILENT( CXLIB_ERR_API, 0x6BA );
      }
      if( (ev = search->probe->advanced.local_evaluator.post) != NULL && CALLABLE( ev )->Traversals( ev ) ) {
        __set_error_string( &query->CSTR__error, "traversal not allowed in local (post) filter" );
        THROW_SILENT( CXLIB_ERR_API, 0x6BC );
      }
    }

    // -- RESULT --
    // Initialize the result portion of the context
    search->offset      = counts.offset;
    search->hits        = counts.hits;
    search->result      = NULL;

    // Debug
    if( query->debug & VGX_QUERY_DEBUG_SEARCH_PRE ) {
      _vxquery_dump__search_context( self, (vgx_base_search_context_t*)search );
    }

  }
  XCATCH( errcode ) {
    iGraphProbe.DeleteSearch( (vgx_base_search_context_t**)&search );
  }
  XFINALLY {}

  return search;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxquery_probe__delete_search_context( vgx_base_search_context_t **search ) {
  if( *search ) {
    vgx_base_search_context_t *B = *search;

    if( B->debug & VGX_QUERY_DEBUG_SEARCH_POST ) {
      _vxquery_dump__search_context( NULL, B );
    }

    switch( (*search)->type ) {
    case VGX_SEARCH_TYPE_ADJACENCY:
      __clear_adjacency_search_context( (vgx_adjacency_search_context_t*)B );
      break;
    case VGX_SEARCH_TYPE_NEIGHBORHOOD:
      __clear_neighborhood_search_context( (vgx_neighborhood_search_context_t*)B );
      break;
    case VGX_SEARCH_TYPE_AGGREGATOR:
      __clear_aggregator_search_context( (vgx_aggregator_search_context_t*)B );
      break;
    case VGX_SEARCH_TYPE_GLOBAL:
      __clear_global_search_context( (vgx_global_search_context_t*)B );
      break;
    default:
      break;
    }

    // Free the adjacency search context and set to NULL
    free( *search );
    *search = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_base_search_context_t * _vxquery_probe__clone_search_context( vgx_Graph_t *self, const vgx_base_search_context_t *other ) {
  switch( other->type ) {
  case VGX_SEARCH_TYPE_ADJACENCY:
    return (vgx_base_search_context_t*)__clone_adjacency_search_context( self, (vgx_adjacency_search_context_t*)other );
  case VGX_SEARCH_TYPE_NEIGHBORHOOD:
    return (vgx_base_search_context_t*)__clone_neighborhood_search_context( self, (vgx_neighborhood_search_context_t*)other );
  case VGX_SEARCH_TYPE_AGGREGATOR:
    return (vgx_base_search_context_t*)__clone_aggregator_search_context( self, (vgx_aggregator_search_context_t*)other );
  case VGX_SEARCH_TYPE_GLOBAL:
    return (vgx_base_search_context_t*)__clone_global_search_context( self, (vgx_global_search_context_t*)other );
  default:
    return NULL;
  }
}




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxquery_probe.h"

test_descriptor_t _vgx_vxquery_probe_tests[] = {
  { "VGX Graph Probe Tests", __utest_vxquery_probe },
  {NULL}
};
#endif

