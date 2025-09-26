/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_traverse.c
 * Author:  Stian Lysne <...>
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

#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );






static int64_t __no_op( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );

static int64_t __match_arc_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __traverse_arcarray_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );

static int64_t __match_arc_once_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __traverse_arcarray_no_collect_stop_at_first_match( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );

static int64_t __traverse_arc_and_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __traverse_arcarray_collect_all( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );

static int64_t __traverse_arc_collect_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __collect_arc_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __traverse_arcarray_collect_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );

static int64_t __traverse_bidirectional_arc_and_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __traverse_bidirectional_arcarray_collect_all( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
__inline static int64_t __no_op( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#define PROCESS_ARCVECTOR_INPUT_CONTEXT( Context )                                \
  if( (Context)->flags->is_halted || (Context)->larc->acquired.head_lock < 0 ) {  \
    FRAMEHASH_PROCESSOR_SET_COMPLETED( &(Context)->arcarray_proc );               \
    FRAMEHASH_PROCESSOR_SET_COMPLETED( &(Context)->multipred_proc_traverse );     \
    FRAMEHASH_PROCESSOR_SET_COMPLETED( &(Context)->multipred_proc_collect );      \
    if( (Context)->flags->explicit_halt ) {                                       \
      return 0;                                                                   \
    }                                                                             \
    return -1;                                                                    \
  } else



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __match_arc_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t match = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_set_archead_predicator( context, fh_cell );
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  PROCESS_ARCVECTOR_INPUT_CONTEXT( context ) {
    __arcvector_traversal_output_context_t *output = processor->processor.output;
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
    __begin_arcvector_filter_accept_arc( context->traverse_filter, context->larc, &filter_match ) {
      output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
      ++match;
    } __end_arcvector_filter_accept_arc;
    if( __is_arcfilter_error( filter_match ) ) {
      output->arc_match = filter_match;
      match = -1;
    }
  }
  return match;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static void __update_output( __arcvector_traversal_output_context_t *output, int64_t n_traversed ) {
  if( n_traversed == 0 ) {
    return;
  }
  if( n_traversed > 0 ) {
    // Inc neighbor vertex count if any arc was followed
    output->n_vertices++;
    // Neighborhood match yet?
    if( output->neighborhood_match == VGX_ARC_FILTER_MATCH_MISS ) {
      output->neighborhood_match = output->arc_match;
    }
    return;
  }
  output->neighborhood_match = __arcfilter_error();
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_arcarray_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t n_traversed_here = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_traversal_output_context_t *output = processor->processor.output;

  __begin_safe_traversal_context( context, fh_cell ) {
    __begin_traverse {
      // Multiple Arc: SECONDARY FRAMEHASH
      __traverse_multiple {
        n_traversed_here = iFramehash.processing.ProcessNolock( &context->multipred_proc_traverse );
      } 
      // Simple Arc: PREDICATOR
      __traverse_simple {
        n_traversed_here = __match_arc_no_collect( processor, fh_cell );
      }
    } __traverse_final_synthetic( true, __match_arc_no_collect, &n_traversed_here, processor );

    __update_output( output, n_traversed_here );

  } __end_safe_traversal_context;

  return __arcvector_traversal_result( context, n_traversed_here );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_cullheap_no_collect( __arcvector_virtual_input_context_t *context, vgx_ExpressEvalContext_t *evalcontext, int64_t k ) {
  int64_t n_traversed = 0;

  vgx_ArcHeadHeapItem_t *cursor = evalcontext->cullheap;
  vgx_ArcHeadHeapItem_t *end = cursor + k;
  f_vgx_ArcFilter direct_recursion = arcfilterfunc.DirectRecursionArcFilter;
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  for( ; cursor < end; ++cursor ) {
    begin_direct_recursion_context( context, cursor ) {
      // Execute direct recursion
      filter_match = VGX_ARC_FILTER_MATCH_MISS;
      if( direct_recursion( context->traverse_filter, context->larc, &filter_match ) ) {
        ++n_traversed;
      } 
    } end_direct_recursion_context;

    // Check error
    if( __is_arcfilter_error( filter_match ) ) {
      return -1;
    }
  }

  return n_traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_cullheap_collect( __arcvector_virtual_input_context_t *context, __arcvector_traversal_output_context_t *output, vgx_ExpressEvalContext_t *evalcontext, int64_t k ) {
  int64_t n_traversed = 0;

  vgx_ArcHeadHeapItem_t *cursor = evalcontext->cullheap;
  vgx_ArcHeadHeapItem_t *end = cursor + k;
  f_vgx_ArcFilter direct_recursion = arcfilterfunc.DirectRecursionArcFilter;
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  for( ; cursor < end; ++cursor ) {
    begin_direct_recursion_context( context, cursor ) {
      // Execute direct recursion
      filter_match = VGX_ARC_FILTER_MATCH_MISS;
      if( direct_recursion( context->traverse_filter, context->larc, &filter_match ) ) {
        ++n_traversed;
        output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
        if( __arcvector_collect_arc( output->collector, context->larc, cursor->score, NULL ) < 0 ) {
          filter_match = __arcfilter_error();
        }
      } 
    } end_direct_recursion_context;

    // Check error
    if( __is_arcfilter_error( filter_match ) ) {
      return -1;
    }
  }

  return n_traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_cullheap_collect_conditional( __arcvector_virtual_input_context_t *context, __arcvector_traversal_output_context_t *output, vgx_ExpressEvalContext_t *evalcontext, int64_t k ) {
  int64_t n_traversed = 0;

  vgx_ArcHeadHeapItem_t *cursor = evalcontext->cullheap;
  vgx_ArcHeadHeapItem_t *end = cursor + k;
  f_vgx_ArcFilter direct_recursion = arcfilterfunc.DirectRecursionArcFilter;
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  for( ; cursor < end; ++cursor ) {
    begin_direct_recursion_context( context, cursor ) {
      // Execute direct recursion
      filter_match = VGX_ARC_FILTER_MATCH_MISS;
      if( direct_recursion( context->traverse_filter, context->larc, &filter_match ) ) {
        output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
        int collected = 0;
        vgx_virtual_ArcFilter_context_t *collect_filter = output->collect_filter;
        vgx_ArcFilter_match collect_match = VGX_ARC_FILTER_MATCH_MISS;
        __begin_arcvector_filter_collect_arc( collect_filter, context->larc, &collect_match ) {
          collected = __arcvector_collect_arc( output->collector, context->larc, cursor->score, NULL );
        } __end_arcvector_filter_collect_arc;
        if( collected < 0 ) {
          filter_match = __arcfilter_error();
        }
        else {
          ++n_traversed;
        }
      } 
    } end_direct_recursion_context;

    // Check error
    if( __is_arcfilter_error( filter_match ) ) {
      return -1;
    }
  }

  return n_traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __match_arc_once_no_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  if( __match_arc_no_collect( processor, fh_cell ) < 0 ) {
    return -1;
  }
  __arcvector_traversal_output_context_t *output = processor->processor.output;
  if( output->arc_match == VGX_ARC_FILTER_MATCH_HIT ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __traverse_arcarray_no_collect_stop_at_first_match( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  if( __traverse_arcarray_no_collect( processor, fh_cell ) < 0 ) {
    return -1;
  }
  __arcvector_traversal_output_context_t *output = processor->processor.output;
  if( output->neighborhood_match == VGX_ARC_FILTER_MATCH_HIT ) {
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    return 1;
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_arc_and_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t traversed = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  PROCESS_ARCVECTOR_INPUT_CONTEXT( context ) {
    __arcvector_set_archead_predicator( context, fh_cell );
    vgx_virtual_ArcFilter_context_t *traverse_filter = context->traverse_filter;
    __arcvector_traversal_output_context_t *output = processor->processor.output;
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
    __begin_arcvector_filter_accept_arc( traverse_filter, context->larc, &filter_match ) {
      output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
      if( __arcvector_collect_arc( output->collector, context->larc, 0.0, processor ) < 0 ) {
        output->arc_match = __arcfilter_error();
        traversed = -1;
      }
      else {
        traversed = 1;
      }
    } __end_arcvector_filter_accept_arc;
    if( __is_arcfilter_error( filter_match ) ) {
      output->arc_match = filter_match;
      traversed = -1;
    }
  }
  return traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __traverse_arcarray_collect_all( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t n_traversed_here = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_traversal_output_context_t *output = processor->processor.output;

  __begin_safe_traversal_context( context, fh_cell ) {
    __begin_traverse {
      // Multiple Arc: SECONDARY FRAMEHASH
      __traverse_multiple {
        n_traversed_here = iFramehash.processing.ProcessNolock( &context->multipred_proc_traverse );
        // Inherit completion
        FRAMEHASH_PROCESSOR_INHERIT_COMPLETION( processor, &context->multipred_proc_traverse );
      }
      // Simple Arc: PREDICATOR
      __traverse_simple {
        n_traversed_here = __traverse_arc_and_collect( processor, fh_cell );
      }
    } __traverse_final_synthetic( true, __traverse_arc_and_collect, &n_traversed_here, processor );

    __update_output( output, n_traversed_here );

  } __end_safe_traversal_context;

  return __arcvector_traversal_result( context, n_traversed_here );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_arc_collect_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t traversed = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_set_archead_predicator( context, fh_cell );
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  PROCESS_ARCVECTOR_INPUT_CONTEXT( context ) {
    vgx_virtual_ArcFilter_context_t *traverse_filter = context->traverse_filter;
    int collected = 0;
    __arcvector_traversal_output_context_t *output = processor->processor.output;
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
    __begin_arcvector_filter_accept_arc( traverse_filter, context->larc, &filter_match ) {
      output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
      vgx_virtual_ArcFilter_context_t *collect_filter = output->collect_filter;
      vgx_ArcFilter_match collect_match = VGX_ARC_FILTER_MATCH_MISS;
      __begin_arcvector_filter_collect_arc( collect_filter, context->larc, &collect_match ) {
        collected = __arcvector_collect_arc( output->collector, context->larc, 0.0, processor );
      } __end_arcvector_filter_collect_arc;
      if( collected < 0 ) {
        output->arc_match = __arcfilter_error();
        traversed = -1;
      }
      else {
        traversed = 1;
      }
    } __end_arcvector_filter_accept_arc;
    if( __is_arcfilter_error( filter_match ) ) {
      output->arc_match = filter_match;
      traversed = -1;
    }
  }
  return traversed;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __collect_arc_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t collected = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_set_archead_predicator( context, fh_cell );
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  __arcvector_traversal_output_context_t *output = processor->processor.output;
  vgx_ArcFilter_match collect_match = VGX_ARC_FILTER_MATCH_MISS;
  _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
  __begin_arcvector_filter_collect_arc( output->collect_filter, context->larc, &collect_match ) {
    if( __arcvector_collect_arc( output->collector, context->larc, 0.0, processor ) < 0 ) {
      collected = -1;
    }
    else {
      collected = 1;
    }
  } __end_arcvector_filter_collect_arc;

  return collected;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __traverse_match( framehash_processing_context_t *traverse ) {
  int64_t n_traversed_here = 0;
  __arcvector_traversal_output_context_t *traverse_output = traverse->processor.output;
  traverse_output->arc_match = VGX_ARC_FILTER_MATCH_MISS; // reset before traversal
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  n_traversed_here = iFramehash.processing.ProcessNolock( traverse );
  // Detect arc match and set neighborhood match
  if( traverse_output->arc_match == VGX_ARC_FILTER_MATCH_HIT ) {
    traverse_output->neighborhood_match = VGX_ARC_FILTER_MATCH_HIT;
    traverse_output->arc_match = VGX_ARC_FILTER_MATCH_MISS; // reset
  }
  return n_traversed_here;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __traverse_arcarray_collect_conditional( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t n_traversed_here = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_traversal_output_context_t *output = processor->processor.output;

  __begin_safe_traversal_context( context, fh_cell ) {
    __begin_traverse {
      // Multiple Arc: SECONDARY FRAMEHASH
      __traverse_multiple {
        // If arc matches traversal filter, proceed with conditional collection
        if( (n_traversed_here = __traverse_match( &context->multipred_proc_traverse )) > 0 ) {
          // Traverse
          framehash_processing_context_t *collect = &context->multipred_proc_collect;
          iFramehash.processing.ProcessNolock( collect );
          // Inherit completion
          FRAMEHASH_PROCESSOR_INHERIT_COMPLETION( processor, collect );
        }
      }
      // Simple Arc: PREDICATOR
      __traverse_simple {
        n_traversed_here = __traverse_arc_collect_conditional( processor, fh_cell );
      }
    } __traverse_final_synthetic( true, __traverse_arc_collect_conditional, &n_traversed_here, processor );

    __update_output( output, n_traversed_here );

  } __end_safe_traversal_context;

  return __arcvector_traversal_result( context, n_traversed_here );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Evaluator_t * __get_cull_filter( const vgx_vertex_probe_t *vertex_probe ) {
  if( vertex_probe ) {
    vgx_Evaluator_t *filter = vertex_probe->advanced.local_evaluator.filter;
    if( filter && CALLABLE( filter )->HasCull( filter ) ) {
      return filter;
    }
  }
  return NULL;
}



/*******************************************************************//**
 * 
 * 
 * Returns:
 *    1 : filter(s) matched, and arcs may have been collected
 *    0 : filter(s) did not match, nothing collected
 *   -1 : error
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_traverse__traverse_arcarray( const vgx_ArcVector_cell_t *V, vgx_neighborhood_probe_t *neighborhood_probe ) {
  if( _vgx_is_neighborhood_probe_halted( neighborhood_probe ) ) {
    return __arcfilter_error();
  }

  framehash_cell_t eph_arcarray = __arcvector_avcell_get_ephemeral_top( V );
  framehash_cell_t eph_multipred;

  // Defaults
  f_framehash_cell_processor_t arcarray_proc            = __traverse_arcarray_no_collect;
  f_framehash_cell_processor_t multipred_proc_traverse  = __match_arc_no_collect;
  f_framehash_cell_processor_t multipred_proc_collect   = __no_op;

  vgx_recursive_probe_t *recursive;
  vgx_Evaluator_t *evaluator;
  vgx_virtual_ArcFilter_context_t *current;
  const vgx_virtual_ArcFilter_context_t *previous;
  vgx_Evaluator_t *culleval;

  // Assign non default processors depending on collector mode and filters
  switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
  case VGX_COLLECTOR_MODE_NONE_CONTINUE:
    // Prefer conditional probe (same as traversing unless explicitly overridden)
    recursive     = &neighborhood_probe->conditional;
    evaluator     = recursive->evaluator;
    current       = recursive->arcfilter;
    previous      = current->previous_context ? current->previous_context : current;
    culleval      = __arcfilter_get_cull_evaluator( current );
    break;
  case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
    // Prefer conditional probe (same as traversing unless explicitly overridden)
    recursive = &neighborhood_probe->conditional;
    evaluator               = recursive->evaluator;
    current                 = recursive->arcfilter;
    previous                = current->previous_context ? current->previous_context : current;
    culleval                = __arcfilter_get_cull_evaluator( current );
    arcarray_proc           = __traverse_arcarray_no_collect_stop_at_first_match;
    multipred_proc_traverse = __match_arc_once_no_collect;
    multipred_proc_collect  = __no_op;
    break;
  case VGX_COLLECTOR_MODE_COLLECT_ARCS:
    // Use traversing probe
    recursive     = &neighborhood_probe->traversing;
    evaluator     = recursive->evaluator;
    current       = recursive->arcfilter;
    previous      = current->previous_context ? current->previous_context : current;
    culleval      = __arcfilter_get_cull_evaluator( current );
    // Don't collect during arcarray traversal in cull mode. We will collect from the culled results instead.
    if( culleval == NULL ) {
      // TODO: Should we use different processor functions for aggregation?
      // Collection filter equals traversal filter
      if( neighborhood_probe->collect_filter_context->superfilter == current ) {
        arcarray_proc           = __traverse_arcarray_collect_all;
        multipred_proc_traverse = __traverse_arc_and_collect;
        multipred_proc_collect  = __no_op;
      }
      // Use the collection filter
      else if( neighborhood_probe->collect_filter_context->type != VGX_ARC_FILTER_TYPE_STOP ) {
        arcarray_proc           = __traverse_arcarray_collect_conditional;
        multipred_proc_traverse = __match_arc_no_collect;
        multipred_proc_collect  = __collect_arc_conditional;
      }
      else { /* Traversal only, no collect */ }
    }
    break;
  default:
    // Use traversing probe
    recursive     = &neighborhood_probe->traversing;
    evaluator     = recursive->evaluator;
    current       = recursive->arcfilter;
    previous      = current->previous_context ? current->previous_context : current;
    culleval      = __arcfilter_get_cull_evaluator( current );
  }

  __arcvector_traversal_output_context_t output = {
    .mode               = neighborhood_probe->collector_mode,
    .arc_match          = VGX_ARC_FILTER_MATCH_MISS,
    .neighborhood_match = VGX_ARC_FILTER_MATCH_MISS,
    .collect_filter     = neighborhood_probe->collect_filter_context,
    .collector          = (vgx_ArcCollector_context_t*)neighborhood_probe->common_collector,
    .n_vertices         = 0
  };

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;

  __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, VGX_PREDICATOR_NONE, neighborhood_probe->current_tail_RO, current->timing_budget, &output.neighborhood_match ) {
    vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
    __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &output.neighborhood_match ) {
      __arcvector_traversal_input_context_t input = {
        .flags                    = &current->timing_budget->flags,
        .traverse_filter          = current,
        .larc                     = &LARC,
        .distance                 = neighborhood_probe->distance,
        .arcarray_proc            = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_arcarray, NULL, arcarray_proc ),
        .multipred_proc_traverse  = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_multipred, NULL, multipred_proc_traverse ),
        .multipred_proc_collect   = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_multipred, NULL, multipred_proc_collect ),
        .current_tail_RO          = neighborhood_probe->current_tail_RO
      };

      FRAMEHASH_PROCESSOR_SET_IO( &input.arcarray_proc, &input, &output );
      FRAMEHASH_PROCESSOR_SET_IO( &input.multipred_proc_traverse, &input, &output );
      FRAMEHASH_PROCESSOR_SET_IO( &input.multipred_proc_collect, &input, &output );

      // Execution with cull
      if( culleval ) {
        // Reset cull heap array
        CALLABLE( culleval )->ClearMCull( culleval );

        // Culling evaluator can be run unlocked
        if( CALLABLE( culleval )->HeadDeref( culleval ) == 0 &&   // filter does not dereference vertex
            CALLABLE( culleval )->Traversals( culleval ) == 0 )  // filter does not use any next arcs
        {
          ;
        }


        // Execute without collection
        if( iFramehash.processing.ProcessNolock( &input.arcarray_proc ) < 0 ) {
          output.neighborhood_match = __arcfilter_error();
        }

        __arcvector_virtual_input_context_t *context = (__arcvector_virtual_input_context_t*)&input;
        int64_t k = culleval->rpn_program.cull;

        switch( _vgx_collector_mode_type( output.mode ) ) {
        case VGX_COLLECTOR_MODE_COLLECT_ARCS:
          // Collection filter same as traversal filter
          if( neighborhood_probe->collect_filter_context->superfilter == current ) {
            __traverse_cullheap_collect( context, &output, &culleval->context, k );
            break;
          }
          // Collection filter and traversal filter are different
          else if( neighborhood_probe->collect_filter_context->type != VGX_ARC_FILTER_TYPE_STOP ) {
            __traverse_cullheap_collect_conditional( context, &output, &culleval->context, k );
            break;
          }
          /* FALLTHRU */
        case VGX_COLLECTOR_MODE_NONE_CONTINUE:
          /* FALLTHRU */
        case VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST:
          /* FALLTHRU */
        default:
          // Traverse with no collection
          __traverse_cullheap_no_collect( context, &culleval->context, k );
        }


      }
      // Normal execution
      else {
        if( iFramehash.processing.ProcessNolock( &input.arcarray_proc ) < 0 ) {
          output.neighborhood_match = __arcfilter_error();
        }
      }

      if( _vgx_collector_mode_collect( output.mode ) && output.collector ) { 
        output.collector->n_neighbors += output.n_vertices;
      }
    } __end_arc_evaluator_context;
  } __end_lockable_arc_context;

  return __arcfilter_THRU( recursive, output.neighborhood_match );
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __traverse_bidirectional_arc_and_collect( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t match = 0;

  __arcvector_bidirectional_traversal_input_context_t *context = processor->processor.input;
  __arcvector_set_archead_predicator( (__arcvector_virtual_input_context_t*)context, fh_cell );

  // Require bi-directional match
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  PROCESS_ARCVECTOR_INPUT_CONTEXT( context ) {
    vgx_virtual_ArcFilter_context_t *traverse_filter = context->traverse_filter;
    __arcvector_bidirectional_traversal_output_context_t *output = processor->processor.output;
    vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
    _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
    __begin_arcvector_filter_accept_arc( traverse_filter, context->larc, &filter_match ) {
      output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
      vgx_GenericArcFilter_context_t *traversal_filter_reverse = (vgx_GenericArcFilter_context_t*)context->reverse.traverse_filter;
      // Update PREDICATOR CONDITION each time: the opposing V_reverse relationship and modifier should match the V1 arc
      traversal_filter_reverse->pred_condition1 = _vgx_predicator_merge_inherit_key( traversal_filter_reverse->pred_condition1, context->larc->head.predicator );
      traversal_filter_reverse->pred_condition2 = _vgx_predicator_merge_inherit_key( traversal_filter_reverse->pred_condition2, context->larc->head.predicator );
      vgx_neighborhood_probe_t *NP = context->neighborhood_probe;
      vgx_recursive_probe_t *TP = &NP->traversing;
      __arcvector_probe_push_conditional( NP, context->reverse.traverse_filter, TP->vertex_probe, TP->arcdir, TP->evaluator ) {
        vgx_Arc_t hasarc2 = {0};
        if( (output->reverse.match = iarcvector.HasArc( context->reverse.arcvector, TP, context->neighborhood_probe, &hasarc2 )) == VGX_ARC_FILTER_MATCH_HIT ) {
          _vgx_arc_set_distance( &hasarc2, context->distance ); // set distance from anchor
          ++match;
          // Exact predicator data match: Collapse fwd and rec arcs to a single bi-directional arc 
          if( _vgx_predicator_data_match( context->larc->head.predicator, hasarc2.head.predicator ) ) {
            context->larc->head.predicator.rel.dir = VGX_ARCDIR_BOTH;
            if( __arcvector_collect_arc( output->collector, context->larc, 0.0, processor ) < 0 ) {
              output->arc_match = __arcfilter_error();
              match = -1;
            }
          }
          // Fw/rev arcs have different data, collect individually
          else {
            if( __arcvector_collect_arc( output->collector, context->larc, 0.0, processor ) < 0 ) {
              output->arc_match = __arcfilter_error();
              match = -1;
            }
            else if( !FRAMEHASH_PROCESSOR_IS_COMPLETED( processor ) ) {
              context->larc->head.predicator = hasarc2.head.predicator;
              if( __arcvector_collect_arc( output->collector, context->larc, 0.0, processor ) < 0 ) {
                output->arc_match = __arcfilter_error();
                match = -1;
              }
            }
          }
        }
        else {
          if( __is_arcfilter_error( (output->arc_match = output->reverse.match) ) ) {
            match = -1;
          }
        }
      } __arcvector_probe_pop_conditional;
    } __end_arcvector_filter_accept_arc;
    if( __is_arcfilter_error( filter_match ) ) {
      output->arc_match = filter_match;
      match = -1;
    }
  }

  return match;
}




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __traverse_bidirectional_arcarray_collect_all( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t n_traversed_here = 0;
  __arcvector_bidirectional_traversal_input_context_t *context = processor->processor.input;
  __arcvector_virtual_input_context_t *vcontext = (__arcvector_virtual_input_context_t*)context;
  __arcvector_traversal_output_context_t *output = processor->processor.output;
  ((vgx_GenericArcFilter_context_t*)context->reverse.traverse_filter)->terminal.current = (vgx_Vertex_t*)APTR_AS_ANNOTATION( fh_cell );
  ((vgx_GenericArcFilter_context_t*)context->reverse.traverse_filter)->terminal.logic = VGX_LOGICAL_AND;

  __begin_safe_traversal_context( vcontext, fh_cell ) {
    __begin_traverse {
      // Multiple Arc: SECONDARY FRAMEHASH
      __traverse_multiple {
        n_traversed_here = iFramehash.processing.ProcessNolock( &context->multipred_proc_traverse );
        // Inherit completion
        FRAMEHASH_PROCESSOR_INHERIT_COMPLETION( processor, &context->multipred_proc_traverse );
      }
      // Simple Arc: PREDICATOR
      __traverse_simple {
        n_traversed_here = __traverse_bidirectional_arc_and_collect( processor, fh_cell );
      }
    } __traverse_final_synthetic( true, __traverse_bidirectional_arc_and_collect, &n_traversed_here, processor );

    __update_output( output, n_traversed_here );

  } __end_safe_traversal_context;

  return __arcvector_traversal_result( vcontext, n_traversed_here );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_traverse__traverse_arcarray_bidirectional( const vgx_ArcVector_cell_t *V1, const vgx_ArcVector_cell_t *V2, vgx_neighborhood_probe_t *neighborhood_probe ) {

  if( _vgx_is_neighborhood_probe_halted( neighborhood_probe ) ) {
    return __arcfilter_error();
  }

  vgx_recursive_probe_t *recursive = &neighborhood_probe->traversing;
  vgx_virtual_ArcFilter_context_t *current = recursive->arcfilter;
  const vgx_virtual_ArcFilter_context_t *previous = current->previous_context ? current->previous_context : current;

  framehash_cell_t eph_arcarray = __arcvector_avcell_get_ephemeral_top( V1 );
  framehash_cell_t eph_multipred;

  f_framehash_cell_processor_t arcarray_proc; 
  f_framehash_cell_processor_t multipred_proc_traverse;
  f_framehash_cell_processor_t multipred_proc_collect;

  switch( _vgx_collector_mode_type( neighborhood_probe->collector_mode ) ) {
  case VGX_COLLECTOR_MODE_COLLECT_ARCS:
    arcarray_proc           = __traverse_bidirectional_arcarray_collect_all;
    multipred_proc_traverse = __traverse_bidirectional_arc_and_collect;
    multipred_proc_collect  = __no_op;
    break;
  default:
    return __arcfilter_error();
  }

  __arcvector_bidirectional_traversal_output_context_t output = {
    .mode               = neighborhood_probe->collector_mode,
    .arc_match          = VGX_ARC_FILTER_MATCH_MISS,
    .neighborhood_match = VGX_ARC_FILTER_MATCH_MISS,
    .collector          = (vgx_ArcCollector_context_t*)neighborhood_probe->common_collector,
    .n_vertices         = 0,
    .reverse = {
      .match              = VGX_ARC_FILTER_MATCH_MISS
    }
  };

  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;

  __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, VGX_PREDICATOR_NONE, neighborhood_probe->current_tail_RO, current->timing_budget, &output.neighborhood_match ) {
    vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
    __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &output.neighborhood_match ) {
      __arcvector_bidirectional_traversal_input_context_t input = {
        .flags                    = &current->timing_budget->flags,
        .traverse_filter          = current,
        .larc                     = &LARC,
        .distance                 = neighborhood_probe->distance,
        .arcarray_proc            = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_arcarray, NULL, arcarray_proc ),
        .multipred_proc_traverse  = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_multipred, NULL, multipred_proc_traverse ),
        .multipred_proc_collect   = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_multipred, NULL, multipred_proc_collect ),
        .current_tail_RO          = neighborhood_probe->current_tail_RO,
        .neighborhood_probe       = neighborhood_probe,
        .reverse = {
          .arcvector          = V2,
          .traverse_filter    = iArcFilter.Clone( current )
        },
      };

      if( input.reverse.traverse_filter == NULL ) {
        output.neighborhood_match = __arcfilter_error();
      }
      else {
        FRAMEHASH_PROCESSOR_SET_IO( &input.arcarray_proc, &input, &output );
        FRAMEHASH_PROCESSOR_SET_IO( &input.multipred_proc_traverse, &input, &output );
        FRAMEHASH_PROCESSOR_SET_IO( &input.multipred_proc_collect, &input, &output );

        // Execute
        int64_t n_proc = iFramehash.processing.ProcessNolock( &input.arcarray_proc );
        
        iArcFilter.Delete( &input.reverse.traverse_filter );

        if( n_proc < 0 ) {
          output.neighborhood_match = __arcfilter_error();
        }
      }

      if( _vgx_collector_mode_collect( output.mode ) && output.collector ) { 
        output.collector->n_neighbors += output.n_vertices;
      }

    } __end_arc_evaluator_context;
  } __end_lockable_arc_context;

  return __arcfilter_THRU( recursive, output.neighborhood_match );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_traverse.h"


test_descriptor_t _vgx_vxarcvector_traverse_tests[] = {
  { "Arcvector Traverse",     __utest_vxarcvector_traverse },
  {NULL}
};
#endif
