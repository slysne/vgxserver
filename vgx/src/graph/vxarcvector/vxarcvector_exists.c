/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxarcvector_exists.c
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

#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct __s_multi_filter_context_t {
  vgx_virtual_ArcFilter_context_t *filter;
  vgx_LockableArc_t *larc;
  int distance;
} __multi_filter_context_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __check_multiple_arc_exists( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t exists = 0;
  __multi_filter_context_t *input = processor->processor.input;
  input->larc->head.predicator.data = APTR_AS_UNSIGNED( fh_cell );
  //
  // ARC HEAD LOCK WILL BE ACQUIRED ONLY WHEN NEEDED AND ONLY ONCE
  //
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  _vgx_arc_set_distance( (vgx_Arc_t*)input->larc, input->distance ); // set distance from anchor
  __begin_arcvector_filter_accept_arc( input->filter, input->larc, &filter_match ) {
    vgx_Arc_t *first_match = (vgx_Arc_t*)processor->processor.output;
    VGX_COPY_ARC( first_match, (vgx_Arc_t*)input->larc );
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    ++exists;
  } __end_arcvector_filter_accept_arc;

  if( __is_arcfilter_error( filter_match ) ) { 
    exists = -1;
  }

  return exists;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_exists__multi_predicator_has_arc( framehash_cell_t *fh_top, vgx_LockableArc_t *larc, int distance, vgx_virtual_ArcFilter_context_t *filter, vgx_Arc_t *first_match ) {
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  filter->current_head = &larc->head;
  __multi_filter_context_t input = {
    .filter   = filter,
    .larc     = larc,
    .distance = distance
  };
  framehash_processing_context_t processor = FRAMEHASH_PROCESSOR_NEW_CONTEXT( fh_top, NULL, __check_multiple_arc_exists );
  FRAMEHASH_PROCESSOR_SET_IO( &processor, &input, first_match );

  int64_t n_traversed;
  IGNORE_WARNING_LOCAL_VARIABLE_NOT_REFERENCED
  __pre_arc_visit( filter, larc, &filter_match ) {
    if( (n_traversed = iFramehash.processing.ProcessNolock( &processor )) == 0 && __again__ > 0 ) {
      // Try synthetic arc
      framehash_cell_t synthetic;
      APTR_SET_UNSIGNED( &synthetic, __set_predicator_synthetic( &larc->head.predicator )->data );
      n_traversed = __check_multiple_arc_exists( &processor, &synthetic );
    }
  } __post_arc_visit( false );
  RESUME_WARNINGS

  if( n_traversed == 0 ) {
    filter_match = VGX_ARC_FILTER_MATCH_MISS;
  }
  else if( n_traversed > 0 ) {
    filter_match = VGX_ARC_FILTER_MATCH_HIT;
  }
  else {
    filter_match = __arcfilter_error();
  }

  return filter_match;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __exists_arc( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t exists = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_set_archead_predicator( context, fh_cell );
  //
  // ASSUME HEAD ACCESS GUARD HAS BEEN SECURED ON THE OUTSIDE
  //
  __arcvector_existence_output_context_t *output = processor->processor.output;
  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;
  _vgx_arc_set_distance( (vgx_Arc_t*)context->larc, context->distance ); // set distance from anchor
  __begin_arcvector_filter_accept_arc( context->traverse_filter, context->larc, &filter_match ) {
    output->arc_match = VGX_ARC_FILTER_MATCH_HIT;
    output->match_arc = (vgx_Arc_t*)context->larc;
    FRAMEHASH_PROCESSOR_SET_COMPLETED( processor );
    ++exists;
  } __end_arcvector_filter_accept_arc;
  if( __is_arcfilter_error( filter_match ) ) {
    exists = -1;
  }
  return exists;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __exists_arc_in_arcarray( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t exists = 0;
  __arcvector_virtual_input_context_t *context = processor->processor.input;
  __arcvector_existence_output_context_t *output = processor->processor.output;
  
  __begin_safe_traversal_context( context, fh_cell ) {
    __begin_traverse {
      // GREEN    Multiple Arc: FRAME
      __traverse_multiple {
        exists = iFramehash.processing.ProcessNolock( &context->multipred_proc_traverse );
        FRAMEHASH_PROCESSOR_INHERIT_COMPLETION( processor, &context->multipred_proc_traverse );
      }
      // BLUE     Simple Arc: PREDICATOR
      __traverse_simple {
        exists = __exists_arc( processor, fh_cell );
      }
    } __traverse_final_synthetic( exists==0, __exists_arc, &exists, processor );
  
    if( output->neighborhood_match == VGX_ARC_FILTER_MATCH_MISS || __is_arcfilter_error( output->arc_match ) ) {
      output->neighborhood_match = output->arc_match;
    }
  } __end_safe_traversal_context;

  return __arcvector_traversal_result( context, exists );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcFilter_match _vxarcvector_exists__has_arc( const vgx_ArcVector_cell_t *V, vgx_recursive_probe_t *recursive, vgx_neighborhood_probe_t *neighborhood_probe, vgx_Arc_t *first_match ) {

  vgx_ArcFilter_match filter_match = VGX_ARC_FILTER_MATCH_MISS;

  vgx_virtual_ArcFilter_context_t *arcfilter_context = recursive->arcfilter;
  const vgx_virtual_ArcFilter_context_t *previous = arcfilter_context->previous_context ? arcfilter_context->previous_context : arcfilter_context;
  vgx_GenericArcFilter_context_t *generic_arcfilter = (vgx_GenericArcFilter_context_t*)arcfilter_context;


  // Readonly ?
  bool readonly = neighborhood_probe->readonly_graph;
  
  // We are looking for the existence of an arc to a specific terminal
  if( arcfilter_context->type != VGX_ARC_FILTER_TYPE_PASS && generic_arcfilter->terminal.logic != VGX_LOGICAL_NO_LOGIC ) {
    framehash_cell_t eph_top;

    vgx_ArcFilterTerminal_t *terminal = &generic_arcfilter->terminal;

    const vgx_Vertex_t *single[2] = { terminal->current, NULL};
    const vgx_Vertex_t **list, **cursor;

    if( terminal->current ) {
      cursor = list = single;
    }
    else {
      cursor = list = terminal->list;
    }

    const vgx_Vertex_t *key_vertex;

    while( cursor && (key_vertex = *cursor++) != NULL ) {

      // Reset each time through the loop, look for evidence of hit below
      filter_match = VGX_ARC_FILTER_MATCH_MISS;

      const vgx_ArcHead_t keyhead = VGX_ARCHEAD_INIT_PREDICATOR_BITS( VGX_PREDICATOR_NONE_BITS, (vgx_Vertex_t*)key_vertex );

      vgx_ArcVector_cell_t arc_cell;
      _vgx_ArcVector_cell_type arc_type = __arcvector_cell_type( _vxarcvector_fhash__get_arc_cell( &neighborhood_probe->graph->arcvector_fhdyn, V, key_vertex, &arc_cell ) );
      // Look up exact terminal and proceed only if it exists
      if( arc_type != VGX_ARCVECTOR_NO_ARCS && arc_type != VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY ) {
        // Terminal exists
        __begin_lockable_arc_context( KEY_LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, keyhead.predicator, keyhead.vertex, arcfilter_context->timing_budget, &filter_match ) {
          vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
          __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &KEY_LARC, &filter_match ) {
            switch( arc_type ) {
            // BLUE: Simple arc found, now apply predicator filter
            case VGX_ARCVECTOR_SIMPLE_ARC:
              KEY_LARC.head.predicator.data = __arcvector_as_predicator_bits( &arc_cell );
              _vgx_arc_set_distance( (vgx_Arc_t*)&KEY_LARC, neighborhood_probe->distance ); // set distance from anchor
              __pre_arc_visit( arcfilter_context, &KEY_LARC, &filter_match ) {
                __begin_arcvector_filter_accept_arc( arcfilter_context, &KEY_LARC, &filter_match ) {
                  VGX_COPY_ARC( first_match, (vgx_Arc_t*)&KEY_LARC );
                } __end_arcvector_filter_accept_arc;
              } __post_arc_visit( filter_match == VGX_ARC_FILTER_MATCH_MISS ); // Only allow synthetic arc (i.e. process again with synthetic predicator) if arc filter was miss on normal predicator
              break;
            // GREEN: Multiple arc found, now scan map of predicators for match against predicator filter
            case VGX_ARCVECTOR_MULTIPLE_ARC:
              __arcvector_set_ephemeral_top( &arc_cell, &eph_top );
              filter_match = _vxarcvector_exists__multi_predicator_has_arc( &eph_top, &KEY_LARC, neighborhood_probe->distance, arcfilter_context, first_match );
              break;
            default:
              // error
              filter_match = __arcfilter_error();
            }
          } __end_arc_evaluator_context;
        } __end_lockable_arc_context;


        switch( terminal->logic ) {
        // Terminate loop on first miss or error
        case VGX_LOGICAL_AND:
          if( filter_match != VGX_ARC_FILTER_MATCH_HIT ) {
            cursor = NULL;
          }
          break;
        // Terminate loop on first hit or error
        case VGX_LOGICAL_OR:
          if( filter_match != VGX_ARC_FILTER_MATCH_MISS ) {
            cursor = NULL;
          }
          break;
        // TODO: support XOR logic ?
        default:
          break;
        }
      }
    }
  }
  // We are looking for the existence of an arc to unspecified terminal
  else {
    framehash_cell_t eph_arcarray = __arcvector_avcell_get_ephemeral_top( V );
    framehash_cell_t eph_multipred;

    __arcvector_existence_output_context_t output = {
      .mode               = neighborhood_probe->collector_mode,
      .arc_match          = VGX_ARC_FILTER_MATCH_MISS,
      .neighborhood_match = VGX_ARC_FILTER_MATCH_MISS
    };

    __begin_lockable_arc_context( LARC, VGX_ARCVECTOR_ARRAY_OF_ARCS, readonly, neighborhood_probe->current_tail_RO, VGX_PREDICATOR_NONE, neighborhood_probe->current_tail_RO, arcfilter_context->timing_budget, &filter_match ) {
      vgx_Vector_t *vector = __simprobe_vector( recursive->vertex_probe );
      __begin_arc_evaluator_context( previous, neighborhood_probe->pre_evaluator, recursive->evaluator, neighborhood_probe->post_evaluator, vector, &LARC, &filter_match ) {
        __arcvector_existence_input_context_t input = {
          .traverse_filter          = arcfilter_context,
          .larc                     = &LARC,
          .distance                 = neighborhood_probe->distance,
          .arcarray_proc            = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_arcarray, NULL, __exists_arc_in_arcarray ),
          .multipred_proc_traverse  = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &eph_multipred, NULL, __exists_arc ),
          .multipred_proc_collect   = {0}
        };


        FRAMEHASH_PROCESSOR_SET_IO( &input.arcarray_proc, &input, &output );
        FRAMEHASH_PROCESSOR_SET_IO( &input.multipred_proc_traverse, &input, &output );

        // Execute
        if( iFramehash.processing.ProcessNolock( &input.arcarray_proc ) < 0 ) {
          filter_match = __arcfilter_error();
        }
        else if( output.match_arc != NULL ) {
          filter_match = VGX_ARC_FILTER_MATCH_HIT;
          VGX_COPY_ARC( first_match, output.match_arc );
        }
      } __end_arc_evaluator_context;
    } __end_lockable_arc_context;
  }

  if( filter_match != VGX_ARC_FILTER_MATCH_HIT ) {
    first_match->head.vertex = NULL;
    if( __is_arcfilter_error( filter_match ) ) {
      first_match->head.predicator = VGX_PREDICATOR_ERROR;
    }
  }

  return __arcfilter_THRU( recursive, filter_match );
}


#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_exists.h"
test_descriptor_t _vgx_vxarcvector_exists_tests[] = {
  { "Arcvector Exists",     __utest_vxarcvector_exists },
  {NULL}
};
#endif
