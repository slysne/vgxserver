/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxdefs.c
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

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX );

DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_ANY       = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_WILDCARD   }, VGX_PREDICATOR_MOD_WILDCARD,   0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_ANY_OUT   = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_OUT,  VGX_PREDICATOR_REL_WILDCARD   }, VGX_PREDICATOR_MOD_WILDCARD,   0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_ANY_IN    = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_IN,   VGX_PREDICATOR_REL_WILDCARD   }, VGX_PREDICATOR_MOD_WILDCARD,   0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_NONE      = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_NONE       }, VGX_PREDICATOR_MOD_NONE,       0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_AMBIGUOUS = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_AMBIGUOUS  }, VGX_PREDICATOR_MOD_NONE,       0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_ERROR     = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_ERROR      }, VGX_PREDICATOR_MOD_NONE,       0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_RELATED   = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_RELATED    }, VGX_PREDICATOR_MOD_STATIC,     0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_SELF      = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_SELF       }, VGX_PREDICATOR_MOD_STATIC,     0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_SIMILAR   = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_SIMILAR    }, VGX_PREDICATOR_MOD_SIMILARITY, 0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_INTEGER   = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_RELATED    }, VGX_PREDICATOR_MOD_INTEGER,    0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_UNSIGNED  = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_RELATED    }, VGX_PREDICATOR_MOD_UNSIGNED,   0 };
DLL_EXPORT const vgx_predicator_t VGX_PREDICATOR_FLOAT     = { VGX_PREDICATOR_EPH_TYPE_NONE, { VGX_ARCDIR_ANY,  VGX_PREDICATOR_REL_RELATED    }, VGX_PREDICATOR_MOD_FLOAT,      0 };
DLL_EXPORT const uint64_t VGX_PREDICATOR_NONE_BITS         = 0;



DLL_EXPORT const vgx_predicator_val_t VGX_PREDICATOR_VAL_INIT = { 0 };
DLL_EXPORT const vgx_predicator_rel_t VGX_PREDICATOR_REL_INIT = { .dir=VGX_ARCDIR_ANY, .enc=VGX_PREDICATOR_REL_WILDCARD };
DLL_EXPORT const vgx_predicator_mod_t VGX_PREDICATOR_MOD_INIT = { .probe={ .type=VGX_PREDICATOR_MOD_WILDCARD, .f=0, .NEG=0, .LTE=0, .GTE=0} };

DLL_EXPORT const vgx_predicator_eph_t VGX_PREDICATOR_EPH_NONE = { .type=VGX_PREDICATOR_EPH_TYPE_NONE, .value=0 };
DLL_EXPORT const vgx_predicator_eph_t VGX_PREDICATOR_EPH_DYNDELTA = { .type=VGX_PREDICATOR_EPH_TYPE_DYNDELTA, .value=0 };
DLL_EXPORT const vgx_predicator_eph_t VGX_PREDICATOR_EPH_DISTANCE = { .type=VGX_PREDICATOR_EPH_TYPE_DISTANCE, .value=0 };



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_NULL( void ) {
  vgx_VertexDescriptor_t descriptor = {0};
  return descriptor;
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_REAL( vgx_VertexTypeEnumeration_t vxtype ) {
  vgx_VertexDescriptor_t descriptor = {0};
  descriptor.writer.threadid = GET_CURRENT_THREAD_ID();
  descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
  descriptor.state.context.sus = VERTEX_STATE_CONTEXT_SUS_SUSPENDED;
  descriptor.state.context.man = VERTEX_STATE_CONTEXT_MAN_REAL;
  descriptor.type.enumeration = vxtype;
  descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_ONE;
  return descriptor;
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_VertexDescriptor_t VERTEX_DESCRIPTOR_NEW_VIRTUAL( vgx_VertexTypeEnumeration_t vxtype ) {
  vgx_VertexDescriptor_t descriptor = {0};
  descriptor.writer.threadid = GET_CURRENT_THREAD_ID();
  descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
  descriptor.state.context.sus = VERTEX_STATE_CONTEXT_SUS_SUSPENDED;
  descriptor.state.context.man = VERTEX_STATE_CONTEXT_MAN_VIRTUAL;
  descriptor.type.enumeration = vxtype;
  descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_ONE;
  return descriptor;
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT const vgx_Similarity_config_t DEFAULT_FEATURE_SIMCONFIG = {
  .fingerprint = {
    .nsegm = 0,
    .nsign = 0
  },
  .vector = {
    .euclidean        = 0,
    .max_size         = MAX_FEATURE_VECTOR_SIZE,
    .min_intersect    = 1,
    .min_cosine       = 0.0f,
    .min_jaccard      = 0.0f,
    .cosine_exponent  = 1.0f,
    .jaccard_exponent = 0.0f
  },
  .threshold = {
    .hamming    = 0,
    .similarity = 1.0f
  }
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT const vgx_Similarity_config_t DEFAULT_EUCLIDEAN_SIMCONFIG = {
  .fingerprint = {
    .ann = {
      .nprojections = 0,
      .ksize = 0
    }
  },
  .vector = {
    .euclidean        = 1,
    .max_size         = MAX_EUCLIDEAN_VECTOR_SIZE,
    .min_intersect    = 1,
    .min_cosine       = 0.0f,
    .min_jaccard      = 0.0f,
    .cosine_exponent  = 1.0f,
    .jaccard_exponent = 0.0f
  },
  .threshold = {
    .hamming    = 0,
    .similarity = 1.0f
  }
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_EXPORT const vgx_Similarity_config_t UNSET_SIMCONFIG = {
  .fingerprint = {
    .nsegm = -1,
    .nsign = -1
  },
  .vector = {
    .euclidean        = 0,
    .max_size         = 0,
    .min_intersect    = -1,
    .min_cosine       = -1.0f,
    .min_jaccard      = -1.0f,
    .cosine_exponent  = -1.0f,
    .jaccard_exponent = -1.0f
  },
  .threshold = {
    .hamming    = -1,
    .similarity = -1.0f
  }
};



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_SIMPLE_VALUE {0}
DLL_EXPORT const vgx_simple_value_t DEFAULT_VGX_SIMPLE_VALUE = __DEFAULT_VGX_SIMPLE_VALUE;


/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_VALUE {         \
  .type = VGX_VALUE_TYPE_NULL,        \
  .data = __DEFAULT_VGX_SIMPLE_VALUE  \
}
DLL_EXPORT const vgx_value_t DEFAULT_VGX_VALUE = __DEFAULT_VGX_VALUE;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_VALUE_CONSTRAINT {    \
  .type   = VGX_VALUE_TYPE_NULL,            \
  .minval = __DEFAULT_VGX_SIMPLE_VALUE,     \
  .maxval = __DEFAULT_VGX_SIMPLE_VALUE      \
}
DLL_EXPORT const vgx_value_constraint_t DEFAULT_VGX_VALUE_CONSTRAINT = __DEFAULT_VGX_VALUE_CONSTRAINT;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_VALUE_CONDITION {   \
  .value1 = __DEFAULT_VGX_VALUE,          \
  .value2 = __DEFAULT_VGX_VALUE,          \
  .vcomp  = VGX_VALUE_ANY                 \
}
DLL_EXPORT const vgx_value_condition_t DEFAULT_VGX_VALUE_CONDITION = __DEFAULT_VGX_VALUE_CONDITION;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_DEGREE_CONDITION  {             \
  .arc_condition_set = NULL,                          \
  .value_condition   = __DEFAULT_VGX_VALUE_CONDITION  \
}
DLL_EXPORT const vgx_DegreeCondition_t DEFAULT_VGX_DEGREE_CONDITION = __DEFAULT_VGX_DEGREE_CONDITION;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_SIMILARITY_CONDITION  {           \
  .positive = true,                                     \
  .probevector = NULL,                                  \
  .simval_condition = __DEFAULT_VGX_VALUE_CONDITION,    \
  .hamval_condition = __DEFAULT_VGX_VALUE_CONDITION     \
}
DLL_EXPORT const vgx_SimilarityCondition_t DEFAULT_VGX_SIMILARITY_CONDITION = __DEFAULT_VGX_SIMILARITY_CONDITION;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_TIMESTAMP_CONDITION  {      \
  .positive = true,                               \
  .tmc_valcond = __DEFAULT_VGX_VALUE_CONDITION,   \
  .tmm_valcond = __DEFAULT_VGX_VALUE_CONDITION,   \
  .tmx_valcond = __DEFAULT_VGX_VALUE_CONDITION    \
}
DLL_EXPORT const vgx_TimestampCondition_t DEFAULT_VGX_TIMESTAMP_CONDITION = __DEFAULT_VGX_TIMESTAMP_CONDITION;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VGX_PROPERTY_CONDITION_SET  { \
  .Length     = NULL,             \
  .Get        = NULL,             \
  .Append     = NULL,             \
  .positive   = true,             \
  .__data      = NULL             \
}
DLL_EXPORT const vgx_PropertyConditionSet_t DEFAULT_VGX_PROPERTY_CONDITION_SET = __DEFAULT_VGX_PROPERTY_CONDITION_SET;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_VERTEX_CONDITION  {           \
  .positive           = true,                   \
  .spec               = VERTEX_PROBE_WILDCARD,  \
  .manifestation      = VERTEX_STATE_CONTEXT_MAN_ANY, \
  .CSTR__vertex_type  = NULL,                   \
  .degree             = -1,                     \
  .indegree           = -1,                     \
  .outdegree          = -1,                     \
  .CSTR__idlist       = NULL,                   \
  .advanced           = {                       \
      .local_evaluator        = {               \
          .filter = NULL,                       \
          .post = NULL                          \
      },                                        \
      .degree_condition       = NULL,           \
      .timestamp_condition    = NULL,           \
      .similarity_condition   = NULL,           \
      .property_condition_set = NULL,           \
      .recursive = {                            \
          .conditional = {                      \
            .evaluator          = NULL,         \
            .vertex_condition   = NULL,         \
            .arc_condition_set  = NULL,         \
            .override           = {             \
                .enable             = false,    \
                .match              = VGX_ARC_FILTER_MATCH_MISS \
            }                                   \
          },                                    \
          .traversing = {                       \
            .evaluator          = NULL,         \
            .vertex_condition   = NULL,         \
            .arc_condition_set  = NULL,         \
            .override           = {             \
                .enable             = false,    \
                .match              = VGX_ARC_FILTER_MATCH_MISS \
            }                                   \
          },                                    \
          .collect_condition_set = NULL,        \
          .collector_mode        = VGX_COLLECTOR_MODE_NONE_CONTINUE  \
      }                                         \
  },                                            \
  .CSTR__error       = NULL                     \
}
DLL_EXPORT const vgx_VertexCondition_t DEFAULT_VERTEX_CONDITION = __DEFAULT_VERTEX_CONDITION;



/*******************************************************************//**
 *
 ***********************************************************************
 */
#define __DEFAULT_RANKING_CONDITION  {                  \
  .sortspec                 = VGX_SORTBY_NONE,       \
  .modifier                 = VGX_PREDICATOR_MOD_NONE,  \
  .vector                   = NULL,                     \
  .CSTR__expression         = NULL,                     \
  .aggregate_condition_set  = NULL,                     \
  .aggregate_deephits       = 0,                        \
  .CSTR__error              = NULL                      \
}
DLL_EXPORT const vgx_RankingCondition_t DEFAULT_RANKING_CONDITION = __DEFAULT_RANKING_CONDITION;



/*******************************************************************//**
*
***********************************************************************
*/
#define __DEFAULT_ARC_CONDITION  {                \
  .positive           = true,                     \
  .graph              = NULL,                     \
  .CSTR__relationship = NULL,                     \
  .modifier           = VGX_PREDICATOR_MOD_NONE,  \
  .vcomp              = VGX_VALUE_ANY,            \
  .value1             = VGX_PREDICATOR_VAL_ZERO,  \
  .value2             = VGX_PREDICATOR_VAL_ZERO   \
}
DLL_EXPORT const vgx_ArcCondition_t DEFAULT_ARC_CONDITION = __DEFAULT_ARC_CONDITION;



/*******************************************************************//**
*
***********************************************************************
*/
#define __DEFAULT_ARC_CONDITION_SET  {          \
  .accept       = true,                         \
  .arcdir       = VGX_ARCDIR_ANY,            \
  .logic        = VGX_LOGICAL_NO_LOGIC,         \
  .graph        = NULL,                         \
  .set          = NULL,                         \
  .simple       = {NULL, NULL},                 \
  .elem         = __DEFAULT_ARC_CONDITION,      \
  .CSTR__error  = NULL                          \
}
DLL_EXPORT const vgx_ArcConditionSet_t DEFAULT_ARC_CONDITION_SET = __DEFAULT_ARC_CONDITION_SET;
