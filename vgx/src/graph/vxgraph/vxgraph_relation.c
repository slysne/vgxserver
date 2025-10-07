/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_relation.c
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



static int              _vxgraph_relation__parse_modifier_value( const vgx_predicator_modifier_enum mod_enum, const void *value, vgx_predicator_val_t *pval );
static vgx_Relation_t * _vxgraph_relation__new_relation( vgx_Graph_t *graph, const char *initial, const char *terminal, const char *relationship, const vgx_predicator_modifier_enum mod_enum, const void *value );
static void _vxgraph_relation__delete_relation( vgx_Relation_t **relation );
static vgx_Relation_t * _vxgraph_relation__forward_only( vgx_Relation_t *relation );
static vgx_Relation_t * _vxgraph_relation__auto_timestamps( vgx_Relation_t *relation );
static vgx_Relation_t * _vxgraph_relation__set( vgx_Relation_t *relation, const char *initial, const char *terminal, const char *relationship, const vgx_predicator_modifier_enum *modifier, const void *value );
static vgx_Relation_t * _vxgraph_relation__add( vgx_Relation_t *relation, CString_t **CSTR__initial, CString_t **CSTR__terminal, CString_t **CSTR__relationship, const vgx_predicator_modifier_enum *modifier, const void *value );
static vgx_Relation_t * _vxgraph_relation__unset( vgx_Relation_t *relation );
static vgx_Relation_t * _vxgraph_relation__set_initial( vgx_Relation_t *relation, const char *initial );
static vgx_Relation_t * _vxgraph_relation__add_initial( vgx_Relation_t *relation, CString_t **CSTR__initial );
static vgx_Relation_t * _vxgraph_relation__set_terminal( vgx_Relation_t *relation, const char *terminal );
static vgx_Relation_t * _vxgraph_relation__add_terminal( vgx_Relation_t *relation, CString_t **CSTR__terminal );
static vgx_Relation_t * _vxgraph_relation__set_relationship( vgx_Relation_t *relation, const char *relationship );
static vgx_Relation_t * _vxgraph_relation__add_relationship( vgx_Relation_t *relation, CString_t **CSTR__relationship );
static vgx_Relation_t * _vxgraph_relation__set_modifier_and_value( vgx_Relation_t *relation, const vgx_predicator_modifier_enum modifier, const void *value );

static vgx_Arc_t * _vxgraph_relation__set_stored_relationship( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Relationship_t *relationship );
static vgx_Arc_t * _vxgraph_relation__set_stored_arc_CS( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Vertex_t *initial, vgx_Vertex_t *terminal, const vgx_Relation_t *relation );
static vgx_Arc_t * _vxgraph_relation__set_stored_arc_OPEN( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Vertex_t *initial, vgx_Vertex_t *terminal, const vgx_Relation_t *relation );



DLL_VISIBLE vgx_IRelation_t iRelation = {
  .ParseModifierValue     = _vxgraph_relation__parse_modifier_value,
  .New                    = _vxgraph_relation__new_relation,
  .Delete                 = _vxgraph_relation__delete_relation,
  .ForwardOnly            = _vxgraph_relation__forward_only,
  .AutoTimestamps         = _vxgraph_relation__auto_timestamps,
  .Set                    = _vxgraph_relation__set,
  .Add                    = _vxgraph_relation__add,
  .Unset                  = _vxgraph_relation__unset,
  .SetInitial             = _vxgraph_relation__set_initial,
  .AddInitial             = _vxgraph_relation__add_initial,
  .SetTerminal            = _vxgraph_relation__set_terminal,
  .AddTerminal            = _vxgraph_relation__add_terminal,
  .SetRelationship        = _vxgraph_relation__set_relationship,
  .AddRelationship        = _vxgraph_relation__add_relationship,
  .SetModifierAndValue    = _vxgraph_relation__set_modifier_and_value,
  .SetStoredRelationship  = _vxgraph_relation__set_stored_relationship,
  .SetStoredArc_CS        = _vxgraph_relation__set_stored_arc_CS,
  .SetStoredArc_OPEN      = _vxgraph_relation__set_stored_arc_OPEN
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxgraph_relation__parse_modifier_value( const vgx_predicator_modifier_enum mod_enum, const void *value, vgx_predicator_val_t *pval ) {
  DWORD min, max;

  switch( _vgx_predicator_value_range( &min, &max, mod_enum ) ) {
  case VGX_PREDICATOR_VAL_TYPE_NONE:
    pval->bits = VGX_PREDICATOR_VAL_ZERO;
    return 0;
  case VGX_PREDICATOR_VAL_TYPE_UNITY:
    pval->uinteger = 1;
    return 0;
  case VGX_PREDICATOR_VAL_TYPE_INTEGER:
  {
    int32_t imin = *(int32_t*)&min;
    int32_t imax = *(int32_t*)&max;
    int32_t *ival = (int32_t*)value;
    if( ival != NULL ) {
      if( *ival < imin || *ival > imax ) {
        return -1; // integer value out of range
      }
      pval->integer = *ival;
    }
    else {
      pval->integer = imin; // default
    }
    return 0;
  }
  case VGX_PREDICATOR_VAL_TYPE_UNSIGNED:
  {
    uint32_t umin = *(uint32_t*)&min;
    uint32_t umax = *(uint32_t*)&max;
    uint32_t *uval = (uint32_t*)value;
    if( uval != NULL ) {
      if( *uval < umin || *uval > umax ) {
        return -1; // unsigned value out of range
      }
      pval->uinteger = *uval;
    }
    else {
      pval->uinteger = umin; // default
    }
    return 0;
  }
  case VGX_PREDICATOR_VAL_TYPE_REAL:
  {
    float fmin = *(float*)&min;
    float fmax = *(float*)&max;
    float *fval = (float*)value;
    if( fval != NULL ) {
      if( *fval < fmin || *fval > fmax ) {
        return -1; // float value out of range
      }
      pval->real = *fval;
    }
    else {
      pval->real = fmin;
    }
    return 0;
  }
  default:
    return -1; // invalid modifier
  }
}



/*******************************************************************//**
 * Return a new Relation object.
 * NOTE: Caller owns memory!
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__new_relation( vgx_Graph_t *graph, const char *initial, const char *terminal, const char *relationship, const vgx_predicator_modifier_enum mod_enum, const void *value ) {
  vgx_Relation_t *relation = NULL;
  XTRY {
    // Allocate relation
    if( (relation = (vgx_Relation_t*)calloc( 1, sizeof(vgx_Relation_t) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x831 );
    }

    relation->graph = graph;

    // Create the relation string components, if any
    if( (initial      && (relation->initial.CSTR__name      = NewEphemeralCString( graph, initial )) == NULL) ||
        (terminal     && (relation->terminal.CSTR__name     = NewEphemeralCString( graph, terminal )) == NULL) ||
        (relationship && (relation->relationship.CSTR__name = NewEphemeralCString( graph, relationship )) == NULL) )
    {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x832 );
    }

    // Set the relation modifier
    relation->relationship.mod_enum = mod_enum;

    // Parse the modifier value if provided
    if( value && iRelation.ParseModifierValue( relation->relationship.mod_enum, value, &relation->relationship.value ) != 0 ) {
      THROW_ERROR( CXLIB_ERR_API, 0x833 );
    }
  }
  XCATCH( errcode ) {
    _vxgraph_relation__delete_relation( &relation );
  }
  XFINALLY {
  }
  return relation;
}



/*******************************************************************//**
 * Delete
 *
 ***********************************************************************
 */
static void _vxgraph_relation__delete_relation( vgx_Relation_t **relation ) {
  if( relation && *relation ) {
    iRelation.Unset( *relation );
    free( *relation );
    *relation = NULL;
  }
}



/*******************************************************************//**
 * ForwardOnly
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__forward_only( vgx_Relation_t *relation ) {
  relation->relationship.mod_enum = relation->relationship.mod_enum | _VGX_PREDICATOR_MOD_FORWARD_ONLY;
  return relation;
}



/*******************************************************************//**
 * AutoTimestamps
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__auto_timestamps( vgx_Relation_t *relation ) {
  relation->relationship.mod_enum = relation->relationship.mod_enum | _VGX_PREDICATOR_MOD_AUTO_TM;
  return relation;
}



/*******************************************************************//**
 * Set the Relation members from supplied arguments. Relation object
 * must already exist. Supplied arguments are "stolen"! Previous members
 * of Relation are discarded and replaced. If an argument is NULL it will
 * be ignored and the corresponding Relation member not modified.
 * 
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__set( vgx_Relation_t *relation, const char *initial, const char *terminal, const char *relationship, const vgx_predicator_modifier_enum *modifier, const void *value ) {
  if( relation ) {
    if( initial && _vxgraph_relation__set_initial( relation, initial ) == NULL ) {
      return NULL; // error
    }
    if( terminal && _vxgraph_relation__set_terminal( relation, terminal ) == NULL ) {
      return NULL; // error
    }
    if( relationship && _vxgraph_relation__set_relationship( relation, relationship ) == NULL ) {
      return NULL; // error
    }
    if( modifier && _vxgraph_relation__set_modifier_and_value( relation, *modifier, value ) == NULL ) {
      return NULL; // error
    }
  }
  return relation;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__add( vgx_Relation_t *relation, CString_t **CSTR__initial, CString_t **CSTR__terminal, CString_t **CSTR__relationship, const vgx_predicator_modifier_enum *modifier, const void *value ) {
  if( relation ) {
    _vxgraph_relation__add_initial( relation, CSTR__initial );
    _vxgraph_relation__add_terminal( relation, CSTR__terminal );
    _vxgraph_relation__add_relationship( relation, CSTR__relationship );
    if( modifier && _vxgraph_relation__set_modifier_and_value( relation, *modifier, value ) == NULL ) {
      return NULL; // error
    }
  }
  return relation;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__unset( vgx_Relation_t *relation ) {
  if( relation ) {
    iString.Discard( &relation->initial.CSTR__name );
    relation->initial.vertex_WL = NULL;
    iString.Discard( &relation->terminal.CSTR__name );
    relation->terminal.vertex_WL = NULL;
    iString.Discard( &relation->relationship.CSTR__name );
    relation->relationship.rel_enum = VGX_PREDICATOR_REL_NONE;
    relation->relationship.mod_enum = VGX_PREDICATOR_MOD_NONE;
    relation->relationship.value.bits = 0;
  }
  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__set_initial( vgx_Relation_t *relation, const char *initial ) {
  if( initial == NULL ) {
    return NULL;
  }
  if( relation ) {
    iString.Discard( &relation->initial.CSTR__name );
    relation->initial.vertex_WL = NULL;
    if( (relation->initial.CSTR__name = NewEphemeralCString( relation->graph, initial )) == NULL ) {
      return NULL; // error
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 * STEALS the cstring
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__add_initial( vgx_Relation_t *relation, CString_t **CSTR__initial ) {
  if( relation ) {
    iString.Discard( &relation->initial.CSTR__name );
    relation->initial.vertex_WL = NULL;
    if( CSTR__initial && *CSTR__initial ) {
      relation->initial.CSTR__name = *CSTR__initial;
      *CSTR__initial = NULL;
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__set_terminal( vgx_Relation_t *relation, const char *terminal ) {
  if( terminal == NULL ) {
    return NULL;
  }
  if( relation ) {
    iString.Discard( &relation->terminal.CSTR__name );
    relation->terminal.vertex_WL = NULL;
    if( (relation->terminal.CSTR__name = NewEphemeralCString( relation->graph, terminal )) == NULL ) {
      return NULL; // error
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 * STEALS the cstring
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__add_terminal( vgx_Relation_t *relation, CString_t **CSTR__terminal ) {
  if( relation ) {
    iString.Discard( &relation->terminal.CSTR__name );
    relation->terminal.vertex_WL = NULL;
    if( CSTR__terminal && *CSTR__terminal ) {
      relation->terminal.CSTR__name = *CSTR__terminal;
      *CSTR__terminal = NULL;
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__set_relationship( vgx_Relation_t *relation, const char *relationship ) {
  if( relation ) {
    // Clear previously set relationship, if any
    iString.Discard( &relation->relationship.CSTR__name );
    relation->relationship.rel_enum = VGX_PREDICATOR_REL_NONE;
    // Set the relationship
    if( relationship ) {
      if( (relation->relationship.CSTR__name = NewEphemeralCString( relation->graph, relationship )) == NULL ) {
        return NULL; // error
      }
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 * STEALS the cstring
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__add_relationship( vgx_Relation_t *relation, CString_t **CSTR__relationship ) {
  if( relation ) {
    // Clear previously set relationship, if any
    iString.Discard( &relation->relationship.CSTR__name );
    relation->relationship.rel_enum = VGX_PREDICATOR_REL_NONE;
    // Steal the relationship
    if( CSTR__relationship && *CSTR__relationship ) {
      relation->relationship.CSTR__name = *CSTR__relationship;
      *CSTR__relationship = NULL;
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Relation_t * _vxgraph_relation__set_modifier_and_value( vgx_Relation_t *relation, const vgx_predicator_modifier_enum modifier, const void *value ) {
  if( relation ) {
    relation->relationship.mod_enum = modifier;
    if( iRelation.ParseModifierValue( relation->relationship.mod_enum, value, &relation->relationship.value ) != 0 ) {
      return NULL; // error
    }
  }
  return relation;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Arc_t * _vxgraph_relation__set_stored_relationship( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Relationship_t *relationship ) {
  // Relationship enumeration already computed - transfer to arc
  if( relationship->rel_enum != VGX_PREDICATOR_REL_NONE ) {
    arc->head.predicator.rel.enc = relationship->rel_enum;
  }
  // Compute relationship enumeration code
  else if( relationship->CSTR__name ) {
    // Own (and possibly create) another reference to the relationship string
    CString_t *CSTR__mapped_instance = NULL;
    arc->head.predicator.rel.enc = iEnumerator_OPEN.Relationship.Encode( graph, relationship->CSTR__name, &CSTR__mapped_instance, true );
  }
  // Default relationship
  else {
    arc->head.predicator.rel.enc = VGX_PREDICATOR_REL_RELATED;
  }
  // Direction = OUT for stored arc
  arc->head.predicator.rel.dir = VGX_ARCDIR_OUT;
  // Set predicator's modifier
  arc->head.predicator.mod = _vgx_predicator_mod_from_enum( relationship->mod_enum );
  // Forward only arc
  if( relationship->mod_enum & _VGX_PREDICATOR_MOD_FORWARD_ONLY ) {
    arc->head.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_FWDARCONLY;
    arc->head.predicator.mod.bits |= _VGX_PREDICATOR_MOD_FWD;
  }
  // Auto timestamps
  else if( relationship->mod_enum & _VGX_PREDICATOR_MOD_AUTO_TM ) {
    arc->head.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_AUTO_TM;
  }
  // Set default predicator value if zero
  if( (arc->head.predicator.val = relationship->value).bits == 0 ) {
    // Assign defaults
    switch( _vgx_predicator_as_modifier_enum( arc->head.predicator ) ) {
    case VGX_PREDICATOR_MOD_TIME_CREATED:
      /* FALLTHRU */
    case VGX_PREDICATOR_MOD_TIME_MODIFIED:
      arc->head.predicator.val.uinteger = _vgx_graph_seconds( graph ); // now
      break;
    case VGX_PREDICATOR_MOD_TIME_EXPIRES:
      arc->head.predicator.val.uinteger = TIME_EXPIRES_NEVER; // never
      break;
    default:
      break;
    }
  }
  return arc;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Arc_t * _vxgraph_relation__set_stored_arc_CS( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Vertex_t *initial, vgx_Vertex_t *terminal, const vgx_Relation_t *relation ) {
  if( initial && terminal ) {
    arc->tail = initial;
    arc->head.vertex = terminal;
    arc->head.predicator.data = 0;
    if( relation->relationship.CSTR__name ) {
      // Own (and possibly create) another reference to the relationship string
      CString_t *CSTR__mapped_instance = NULL;
      arc->head.predicator.rel.enc = iEnumerator_CS.Relationship.Encode( graph, relation->relationship.CSTR__name, &CSTR__mapped_instance, true );
    }
    else {
      arc->head.predicator.rel.enc = VGX_PREDICATOR_REL_RELATED; // default
    }
    arc->head.predicator.rel.dir = VGX_ARCDIR_OUT;
    arc->head.predicator.mod = _vgx_predicator_mod_from_enum( relation->relationship.mod_enum );
    // Forward only arc
    if( relation->relationship.mod_enum & _VGX_PREDICATOR_MOD_FORWARD_ONLY ) {
      arc->head.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_FWDARCONLY;
      arc->head.predicator.mod.bits |= _VGX_PREDICATOR_MOD_FWD;
    }
    // Auto timestamps
    else if( relation->relationship.mod_enum & _VGX_PREDICATOR_MOD_AUTO_TM ) {
      arc->head.predicator.eph.type = VGX_PREDICATOR_EPH_TYPE_AUTO_TM;
    }

    if( (arc->head.predicator.val = relation->relationship.value).bits == 0 ) {
      // Assign defaults
      switch( _vgx_predicator_as_modifier_enum( arc->head.predicator ) ) {
      case VGX_PREDICATOR_MOD_TIME_CREATED:
        /* FALLTHRU */
      case VGX_PREDICATOR_MOD_TIME_MODIFIED:
        arc->head.predicator.val.uinteger = _vgx_graph_seconds( graph ); // now
        break;
      case VGX_PREDICATOR_MOD_TIME_EXPIRES:
        arc->head.predicator.val.uinteger = TIME_EXPIRES_NEVER; // never
        break;
      default:
        break;
      }
    }
    return arc;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_Arc_t * _vxgraph_relation__set_stored_arc_OPEN( vgx_Graph_t *graph, vgx_Arc_t *arc, vgx_Vertex_t *initial, vgx_Vertex_t *terminal, const vgx_Relation_t *relation ) {
  vgx_Arc_t *stored_arc;
  GRAPH_LOCK( graph ) {
    stored_arc = _vxgraph_relation__set_stored_arc_CS( graph, arc, initial, terminal, relation );
  } GRAPH_RELEASE;
  return stored_arc;
} 




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_relation.h"
  
test_descriptor_t _vgx_vxgraph_relation_tests[] = {
  { "VGX Graph Relation Tests", __utest_vxgraph_relation },
  {NULL}
};
#endif
