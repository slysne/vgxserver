/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __vxtest_macro.h
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

#ifndef __VXTEST_MACRO_H  
#define __VXTEST_MACRO_H

#include "_vxarcvector.h"



static bool __INITIALIZE_GRAPH_FACTORY( const char *basedir, bool euclidean ) {
  if( igraphfactory.IsInitialized() ) {
    return false;
  }

  vgx_context_t VGX_CONTEXT = {0};
  strncpy( VGX_CONTEXT.sysroot, basedir, 254 );
  vector_type_t vmode = euclidean ? __VECTOR__MASK_EUCLIDEAN : __VECTOR__MASK_FEATURE; 
  igraphfactory.Initialize( &VGX_CONTEXT, vmode, false, false, NULL );
  return true;
}


static void __DESTROY_GRAPH_FACTORY( bool do_it ) {
  if( do_it ) {
    igraphfactory.Shutdown();
  }
}



static const char * __EXPECT_ARC( const char *message, _vgx_ArcVector_cell_type expected_cell_type, vgx_ArcVector_cell_t *arcvector, int64_t expected_degree ) {
  static char error[1024] = {'\0'};
  _vgx_ArcVector_cell_type cell_type = iarcvector.CellType( arcvector );
  if( cell_type != expected_cell_type ) {
    snprintf( error, 1023, "%s: ArcVector type should be %d, got %d", message, expected_cell_type, cell_type );
    return error;
  }
  if( expected_degree >= 0 ) {
    int64_t degree = iarcvector.Degree( arcvector );
    if( degree != expected_degree ) {
      snprintf( error, 1023, "%s: ArcVector degree should be %lld, got %lld", message, expected_degree, degree );
      return error;
    }
  }
  return NULL; // ok!
}


static const char * __EXPECT_PREDICATOR( const char *message, vgx_predicator_t expected_predicator, vgx_ArcVector_cell_t *arc_cell ) {
  static char error[1024] = {'\0'};
  vgx_predicator_t pred = { .data = __arcvector_as_predicator_bits( arc_cell ) };
  if( pred.rel.enc != expected_predicator.rel.enc ) {
    snprintf( error, 1023, "%s: Arc Cell predicator.rel.enc should be %d, got %d", message, expected_predicator.rel.enc, pred.rel.enc );
    return error;
  }
  if( pred.rel.dir != expected_predicator.rel.dir ) {
    snprintf( error, 1023, "%s: Arc Cell predicator.rel.dir should be %d, got %d", message, expected_predicator.rel.dir, pred.rel.dir );
    return error;
  }
  if( pred.mod.bits != expected_predicator.mod.bits ) {
    snprintf( error, 1023, "%s: Arc Cell predicator.mod should be 0x%02x, got 0x%02x", message, expected_predicator.mod.bits, pred.mod.bits );
    return error;
  }
  if( pred.val.bits != expected_predicator.val.bits ) {
    snprintf( error, 1023, "%s: Arc Cell predicator.val should be %lu, got %lu", message, expected_predicator.val.bits, pred.val.bits );
    return error;
  }
  return NULL; // ok!
}


static const char * __EXPECT_RELATIONSHIP( const char *message, vgx_predicator_rel_enum expected_relationship, vgx_ArcVector_cell_t *arc_cell ) {
  static char error[1024] = {'\0'};
  vgx_predicator_t pred = { .data = __arcvector_as_predicator_bits( arc_cell ) };
  if( pred.rel.enc != expected_relationship ) {
    int expected = expected_relationship;
    int actual = pred.rel.enc;
    snprintf( error, 1023, "%s: Arc Cell relationship should be 0x%04x, got 0x%04x", message, expected, actual );
    return error;
  }
  return NULL; // ok!
}


static const char * __EXPECT_MODIFIER( const char *message, vgx_predicator_mod_t expected_modifier, vgx_ArcVector_cell_t *arc_cell ) {
  static char error[1024] = {'\0'};
  vgx_predicator_t pred = { .data = __arcvector_as_predicator_bits( arc_cell ) };
  if( pred.mod.bits != expected_modifier.bits ) {
    int expected = expected_modifier.bits;
    int actual = pred.mod.bits;
    snprintf( error, 1023, "%s: Arc Cell modifier should be 0x%02x, got 0x%02x", message, expected, actual );
    return error;
  }
  return NULL; // ok!
}


static const char * __EXPECT_VALUE( const char *message, vgx_predicator_val_t expected_value, vgx_ArcVector_cell_t *arc_cell ) {
  static char error[1024] = {'\0'};
  vgx_predicator_t pred = { .data = __arcvector_as_predicator_bits( arc_cell ) };
  if( pred.val.bits != expected_value.bits ) {
    snprintf( error, 1023, "%s: Arc Cell value should be %lu(%f), got %lu(%f)", message, expected_value.bits, expected_value.real, pred.val.bits, pred.val.real );
    return error;
  }
  return NULL; // ok!
}


static const char * __EXPECT_VERTEX( const char *message, vgx_Vertex_t *expected_vertex, vgx_ArcVector_cell_t *simple_arc_cell ) {
  static char error[1024] = {'\0'};
  vgx_Vertex_t *cell_vertex = __arcvector_get_vertex( simple_arc_cell );
  if( cell_vertex != expected_vertex ) {
    const char *expected_vertex_id = CALLABLE(expected_vertex)->IDString(expected_vertex);
    const char *cell_vertex_id = CALLABLE(cell_vertex)->IDString(cell_vertex);
    snprintf( error, 1023, "%s: Simple Arc Cell vertex should be %s @ %p, got %s @ %p", message, expected_vertex_id, expected_vertex, cell_vertex_id, cell_vertex );
    return error;
  }
  return NULL; // ok!
}



static const char * __EXPECT_NO_ARCS( const char *message, vgx_ArcVector_cell_t *arcvector ) {
  const char *submsg;
  if( (submsg = __EXPECT_ARC( message, VGX_ARCVECTOR_NO_ARCS, arcvector, 0 )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static const char * __EXPECT_SIMPLE_ARC( const char *message, vgx_ArcVector_cell_t *arcvector, vgx_Vertex_t *terminal, vgx_predicator_rel_enum expected_rel, vgx_predicator_mod_t expected_mod, vgx_predicator_val_t expected_val ) {
  const char *submsg;
  if( (submsg = __EXPECT_ARC( message, VGX_ARCVECTOR_SIMPLE_ARC, arcvector, 1 )) != NULL ) {
    return submsg;
  }
  if( (submsg = __EXPECT_VERTEX( message, terminal, arcvector )) != NULL ) {
    return submsg;
  }
  if( (submsg = __EXPECT_RELATIONSHIP( message, expected_rel, arcvector )) != NULL ) {
    return submsg;
  }
  if( (submsg = __EXPECT_MODIFIER( message, expected_mod, arcvector )) != NULL ) {
    return submsg;
  }
  if( (submsg = __EXPECT_VALUE( message, expected_val, arcvector )) != NULL ) {
    return submsg;
  }

  return NULL; // ok!
}


static const char * __EXPECT_ARRAY_OF_ARCS( const char *message, vgx_ArcVector_cell_t *arcvector, int64_t expected_degree ) {
  const char *submsg;
  if( (submsg = __EXPECT_ARC( message, VGX_ARCVECTOR_ARRAY_OF_ARCS, arcvector, expected_degree )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static const char * __EXPECT_MULTIPLE_ARC( const char *message, vgx_ArcVector_cell_t *arcvector, int64_t expected_degree ) {
  const char *submsg;
  if( (submsg = __EXPECT_ARC( message, VGX_ARCVECTOR_MULTIPLE_ARC, arcvector, expected_degree )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static const char * __EXPECT_NO_ARC_IN_ARRAY( const char *message, framehash_dynamic_t *dyn, vgx_ArcVector_cell_t *arcvector, vgx_Vertex_t *key_vertex ) {
  static char error[1024];
  const char *submsg;
  vgx_ArcVector_cell_t cell;
  if( (submsg = __EXPECT_ARRAY_OF_ARCS( message, arcvector, -1 )) != NULL ) {
    return submsg;
  }
  iarcvector.GetArcCell( dyn, arcvector, key_vertex, &cell );
  if( (submsg = __EXPECT_NO_ARCS( message, &cell )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static const char * __EXPECT_SIMPLE_ARC_IN_ARRAY( const char *message, framehash_dynamic_t *dyn, vgx_ArcVector_cell_t *arcvector, vgx_Vertex_t *key_vertex, vgx_predicator_rel_enum expected_rel, vgx_predicator_mod_t expected_mod, vgx_predicator_val_t expected_val ) {
  static char error[1024];
  const char *submsg;
  vgx_ArcVector_cell_t cell;
  if( (submsg = __EXPECT_ARRAY_OF_ARCS( message, arcvector, -1 )) != NULL ) {
    return submsg;
  }
  iarcvector.GetArcCell( dyn, arcvector, key_vertex, &cell );
  if( (submsg = __EXPECT_SIMPLE_ARC( message, &cell, key_vertex, expected_rel, expected_mod, expected_val )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static const char * __EXPECT_MULTIPLE_ARC_IN_ARRAY( const char *message, framehash_dynamic_t *dyn, vgx_ArcVector_cell_t *arcvector, vgx_Vertex_t *key_vertex, int64_t expected_degree ) {
  static char error[1024];
  const char *submsg;
  vgx_ArcVector_cell_t cell;
  if( (submsg = __EXPECT_ARRAY_OF_ARCS( message, arcvector, -1 )) != NULL ) {
    return submsg;
  }
  iarcvector.GetArcCell( dyn, arcvector, key_vertex, &cell );
  if( (submsg = __EXPECT_MULTIPLE_ARC( message, &cell, expected_degree )) != NULL ) {
    return submsg;
  }
  return NULL; // ok!
}


static vgx_Arc_t * SET_STATIC_ARC( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_rel_enum enc, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val.integer = 0;
  arc->head.predicator.rel.enc = enc;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod.bits = VGX_PREDICATOR_MOD_STATIC;
  return arc;
}


static vgx_Arc_t * SET_ARC_STATIC_QUERY( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_rel_enum enc, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val.integer = 0;
  arc->head.predicator.rel.enc = enc;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod.bits = VGX_PREDICATOR_MOD_STATIC;
  return arc;
}


static vgx_Arc_t * SET_ARC( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_rel_enum enc, vgx_predicator_mod_t mod, vgx_predicator_val_t val, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val = val;
  arc->head.predicator.rel.enc = enc;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod = mod;
  return arc;
}


static vgx_Arc_t * SET_ARC_REL_QUERY( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_rel_enum enc, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val.integer = 0;
  arc->head.predicator.rel.enc = enc;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod.bits = VGX_PREDICATOR_MOD_WILDCARD;
  return arc;
}


static vgx_Arc_t * SET_ARC_MOD_QUERY( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_mod_t mod, vgx_predicator_val_t val, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val = val;
  arc->head.predicator.rel.enc = VGX_PREDICATOR_REL_WILDCARD;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod = mod;
  return arc;
}


static vgx_Arc_t * SET_ARC_QUERY( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_predicator_rel_enum enc, vgx_predicator_mod_t mod, vgx_predicator_val_t val, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val = val;
  arc->head.predicator.rel.enc = enc;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod = mod;
  return arc;
}


static vgx_Arc_t * SET_ARC_WILD_QUERY( vgx_Arc_t *arc, vgx_Vertex_t *tail, vgx_Vertex_t *head, vgx_arc_direction direction ) {
  arc->tail = tail;
  arc->head.vertex = head;
  arc->head.predicator.val.integer = 0;
  arc->head.predicator.rel.enc = VGX_PREDICATOR_REL_WILDCARD;
  arc->head.predicator.rel.dir = direction;
  arc->head.predicator.mod.bits = VGX_PREDICATOR_MOD_WILDCARD;
  return arc;
}


#define EXPECT_PREDICATOR( TestMessage, ExpectedPredicator, ArcCellPtr )                      \
do {                                                                                          \
  const char *error = __EXPECT_PREDICATOR( TestMessage, ExpectedPredicator, ArcCellPtr );     \
  TEST_ASSERTION( error == NULL, error );                                                     \
} WHILE_ZERO


#define EXPECT_RELATIONSHIP( TestMessage, ExpectedRelationship, ArcCellPtr )                  \
do {                                                                                          \
  const char *error = __EXPECT_RELATIONSHIP( TestMessage, ExpectedRelationship, ArcCellPtr ); \
  TEST_ASSERTION( error == NULL, error );                                                     \
} WHILE_ZERO


#define EXPECT_MODIFIER( TestMessage, ExpectedModifier, ArcCellPtr )                          \
do {                                                                                          \
  const char *error = __EXPECT_MODIFIER( TestMessage, ExpectedModifier, ArcCellPtr );         \
  TEST_ASSERTION( error == NULL, error );                                                     \
} WHILE_ZERO


#define EXPECT_VALUE( TestMessage, ExpectedValue, ArcCellPtr )                                \
do {                                                                                          \
  const char *error = __EXPECT_VALUE( TestMessage, ExpectedValue, ArcCellPtr );               \
  TEST_ASSERTION( error == NULL, error );                                                     \
} WHILE_ZERO


#define EXPECT_ARC( TestMessage, ExpectedArcVectorCellType, ArcVectorPtr, ExpectedDegree )  \
do {                                                                                        \
  const char *error = __EXPECT_ARC( TestMessage, ExpectedArcVectorCellType, ArcVectorPtr, ExpectedDegree ); \
  TEST_ASSERTION( error == NULL, error );                                                   \
} WHILE_ZERO


#define EXPECT_NO_ARCS( TestMessage, ArcVectorPtr )                  \
do {                                                                 \
  const char *error = __EXPECT_NO_ARCS( TestMessage, ArcVectorPtr ); \
  TEST_ASSERTION( error == NULL, error );                            \
} WHILE_ZERO


#define EXPECT_SIMPLE_ARC( TestMessage, ArcVectorPtr, TerminalVertex, ExpectedRelationship, ExpectedModifier, ExpectedValue )  \
do {                                                                                          \
  const char *error = __EXPECT_SIMPLE_ARC( TestMessage, ArcVectorPtr, TerminalVertex, ExpectedRelationship, ExpectedModifier, ExpectedValue ); \
  TEST_ASSERTION( error == NULL, error );                                                     \
} WHILE_ZERO


#define EXPECT_ARRAY_OF_ARCS( TestMessage, ArcVectorPtr, ExpectedDegree )                   \
do {                                                                                        \
  const char *error = __EXPECT_ARRAY_OF_ARCS( TestMessage, ArcVectorPtr, ExpectedDegree );  \
  TEST_ASSERTION( error == NULL, error );                                                   \
} WHILE_ZERO


#define EXPECT_NO_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex )                  \
do {                                                                                                      \
  const char *error = __EXPECT_NO_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex ); \
  TEST_ASSERTION( error == NULL, error );                                                                 \
} WHILE_ZERO


#define EXPECT_SIMPLE_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex, ExpectedRelationship, ExpectedModifier, ExpectedValue )   \
do {                                                                                                                                                  \
  const char *error = __EXPECT_SIMPLE_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex, ExpectedRelationship, ExpectedModifier, ExpectedValue ); \
  TEST_ASSERTION( error == NULL, error );                                                                                           \
} WHILE_ZERO


#define EXPECT_MULTIPLE_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex, ExpectedDegree )                    \
do {                                                                                                                              \
  const char *error = __EXPECT_MULTIPLE_ARC_IN_ARRAY( TestMessage, FramehashDynamic, ArcVectorPtr, KeyVertex, ExpectedDegree );   \
  TEST_ASSERTION( error == NULL, error );                                                                                         \
} WHILE_ZERO






#endif
