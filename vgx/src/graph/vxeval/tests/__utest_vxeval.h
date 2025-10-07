/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    __utest_vxeval.h
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

#ifndef __UTEST_VXEVAL_H
#define __UTEST_VXEVAL_H

#include "__vxtest_macro.h"

static vgx_LockableArc_t ARC_ROOT_to_A = {0};
static vgx_LockableArc_t ARC_ROOT_to_B = {0};
static vgx_LockableArc_t ARC_ROOT_to_C = {0};
static vgx_LockableArc_t ARC_ROOT_rel_D = {0};

static vgx_LockableArc_t *SELECTED_TEST_ARC = NULL;
static CString_t *CSTR__error = NULL;


#define NEW_STRING( Chars )         NewEphemeralCString( graph, Chars )
#define DELETE_STRING( CSTRING )    iString.Discard( ((CString_t**)&(CSTRING)) )


#define INNER( E, line ) "/* line: " #line "*/" E
#define OUTER( E, line ) INNER( E, line )
#define XEXPRESSION( E ) OUTER( E, __LINE__ )


#define ASSERT( Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=true }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define UNTRUE( Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=false }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define ASSERT_CONTINUE( Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=true }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = false \
}

#define REFUTE_CONTINUE( Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=false }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = false \
}

#define nolineINTEGER( ExpectedResult, Expression ) { \
  .expression = (Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=(ExpectedResult) }, \
  .arc = SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define INTEGER( ExpectedResult, Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=(ExpectedResult) }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define REAL( ExpectedResult, Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_REAL, .real=(ExpectedResult) }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define STRING( ExpectedResult, Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_CSTRING, .CSTR__str=NEW_STRING(ExpectedResult) }, \
  .arc = SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define SET( ExpectedSize, Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_SET, .integer=(ExpectedSize) }, \
  .arc = SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define NONE( Expression ) { \
  .expression = XEXPRESSION(Expression), \
  .result = { .type=STACK_ITEM_TYPE_NONE, .bits=0 }, \
  .arc=SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

// Supply the raw expression only, stringify it for the evaluator to parse, assign raw expression to expected result for comparison
#define XINTEGER( Expression ) { \
  .expression = ( #Expression ), \
  .result = { .type=STACK_ITEM_TYPE_INTEGER, .integer=(Expression) }, \
  .arc = SELECTED_TEST_ARC, \
  .new_evaluator = true \
}

#define XREAL( Expression ) { \
  .expression = ( #Expression ), \
  .result = { .type=STACK_ITEM_TYPE_REAL, .real=((double)Expression) }, \
  .arc = SELECTED_TEST_ARC, \
  .new_evaluator = true \
}






typedef struct __s_test {
  const char *expression;
  vgx_EvalStackItem_t result;
  vgx_LockableArc_t *arc;
  bool new_evaluator;
} __test;




/**************************************************************************//**
 * __test_constructor
 *
 ******************************************************************************
 */
int __test_constructor( vgx_Graph_t *graph, vgx_Vector_t *vector, __test *test, int opcount, int runtime_depth, int n_strings, int this_accesses, int head_accesses, int traversals, int lookbacks, int identifiers, double rankscore, vgx_ExpressEvalMemory_t *evalmem, CString_t **CSTR__err ) {
  int ret = 0;

  int assigned = CharsContainsConst( test->expression, ":=" );

#define ASSERT_OR_THROW( Condition, FailureMessage )   if( !(Condition) ) { THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x001, FailureMessage ); }

  XTRY {
    ASSERT_OR_THROW( g_dummy != NULL,                            "Dummy vertex exists" );

    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    vgx_Evaluator_t *evaluator = NULL;
    double now = _vgx_graph_milliseconds( graph ) / 1000.0;
    evaluator = iEvaluator.NewEvaluator( graph, test->expression, vector, CSTR__err );

    ASSERT_OR_THROW( evaluator != NULL,                          "Evaluator instance" );
    int64_t refc = ATOMIC_READ_i64( &evaluator->_refc_atomic );
    ASSERT_OR_THROW( refc == 1LL + assigned,                     "Ownership refcount" );
    ASSERT_OR_THROW( evaluator->graph == graph,                  "Graph assigned" );
    int64_t opid;
    GRAPH_LOCK( graph ) {
      opid = iOperation.GetId_LCK( &graph->operation );
    } GRAPH_RELEASE;
    ASSERT_OR_THROW( evaluator->current.op == opid,              "Graph operation count" );
    int64_t order = GraphOrder( graph );
    int64_t size = GraphSize( graph );
    ASSERT_OR_THROW( evaluator->current.order == order,          "Graph order" );
    ASSERT_OR_THROW( evaluator->current.size == size,            "Graph size" );
    ASSERT_OR_THROW( evaluator->current.tnow == now,             "Graph time" );
    if( vector ) {
      ASSERT_OR_THROW( evaluator->current.vector == vector,      "Vector" );
    }
    else {
      ASSERT_OR_THROW( evaluator->current.vector && evaluator->current.vector->metas.flags.nul == 1,   "NULL Vector" );
    }

    vgx_ExpressEvalProgram_t *program = &evaluator->rpn_program;
    ASSERT_OR_THROW( program->operations != NULL,                "Program operations" );
    ASSERT_OR_THROW( program->parser._sz >= program->length + program->n_passthru,     "Allocated size" );
    ASSERT_OR_THROW( program->length + program->n_passthru == opcount,                 "Number of operations + 1 dummy" );
    if( n_strings == 0 ) {
      ASSERT_OR_THROW( program->strings == NULL,                 "No strings" );
    }
    else {
      ASSERT_OR_THROW( program->strings != NULL,                 "Strings" );
      vgx_ExpressEvalString_t *node = program->strings;
      for( int n=0; n<n_strings; n++ ) {
        ASSERT_OR_THROW( node != NULL,                           "String node exists" );
        ASSERT_OR_THROW( node->CSTR__literal != NULL,            "Node contains string" );
        node = node->next;
      }
    }
    ASSERT_OR_THROW( program->CSTR__expression != NULL,          "Expression" );
    if( !assigned ) {
      ASSERT_OR_THROW( program->CSTR__assigned == NULL,          "No assignment" );
    }
    else {
      ASSERT_OR_THROW( program->CSTR__assigned != NULL,          "Assignment" );
    }
    // tail
    ASSERT_OR_THROW( program->deref.tail == lookbacks,           "Previous vertex deref" );
    ASSERT_OR_THROW( CALLABLE( evaluator )->PrevDeref( evaluator ) == lookbacks, "n tail lookbacks" );
    // this
    ASSERT_OR_THROW( program->deref.this == this_accesses,       "Local vertex deref" );
    ASSERT_OR_THROW( CALLABLE( evaluator )->ThisDeref( evaluator ) == this_accesses, "n this deref" );
    // head
    ASSERT_OR_THROW( program->deref.head == head_accesses,      "Vertex derefs" );
    ASSERT_OR_THROW( CALLABLE( evaluator )->HeadDeref( evaluator ) == head_accesses, "n head deref" );
    // arc
    ASSERT_OR_THROW( program->deref.arc == traversals,         "Arc traversals" );
    ASSERT_OR_THROW( CALLABLE( evaluator )->Traversals( evaluator ) == traversals, "n arc traversals" );
    // ids
    ASSERT_OR_THROW( program->identifiers == identifiers,                "Identifiers" );
    ASSERT_OR_THROW( CALLABLE( evaluator )->Identifiers( evaluator ) == identifiers, "n identifiers" );

    vgx_ExpressEvalStack_t *stack = &program->stack;
    ASSERT_OR_THROW( stack->data != NULL,                        "Runtime stack" );
    ASSERT_OR_THROW( stack->eval_depth.max == runtime_depth,     "Max two items on stack at runtime" );
    ASSERT_OR_THROW( stack->sz == runtime_depth + 2,             "Stack size is max runtime size plus two dummy" );

    // Initial context should be populated with dummy data
    vgx_ExpressEvalContext_t *context = &evaluator->context;
    ASSERT_OR_THROW( context->TAIL == g_dummy,                         "Dummy tail" );
    ASSERT_OR_THROW( context->VERTEX == g_dummy,                       "Dummy vertex" );
    ASSERT_OR_THROW( context->HEAD == g_dummy,                         "Dummy head" );
    ASSERT_OR_THROW( context->arrive.data == VGX_PREDICATOR_NONE.data, "Prev arc not set" );
    ASSERT_OR_THROW( context->exit.data == VGX_PREDICATOR_NONE.data,   "Arc not set" );

    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, rankscore );
    ASSERT_OR_THROW( context->TAIL == arc->tail,                         "Tail set" );
    ASSERT_OR_THROW( context->VERTEX == arc->tail,                       "Vertex set" );
    ASSERT_OR_THROW( context->HEAD == arc->head.vertex,                  "Head set" );
    ASSERT_OR_THROW( context->arrive.data == arc->head.predicator.data,  "Prev arc set" );
    ASSERT_OR_THROW( context->exit.data == arc->head.predicator.data,    "Arc set" );
    ASSERT_OR_THROW( fabs(context->rankscore - rankscore) < 1e-6,        "Rank score set" );

    ASSERT_OR_THROW( context->memory->data == context->memory->__data,   "Default evaluator memory" );

    if( evalmem ) {
      CALLABLE( evaluator )->OwnMemory( evaluator, evalmem );
      ASSERT_OR_THROW( context->memory->data == evalmem->data,           "External evaluator memory" );
    }

    vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    ASSERT_OR_THROW( result != NULL,                             "Evaluation has result" );
    ASSERT_OR_THROW( result->type == test->result.type,          "Result type" );
    if( test->result.type == STACK_ITEM_TYPE_INTEGER ) {
      ASSERT_OR_THROW( result->integer == test->result.integer,                   "expression = integer result" );
      ASSERT_OR_THROW( iEvaluator.GetInteger( result ) == test->result.integer,   "integer result" );
      if( test->result.integer > 0 ) {
        ASSERT_OR_THROW( iEvaluator.IsPositive( result ) == 1,                    "positive result" );
      }
      else {
        ASSERT_OR_THROW( iEvaluator.IsPositive( result ) == 0,                    "not positive result" );
      }
    }
    else if( test->result.type == STACK_ITEM_TYPE_REAL ) {
      ASSERT_OR_THROW( result->real == test->result.real,                         "expression = real result" );
      ASSERT_OR_THROW( iEvaluator.GetReal( result ) == test->result.real,         "real result" );
      if( test->result.real > 0 ) {
        ASSERT_OR_THROW( iEvaluator.IsPositive( result ) == 1,                    "positive result" );
      }
      else {
        ASSERT_OR_THROW( iEvaluator.IsPositive( result ) == 0,                    "not positive result" );
      }
    }
    else if( test->result.type == STACK_ITEM_TYPE_CSTRING ) {
      ASSERT_OR_THROW( CStringEquals( test->result.CSTR__str, result->CSTR__str ), "expression = cstring result" );
    }

    iEvaluator.DiscardEvaluator( &evaluator );
    ASSERT_OR_THROW( evaluator == NULL,                          "Evaluator destroyed" );
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;
}





/**************************************************************************//**
 * __test_expressions
 *
 ******************************************************************************
 */
static int __test_expressions( vgx_Graph_t *graph, __test *tests, double rankscore, vgx_ExpressEvalMemory_t *evalmem, vgx_EvalStackItem_t *default_prop  ) {
  int ret = 0;
  vgx_Evaluator_t *evaluator = NULL;
  const __test *cursor = tests;
  const char *expr;


  XTRY {
    while( (expr = cursor->expression) != NULL ) {
      const vgx_EvalStackItem_t *expected = &cursor->result;
      vgx_LockableArc_t *arc = cursor->arc;
      bool new_eval = cursor->new_evaluator;
      ++cursor;

      if( evaluator && new_eval ) {
        iEvaluator.DiscardEvaluator( &evaluator );
      }

      if( evaluator == NULL ) {
        if( (evaluator = iEvaluator.NewEvaluator( graph, expr, NULL, &CSTR__error )) == NULL ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x00C, "Evaluator from \"%s\" (error=%s)", expr, CSTR__error ? CStringValue( CSTR__error ) : "?" );
        }
      }

      // Set tail and head context
      CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, rankscore );
      // Assign memory
      if( evalmem ) {
        CALLABLE( evaluator )->OwnMemory( evaluator, evalmem );
      }

      // Make new default property from provided stack item and set context
      vgx_EvalStackItem_t Default = {0};
      vgx_EvalStackItem_t *pDefault = NULL;
      if( default_prop ) {
        Default.type = default_prop->type;
        Default.bits = default_prop->bits;
        switch( Default.type ) {
        case STACK_ITEM_TYPE_CSTRING:
          Default.CSTR__str = __smart_own_cstring_CS( graph, default_prop->CSTR__str );
          break;
        case STACK_ITEM_TYPE_VECTOR:
          CALLABLE( Default.vector )->Incref( (vgx_Vector_t*)Default.vector );
          break;
        default:
          break;
        }
        pDefault = &Default;
      }
      CALLABLE( evaluator )->SetDefaultProp( evaluator, pDefault );
      pDefault = NULL;

      // Run evaluator
      vgx_EvalStackItem_t *result = CALLABLE( evaluator )->EvalArc( evaluator, arc );

      // Check results
      if( result->type != expected->type ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x001, "Expression %s: expected type %02x, got %02x", expr, expected->type, result->type );
      }
      switch( expected->type ) {
      case STACK_ITEM_TYPE_INTEGER:
        if( result->integer != expected->integer ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x002, "%s = %lld (got %lld)", expr, expected->integer, result->integer );
        }
        break;
      case STACK_ITEM_TYPE_REAL:
        if( (isnan( expected->real ) ^ isnan( result->real )) || fabs( result->real - expected->real ) > 1e-6 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x003, "%s = %g (got %g)", expr, expected->real, result->real );
        }
        break;
      case STACK_ITEM_TYPE_NAN:
        if( !isnan( result->real ) ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x004, "%s = nan (got %g)", expr, result->real );
        }
        break;
      case STACK_ITEM_TYPE_NONE:
        if( result->type != STACK_ITEM_TYPE_NONE ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x005, "%s = null (got %d)", expr, result->type );
        }
        break;
      case STACK_ITEM_TYPE_VERTEX:
        if( result->ptr != expected->ptr ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x006, "%s = %llp (got %llp)", expr, expected->ptr, result->ptr );
        }
        break;
      case STACK_ITEM_TYPE_CSTRING:
        if( !CStringEquals( result->CSTR__str, expected->CSTR__str ) ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x007, "%s = \"%s\" (got \"%s\")", expr, CStringValue( expected->CSTR__str ), result->CSTR__str ? CStringValue( result->CSTR__str ) : "" );
        }
        DELETE_STRING( expected->CSTR__str );
        break;
      case STACK_ITEM_TYPE_VECTOR:
        if( result->ptr != expected->ptr ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x008, "%s = %llp (got %llp)", expr, expected->ptr, result->ptr );
        }
        break;
      case STACK_ITEM_TYPE_BITVECTOR:
        if( result->bits != expected->bits ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x009, "%s = 0x%016llx (got 0x%016llx)", expr, expected->bits, result->bits );
        }
        break;
      case STACK_ITEM_TYPE_KEYVAL:
        {
          int rkey = vgx_cstring_array_map_key( &result->bits );
          int ekey = vgx_cstring_array_map_key( &expected->bits );
          float rval = vgx_cstring_array_map_val( &result->bits );
          float eval = vgx_cstring_array_map_val( &expected->bits );
          if( rkey != ekey || fabs( (double)rval - eval ) > 1e-6 ) {
            THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x00A, "%s = (%d,%g) (got (%d,%g))", expr, ekey, eval, rkey, rval );
          }
        }
        break;
      case STACK_ITEM_TYPE_VERTEXID:
        if( result->ptr != expected->ptr ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x00B, "%s = %llp (got %llp)", expr, expected->ptr, result->ptr );
        }
        break;
      case STACK_ITEM_TYPE_SET:
        if( result->integer != expected->integer  ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_ASSERTION, 0x002, "%s = %lld (got %lld)", expr, expected->integer, result->integer );
        }
        break;
      default:
        break;
      }
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iString.Discard( &CSTR__error );
    iEvaluator.DiscardEvaluator( &evaluator );
  }
  return ret;
}




/**************************************************************************//**
 * __is_syntax_error
 *
 ******************************************************************************
 */
static int __is_syntax_error( vgx_Graph_t *graph, const char *expression ) {
  int ret = 0;
  vgx_Evaluator_t *evaluator = NULL;
  XTRY {
    iString.Discard( &CSTR__error );
    if( (evaluator = iEvaluator.NewEvaluator( graph, expression, NULL, &CSTR__error )) != NULL ) {
      THROW_SILENT( CXLIB_ERR_ASSERTION, 0x001 );
    }
    if( CSTR__error == NULL ) {
      THROW_SILENT( CXLIB_ERR_ASSERTION, 0x002 );
    }
    ret = 1;
  }
  XCATCH( errcode ) {
    ret = 0;
  }
  XFINALLY {
    iEvaluator.DiscardEvaluator( &evaluator );
  }
  return ret;
}



/**************************************************************************//**
 * __populate_graph
 *
 ******************************************************************************
 */
static int __populate_graph( vgx_Graph_t *graph ) {
  int ret = 0;
  XTRY {
    CString_t *CSTR__type_root = NewEphemeralCString( graph, "root" );
    CString_t *CSTR__type_node = NewEphemeralCString( graph, "node" );
    CString_t *CSTR__ROOT = NewEphemeralCString( graph, "ROOT" );
    CString_t *CSTR__A = NewEphemeralCString( graph, "A" );
    CString_t *CSTR__B = NewEphemeralCString( graph, "B" );
    CString_t *CSTR__C = NewEphemeralCString( graph, "C" );
    CString_t *CSTR__D = NewEphemeralCString( graph, "D" );
    vgx_Relation_t *relation;
    int val;

    vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

    if( CALLABLE( graph )->simple->CreateVertex( graph, CSTR__ROOT, CSTR__type_root, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x001, "Failed to created ROOT" );
    }
    if( CALLABLE( graph )->simple->CreateVertex( graph, CSTR__A, CSTR__type_node, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x002, "Failed to create A" );
    }
    if( CALLABLE( graph )->simple->CreateVertex( graph, CSTR__B, CSTR__type_node, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x003, "Failed to create B" );
    }
    if( CALLABLE( graph )->simple->CreateVertex( graph, CSTR__C, CSTR__type_node, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x004, "Failed to create C" );
    }
    if( CALLABLE( graph )->simple->CreateVertex( graph, CSTR__D, CSTR__type_node, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x005, "Failed to create D" );
    }

    // ROOT-[to,M_INT,200]->A
    val = 100;
    relation = iRelation.New( graph, "ROOT", "A", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x006, "Failed to create relation" );
    }
    if( CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x007, "Failed to connect (ROOT)-[to,100]->(A)" );
    }
    iRelation.Delete( &relation );

    // ROOT-[to,M_INT,200]->B
    val = 200;
    relation = iRelation.New( graph, "ROOT", "B", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x008, "Failed to create relation" );
    }
    if( CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x009, "Failed to connect (ROOT)-[to,200]->(B)" );
    }
    iRelation.Delete( &relation );

    // ROOT-[to,M_INT,300]->C
    val = 300;
    relation = iRelation.New( graph, "ROOT", "C", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00A, "Failed to create relation" );
    }
    if( CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00B, "Failed to connect (ROOT)-[to,300]->(C)" );
    }
    iRelation.Delete( &relation );

    // ROOT-[rel,M_INT,400]->D
    val = 400;
    relation = iRelation.New( graph, "ROOT", "D", "rel", VGX_PREDICATOR_MOD_INTEGER, &val );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00C, "Failed to create relation" );
    }
    if( CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00D, "Failed to connect (ROOT)-[to,400]->(D)" );
    }
    iRelation.Delete( &relation );

    ARC_ROOT_to_A.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_A.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_B.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_B.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_C.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_C.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__C, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_rel_D.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_rel_D.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__D, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );

    vgx_Vertex_t *ROOT = ARC_ROOT_to_A.tail;
    vgx_Vertex_t *A = ARC_ROOT_to_A.head.vertex;
    vgx_Vertex_t *B = ARC_ROOT_to_B.head.vertex;
    vgx_Vertex_t *C = ARC_ROOT_to_C.head.vertex;
    vgx_Vertex_t *D = ARC_ROOT_rel_D.head.vertex;

    // RANK
    CALLABLE( A )->SetRank( A, 1.0, 0.0 );
    CALLABLE( B )->SetRank( B, 2.0, 10.0 );
    CALLABLE( C )->SetRank( C, 3.0, 100.0 );
    CALLABLE( D )->SetRank( D, 4.0, 1000.0 );

    // PROPERTIES
    vgx_VertexProperty_t *propR = iVertexProperty.NewInteger( graph, "propR", 0xA ); 
    vgx_VertexProperty_t *prop1 = iVertexProperty.NewInteger( graph, "prop1", 1 ); 
    vgx_VertexProperty_t *prop2 = iVertexProperty.NewInteger( graph, "prop2", 2 ); 
    vgx_VertexProperty_t *prop3 = iVertexProperty.NewInteger( graph, "prop3", 3 ); 
    vgx_VertexProperty_t *propS = iVertexProperty.NewString( graph, "propS", "head property" ); 
    if( !propR || !prop1 || !prop2 || !prop3 || !propS ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00E, "Failed to create properties" );
    }
    // ROOT has one property
    if( CALLABLE( ROOT )->SetProperty( ROOT, propR ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x00F, "Failed to set property" );
    }
    // A has no properties
    // B has four properties
    if( CALLABLE( B )->SetProperty( B, prop1 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x010, "Failed to set property" );
    }
    if( CALLABLE( B )->SetProperty( B, prop2 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x011, "Failed to set property" );
    }
    if( CALLABLE( B )->SetProperty( B, prop3 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x012, "Failed to set property" );
    }
    if( CALLABLE( B )->SetProperty( B, propS ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x013, "Failed to set property" );
    }
    // C has two properties
    if( CALLABLE( C )->SetProperty( C, prop1 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x014, "Failed to set property" );
    }
    if( CALLABLE( C )->SetProperty( C, prop2 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x015, "Failed to set property" );
    }
    // D has one property
    if( CALLABLE( D )->SetProperty( D, prop1 ) != 1 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x016, "Failed to set property" );
    }
    iVertexProperty.Delete( &propR );
    iVertexProperty.Delete( &prop1 );
    iVertexProperty.Delete( &prop2 );
    iVertexProperty.Delete( &prop3 );
    iVertexProperty.Delete( &propS );

    // VECTORS
    vgx_Similarity_t *sim = graph->similarity;
    vgx_Vertex_t *vertices[] = { A, B, D, ROOT, NULL };
    vgx_Vertex_t **cursor = vertices;
    vgx_Vertex_t *vertex;
    vgx_Vector_t *vector;
    vgx_Vector_t *head_vector;
    vgx_Vector_t *tail_vector;

    if( igraphfactory.EuclideanVectors() ) {
      float head_elements[32] = {
        -1.5f, -1.4f, -1.3f, -1.2f, -1.1f, -1.0f, -0.9f, -0.8f, -0.7f, -0.6f, -0.5f, -0.4f, -0.3f, -0.2f, -0.1f,  0.0f,
         0.1f,  0.2f,  0.3f,  0.4f,  0.5f,  0.6f,  0.7f,  0.8f,  0.9f,  1.0f,  1.1f,  1.2f,  1.3f,  1.4f,  1.5f,  1.6f
      };
      float tail_elements[32] = {
        -0.5f, -0.4f, -0.3f, -0.2f, -0.1f,  0.0f,  0.1f,  0.2f,  0.3f,  0.4f,  0.5f,  0.6f,  0.7f,  0.8f,  0.9f,  1.0f,
         1.1f,  1.2f,  1.3f,  1.4f,  1.5f,  1.6f,  1.7f,  1.8f,  1.9f,  2.0f,  2.1f,  2.2f,  2.3f,  2.4f,  2.5f,  2.6f
      };

      head_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, head_elements, 32, true, NULL );
      tail_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, tail_elements, 32, true, NULL );
    }
    else {
      ext_vector_feature_t head_elements[] = {
        { .weight=1.0f, .term="first_dim" },
        { .weight=0.8f, .term="second_dim" },
        { .weight=0.6f, .term="third_dim" },
        { .weight=0.4f, .term="fourth_dim" },
        { .weight=0.2f, .term="fifth_dim" },
        {0}
      };
      uint16_t sz_head_elements = ivectoralloc.CountExternalElements( head_elements );
      head_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, head_elements, sz_head_elements, true, NULL );
      ext_vector_feature_t tail_elements[] = {
        { .weight=0.8f, .term="second_dim" },
        { .weight=0.6f, .term="third_dim" },
        { .weight=0.4f, .term="fourth_dim" },
        {0}
      };
      uint16_t sz_tail_elements = ivectoralloc.CountExternalElements( tail_elements );
      tail_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, tail_elements, sz_tail_elements, true, NULL );
    }

    if( head_vector == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x017, "Failed to create head vector" );
    }

    if( tail_vector == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x018, "Failed to create tail vector" );
    }

    while( (vertex = *cursor++) != NULL ) {
      if( vertex == ROOT ) {
        vector = tail_vector;
      }
      else {
        vector = head_vector;
      }
      CALLABLE( vector )->Incref( vector );
      CALLABLE( vertex )->SetVector( vertex, &vector );
      if( vector ) {
        CALLABLE( vector )->Decref( vector );
      }
    }

    CALLABLE( head_vector )->Decref( head_vector );
    CALLABLE( tail_vector )->Decref( tail_vector );

    // SET THE TEST ARCS
    vgx_ArcHead_t archead;
    // ROOT-[to]->A
    relation = iRelation.New( graph, "ROOT", "A", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x020, "Failed to create relation" );
    }
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    if( archead.vertex != ARC_ROOT_to_A.head.vertex ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x021, "Expected terminal A" );
    }
    ARC_ROOT_to_A.head.predicator = archead.predicator;
    // ROOT-[to]->B
    relation = iRelation.New( graph, "ROOT", "B", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x022, "Failed to create relation" );
    }
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    if( archead.vertex != ARC_ROOT_to_B.head.vertex ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x023, "Expected terminal B" );
    }
    ARC_ROOT_to_B.head.predicator = archead.predicator;
    // ROOT-[to]->C
    relation = iRelation.New( graph, "ROOT", "C", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x024, "Failed to create relation" );
    }
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    if( archead.vertex != ARC_ROOT_to_C.head.vertex ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x025, "Expected terminal C" );
    }
    ARC_ROOT_to_C.head.predicator = archead.predicator;
    // ROOT-[rel]->D
    relation = iRelation.New( graph, "ROOT", "D", "rel", VGX_PREDICATOR_MOD_INTEGER, NULL );
    if( relation == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x026, "Failed to create relation" );
    }
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    if( archead.vertex != ARC_ROOT_rel_D.head.vertex ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x027, "Expected terminal C" );
    }
    ARC_ROOT_rel_D.head.predicator = archead.predicator;

    iString.Discard( &CSTR__D );
    iString.Discard( &CSTR__C );
    iString.Discard( &CSTR__B );
    iString.Discard( &CSTR__A );
    iString.Discard( &CSTR__ROOT );
    iString.Discard( &CSTR__type_root );
    iString.Discard( &CSTR__type_node );
  }
  XCATCH( errcode ) {
    if( CSTR__error ) {
      REASON( 0x000, "%s", CStringValue( CSTR__error ) );
    }
    ret = -1;
  }
  XFINALLY {
  }
  iString.Discard( &CSTR__error );
  return ret;
}



IGNORE_WARNING_LARGE_STACK_ALLOCATION
BEGIN_UNIT_TEST( __utest_vxeval ) {

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();

  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );

  vgx_Graph_t *graph = NULL;




  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Initialization
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Initializations" ) {
    // Graph factory should have initialized the stack evaluator
    TEST_ASSERTION( g_dummy != NULL,                    "Dummy vertex created" );
    TEST_ASSERTION( _vxeval_parser__tokenizer != NULL,  "Tokenizer created" );
    TEST_ASSERTION( _vxeval_operations != NULL,               "Operation map created" );
    
    // Destroy it
    TEST_ASSERTION( iEvaluator.Destroy() == 0,          "De-initialized" );
    
    // Verify destroyed
    TEST_ASSERTION( g_dummy == NULL,                    "Dummy vertex does not exist" );
    TEST_ASSERTION( _vxeval_parser__tokenizer == NULL,  "Tokenizer does not exist" );
    TEST_ASSERTION( _vxeval_operations == NULL,               "Operation map does not exist" );

    // Re-initialize it
    TEST_ASSERTION( iEvaluator.Initialize() == 0,       "Initialized" );

    // Verify initialized
    TEST_ASSERTION( g_dummy != NULL,                    "Dummy vertex created" );
    TEST_ASSERTION( _vxeval_parser__tokenizer != NULL,  "Tokenizer created" );
    TEST_ASSERTION( _vxeval_operations != NULL,               "Operation map created" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Populate graph
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Populate Graph" ) {
    TEST_ASSERTION( __populate_graph( graph ) == 0, "Populate Graph" );
    /*
    CString_t *CSTR__type_root = NewEphemeralCString( graph, "root" );
    CString_t *CSTR__type_node = NewEphemeralCString( graph, "node" );
    CString_t *CSTR__ROOT = NewEphemeralCString( graph, "ROOT" );
    CString_t *CSTR__A = NewEphemeralCString( graph, "A" );
    CString_t *CSTR__B = NewEphemeralCString( graph, "B" );
    CString_t *CSTR__C = NewEphemeralCString( graph, "C" );
    CString_t *CSTR__D = NewEphemeralCString( graph, "D" );
    vgx_Relation_t *relation;
    int val;
    int ret;

    vgx_VertexAccessMode_t writable = VGX_VERTEX_ACCESS_WRITABLE;
    vgx_AccessReason_t reason = VGX_ACCESS_REASON_NONE;

    ret = CALLABLE( graph )->simple->CreateVertex( graph, CSTR__ROOT, CSTR__type_root, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,    "Created ROOT" );
    ret = CALLABLE( graph )->simple->CreateVertex( graph, CSTR__A, CSTR__type_node, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,    "Created A" );
    ret = CALLABLE( graph )->simple->CreateVertex( graph, CSTR__B, CSTR__type_node, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,    "Created B" );
    ret = CALLABLE( graph )->simple->CreateVertex( graph, CSTR__C, CSTR__type_node, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,    "Created C" );
    ret = CALLABLE( graph )->simple->CreateVertex( graph, CSTR__D, CSTR__type_node, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,    "Created D" );

    // ROOT-[to,M_INT,200]->A
    val = 100;
    relation = iRelation.New( graph, "ROOT", "A", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    ret = CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,           "Connected (ROOT)-[to,100]->(A)" );
    iRelation.Delete( &relation );

    // ROOT-[to,M_INT,200]->B
    val = 200;
    relation = iRelation.New( graph, "ROOT", "B", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    ret = CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,           "Connected (ROOT)-[to,200]->(B)" );
    iRelation.Delete( &relation );

    // ROOT-[to,M_INT,300]->C
    val = 300;
    relation = iRelation.New( graph, "ROOT", "C", "to", VGX_PREDICATOR_MOD_INTEGER, &val );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    ret = CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,           "Connected (ROOT)-[to,300]->(C)" );
    iRelation.Delete( &relation );

    // ROOT-[rel,M_INT,400]->D
    val = 400;
    relation = iRelation.New( graph, "ROOT", "D", "rel", VGX_PREDICATOR_MOD_INTEGER, &val );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    ret = CALLABLE( graph )->simple->Connect( graph, relation, -1, NULL, 0, &reason, &CSTR__error );
    TEST_ASSERTION( ret == 1,           "Connected (ROOT)-[rel,400]->(D)" );
    iRelation.Delete( &relation );

    ARC_ROOT_to_A.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_A.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__A, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_B.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_B.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__B, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_C.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_to_C.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__C, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_rel_D.tail = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__ROOT, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );
    ARC_ROOT_rel_D.head.vertex = CALLABLE( graph )->simple->OpenVertex( graph, CSTR__D, VGX_VERTEX_ACCESS_WRITABLE, 0, &reason, &CSTR__error );

    vgx_Vertex_t *ROOT = ARC_ROOT_to_A.tail;
    vgx_Vertex_t *A = ARC_ROOT_to_A.head.vertex;
    vgx_Vertex_t *B = ARC_ROOT_to_B.head.vertex;
    vgx_Vertex_t *C = ARC_ROOT_to_C.head.vertex;
    vgx_Vertex_t *D = ARC_ROOT_rel_D.head.vertex;

    // RANK
    CALLABLE( A )->SetRank( A, 1.0, 0.0 );
    CALLABLE( B )->SetRank( B, 2.0, 10.0 );
    CALLABLE( C )->SetRank( C, 3.0, 100.0 );
    CALLABLE( D )->SetRank( D, 4.0, 1000.0 );

    // PROPERTIES
    vgx_VertexProperty_t *propR = iVertexProperty.NewInteger( graph, "propR", 0xA ); 
    vgx_VertexProperty_t *prop1 = iVertexProperty.NewInteger( graph, "prop1", 1 ); 
    vgx_VertexProperty_t *prop2 = iVertexProperty.NewInteger( graph, "prop2", 2 ); 
    vgx_VertexProperty_t *prop3 = iVertexProperty.NewInteger( graph, "prop3", 3 ); 
    vgx_VertexProperty_t *propS = iVertexProperty.NewString( graph, "propS", "head property" ); 
    TEST_ASSERTION( propR && prop1 && prop2 && prop3 && propS,             "Created properties" );
    // ROOT has one property
    TEST_ASSERTION( CALLABLE( ROOT )->SetProperty( ROOT, propR ) == 1,  "Set property" );
    // A has no properties
    // B has four properties
    TEST_ASSERTION( CALLABLE( B )->SetProperty( B, prop1 ) == 1,  "Set property" );
    TEST_ASSERTION( CALLABLE( B )->SetProperty( B, prop2 ) == 1,  "Set property" );
    TEST_ASSERTION( CALLABLE( B )->SetProperty( B, prop3 ) == 1,  "Set property" );
    TEST_ASSERTION( CALLABLE( B )->SetProperty( B, propS ) == 1,  "Set property" );
    // C has two properties
    TEST_ASSERTION( CALLABLE( C )->SetProperty( C, prop1 ) == 1,  "Set property" );
    TEST_ASSERTION( CALLABLE( C )->SetProperty( C, prop2 ) == 1,  "Set property" );
    // D has one property
    TEST_ASSERTION( CALLABLE( D )->SetProperty( D, prop1 ) == 1,  "Set property" );
    iVertexProperty.Delete( &propR );
    iVertexProperty.Delete( &prop1 );
    iVertexProperty.Delete( &prop2 );
    iVertexProperty.Delete( &prop3 );
    iVertexProperty.Delete( &propS );

    // VECTORS
    vgx_Similarity_t *sim = graph->similarity;
    ext_vector_feature_t head_elements[] = {
      { .weight=1.0f, .term="first_dim" },
      { .weight=0.8f, .term="second_dim" },
      { .weight=0.6f, .term="third_dim" },
      { .weight=0.4f, .term="fourth_dim" },
      { .weight=0.2f, .term="fifth_dim" },
      {0}
    };
    uint16_t sz_head_elements = ivectoralloc.CountExternalElements( head_elements );
    vgx_Vector_t *head_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, head_elements, sz_head_elements, true, NULL );
    ext_vector_feature_t tail_elements[] = {
      { .weight=0.8f, .term="second_dim" },
      { .weight=0.6f, .term="third_dim" },
      { .weight=0.4f, .term="fourth_dim" },
      {0}
    };
    uint16_t sz_tail_elements = ivectoralloc.CountExternalElements( tail_elements );
    vgx_Vector_t *tail_vector = CALLABLE( sim )->NewInternalVectorFromExternal( sim, tail_elements, sz_tail_elements, true, NULL );
    vgx_Vertex_t *vertices[] = { A, B, D, ROOT, NULL };
    vgx_Vertex_t **cursor = vertices;
    vgx_Vertex_t *vertex;
    vgx_Vector_t *vector;

    while( (vertex = *cursor++) != NULL ) {
      if( vertex == ROOT ) {
        vector = tail_vector;
      }
      else {
        vector = head_vector;
      }
      CALLABLE( vector )->Incref( vector );
      CALLABLE( vertex )->SetVector( vertex, &vector );
      if( vector ) {
        CALLABLE( vector )->Decref( vector );
      }
    }

    CALLABLE( head_vector )->Decref( head_vector );
    CALLABLE( tail_vector )->Decref( tail_vector );

    // SET THE TEST ARCS
    vgx_ArcHead_t archead;
    // ROOT-[to]->A
    relation = iRelation.New( graph, "ROOT", "A", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    TEST_ASSERTION( archead.vertex == ARC_ROOT_to_A.head.vertex,  "Expected terminal A" );
    ARC_ROOT_to_A.head.predicator = archead.predicator;
    // ROOT-[to]->B
    relation = iRelation.New( graph, "ROOT", "B", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    TEST_ASSERTION( archead.vertex == ARC_ROOT_to_B.head.vertex,  "Expected terminal B" );
    ARC_ROOT_to_B.head.predicator = archead.predicator;
    // ROOT-[to]->C
    relation = iRelation.New( graph, "ROOT", "C", "to", VGX_PREDICATOR_MOD_INTEGER, NULL );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    TEST_ASSERTION( archead.vertex == ARC_ROOT_to_C.head.vertex,  "Expected terminal C" );
    ARC_ROOT_to_C.head.predicator = archead.predicator;
    // ROOT-[rel]->D
    relation = iRelation.New( graph, "ROOT", "D", "rel", VGX_PREDICATOR_MOD_INTEGER, NULL );
    TEST_ASSERTION( relation != NULL,   "Created relation" );
    archead = CALLABLE( graph )->simple->ArcValue( graph, relation, 0, &reason );
    iRelation.Delete( &relation );
    TEST_ASSERTION( archead.vertex == ARC_ROOT_rel_D.head.vertex,  "Expected terminal D" );
    ARC_ROOT_rel_D.head.predicator = archead.predicator;
    

    iString.Discard( &CSTR__D );
    iString.Discard( &CSTR__C );
    iString.Discard( &CSTR__B );
    iString.Discard( &CSTR__A );
    iString.Discard( &CSTR__ROOT );
    iString.Discard( &CSTR__type_root );
    iString.Discard( &CSTR__type_node );
    iString.Discard( &CSTR__error );
    */
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Constructor
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Evaluator Constructor" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    __test test = {
      .expression     =  "1+1",
      .result         =  {
        .type           = STACK_ITEM_TYPE_INTEGER,
        .integer        = 2
      },
      .arc            = arc,
      .new_evaluator  = true
    };

    int ret = __test_constructor( graph, NULL, &test, 3, 2, 0, 0, 0, 0, 0, 0, 1.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0,     "Evaluator constructor" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Various evaluators
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Evaluator Constructor" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    int ret;
    vgx_Evaluator_t *evaluator;
    vgx_EvalStackItem_t *stackitem;

    // Test 1
    __test test1 = {  
      .expression  =  "(1 + 2) && 'thing' in vertex ",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 0 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test1, 7, 3, 1, 1, 0, 0, 0, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 1" );

    // Test 2
    __test test2 = {
      .expression  =  "(1 + 2) && 'thing' in next ",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 0 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test2, 7, 3, 1, 0, 1, 1, 0, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 2" );

    // Test 3
    __test test3 = {
      .expression  =  "(1 + 2) && next.arc.value > 0",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 1 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test3, 7, 3, 0, 0, 0, 1, 0, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 3" );

    // Test 4
    __test test4 = {
      .expression  =  "(1 + 2) && next.arc.value > 0 || next['thing']",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 1 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test4, 10, 3, 0, 0, 1, 2, 0, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 4" );

    // Test 5
    __test test5 = {
      .expression  =  "'thing' in vertex || 'thing' in next || 'stuff' in next || prev.arc.value == 1234 || next.arc.type =='nope'",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 0 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test5, 19, 3, 3, 1, 2, 3, 1, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 5" );

    // Test 6
    __test test6 = {
      .expression  =  "savefunc := (1 + 2) && 'thing' in next ",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 0 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test6, 7, 3, 1, 0, 1, 1, 0, 0, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 6" );
    evaluator = iEvaluator.NewEvaluator( graph, "savefunc", NULL, &CSTR__error );
    TEST_ASSERTION( evaluator != NULL,                              "Calling pre-defined evaluator" );
    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, 0.0 );
    stackitem = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    TEST_ASSERTION( stackitem->type == test6.result.type,           "Item type" );
    TEST_ASSERTION( stackitem->integer == test6.result.integer,     "Integer value" );
    iEvaluator.DiscardEvaluator( &evaluator );

    // Test 7
    ext_vector_feature_t elements[] = { // NOTE: SAME VECTOR AS SELECTED HEAD
      { .weight=1.0f, .term="first_dim" },
      { .weight=0.8f, .term="second_dim" },
      { .weight=0.6f, .term="third_dim" },
      { .weight=0.4f, .term="fourth_dim" },
      { .weight=0.2f, .term="fifth_dim" },
      {0}
    };
    uint16_t sz_elements = ivectoralloc.CountExternalElements( elements );
    vgx_Vector_t *vector = CALLABLE( graph->similarity )->NewInternalVectorFromExternal( graph->similarity, elements, sz_elements, true, NULL );
    __test test7 = {
      .expression  =  "vectorfunc := sim( vector, next.vector ) > 0.9999 && sim( vertex.vector, vector ) in range( 0.3, 0.9 )",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 1 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, vector, &test7, 13, 4, 0, 1, 1, 1, 0, 0, 0.0, NULL, &CSTR__error );
    CALLABLE( vector )->Decref( vector );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 7" );
    evaluator = iEvaluator.NewEvaluator( graph, "vectorfunc", NULL, &CSTR__error );
    TEST_ASSERTION( evaluator != NULL,                              "Calling pre-defined evaluator" );
    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, 0.0 );
    stackitem = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    TEST_ASSERTION( stackitem->type == test7.result.type,           "Item type is integer" );
    TEST_ASSERTION( stackitem->integer == test7.result.integer,     "Should evaluate to %lld", test7.result.integer );
    iEvaluator.DiscardEvaluator( &evaluator );

    // Test 8
    __test test8 = {
      .expression  =  "context.rank",
      .result = { .type = STACK_ITEM_TYPE_REAL, .real = 1000.0 },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test8, 1, 1, 0, 0, 0, 0, 0, 0, 1000.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 8" );

    // Test 9
    __test test9 = {
      .expression  =  "stackfunc := ( 'key1', next.id, 'key2', typedec( next.type ), 'key3', reldec( next.arc.type ), 'key4', next.internalid )",
      .result = { .type = STACK_ITEM_TYPE_CSTRING, .CSTR__str = NewEphemeralCString( graph, "key1" ) },
      .arc = arc,
      .new_evaluator = true
    };
    ret = __test_constructor( graph, NULL, &test9, 11, 8, 4, 0, 3, 4, 0, 1, 0.0, NULL, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 9" );
    iString.Discard( (CString_t**)&test9.result.CSTR__str );
    evaluator = iEvaluator.NewEvaluator( graph, "stackfunc", NULL, &CSTR__error );
    TEST_ASSERTION( evaluator != NULL,                              "Calling pre-defined evaluator" );
    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, 0.0 );
    stackitem = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    // key1
    TEST_ASSERTION( stackitem[0].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[0].CSTR__str, "key1" ),             "key1" );
    // next.id = B
    TEST_ASSERTION( stackitem[1].type == STACK_ITEM_TYPE_VERTEXID,                    "vertexid" );
    TEST_ASSERTION( CharsEqualsConst( stackitem[1].vertexid->idprefix.data, "B" ),    "id = B" );
    // key2
    TEST_ASSERTION( stackitem[2].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[2].CSTR__str, "key2" ),             "key2" );
    // typedec( next.type ) = node
    TEST_ASSERTION( stackitem[3].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[3].CSTR__str, "node" ),             "typedec( vertex.type ) = node" );
    // key3
    TEST_ASSERTION( stackitem[4].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[4].CSTR__str, "key3" ),             "key3" );
    // reldec( next.arc.type ) = to
    TEST_ASSERTION( stackitem[5].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[5].CSTR__str, "to" ),               "reldec( next.arc.type ) = to" );
    // key4
    TEST_ASSERTION( stackitem[6].type == STACK_ITEM_TYPE_CSTRING,                     "cstring" );
    TEST_ASSERTION( CStringEqualsChars( stackitem[6].CSTR__str, "key4" ),             "key4" );
    // next.internalid
    TEST_ASSERTION( stackitem[7].type == STACK_ITEM_TYPE_VERTEX,                      "vertex" );
    TEST_ASSERTION( idmatch( __vertex_internalid( stackitem[7].vertex ), __vertex_internalid( arc->head.vertex ) ),   "internalid" );
    iEvaluator.DiscardEvaluator( &evaluator );

    // Test 10
    __test test10 = {
      .expression  =  "do( store(0,100), store(4,104), store(-1,pi) )",
      .result = { .type = STACK_ITEM_TYPE_INTEGER, .integer = 1 },
      .arc = arc,
      .new_evaluator = true
    };
    vgx_ExpressEvalMemory_t *evalmem = iEvaluator.NewMemory( 10 );
    ret = __test_constructor( graph, NULL, &test10, 11, 4, 0, 0, 0, 0, 0, 0, 0.0, evalmem, &CSTR__error );
    if( CSTR__error ) {
      TEST_ASSERTION( false, CStringValue( CSTR__error ) );
      iString.Discard( &CSTR__error );
    }
    TEST_ASSERTION( ret == 0, "Test 10" );
    // True
    evaluator = iEvaluator.NewEvaluator( graph, "load(0) == 100 && load(4) == 104 && load(1023) == pi", NULL, &CSTR__error );
    TEST_ASSERTION( evaluator != NULL,                            "Created evaluator" );
    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, 0.0 );
    CALLABLE( evaluator )->OwnMemory( evaluator, evalmem );
    stackitem = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    TEST_ASSERTION( stackitem->type == STACK_ITEM_TYPE_INTEGER,   "Integer" );
    TEST_ASSERTION( stackitem->integer == 1,                      "True" );
    iEvaluator.DiscardEvaluator( &evaluator );
    // False
    evaluator = iEvaluator.NewEvaluator( graph, "load(0) == 100 && load(5) == 105 && load(1023) == pi", NULL, &CSTR__error );
    TEST_ASSERTION( evaluator != NULL,                            "Created evaluator" );
    CALLABLE( evaluator )->SetContext( evaluator, arc->tail, &arc->head, NULL, 0.0 );
    CALLABLE( evaluator )->OwnMemory( evaluator, evalmem );
    stackitem = CALLABLE( evaluator )->EvalArc( evaluator, arc );
    TEST_ASSERTION( stackitem->type == STACK_ITEM_TYPE_INTEGER,   "Integer" );
    TEST_ASSERTION( stackitem->integer == 0,                      "False" );
    iEvaluator.DiscardEvaluator( &evaluator );

    iEvaluator.DiscardMemory( &evalmem );


  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Sandbox
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sandbox" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    __test sandbox[] = {
      ASSERT(         "isnan( 'string' )"   ),
      ASSERT(         "isnan( asin(2) )"    ),
      UNTRUE(         "isnan( 1000 )"       ),
      UNTRUE(         "isnan( 123.4 )"      ),


      ASSERT(         "next.type in { 'thing', 'node', 'stuff' }" ),
      ASSERT(         "modtostr( M_SIM )  == 'M_SIM'"   ), 
      INTEGER( 1,     "mytest0 := 'nix' notin {'name', 'value', 3.14, vertex}"        ),
      ASSERT(         "1 in {1}"                ),


      INTEGER( 0,     "1 in 2" ),

      INTEGER( 1,     "1 == +1" ),
      INTEGER( 1,     "1 == !0" ),



      INTEGER( 2,         "(1) + 1" ),


      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, sandbox, 0.0, NULL, NULL ) >= 0,   "Sandbox" );

  } END_TEST_SCENARIO




  /*******************************************************************//**
   * Basic Expressions
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Basic Expressions" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    // Basic arithmetic
    __test basic_arithmetic[] = {
      // Basic parsing
      INTEGER(  0,      "0"                 ),
      INTEGER(  0,      " 0"                ),
      INTEGER(  0,      " 0 "               ),
      INTEGER(  0,      "-0"                ),
      INTEGER(  0,      "+0"                ),
      INTEGER(  1,      "1"                 ),
      INTEGER(  1,      " 1"                ),
      INTEGER(  1,      " 1 "               ),
      INTEGER(  -1,     "-1"                ),
      INTEGER(  1,      "--1"               ),
      INTEGER(  -1,     "---1"              ),
      INTEGER(  1,      "+1"                ),
      INTEGER(  -1,     "+-1"               ),
      INTEGER(  1,      "-+-1"              ),

      // Unary
      XINTEGER(         0                   ),
      XINTEGER(         1                   ),
      XINTEGER(         +1                  ),
      XINTEGER(         + +1                ),
      XINTEGER(         + -1                ),
      XINTEGER(         - +1                ),
      XINTEGER(         - -1                ),
      XINTEGER(         - - -1              ),
      XINTEGER(         - + -1              ),

      // Addition and Subtraction
      XINTEGER(         1 + 1               ),
      XINTEGER(         1 + -1              ),
      XINTEGER(         -1 + 1              ),
      XINTEGER(         -1 + -1             ),
      XINTEGER(         1 - 2               ),
      XINTEGER(         5 - 5               ),
      XINTEGER(         - 5 + 6             ),
      XINTEGER(         - 6 + 5             ),
      XINTEGER(         5 + 5 - 10          ),
      XINTEGER(         5 + ( 5 - 10 )      ),
      XINTEGER(         5 - 10 - 5          ),
      XINTEGER(         5 - ( 10 - 5 )      ),
      XINTEGER(         5 - ( 10 + 5 )      ),

      // Multiplication
      XINTEGER(         1 * 1               ),
      XINTEGER(         1 * 2               ),
      XINTEGER(         2 * 1               ),
      XINTEGER(         2 * 2               ),
      XINTEGER(         2 * 3               ),
      XINTEGER(         3 * 2               ),
      XINTEGER(         3 * -2              ),
      XINTEGER(        -3 * 2               ),
      XINTEGER(        -3 * -2              ),
      XINTEGER(    - ( -3 * -2 )            ),
      XINTEGER(       ( 2 + 3 ) * ( 4 + 5 ) ),
      XINTEGER(         2 + 3   *   4 + 5   ),
      XINTEGER(         2 * 3   +   4 * 5   ),
      XINTEGER(       ( 2 * 3 ) + ( 4 * 5 ) ),
      XINTEGER(         2 * ( 3 + 4 ) * 5   ),

      // Division (everything promoted to real)
      REAL(  1.0,      "1 / 1"              ),
      XREAL(            1 / 1.0             ),
      XREAL(            1 / 2.0             ),
      XREAL(            2 / 1.0             ),
      XREAL(            2 / 2.0             ),
      XREAL(            2 / 3.0             ),
      XREAL(            3 / 2.0             ),
      XREAL(            3 / -2.0            ),
      XREAL(       - ( -3 / -2.0 )          ),
      XREAL(        ( 2.0 + 3 ) / ( 4 + 5 ) ),
      XREAL(          2.0 + 3.0 /   4 + 5   ),
      XREAL(          2.0 / 3   +  4.0 / 5  ),
      XREAL(        ( 2.0 / 3 ) + (4.0 / 5 )),
      XREAL(          2.0 / ( 3 + 4 ) / 5   ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, basic_arithmetic, 0.0, NULL, NULL ) >= 0,   "Basic arithmetic" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Comments
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Comments" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    // Comments
    __test comments[] = {
      // Single line
      INTEGER(  5,      "5 /* comment */"         ),
      INTEGER(  5,      "/* comment */ 5"         ),
      INTEGER(  5,      "5/* comment */"          ),
      INTEGER(  5,      "/* comment */5"          ),

      INTEGER(  5,      "5 /** comment **/"       ),
      INTEGER(  5,      "1 /**/ + 4"              ),
      INTEGER(  5,      "1 /***/ + 4"             ),
      INTEGER(  5,      "1 /****/ + 4"            ),
      INTEGER(  5,      "1 /*****/ + 4"           ),
      INTEGER(  5,      "1 /******/ + 4"          ),
      INTEGER(  5,      "1 /*******/ + 4"         ),
      INTEGER(  5,      "1 /********/ + 4"        ),
      INTEGER(  5,      "1 /*********/ + 4"       ),
      INTEGER(  5,      "1 /**********/ + 4"      ),
      INTEGER(  5,      "1 /***********/ + 4"     ),

      INTEGER(  5,      "1 /* */ + /* */ 1 /* */ + /* */ 3"  ),
      INTEGER(  5,      "1/**/+/**/1/**/+/**/3"   ),

      INTEGER(  5,      "1 /* one */ + /* plus */ 1 /* one */ + /* plus */ 3 /* three */"  ),
      INTEGER(  5,      "1/*one*/+/*plus*/1/*one*/+/*plus*/3/*three*/"  ),

      INTEGER(  5,      "1 + 4 // + 1"            ),
      INTEGER(  5,      "1 + 4 // /**/ + 1  "     ),
      INTEGER(  5,      "1 + 4 // + /**/ 1  "     ),
      INTEGER(  5,      "1 + /**/4/**/ // + 1"    ),

      // Multiline
      INTEGER(  3,      "\n"
                        "/*******************************************************************//**\n"
                        " * Multiline Comments\n"
                        " * This expression will evaluate something complex \n"
                        " * using multiple lines of code with comments \n"
                        " ***********************************************************************\n"
                        " */\n"
                        "// First a number\n"
                        "10 // start with 10\n"
                        " + // then add\n"
                        " 5 // 5 \n"
                        "/* should have 15 at this point, subtract 7*/ - 7\n"
                        "// - 6\n"
                        "\n"
                        "- /* then subtract 6 */ 6 // to get 2\n"
                        "- (\n"
                        "  /* one */ 1\n"
                        "\n"
                        "      + // plus\n"
                        "\n"
                        "  1 /* one */\n"
                        "\n"
                        "+8//now we'll go negative...\n"
                        ")\n"
                        "\n"
                        "/* and bring it up to three */ + 11\n"
                        "\n"
                        "/* This concludes \n"
                        "   our program\n"
                        "*/\n"
                        "\n"
                        "\n"
                        "\n"
             ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, comments, 0.0, NULL, NULL ) >= 0,   "Comments" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Constants
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Constants" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    int64_t opid;
    GRAPH_LOCK( graph ) {
      opid = iOperation.GetId_LCK( &graph->operation );
    } GRAPH_RELEASE;

    int64_t order = GraphOrder( graph );
    int64_t size = GraphSize( graph );

    // Constants
    __test constants[] = {
      REAL(  M_PI,          "pi"              ),
      REAL(  M_E,           "e"               ),
      REAL(  sqrt(2),       "root2"           ),
      REAL(  sqrt(3),       "root3"           ),
      REAL(  sqrt(5),       "root5"           ),
      REAL(  (1+sqrt(5))/2, "phi"             ),
      REAL(  __M_ZETA3,     "zeta3"           ),
      REAL(  1e100,         "googol"          ),
      INTEGER(  -1,         "R1"              ),
      INTEGER(  -2,         "R2"              ),
      INTEGER(  -3,         "R3"              ),
      INTEGER(  -4,         "R4"              ),

      REAL(  50.0,          "context.rank"          ),
      REAL(  51.0,          "context.rank + 1"      ),
      REAL(  52.0,          "2 + context.rank"      ),
      ASSERT(        "context.rank == .rank" ),

      XINTEGER(     true    ),
      XINTEGER(     false    ),

      INTEGER( TIME_EXPIRES_NEVER,        "T_NEVER"     ),
      INTEGER( TIMESTAMP_MIN,             "T_MIN"       ),
      INTEGER( TIMESTAMP_MAX,             "T_MAX"       ),

      INTEGER( VGX_ARCDIR_ANY,         "D_ANY"       ),
      INTEGER( VGX_ARCDIR_IN,          "D_IN"        ),
      INTEGER( VGX_ARCDIR_OUT,         "D_OUT"       ),
      INTEGER( VGX_ARCDIR_BOTH,        "D_BOTH"      ),
      ASSERT(      "dirtostr( D_ANY )  == 'D_ANY'"   ), 
      ASSERT(      "dirtostr( D_IN )   == 'D_IN'"    ), 
      ASSERT(      "dirtostr( D_OUT )  == 'D_OUT'"   ), 
      ASSERT(      "dirtostr( D_BOTH ) == 'D_BOTH'"  ), 
      UNTRUE(     "dirtostr( D_IN )   == 'FALSE'"   ), 

      INTEGER( VGX_PREDICATOR_MOD_STATIC,         "M_STAT"    ),
      INTEGER( VGX_PREDICATOR_MOD_SIMILARITY,     "M_SIM"     ),
      INTEGER( VGX_PREDICATOR_MOD_DISTANCE,       "M_DIST"    ),
      INTEGER( VGX_PREDICATOR_MOD_LSH,            "M_LSH"     ),
      INTEGER( VGX_PREDICATOR_MOD_INTEGER,        "M_INT"     ),
      INTEGER( VGX_PREDICATOR_MOD_UNSIGNED,       "M_UINT"    ),
      INTEGER( VGX_PREDICATOR_MOD_FLOAT,          "M_FLT"     ),
      INTEGER( VGX_PREDICATOR_MOD_COUNTER,        "M_CNT"     ),
      INTEGER( VGX_PREDICATOR_MOD_ACCUMULATOR,    "M_ACC"     ),
      INTEGER( VGX_PREDICATOR_MOD_TIME_CREATED,   "M_TMC"     ),
      INTEGER( VGX_PREDICATOR_MOD_TIME_MODIFIED,  "M_TMM"     ),
      INTEGER( VGX_PREDICATOR_MOD_TIME_EXPIRES,   "M_TMX"     ),
      ASSERT(      "modtostr( M_STAT ) == 'M_STAT'"  ), 
      ASSERT(      "modtostr( M_SIM )  == 'M_SIM'"   ), 
      ASSERT(      "modtostr( M_DIST ) == 'M_DIST'"  ), 
      ASSERT(      "modtostr( M_LSH )  == 'M_LSH'"   ), 
      ASSERT(      "modtostr( M_INT )  == 'M_INT'"   ), 
      ASSERT(      "modtostr( M_UINT ) == 'M_UINT'"  ), 
      ASSERT(      "modtostr( M_FLT )  == 'M_FLT'"   ), 
      ASSERT(      "modtostr( M_CNT )  == 'M_CNT'"   ), 
      ASSERT(      "modtostr( M_ACC )  == 'M_ACC'"   ), 
      ASSERT(      "modtostr( M_TMC )  == 'M_TMC'"   ), 
      ASSERT(      "modtostr( M_TMM )  == 'M_TMM'"   ), 
      ASSERT(      "modtostr( M_TMX )  == 'M_TMX'"   ), 
      UNTRUE(      "modtostr( M_INT )  == 'FALSE'"   ), 

      ASSERT(              "graph.ts > 0"  ),
      ASSERT(              "graph.age > 0" ),
      INTEGER( order,      "graph.order"   ),
      INTEGER( size,       "graph.size"    ),
      INTEGER( opid,       "graph.op"      ),
      ASSERT(              "isnan(vector)" ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, constants, 50.0, NULL, NULL ) >= 0,   "Constants" );

    double now = _vgx_graph_milliseconds( graph ) / 1000.0;
    __test graph_ts[] = {
      REAL( now,   "graph.ts"  ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, graph_ts, 0.0, NULL, NULL ) >= 0,   "Graph Current Time" );

    // Literals
    __test literals[] = {
      XINTEGER(     0      ),
      XINTEGER(     1      ),
      XINTEGER(     -1     ),
      XINTEGER(     +1     ),
      INTEGER( 1125899906842624LL,    "1125899906842624"  ),
      INTEGER( -1125899906842624LL,   "-1125899906842624" ),

      XREAL(       0.0 ),
      XREAL(       1.0 ),
      XREAL(       -1.0 ),
      XREAL(       +1.0 ),
      XREAL(       12345678.0 ),
      XREAL(       12345678.91234 ),
      XREAL(      -12345678.0 ),
      XREAL(      -12345678.91234 ),
      XREAL(       1e9 ),
      XREAL(       1e-9 ),
      XREAL(       12e9 ),
      XREAL(       12e-9 ),
      XREAL(       1.2e9 ),
      XREAL(       1.2e-9 ),
      XREAL(       1.23e9 ),
      XREAL(       1.23e-9 ),
      XREAL(       1.23e+9 ),
      XREAL(      -1.23e-9 ),
      XREAL(      +1.23e+9 ),

      XINTEGER(   0x0       ),
      XINTEGER(   0x0000    ),
      XINTEGER(   0x1       ),
      XINTEGER(   0x0001    ),
      XINTEGER(   0x1000    ),
      XINTEGER(   -0x1000   ),

      XINTEGER(   0x123456789 ),
      XINTEGER(   0xabcdef    ),
      XINTEGER(   0xABCDEF    ),
      XINTEGER(   0x0123456789abcdef  ),

      STRING( "hello",  "'hello'" ),
      STRING( "",       "''" ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, literals, 0.0, NULL, NULL ) >= 0,   "Literals" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Bitwise
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Bitwise" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    __test bitwise[] = {
      // Unary NOT
      INTEGER(  -1,     "~0"    ),
      INTEGER(  0,     "~~0"    ),
      INTEGER(  -2,     "~1"    ),
      INTEGER(  1,     "~~1"    ),

      // Shift Left
      INTEGER(  0,    "0 << 0"  ),
      INTEGER(  0,    "0 << 1"  ),
      INTEGER(  0,    "0 << 2"  ),
      INTEGER(  1,    "1 << 0"  ),
      INTEGER(  2,    "1 << 1"  ),
      INTEGER(  4,    "1 << 2"  ),
      INTEGER(  8,    "1 << 3"  ),
      XINTEGER(      2 << 1     ),
      XINTEGER(      3 << 2     ),
      XINTEGER(      4 << 3     ),
      XINTEGER(      5 << 4     ),
      XINTEGER(      6 << 5     ),
      INTEGER(  1LL<<31,    "1 << 31" ),
      INTEGER(  1LL<<32,    "1 << 32" ),
      INTEGER(  1LL<<63,    "1 << 63" ),
      INTEGER(  113LL<<40,  "113 << 40" ),
      INTEGER(  1,    "1 << 64" ),
      INTEGER(  2,    "1 << 65" ),
      XINTEGER(  1 << 2 << 3 << 4 ),
      XINTEGER(  1 << 2 << 3 ),
      XINTEGER(  (1 << 2) << 3 ),
      XINTEGER(  1 << (2 << 3) ),
      XINTEGER(  (1 << 2 << 3) ),

      // Shift Right
      INTEGER(  0,    "0 >> 0"  ),
      INTEGER(  0,    "0 >> 1"  ),
      INTEGER(  0,    "0 >> 2"  ),
      INTEGER(  1,    "1 >> 0"  ),
      INTEGER(  0,    "1 >> 1"  ),
      INTEGER(  0,    "1 >> 2"  ),
      INTEGER(  0,    "1 >> 63" ),
      INTEGER(  1,    "1 >> 64" ),
      INTEGER(  0,    "1 >> 65" ),
      XINTEGER(     112 >> 1     ),
      XINTEGER(     113 >> 2     ),
      XINTEGER(     114 >> 3     ),
      XINTEGER(     115 >> 4     ),
      XINTEGER(     116 >> 5     ),
      XINTEGER(   1000000 >> 3 >> 2     ),
      XINTEGER(   (1000000 >> 3) >> 2   ),
      XINTEGER(   1000000 >> (3 >> 2)   ),
      XINTEGER(   (1000000 >> 3 >> 2)   ),

      // Bitwise OR
      INTEGER(  0x00,    "0x00 | 0x00"  ),
      INTEGER(  0x01,    "0x00 | 0x01"  ),
      INTEGER(  0x11,    "0x10 | 0x01"  ),
      INTEGER(  0x01,    "0x01 | 0x01"  ),
      INTEGER(  0xFF,    "0x0F | 0xF0"  ),
      XINTEGER(       0xabcdef | 0x123456789   ),
      XINTEGER(       0x123456789 | 0xabcdef   ),
      XINTEGER(       0x8000000000000000 | 0x1 ),
      XINTEGER(      111  |  222  |  333    ),
      XINTEGER(     (111  |  222) |  333    ),
      XINTEGER(      111  | (222  |  333)   ),
      XINTEGER(     (111  |  222  |  333)   ),

      // Bitwise XOR
      INTEGER(  0x00,    "0x00 ^ 0x00"  ),
      INTEGER(  0x01,    "0x00 ^ 0x01"  ),
      INTEGER(  0x11,    "0x10 ^ 0x01"  ),
      INTEGER(  0x00,    "0x01 ^ 0x01"  ),
      INTEGER(  0xFF,    "0x0F ^ 0xF0"  ),
      INTEGER(  0xE7,    "0x1F ^ 0xF8"  ),
      XINTEGER(       0xabcdef ^ 0x123456789   ),
      XINTEGER(       0x123456789 ^ 0xabcdef   ),
      XINTEGER(       0x8000000000000000 ^ 0x1 ),
      XINTEGER(      111  ^  222  ^  333    ),
      XINTEGER(     (111  ^  222) ^  333    ),
      XINTEGER(      111  ^ (222  ^  333)   ),
      XINTEGER(     (111  ^  222  ^  333)   ),

      // Bitwise AND
      INTEGER(  0x00,    "0x00 & 0x00"  ),
      INTEGER(  0x00,    "0x00 & 0x01"  ),
      INTEGER(  0x00,    "0x10 & 0x01"  ),
      INTEGER(  0x01,    "0x01 & 0x01"  ),
      INTEGER(  0xA0,    "0xFF & 0xA0"  ),
      XINTEGER(       0xabcdef & 0x123456789   ),
      XINTEGER(       0x123456789 & 0xabcdef   ),
      XINTEGER(      111  &  222   & 333    ),
      XINTEGER(     (111  &  222)  & 333    ),
      XINTEGER(      111  & (222   & 333)   ),
      XINTEGER(     (111  &  222   & 333)   ),

      // Combinations
      XINTEGER(    111  |  222  &  333  ^  444   ),
      XINTEGER(    111  |  222  ^  333  &  444   ),
      XINTEGER(    111  &  222  ^  333  |  444   ),
      XINTEGER(    111  &  222  |  333  ^  444   ),
      XINTEGER(    111  ^  222  |  333  &  444   ),
      XINTEGER(    111  ^  222  &  333  |  444   ),
      XINTEGER(   (111  |  222) &  333  ^  444   ),
      XINTEGER(    111  | (222  &  333) ^  444   ),
      XINTEGER(    111  |  222  & (333  ^  444)  ),
      XINTEGER(   (111  |  222  &  333) ^  444   ),
      XINTEGER(    111  | (222  &  333  ^  444)  ),
      XINTEGER(   (111  |  222) & (333  ^  444)  ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, bitwise, 0.0, NULL, NULL ) >= 0,   "Bitwise" );


  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Cast
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Cast" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    float tail_mag = CALLABLE( arc->tail->vector )->Magnitude( arc->tail->vector );
    int tail_len = CALLABLE( arc->tail->vector )->Length( arc->tail->vector );

    __test cast[] = {
      INTEGER( 0,         "int( 0 )"              ),
      INTEGER( 1,         "int( 1 )"              ),
      INTEGER( -1,        "int( -1 )"             ),
      INTEGER( 0,         "int( 0.7 )"            ),
      INTEGER( 1,         "int( 1.7 )"            ),
      INTEGER( -1,        "int( -1.7 )"           ),
      INTEGER( LLONG_MAX, "int( -1/-0.0 )"        ),
      INTEGER( LLONG_MIN, "int( 1/-0.0 )"         ),
      INTEGER( 0,         "int( vertex['nope'] )" ),
      ASSERT(             "int( vertex ) == vertex.address" ), // address
      ASSERT(             "s1 = '123'; s2 = '123'; int(s1) != int(s2)"   ), // address
      ASSERT(             "int( '123' ) notin { 0, 123 }"   ), // address
      ASSERT(             "int( vertex.vector ) > 1000"     ), // address

      INTEGER( 0,         "asint( 0 )"          ),
      INTEGER( 1,         "asint( 1 )"          ),
      INTEGER( -1,        "asint( -1 )"         ),
      ASSERT(             "asint( 1.0 ) == 0x3ff0000000000000"      ),
      ASSERT(             "asint( asin(2) ) in {0xfff8000000000000, 0x7ff8000000000000}"  ), // nan
      ASSERT(             "asint( -1/-0.0 ) == 0x7ff0000000000000"  ), // inf
      ASSERT(             "asint( 1/-0.0 ) == 0xfff0000000000000"   ), // -inf
      INTEGER( 0,         "asint( vertex['nope'] )"                 ),
      ASSERT(             "asint( vertex ) == vertex.address"       ),
      INTEGER( 123,       "asint( '123' )"              ),
      INTEGER( 123,       "asint( '123.1' )"            ),
      INTEGER( 255,       "asint( '0xff' )"             ),
      INTEGER( 0,         "asint( 'hello' )"            ),
      INTEGER( tail_len,  "asint( vertex.vector )"      ), // length

      INTEGER( 0,         "asbits( 0 )"          ),
      INTEGER( 1,         "asbits( 1 )"          ),
      INTEGER( -1,        "asbits( -1 )"         ),
      ASSERT(             "asbits( 1.0 ) == 0x3ff0000000000000"      ),
      ASSERT(             "asbits( asin(2) ) in {0xfff8000000000000, 0x7ff8000000000000}"  ), // nan
      ASSERT(             "asbits( -1/-0.0 ) == 0x7ff0000000000000"  ), // inf
      ASSERT(             "asbits( 1/-0.0 ) == 0xfff0000000000000"   ), // -inf
      INTEGER( 0,         "asbits( vertex['nope'] )"                 ),
      ASSERT(             "asbits( vertex ) == vertex.address"       ),
      ASSERT(             "s = '123'; asbits( s ) == int( s )"       ), // address
      ASSERT(             "s = '123.1'; asbits( s ) == int( s )"     ), // address
      ASSERT(             "s = '0xff'; asbits( s ) == int( s )"      ), // address
      ASSERT(             "s = 'hello'; asbits( s ) == int( s )"     ), // address
      ASSERT(             "asbits( vertex.vector ) == int( vertex.vector )"      ), // address

      REAL(    0.0,       "real( 0 )"                   ),
      REAL(    1.0,       "real( 1 )"                   ),
      REAL(   -1.0,       "real( -1 )"                  ),
      REAL(    0.0,       "real( 0.0 )"                 ),
      REAL(    0.1,       "real( 0.1 )"                 ),
      REAL(   -0.1,       "real( -0.1 )"                ),
      ASSERT(             "isinf( real( -1/-0.0 ) )"    ),
      ASSERT(             "isnan( real( asin(2) ) )"    ),
      REAL(    0.0,       "real( vertex['nope'] )"      ),
      ASSERT(             "real( vertex ) == vertex.address"    ), // address
      ASSERT(             "real( '123' ) notin { 0.0, 123.0 }"  ), // address
      ASSERT(             "real( vertex.vector ) > 1000"        ), // address
      INTEGER( 123,       "int( real( 123 ) )"          ),
      REAL(    123.0,     "real( int( 123.456 ) )"      ),


      REAL(    0.0,       "asreal( 0.0 )"                   ),
      REAL(    1.0,       "asreal( 1.0 )"                   ),
      REAL(   -1.0,       "asreal( -1.0 )"                  ),
      REAL(    1.0,       "asreal( 0x3ff0000000000000 )"    ),
      ASSERT(             "isnan( asreal( 0xfff8000000000000 ) )"  ),
      ASSERT(             "isinf( asreal( 0x7ff0000000000000 ) )"  ),
      ASSERT(             "isinf( asreal( 0xfff0000000000000 ) )"  ),
      REAL(    0.0,       "asreal( vertex['nope'] )"        ),
      ASSERT(             "asreal( vertex ) == vertex.address"     ), // address
      REAL(    123.0,     "asreal( '123' )"                 ),
      ASSERT(             "asreal( '1000000000000000000000' ) / 1e21 == 1"  ),
      UNTRUE(             "asreal( '1001000000000000000000' ) / 1e21 == 1"  ),
      REAL(    123.0,     "asreal( '123.0' )"               ),
      ASSERT(             "asreal( '123.1' ) / 123.1 == 1"  ),
      ASSERT(             "asreal( '-123.1' ) / -123.1 == 1" ),
      ASSERT(             "asreal( '1e21' ) / 1e21 == 1"    ),
      UNTRUE(             "asreal( '1.1e21' ) / 1e21 == 1"  ),
      ASSERT(             "isinf( asreal( 'inf' ) )"        ),
      ASSERT(             "isinf( asreal( '-inf' ) )"       ),
      ASSERT(             "isnan( asreal( 'hello' ) )"      ),
      REAL(    tail_mag,  "asreal( vertex.vector )"         ), // magnitude


      REAL(   1234567.0,  "asreal( asint( 1234567.0 ) )"  ),
      REAL(   M_PI,       "asreal( asint( pi ) )"         ),
      REAL(   sqrt(2),    "asreal( asint( sqrt(2) ) )"    ),

      // string
      STRING( "1234",         "str( 1234 )"                   ),
      ASSERT(                 "str( 3.14 ) == '3.14*'"        ),
      STRING( "ROOT",         "str( vertex.id )"              ),
      ASSERT(                 "len( str( vertex ) ) == 32"    ),
      ASSERT(                 "str( vertex ) == str( vertex.internalid )"  ),
      STRING( "B",            "str( next.id )"                ),
      ASSERT(                 "len( str( next ) ) == 32"      ),
      ASSERT(                 "str( next ) == str( next.internalid )"  ),
      STRING( "null",         "str( null )"                   ),
      STRING( "inf",          "str( exp( 10000 ) )"           ),
      STRING( "{1:2.00000}",  "str( keyval( 1, 2 ) )"         ),
      ASSERT(                 "str( vertex.vector ) == '<vector @*' "  ),
      
      // bitvector
      ASSERT(             "bitvector( 1000 ) == 1000"           ),
      ASSERT(             "isbitvector( bitvector( 1000 ) )"    ),
      UNTRUE(             "isbitvector( 1000 )"                 ),

      // bytes
      ASSERT(             "bytes() == b''"                      ),
      ASSERT(             "b'' == bytes()"                      ),
      ASSERT(             "bytes() == bytes()"                  ),
      ASSERT(             "bytes(1) != bytes()"                 ),
      ASSERT(             "bytes(0) == b'\\x00'"                ),
      ASSERT(             "bytes(1) == b'\\x01'"                ),
      ASSERT(             "bytes(2) != b'\\x01'"                ),
      ASSERT(             "bytes(0x10,0x20,0x30) == b'\\x10\\x20\\x30'" ),
      ASSERT(             "bytes(255) == b'\\xff'"              ),
      ASSERT(             "bytes(256) == b'\\x00'"              ),

      // keyval
      ASSERT(             "iskeyval( keyval( 123, pi ) )"       ),
      UNTRUE(             "iskeyval( 123 )"                     ),
      ASSERT(             "int( keyval( 123, pi ) ) == 123"     ),
      ASSERT(             "real( keyval( 123, pi ) ) == pi"     ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, cast, 0.0, NULL, NULL ) >= 0,   "Cast" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Conditional
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Conditional" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    
    __test conditional[] = {
      // Basics
      // ==
      ASSERT(  "0 == 0"     ),
      UNTRUE(  "0 == 1"     ),
      UNTRUE(  "1 == 0"     ),
      ASSERT(  "1 == 1"     ),
      // !=
      UNTRUE(  "0 != 0"     ),
      ASSERT(  "0 != 1"     ),
      ASSERT(  "1 != 0"     ),
      UNTRUE(  "1 != 1"     ),
      // >
      UNTRUE(  "0 > 0"      ),
      UNTRUE(  "0 > 1"      ),
      ASSERT(  "1 > 0"      ),
      UNTRUE(  "1 > 1"      ),
      // >=
      ASSERT(  "0 >= 0"     ),
      UNTRUE(  "0 >= 1"     ),
      ASSERT(  "1 >= 0"     ),
      ASSERT(  "1 >= 1"     ),
      // <
      UNTRUE(  "0 < 0"      ),
      ASSERT(  "0 < 1"      ),
      UNTRUE(  "1 < 0"      ),
      UNTRUE(  "1 < 1"      ),
      // <=
      ASSERT(  "0 <= 0"     ),
      ASSERT(  "0 <= 1"     ),
      UNTRUE(  "1 <= 0"     ),
      ASSERT(  "1 <= 1"     ),

      UNTRUE(  "0.9 == 1"   ),
      ASSERT(  "1.0 == 1"   ),
      UNTRUE(  "1.1 == 1"   ),

      ASSERT(  "1 != 0.9"   ),
      UNTRUE(  "1 != 1.0"   ),
      ASSERT(  "1 != 1.1"   ),

      UNTRUE(  "0.9 >  1"   ),
      UNTRUE(  "0.9 >= 1"   ),
      UNTRUE(  "1.0 >  1"   ),
      ASSERT(  "1.0 >= 1"   ),
      ASSERT(  "1.1 >  1"   ),
      ASSERT(  "1.1 >= 1"   ),
      UNTRUE(  "1.1 >  1.1" ),
      ASSERT(  "1.1 >= 1.1" ),
      UNTRUE(  "1.1 >  1.2" ),
      UNTRUE(  "1.1 >= 1.2" ),

      UNTRUE(  "1   <  0.9" ),
      UNTRUE(  "1   <= 0.9" ),
      UNTRUE(  "1   <  1.0" ),
      ASSERT(  "1   <= 1.0" ),
      ASSERT(  "1   <  1.1" ),
      ASSERT(  "1   <= 1.1" ),
      UNTRUE(  "1.1 <  1.1" ),
      ASSERT(  "1.1 <= 1.1" ),
      UNTRUE(  "1.2 <  1.1" ),
      UNTRUE(  "1.2 <= 1.1" ),

      XINTEGER(  10  >  5  ==  5  <  2    ),
      XINTEGER(  10  >  5  !=  5  <  2    ),
      XINTEGER( (10  >  5) == (5  <  2)   ),
      XINTEGER( (10  >  5) != (5  <  2)   ),
      
      #if defined(__clang__)
      #pragma clang diagnostic push
      #pragma clang diagnostic ignored "-Wparentheses"
      #endif
      XINTEGER(  10  > (5  ==  5) <  2    ),
      XINTEGER(  10  > (5  !=  5) <  2    ),
      #if defined(__clang__)
      #pragma clang diagnostic pop
      #endif

      XINTEGER( (10  >  5  ==  5) <  2    ),
      XINTEGER(  10  > (5  !=  5  <  2)   ),

      XINTEGER(((10  >  5) ==  5) <  2    ),
      XINTEGER(  10  > ((5  != 5) <  2)   ),

      ASSERT(   "  ''  == ''  " ),
      UNTRUE(   "  ''  == 'x' " ),
      ASSERT(   "  'x' == 'x' " ),
      UNTRUE(   "  'x' == ''  " ),
      ASSERT(   "  str('')  == str('')  " ),
      UNTRUE(   "  str('')  == str('x') " ),
      ASSERT(   "  str('x') == str('x') " ),
      UNTRUE(   "  str('x') == str('')  " ),


      // Ternary conditional
      INTEGER( 5,      " 2 > 1 ? 5 : 3 "      ),
      INTEGER( 3,      " 2 < 1 ? 5 : 3 "      ),
      INTEGER( 6,      " 2 > 1 ? 5+1 : 3-1 "  ),
      INTEGER( 2,      " 2 < 1 ? 5+1 : 3-1 "  ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? 100 * 3 + 1 : 1 - 200 * 4 ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? 100 * 3 + 1 : 1 - 200 * 4 ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? 10 > 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 > 6 ? 5 : 4 ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? 10 > 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 > 6 ? 5 : 4 ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? 10 < 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 > 6 ? 5 : 4 ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? 10 < 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 > 6 ? 5 : 4 ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? 10 > 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 < 6 ? 5 : 4 ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? 10 > 9 ? 17 : 15 * 3 + 1 : 1 - 200 * 7 < 6 ? 5 : 4 ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? (10 < 9 ? 17 : 15 * 3 + 1) : (1 - 200 * 7 < 6 ? 5 : 4) ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? (10 < 9 ? 17 : 15 * 3 + 1) : (1 - 200 * 7 < 6 ? 5 : 4) ),
      XINTEGER(     1.1 > 0.9 + 0.1 ? (10 < 9 ? 17 : 15) * 3 + 1 : 1 - (200 * 7 < 6 ? 5 : 4) ),
      XINTEGER(     1.1 < 0.9 + 0.1 ? (10 < 9 ? 17 : 15) * 3 + 1 : 1 - (200 * 7 < 6 ? 5 : 4) ),
      INTEGER( 7,      "0?1:0?2:0?3:0?4:0?5:0?6:7"  ),
      INTEGER( 6,      "0?1:0?2:0?3:0?4:0?5:1?6:7"  ),
      INTEGER( 5,      "0?1:0?2:0?3:0?4:1?5:1?6:7"  ),
      INTEGER( 4,      "0?1:0?2:0?3:1?4:1?5:1?6:7"  ),
      INTEGER( 3,      "0?1:0?2:1?3:1?4:1?5:1?6:7"  ),
      INTEGER( 2,      "0?1:1?2:1?3:1?4:1?5:1?6:7"  ),
      INTEGER( 1,      "1?1:1?2:1?3:1?4:1?5:1?6:7"  ),

      // Contains
      ASSERT(   "'prop1' in next"         ),
      ASSERT(   "'prop2' in next"         ),
      ASSERT(   "'prop3' in next"         ),
      ASSERT(   "'propS' in next"         ),
      UNTRUE(   "'nix'   in next"         ),
      UNTRUE(   "'prop1' in vertex"       ),
      UNTRUE(   "'prop2' in vertex"       ),
      UNTRUE(   "'prop3' in vertex"       ),
      UNTRUE(   "'propS' in vertex"       ),
      UNTRUE(   "'nix'   in vertex"       ),
      UNTRUE(   "'zeroth_dim' in vertex.vector"  ),
      UNTRUE(   "'zeroth_dim' in next.vector"    ),
      UNTRUE(   "'first_dim' in vertex.vector"   ),
      ASSERT(   "'first_dim' in next.vector"     ),
      ASSERT(   "'second_dim' in vertex.vector"  ),
      ASSERT(   "'second_dim' in next.vector"    ),
      ASSERT(   "'third_dim' in vertex.vector"   ),
      ASSERT(   "'third_dim' in next.vector"     ),
      ASSERT(   "'fourth_dim' in vertex.vector"  ),
      ASSERT(   "'fourth_dim' in next.vector"    ),
      UNTRUE(   "'fifth_dim' in vertex.vector"   ),
      ASSERT(   "'fifth_dim' in next.vector"     ),
      ASSERT(   "'R'    in vertex.id"      ),
      UNTRUE(   "'R'    in next.id"        ),
      ASSERT(   "'OO'   in vertex.id"      ),
      UNTRUE(   "'OO'   in next.id"        ),
      ASSERT(   "'ROOT' in vertex.id"      ),
      UNTRUE(   "'ROOT' in next.id"        ),
      UNTRUE(   "'B'    in vertex.id"      ),
      ASSERT(   "'B'    in next.id"        ),
      UNTRUE(   "'BOO'  in vertex.id"      ),
      UNTRUE(   "'BOO'  in next.id"        ),

      UNTRUE(   "5 in 100"                 ),
      UNTRUE(   "5.1 in 100"               ),
      UNTRUE(   "vertex in 100"            ),
      UNTRUE(   "vertex.id in 100"         ),
      UNTRUE(   "vertex.vector in 100"     ),
      UNTRUE(   "'hello' in 100"           ),
      UNTRUE(   "'100' in 100"             ),

      UNTRUE(   "5 in 1.23"                ),
      UNTRUE(   "5.1 in 1.23"              ),
      UNTRUE(   "vertex in 1.23"           ),
      UNTRUE(   "vertex.id in 1.23"        ),
      UNTRUE(   "vertex.vector in 1.23"    ),
      UNTRUE(   "'hello' in 1.23"          ),
      UNTRUE(   "'100' in 1.23"            ),

      UNTRUE(   "5 in vertex"              ),
      UNTRUE(   "5.1 in vertex"            ),
      UNTRUE(   "vertex in vertex"         ),
      UNTRUE(   "vertex.id in vertex"      ),
      UNTRUE(   "vertex.vector in vertex"  ),
      UNTRUE(   "'hello' in vertex"        ),
      UNTRUE(   "'100' in vertex"          ),

      UNTRUE(   "5 in vertex.id"              ),
      UNTRUE(   "5.1 in vertex.id"            ),
      UNTRUE(   "vertex in vertex.id"         ),
      UNTRUE(   "vertex.id in vertex.id"      ),
      UNTRUE(   "vertex.vector in vertex.id"  ),
      UNTRUE(   "'hello' in vertex.id"        ),
      UNTRUE(   "'100' in vertex.id"          ),

      UNTRUE(   "5 in vertex.vector"              ),
      UNTRUE(   "5.1 in vertex.vector"            ),
      UNTRUE(   "vertex in vertex.vector"         ),
      UNTRUE(   "vertex.id in vertex.vector"      ),
      UNTRUE(   "vertex.vector in vertex.vector"  ),
      UNTRUE(   "'hello' in vertex.vector"        ),
      UNTRUE(   "'100' in vertex.vector"          ),

      UNTRUE(   "5 in 'hello'"                    ),
      UNTRUE(   "5.1 in 'hello'"                  ),
      UNTRUE(   "vertex in 'hello'"               ),
      UNTRUE(   "vertex.id in 'hello'"            ),
      UNTRUE(   "vertex.vector in 'hello'"        ),
      ASSERT(   "'hello' in 'hello'"              ),  // true!
      UNTRUE(   "'100' in 'hello'"                ),

      UNTRUE(   "5 in '100'"                    ),
      UNTRUE(   "5.1 in '100'"                  ),
      UNTRUE(   "vertex in '100'"               ),
      UNTRUE(   "vertex.id in '100'"            ),
      UNTRUE(   "vertex.vector in '100'"        ),
      UNTRUE(   "'hello' in '100'"              ),
      ASSERT(   "'100' in '100'"                ), // true!

      // NaN always compares false with less/greater than
      UNTRUE(   "vertex['nix'] < 0"             ),
      UNTRUE(   "vertex['nix'] <= 0"            ),
      UNTRUE(   "vertex['nix'] > 0"             ),
      UNTRUE(   "vertex['nix'] >= 0"            ),
      UNTRUE(   "vertex['nix'] < 1.0"           ),
      UNTRUE(   "vertex['nix'] <= 1.0"          ),
      UNTRUE(   "vertex['nix'] > 1.0"           ),
      UNTRUE(   "vertex['nix'] >= 1.0"          ),

      // Equality
      UNTRUE(   "vertex['nix'] == 0"            ),
      UNTRUE(   "vertex['nix'] == 1.0"          ),
      ASSERT(   "vertex['nix'] == null"         ),
      ASSERT(   "isnan( vertex['nix'] )"        ),
      // Inequality
      ASSERT(   "vertex['nix'] != 0"            ),
      ASSERT(   "vertex['nix'] != 1.0"          ),
      UNTRUE(   "vertex['nix'] != null"         ),

      // Set Contains 
      ASSERT(     "1 in {1}"             ),
      ASSERT(     "1 in {1.0}"           ),
      ASSERT(     "1.0 in {1}"           ),
      ASSERT(     "1.0 in {1.0}"         ),

      UNTRUE(     "0 in {1,2,3,4,5,6}"   ),
      ASSERT(     "1 in {1,2,3,4,5,6}"   ),
      ASSERT(     "2 in {1,2,3,4,5,6}"   ),
      ASSERT(     "3 in {1,2,3,4,5,6}"   ),
      ASSERT(     "4 in {1,2,3,4,5,6}"   ),
      ASSERT(     "5 in {1,2,3,4,5,6}"   ),
      ASSERT(     "6 in {1,2,3,4,5,6}"   ),
      UNTRUE(     "7 in {1,2,3,4,5,6}"   ),

      ASSERT(     "7    in {9,2,5,4,6, 7 ,1,0,9,8, 3.1, 1}"     ),
      ASSERT(     "7.0  in {9,2,5,4,6, 7 ,1,0,9,8, 3.1, 1}"     ),
      UNTRUE(     "3    in {9,2,5,4,6, 7 ,1,0,9,8, 3.1, 1}"     ),
      ASSERT(     "3.1  in {9,2,5,4,6, 7 ,1,0,9,8, 3.1, 1}"     ),

      ASSERT(     "'yes'       in { 'yes', 'no', 'hello', 'maybe' } " ),
      ASSERT(     "'no'        in { 'yes', 'no', 'hello', 'maybe' } " ),
      ASSERT(     "'hello'     in { 'yes', 'no', 'hello', 'maybe' } " ),
      ASSERT(     "'maybe'     in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "'nope'      in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "7           in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "1.0         in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "next        in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "next.id     in { 'yes', 'no', 'hello', 'maybe' } " ),
      UNTRUE(     "next.vector in { 'yes', 'no', 'hello', 'maybe' } " ),

      ASSERT(      "'test this string' == '*' " ),
      ASSERT(      "'test this string' == 't*' " ),
      ASSERT(      "'test this string' != 'e*' " ),
      ASSERT(      "'test this string' == 'te*' " ),
      ASSERT(      "'test this string' != 'es*' " ),
      ASSERT(      "'test this string' == 'tes*' " ),
      ASSERT(      "'test this string' == '*g' " ),
      ASSERT(      "'test this string' != '*n' " ),
      ASSERT(      "'test this string' == '*ng' " ),
      ASSERT(      "'test this string' != '*in' " ),
      ASSERT(      "'test this string' == '*ing' " ),
      ASSERT(      "'test this string' == 't*g' " ),
      ASSERT(      "'test this string' != 'ts*g' " ),
      ASSERT(      "'test this string' != 't*ig' " ),
      ASSERT(      "'test this string' == 'te*ng' " ),
      ASSERT(      "'test this string' == '*test this string*' " ),
      ASSERT(      "'test this string' == '*test this strin*' " ),
      ASSERT(      "'test this string' == '*est this string*' " ),
      ASSERT(      "'test this string' == '*est this strin*' " ),
      ASSERT(      "'test this string' == '*st this stri*' " ),
      ASSERT(      "'test this string' == '*t this str*' " ),
      ASSERT(      "'test this string' == '* this st*' " ),
      ASSERT(      "'test this string' != '* thiS st*' " ),
      ASSERT(      "'test this string' == '*this s*' " ),
      ASSERT(      "'test this string' == '*his *' " ),
      ASSERT(      "'test this string' == '*is*' " ),
      ASSERT(      "'test this string' == '*i*' " ),
      ASSERT(      "'test this string' != '*j*' " ),
      ASSERT(      "'*' == 'test this string'" ),
      ASSERT(      "'t*' == 'test this string'" ),
      ASSERT(      "'e*' != 'test this string'" ),
      ASSERT(      "'*g' == 'test this string'" ),
      ASSERT(      "'*n' != 'test this string'" ),
      ASSERT(      "'t*g' == 'test this string'" ),
      ASSERT(      "'ts*g' != 'test this string'" ),
      ASSERT(      "'te*ng' == 'test this string'" ),
      ASSERT(      "'*test this string*' == 'test this string'" ),
      ASSERT(      "'*st this str*' == 'test this string'" ),
      ASSERT(      "'* thiS st*' != 'test this string'" ),
      ASSERT(      "'*his*' == 'test this string'" ),
      ASSERT(      "'*i*' == 'test this string'" ),
      ASSERT(      "'*j*' != 'test this string'" ),

      ASSERT(      "'test this string'  notin { 'test', 'this', 'string' } " ),
      ASSERT(      "'test this string'  in { 'test', 'this', '*string*' } " ),
      ASSERT(      "'test this string'  in { 'test', '*this*', 'string' } " ),
      ASSERT(      "'test this string'  in { '*test*', 'this', 'string' } " ),
      ASSERT(      "'test this string'  notin { '*tst*', '*ths*', '*sring*' } " ),
      ASSERT(      "'test this string'  in { 'test*', 'this', 'string' } " ),
      ASSERT(      "'test this string'  notin { 'test', 'this*', 'string' } " ),
      ASSERT(      "'test this string'  notin { 'test', 'this', 'string*' } " ),
      ASSERT(      "'test this string'  in { 'test', 'this', '*string' } " ),
      ASSERT(      "'test this string'  in { 'test*string', 'this', 'string' } " ),
      ASSERT(      "'test this string'  in { 'test', 'this', 'test*string' } " ),

      ASSERT(      "1               in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "1.0             in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "2               in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "2.0             in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "vertex          in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "'hello'         in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "'hel*'          in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "vertex.vector   in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "'hi'            in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "vertex.id       in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "'ROOT'          in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "3               in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(      "3.0             in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),

      UNTRUE(     "1.001           in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "2.001           in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "next            in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "'hellow'        in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "next.vector     in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "' hi'           in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "next.id         in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      UNTRUE(     "3.001           in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),

      ASSERT(     "3  in { 3, 8+2, 7 }"   ),
      ASSERT(     "10 in { 3, 8+2, 7 }"   ),
      ASSERT(     "7  in { 3, 8+2, 7 }"   ),
      UNTRUE(     "8  in { 3, 8+2, 7 }"   ),
      UNTRUE(     "2  in { 3, 8+2, 7 }"   ),

      ASSERT(     "8.0 in { 3, log2(256), 7 }"   ),
      ASSERT(     "9.0 in { 3, 1 + log2(256), 7 }"   ),
      ASSERT(     "9.0 in { 3, 1 + log2( 2 ** 8 ), 7 }"   ),
      ASSERT(     "9.0 in { 3, 1 + log2( (357 - 1) - sum( 50, 20, 30 ) ), 7 }"   ),

      REAL( 3.0,           "log2( 4 * ('hello' in { 'this', 'hello', 123, 'that' }) - (7 in {6}) + 16 / ((next.id in { 1, 2, 'B', next.vector }) * 4) )"   ),

      ASSERT(     "10    in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      ASSERT(     "100   in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      ASSERT(     "1000  in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      UNTRUE(     "50    in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      UNTRUE(     "15    in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      UNTRUE(     "3     in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      UNTRUE(     "1     in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),
      UNTRUE(     "2     in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }" ),

      INTEGER(  11,        "sum( 1, 2, 3, (100 in { 10, ( 50 + sum( 20, 15, 3 * sum(1,2,1,1) ) ), 1000 }), 4 )" ),


      // Value in range
      UNTRUE(     "-1e10   in range( 0, 2 )" ),
      UNTRUE(     "-2      in range( 0, 2 )" ),
      UNTRUE(     "-1      in range( 0, 2 )" ),
      UNTRUE(     "-0.01   in range( 0, 2 )" ),
      ASSERT(     "0       in range( 0, 2 )" ),
      ASSERT(     "0.0     in range( 0, 2 )" ),
      ASSERT(     "0.01    in range( 0, 2 )" ),
      ASSERT(     "1.99    in range( 0, 2 )" ),
      UNTRUE(     "2       in range( 0, 2 )" ),
      UNTRUE(     "2.0     in range( 0, 2 )" ),
      UNTRUE(     "2.01    in range( 0, 2 )" ),
      UNTRUE(     "3       in range( 0, 2 )" ),
      UNTRUE(     "1e10    in range( 0, 2 )" ),

      UNTRUE(     "-1e10   in range( 0.00001, 2 )" ),
      UNTRUE(     "-2      in range( 0.00001, 2 )" ),
      UNTRUE(     "-1      in range( 0.00001, 2 )" ),
      UNTRUE(     "-0.01   in range( 0.00001, 2 )" ),
      UNTRUE(     "0       in range( 0.00001, 2 )" ),
      UNTRUE(     "0.0     in range( 0.00001, 2 )" ),
      ASSERT(     "0.01    in range( 0.00001, 2 )" ),
      ASSERT(     "1.99    in range( 0.00001, 2 )" ),
      UNTRUE(     "2       in range( 0.00001, 2 )" ),
      UNTRUE(     "2.0     in range( 0.00001, 2 )" ),
      UNTRUE(     "2.01    in range( 0.00001, 2 )" ),
      UNTRUE(     "3       in range( 0.00001, 2 )" ),
      UNTRUE(     "1e10    in range( 0.00001, 2 )" ),

      UNTRUE(     "-1e10   in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "-2      in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "-1      in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "-0.01   in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "0       in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "0.0     in range( 0.00001, 1.99999 )" ),
      ASSERT(     "0.01    in range( 0.00001, 1.99999 )" ),
      ASSERT(     "1.99    in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "2       in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "2.0     in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "2.01    in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "3       in range( 0.00001, 1.99999 )" ),
      UNTRUE(     "1e10    in range( 0.00001, 1.99999 )" ),

      UNTRUE(     "-1e10   in range( 0, 1.99999 )" ),
      UNTRUE(     "-2      in range( 0, 1.99999 )" ),
      UNTRUE(     "-1      in range( 0, 1.99999 )" ),
      UNTRUE(     "-0.01   in range( 0, 1.99999 )" ),
      ASSERT(     "0       in range( 0, 1.99999 )" ),
      ASSERT(     "0.0     in range( 0, 1.99999 )" ),
      ASSERT(     "0.01    in range( 0, 1.99999 )" ),
      ASSERT(     "1.99    in range( 0, 1.99999 )" ),
      UNTRUE(     "2       in range( 0, 1.99999 )" ),
      UNTRUE(     "2.0     in range( 0, 1.99999 )" ),
      UNTRUE(     "2.01    in range( 0, 1.99999 )" ),
      UNTRUE(     "3       in range( 0, 1.99999 )" ),
      UNTRUE(     "1e10    in range( 0, 1.99999 )" ),

      UNTRUE(     "5       in range( 10, 0 )" ),

      INTEGER( 2,          "(-2.5 in range( -3, -2 )) + (-1000 in range( -1234.5, 0 ))" ),
      INTEGER( 2,          "-2.5 in range( -3, -2 ) + (-1000 in range( -1234.5, 0 ))" ),   // Mysterious crash here

      INTEGER( 0,          "(-2.5 in range( -3, -2 )) - (-1000 in range( -1234.5, 0 ))" ),
      INTEGER( 0,          "-2.5 in range( -3, -2 ) - (-1000 in range( -1234.5, 0 ))" ),
      INTEGER( 1,          "-2.5 in range( -3, -2 ) - - (1000 in range( -1234.5, 0 ))" ),

      INTEGER( 6,          "(-2.5 in range( -3, -2 )) + 5 * (-1000 in range( -1234.5, 0 ))" ),
      INTEGER( 11,         "10 * (-2.5 in range( -3, -2 )) + (-1000 in range( -1234.5, 0 ))" ),
      INTEGER( 15,         "10 * (-2.5 in range( -3, -2 )) + 5 * (-1000 in range( -1234.5, 0 ))" ),

      ASSERT(      "0.5 in range( 2 in range( 3, 8), 5 in range( 3, 8 ) )" ),
      UNTRUE(      "0.5 in range( 5 in range( 3, 8), 2 in range( 3, 8 ) )" ),


      // String comparisons
      ASSERT(   "'X' == 'X'"      ),
      UNTRUE(   "'X' != 'X'"      ),
      UNTRUE(   "'X' == 'Y'"      ),
      ASSERT(   "'X' != 'Y'"      ),

      {0}
    };

    // Vectors should not match with a high sim threshold
    graph->similarity->params.threshold.similarity = 0.99f;
    graph->similarity->params.threshold.hamming = 1;

    TEST_ASSERTION( __test_expressions( graph, conditional, 0.0, NULL, NULL ) >= 0,   "conditional" );

    // Vectors should match with a lower sim threshold
    graph->similarity->params.threshold.similarity = 0.6f;
    graph->similarity->params.threshold.hamming = 26;

    __test vec_match[] = {
      UNTRUE(     "next.vector     in { 'yes', 'no', 'hello', 'maybe' } " ),
      ASSERT(     "vertex.vector   in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ),
      ASSERT(     "next.vector     in { 1, 2.0, vertex, 'hello', vertex.vector, 'hi', vertex.id, 3 } " ), // this didn't match with sim threshold 0.99 but does now
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vec_match, 0.0, NULL, NULL ) >= 0,   "vector match in set" );

    __test rand_cond[] = {
      ASSERT(    "(random()-0.5>0 ? 1 : random()-0.5>0 ? 2 : random()-0.5>0 ? 3 : random()-0.5>0 ? 4 : random()-0.5>0 ? 5 : random()-0.5>0 ? 6 : random()-0.5>0 ? 7 : random()-0.5>0 ? 8 : random()-0.5>0 ? 9 : random()-0.5>0 ? 10 : random()-0.5>0 ? 11 : random()-0.5>0 ? 12 : random()-0.5>0 ? 13 : random()-0.5>0 ? 14 : random()-0.5>0 ? 15 : random()-0.5>0 ? 16 : random()-0.5>0 ? 17 : random()-0.5>0 ? 18 : random()-0.5>0 ? 19 : random()-0.5>0 ? 20 : random()-0.5>0 ? 21 : random()-0.5>0 ? 22 : random()-0.5>0 ? 23 : random()-0.5>0 ? 24 : random()-0.5>0 ? 25 : random()-0.5>0 ? 26 : random()-0.5>0 ? 27 : random()-0.5>0 ? 28 : random()-0.5>0 ? 29 : random()-0.5>0 ? 30 : 31) in {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}"  ),
      ASSERT(    "(random()-0.5>0 ? 1 : random()-0.5>0 ? 2 : random()-0.5>0 ? 3 : random()-0.5>0 ? 4 : random()-0.5>0 ? 5 : random()-0.5>0 ? 6 : random()-0.5>0 ? 7 : random()-0.5>0 ? 8 : random()-0.5>0 ? 9 : random()-0.5>0 ? 10 : random()-0.5>0 ? 11 : random()-0.5>0 ? 12 : random()-0.5>0 ? 13 : random()-0.5>0 ? 14 : random()-0.5>0 ? 15 : random()-0.5>0 ? 16 : random()-0.5>0 ? 17 : random()-0.5>0 ? 18 : random()-0.5>0 ? 19 : random()-0.5>0 ? 20 : random()-0.5>0 ? 21 : random()-0.5>0 ? 22 : random()-0.5>0 ? 23 : random()-0.5>0 ? 24 : random()-0.5>0 ? 25 : random()-0.5>0 ? 26 : random()-0.5>0 ? 27 : random()-0.5>0 ? 28 : random()-0.5>0 ? 29 : random()-0.5>0 ? 30 : 31) in range( 1, 32 )"  ),
      {0}
    };

    for( int n=0; n<10000; n++ ) {
      TEST_ASSERTION( __test_expressions( graph, rand_cond, 0.0, NULL, NULL ) >= 0,   "random conditional" );
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * String
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "String" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    float tail_mag = CALLABLE( arc->tail->vector )->Magnitude( arc->tail->vector );
    int tail_len = CALLABLE( arc->tail->vector )->Length( arc->tail->vector );

    __test string[] = {

      // Basic / concat
      STRING( "",             "''"                            ),
      STRING( "x",            "'x'"                           ),
      STRING( "xx",           "'x' + 'x'"                     ),
      STRING( "x1",           "'x' + 1"                       ),
      INTEGER( 1,             "isbitvector( 1 + 'x' )"        ),
      STRING( "1x",           "'' + 1 + 'x'"                  ),

      // strcmp
      INTEGER( 0,             "strcmp( '', '' )"              ),
      INTEGER( 0,             "strcmp( 'thing', 'thing' )"    ),
      INTEGER( -1,            "strcmp( 'thing1', 'thing2' )"  ),
      INTEGER( 1,             "strcmp( 'thing2', 'thing1' )"  ),
      ASSERT(          "strcmp( 'x',  123 ) == null"   ),
      ASSERT(          "strcmp( 123, 'x' ) == null"    ),
      ASSERT(          "strcmp( 123, 123 ) == null"    ),

      // strcasecmp
      ASSERT(          "strcasecmp( '', '' ) == 0"             ),
      ASSERT(          "strcasecmp( 'thing', 'thing' ) == 0"   ),
      ASSERT(          "strcasecmp( 'Thing', 'thing' ) == 0"   ),
      ASSERT(          "strcasecmp( 'thing', 'Thing' ) == 0"   ),
      ASSERT(          "strcasecmp( 'THIng', 'thING' ) == 0"   ),
      ASSERT(          "strcasecmp( 'thing1', 'thing2' ) < 0"  ),
      ASSERT(          "strcasecmp( 'Thing1', 'thing2' ) < 0"  ),
      ASSERT(          "strcasecmp( 'thing1', 'Thing2' ) < 0"  ),
      ASSERT(          "strcasecmp( 'thing2', 'thing1' ) > 0"  ),
      ASSERT(          "strcasecmp( 'Thing2', 'thing1' ) > 0"  ),
      ASSERT(          "strcasecmp( 'thing2', 'Thing1' ) > 0"  ),
      ASSERT(          "strcasecmp( 'x',  123 ) == null"       ),
      ASSERT(          "strcasecmp( 123, 'x' ) == null"        ),
      ASSERT(          "strcasecmp( 123, 123 ) == null"        ),

      // startswith
      ASSERT(          "startswith( 'Hello', '' )"             ),
      ASSERT(          "startswith( 'Hello', 'H' )"            ),
      ASSERT(          "startswith( 'Hello', 'He' )"           ),
      ASSERT(          "startswith( 'Hello', 'Hello' )"        ),
      UNTRUE(          "startswith( 'Hello', 'Hello!' )"       ),
      UNTRUE(          "startswith( 'Hello', 'ello' )"         ),
      ASSERT(          "startswith( 'Hello', 123 ) == null"    ),
      ASSERT(          "startswith( 123, 123 ) == null"        ),
      ASSERT(          "startswith( 123, 'Hello' ) == null"    ),

      // endswith
      ASSERT(          "endswith( 'Hello', '' )"             ),
      ASSERT(          "endswith( 'Hello', 'o' )"            ),
      ASSERT(          "endswith( 'Hello', 'lo' )"           ),
      ASSERT(          "endswith( 'Hello', 'Hello' )"        ),
      UNTRUE(          "endswith( 'Hello', '!Hello' )"       ),
      UNTRUE(          "endswith( 'Hello', 'Hell' )"         ),
      ASSERT(          "endswith( 'Hello', 123 ) == null"    ),
      ASSERT(          "endswith( 123, 123 ) == null"        ),
      ASSERT(          "endswith( 123, 'Hello' ) == null"    ),
     
      // Join
      STRING( "x,y",          "join( ',', 'x', 'y' )" ),
      STRING( "x",            "join( ',', 'x' )" ),
      STRING( "",             "join( ',' )" ),
      ASSERT(          "join() == null" ),
      STRING( "This function is variadic",        "join( ' ', 'This', 'function', 'is', 'variadic' )" ),
      STRING( "Implicit cast: 123 3.14159 ROOT",  "join( ' ', 'Implicit', 'cast:', 123, pi, vertex.id )" ),

      // Replace
      STRING( "This is output", "replace( 'This is input', 'input', 'output' )" ),
      STRING( "xtxexsxtx",      "replace( 'test', '', 'x' )" ),
      STRING( "test",           "replace( 'xtxexsxtx', 'x', '' )" ),

      // Slice
      STRING( "",             "slice( 'Hello', 0, 0 )" ),
      STRING( "H",            "slice( 'Hello', 0, 1 )" ),
      STRING( "He",           "slice( 'Hello', 0, 2 )" ),
      STRING( "Hel",          "slice( 'Hello', 0, 3 )" ),
      STRING( "Hell",         "slice( 'Hello', 0, 4 )" ),
      STRING( "Hello",        "slice( 'Hello', 0, 5 )" ),
      STRING( "Hello",        "slice( 'Hello', 0, 6 )" ),
      STRING( "Hello",        "slice( 'Hello', 0, null )" ),
      STRING( "Hello",        "slice( 'Hello', null, null )" ),
      STRING( "Hell",         "slice( 'Hello', null, -1 )" ),
      STRING( "Hel",          "slice( 'Hello', null, -2 )" ),
      STRING( "He",           "slice( 'Hello', null, -3 )" ),
      STRING( "H",            "slice( 'Hello', null, -4 )" ),
      STRING( "",             "slice( 'Hello', null, -5 )" ),
      STRING( "",             "slice( 'Hello', null, -6 )" ),
      STRING( "",             "slice( 'Hello', 1, 0 )" ),
      STRING( "",             "slice( 'Hello', 1, 1 )" ),
      STRING( "e",            "slice( 'Hello', 1, 2 )" ),
      STRING( "el",           "slice( 'Hello', 1, 3 )" ),
      STRING( "ell",          "slice( 'Hello', 1, 4 )" ),
      STRING( "ello",         "slice( 'Hello', 1, null )" ),
      STRING( "llo",          "slice( 'Hello', 2, null )" ),
      STRING( "lo",           "slice( 'Hello', 3, null )" ),
      STRING( "o",            "slice( 'Hello', 4, null )" ),
      STRING( "",             "slice( 'Hello', 5, null )" ),
      STRING( "Hello",        "slice( 'Hello', -5, null )" ),
      STRING( "ello",         "slice( 'Hello', -4, null )" ),
      STRING( "llo",          "slice( 'Hello', -3, null )" ),
      STRING( "lo",           "slice( 'Hello', -2, null )" ),
      STRING( "o",            "slice( 'Hello', -1, null )" ),
      STRING( "ell",          "slice( 'Hello', -4, -1 )" ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, string, 0.0, NULL, NULL ) >= 0,   "String" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Variables
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Variables" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    float tail_mag = CALLABLE( arc->tail->vector )->Magnitude( arc->tail->vector );
    int tail_len = CALLABLE( arc->tail->vector )->Length( arc->tail->vector );

    __test variables[] = {
      INTEGER( 5,         "a = 5"                     ),

      INTEGER( 5,         ";a = 5"                    ),
      INTEGER( 5,         "a = 5;"                    ),
      INTEGER( 5,         ";a = 5;"                   ),

      INTEGER( 5,         ";;a = 5"                   ),
      INTEGER( 5,         "a = 5;;"                   ),
      INTEGER( 5,         ";;a = 5;;"                 ),

      INTEGER( 5,         "a = 5; a"                  ),
      INTEGER( 5,         ";a = 5; a"                 ),
      INTEGER( 5,         ";;a = 5;; a"               ),
      INTEGER( 5,         ";;;a = 5;;; a;;;"          ),

      INTEGER( 17,         "a = 17; b = a"            ),
      INTEGER( 17,         ";a = 17; b = a"           ),
      INTEGER( 17,         ";a = 17;; b = a"          ),
      INTEGER( 17,         ";;a = 17; b = a"          ),


      INTEGER( 10,        "a = 5; a + a"              ),
      INTEGER( 10,        ";a = 5;; a + a"            ),
      INTEGER( 10,        ";;;a = 5;;;; a + a"        ),

      INTEGER( 50,        "a = 5; b = 10; c = a * b"            ),
      INTEGER( 50,        ";a = 5;; b = 10;;; c = a * b"        ),
      INTEGER( 50,        ";;a = 5;;;; b = 10;;;;;; c = a * b"  ),

      ASSERT(      "a = sqrt(2) + 1; b = ((a ** 2) - 1)/2; a == b"       ),
      ASSERT(      ";a = sqrt(2) + 1; b = ((a ** 2) - 1)/2; a == b"      ),
      ASSERT(      ";;a = sqrt(2) + 1; b = ((a ** 2) - 1)/2; a == b"     ),
      ASSERT(      "1;;a = sqrt(2) + 1; b = ((a ** 2) - 1)/2; a == b"    ),
      ASSERT(      ";;1;;a = sqrt(2) + 1; b = ((a ** 2) - 1)/2; a == b"  ),

      INTEGER( 4,         "one=1; two=2; three=3; four=4; mypi=3.14; test = ((two*three*mypi) / 6.28) == three; ok = four * test; ok"       ),
      INTEGER( 4,         ";one=1; two=2; three=3; four=4; mypi=3.14; test = ((two*three*mypi) / 6.28) == three; ok = four * test; ok"      ),
      INTEGER( 4,         ";one=1;; two=2; three=3; four=4; mypi=3.14; test = ((two*three*mypi) / 6.28) == three; ok = four * test; ok"     ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; test = ((two*three*mypi) / 6.28) == three; ok = four * test; ok"  ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three; ok = four * test; ok"           ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three; ok = four * test;;;; ok"        ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three;;;;;; ok = four * test;;;; ok"   ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three;;;sqrt(2);;; ok = four * test;;;; ok"            ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three;;;sqrt(2+1);;; ok = four * test;;;; ok"          ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three;;;sqrt(2+one);;; ok = four * test;;;; ok"        ),
      INTEGER( 4,         ";one=1;; two=2;;;; three=3; four=4; mypi=3.14; 1++++++++1;;;;++++++++1;;; test = ((two*three*mypi) / 6.28) == three;;;sqrt(2+one+three);;; ok = four * test;;;; ok"  ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, variables, 0.0, NULL, NULL ) >= 0,   "Variables" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Arc
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Arc" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test arctest[] = {
      ASSERT(    "next.arc.value    == 200"              ),
      UNTRUE(    "next.arc.value    != 200"              ),
      ASSERT(    "next.arc.value    != 199"              ),

      ASSERT(    "next.arc.dist   == 1"                ),
      UNTRUE(    "next.arc.dist   != 1"                ),
      ASSERT(    "next.arc.dist   != 2"                ),

      ASSERT(    "next.arc.type   == relenc( 'to' )"   ),
      UNTRUE(    "next.arc.type   != relenc( 'to' )"   ),
      ASSERT(    "next.arc.type   != relenc( 'rel' )"  ),

      ASSERT(    "reldec( relenc( 'to' ) ) == 'to'"    ),
      ASSERT(    "reldec( relenc( 'to' ) ) != 'rel'"   ),
      ASSERT(    "reldec( relenc( 'rel' ) ) == 'rel'"  ),
      ASSERT(    "reldec( relenc( 'rel' ) ) != 'to'"   ),

      ASSERT(    "next.arc.type   == 'to'"             ),
      UNTRUE(    "next.arc.type   != 'to'"             ),
      ASSERT(    "next.arc.type   != 'rel'"            ),

      ASSERT(    "next.arc.type   in { 'to' }"                       ),
      ASSERT(    "next.arc.type   in { 'likes', 'to' }"              ),
      ASSERT(    "next.arc.type   in { 'likes', 'rel', 'to' }"       ),
      ASSERT(    "next.arc.type   in { 'likes', 'to', 'rel' }"       ),
      ASSERT(    "next.arc.type   in { 'to', 'likes', 'rel' }"       ),
      ASSERT(    "next.arc.type   in { 'to', 'likes', 'rel', 'to' }" ),
      UNTRUE(    "next.arc.type   notin { 'to', 'likes', 'rel' }"    ),
      UNTRUE(    "next.arc.type   in { 'likes', 'rel' }"             ),
      UNTRUE(    "next.arc.type   in { 'rel' }"                      ),
      ASSERT(    "next.arc.type   notin { 'likes', 'rel' }"          ),

      ASSERT(    "'to' == next.arc.type"               ),
      UNTRUE(    "'to' != next.arc.type"               ),
      ASSERT(    "'rel' != next.arc.type"              ),

      ASSERT(    "next.arc.dir    == D_OUT"            ),
      UNTRUE(    "next.arc.dir    != D_OUT"            ),
      ASSERT(    "next.arc.dir    != D_IN"             ),

      ASSERT(    "next.arc.mod    == M_INT"            ),
      UNTRUE(    "next.arc.mod    != M_INT"            ),
      ASSERT(    "next.arc.mod    != M_FLT"            ),

      ASSERT(    "isint( next.arc )" ),
      ASSERT(    "next.arc != 0" ),

      // Shorthand equivalents
      ASSERT(    "next.arc.value == .arc.value"        ),
      ASSERT(    "next.arc.dist  == .arc.dist"         ),
      ASSERT(    "next.arc.type  == .arc.type"         ),
      ASSERT(    "next.arc.type  != .arc.value"        ),
      ASSERT(    "next.arc.dir   == .arc.dir"          ),
      ASSERT(    "next.arc.mod   == .arc.mod"          ),
      ASSERT(    "next.arc       == .arc"              ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, arctest, 0.0, NULL, NULL ) >= 0,   "Arc" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Geo
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Geo" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    // Bos
    double bos_lat = 42.358433;
    double bos_lon = -71.059776;
    // Nyc
    double nyc_lat = 40.714268;
    double nyc_lon = -74.005974;
    // Tok
    double tok_lat = 35.689487;
    double tok_lon = 139.691711;

    double bos_nyc_dist = 305940.0;
    double bos_tok_dist = 10794670.0;

    // Accept 0.5% error
    CString_t *CSTR__bos_nyc = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "abs( 1 - (geodist( %g, %g, %g, %g ) / %g) ) < 0.005", bos_lat, bos_lon, nyc_lat, nyc_lon, bos_nyc_dist );
    CString_t *CSTR__bos_tok = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "abs( 1 - (geodist( %g, %g, %g, %g ) / %g) ) < 0.005", bos_lat, bos_lon, tok_lat, tok_lon, bos_tok_dist );
    const char *bos_nyc = CStringValue( CSTR__bos_nyc );
    const char *bos_tok = CStringValue( CSTR__bos_tok );

    double max_dist = MAX_DISTANCE;
    CString_t *CSTR__geoprox = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "abs( 1 - (geoprox( %g, %g, %g, %g ) / ((%g - geodist( %g, %g, %g, %g )) / %g)) ) < 0.0001  ", bos_lat, bos_lon, nyc_lat, nyc_lon, max_dist, bos_lat, bos_lon, nyc_lat, nyc_lon, max_dist );
    const char *geoprox = CStringValue( CSTR__geoprox );

    __test geo[] = {
      nolineINTEGER( true,  bos_nyc  ),
      nolineINTEGER( true,  bos_tok  ),
      nolineINTEGER( true,  geoprox  ),
      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, geo, 0.0, NULL, NULL ) >= 0,   "Geo" );

    iString.Discard( &CSTR__bos_nyc );
    iString.Discard( &CSTR__bos_tok );
    iString.Discard( &CSTR__geoprox );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Logical
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Logical" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    
    __test logical[] = {
      // Logical NOT
      ASSERT(    "!0"          ),
      ASSERT(    "!false"      ),
      ASSERT(    "!( 1 - 1)"   ),
      ASSERT(    "!(1 == 2)"   ),
      UNTRUE(    "!1"          ),
      UNTRUE(    "!true"       ),
      UNTRUE(    "!( 1 < 2)"   ),

      // Logical OR
      UNTRUE(    "false || false"  ),
      ASSERT(    "false || true"   ),
      ASSERT(    "true  || false"  ),
      ASSERT(    "true  || true"   ),

      UNTRUE(    " 0  ||  0  ||  0"   ),
      ASSERT(    " 0  ||  0  ||  1"   ),
      ASSERT(    " 0  ||  1  ||  0"   ),
      ASSERT(    "(1  ||  0) ||  0"   ),
      ASSERT(    " 1  || (0  ||  0)"  ),


      // Logical AND
      UNTRUE(    "false && false"  ),
      UNTRUE(    "false && true"   ),
      UNTRUE(    "true  && false"  ),
      ASSERT(    "true  && true"   ),

      UNTRUE(   " 0  &&  0  &&  0"   ),
      UNTRUE(   " 0  &&  0  &&  1"   ),
      UNTRUE(   " 0  &&  1  &&  0"   ),
      UNTRUE(   "(1  &&  0) &&  0"   ),
      UNTRUE(   " 1  && (0  &&  0)"  ),

      // Mixed - test precedence
      XINTEGER(    0   ||   0   ||   0    ),
      XINTEGER(    0   ||   0   ||   1    ),
      XINTEGER(    0   ||   1   ||   0    ),
      XINTEGER(    0   ||   1   ||   1    ),
      XINTEGER(    1   ||   0   ||   0    ),
      XINTEGER(    1   ||   0   ||   1    ),
      XINTEGER(    1   ||   1   ||   0    ),
      XINTEGER(    1   ||   1   ||   1    ),

      XINTEGER(    0   ||   0   &&   0    ),
      XINTEGER(    0   ||   0   &&   1    ),
      XINTEGER(    0   ||   1   &&   0    ),
      XINTEGER(    0   ||   1   &&   1    ),
      XINTEGER(    1   ||   0   &&   0    ),
      XINTEGER(    1   ||   0   &&   1    ),
      XINTEGER(    1   ||   1   &&   0    ),
      XINTEGER(    1   ||   1   &&   1    ),

      XINTEGER(    0   &&   0   ||   0    ),
      XINTEGER(    0   &&   0   ||   1    ),
      XINTEGER(    0   &&   1   ||   0    ),
      XINTEGER(    0   &&   1   ||   1    ),
      XINTEGER(    1   &&   0   ||   0    ),
      XINTEGER(    1   &&   0   ||   1    ),
      XINTEGER(    1   &&   1   ||   0    ),
      XINTEGER(    1   &&   1   ||   1    ),

      XINTEGER(    0   &&   0   &&   0    ),
      XINTEGER(    0   &&   0   &&   1    ),
      XINTEGER(    0   &&   1   &&   0    ),
      XINTEGER(    0   &&   1   &&   1    ),
      XINTEGER(    1   &&   0   &&   0    ),
      XINTEGER(    1   &&   0   &&   1    ),
      XINTEGER(    1   &&   1   &&   0    ),
      XINTEGER(    1   &&   1   &&   1    ),

      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, logical, 0.0, NULL, NULL ) >= 0,   "Logical" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Feature Vector
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Feature Vector" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    float tail_mag = CALLABLE( arc->tail->vector )->Magnitude( arc->tail->vector );
    float head_mag = CALLABLE( arc->head.vertex->vector )->Magnitude( arc->head.vertex->vector );

    // Vectors should not match with a high sim threshold
    graph->similarity->params.threshold.similarity = 0.99f;
    graph->similarity->params.threshold.hamming = 1;
    __test vector_miss[] = {
      // ROOT vector
      ASSERT(         "vertex.vector != 0"               ),
      INTEGER( 3,     "asint( vertex.vector )"           ),
      REAL( tail_mag, "asreal( vertex.vector )"          ),
      UNTRUE(         "'first_dim'  in vertex.vector"    ),
      ASSERT(         "'first_dim'  notin vertex.vector" ),
      ASSERT(         "'second_dim' in vertex.vector"    ),
      ASSERT(         "'third_dim'  in vertex.vector"    ),
      ASSERT(         "'fourth_dim' in vertex.vector"    ),
      UNTRUE(         "'fifth_dim'  in vertex.vector"    ),
      // HEAD vector
      ASSERT(         "next.vector != 0"                 ),
      INTEGER( 5,     "asint( next.vector )"             ),
      REAL( head_mag, "asreal( next.vector )"            ),
      ASSERT(         "'first_dim'  in next.vector"      ),
      ASSERT(         "'second_dim' in next.vector"      ),
      ASSERT(         "'third_dim'  in next.vector"      ),
      ASSERT(         "'fourth_dim' in next.vector"      ),
      ASSERT(         "'fifth_dim'  in next.vector"      ),
      // SIM
      ASSERT(         "vertex.vector != next.vector"     ),
      ASSERT(         "next.vector   != vertex.vector"   ),
      UNTRUE(         "vertex.vector == next.vector"     ),
      UNTRUE(         "next.vector   == vertex.vector"   ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vector_miss, 0.0, NULL, NULL ) >= 0,   "Vertex Vectors Miss" );

    // Vectors should match with a lower sim threshold
    graph->similarity->params.threshold.similarity = 0.6f;
    graph->similarity->params.threshold.hamming = 26;
    __test vector_match[] = {
      ASSERT(    "len( vertex.vector ) == 3"       ),
      ASSERT(    "len( next.vector ) == 5"         ),
      ASSERT(    "vertex.vector == next.vector"    ),
      ASSERT(    "next.vector   == vertex.vector"  ),
      UNTRUE(    "vertex.vector != next.vector"    ),
      UNTRUE(    "next.vector   != vertex.vector"  ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vector_match, 0.0, NULL, NULL ) >= 0,   "Vertex Vectors Match" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   *
   * SWITCH TO EUCLIDEAN MODE
   * 
   * 
   * 
   * 
   * 
   ***********************************************************************
   */

  /*******************************************************************//**
   * DESTROY GRAPH
   ***********************************************************************
   */

  if( ARC_ROOT_to_A.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_A.tail );
  }
  if( ARC_ROOT_to_B.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_B.tail );
  }
  if( ARC_ROOT_to_C.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_C.tail );
  }
  if( ARC_ROOT_rel_D.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_rel_D.tail );
  }
  if( ARC_ROOT_to_A.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_A.head.vertex );
  }
  if( ARC_ROOT_to_B.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_B.head.vertex );
  }
  if( ARC_ROOT_to_C.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_C.head.vertex );
  }
  if( ARC_ROOT_rel_D.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_rel_D.head.vertex );
  }

  CALLABLE( graph )->advanced->CloseOpenVertices( graph );

  CALLABLE( graph )->simple->Truncate( graph, NULL );
  
  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  iString.Discard( &CSTR__error );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );


  /*******************************************************************//**
   * RE-INITIALIZE IN EUCLIDEAN MODE
   ***********************************************************************
   */

  CSTR__graph_path = CStringNew( TestName );
  CSTR__graph_name = CStringNew( "VGX_Graph_Euclidean" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  char basedir_euclidean[MAX_PATH+1] = {0};
  snprintf( basedir_euclidean, MAX_PATH, "%s_euclidean", GetCurrentTestDirectory() );

  INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir_euclidean, true );


  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph (Euclidean)" ) {
    graph = igraphfactory.OpenGraph( CSTR__graph_path, CSTR__graph_name, true, NULL, 0 );
    TEST_ASSERTION( graph != NULL, "graph constructed, graph=%llp", graph );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Euclidean Initialization
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Initializations (Euclidean)" ) {
    // Graph factory should have initialized the stack evaluator
    TEST_ASSERTION( g_dummy != NULL,                    "Dummy vertex created" );
    TEST_ASSERTION( _vxeval_parser__tokenizer != NULL,  "Tokenizer created" );
    TEST_ASSERTION( _vxeval_operations != NULL,               "Operation map created" );
    
    // Destroy it
    TEST_ASSERTION( iEvaluator.Destroy() == 0,          "De-initialized" );
    
    // Verify destroyed
    TEST_ASSERTION( g_dummy == NULL,                    "Dummy vertex does not exist" );
    TEST_ASSERTION( _vxeval_parser__tokenizer == NULL,  "Tokenizer does not exist" );
    TEST_ASSERTION( _vxeval_operations == NULL,               "Operation map does not exist" );

    // Re-initialize it
    TEST_ASSERTION( iEvaluator.Initialize() == 0,       "Initialized" );

    // Verify initialized
    TEST_ASSERTION( g_dummy != NULL,                    "Dummy vertex created" );
    TEST_ASSERTION( _vxeval_parser__tokenizer != NULL,  "Tokenizer created" );
    TEST_ASSERTION( _vxeval_operations != NULL,               "Operation map created" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Populate graph
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Populate Graph" ) {
    TEST_ASSERTION( __populate_graph( graph ) == 0,   "Populate Graph" );
   } END_TEST_SCENARIO



  /*******************************************************************//**
   * Math
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Math" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    vgx_ExpressEvalMemory_t *evalmem = iEvaluator.NewMemory( 5 );

    vgx_EvalStackItem_t *item;

    // m[0] is bytearray
    item = &evalmem->data[ 0 ];
    item->type = STACK_ITEM_TYPE_CSTRING;
    item->CSTR__str = iString.New( NULL, "0123456789" );
    CStringAttributes( (CString_t*)item->CSTR__str ) = CSTRING_ATTR_BYTEARRAY;

    // m[1] is bytes
    item = &evalmem->data[ 1 ];
    item->type = STACK_ITEM_TYPE_CSTRING;
    item->CSTR__str = iString.New( NULL, "0123456789" );
    CStringAttributes( (CString_t*)item->CSTR__str ) = CSTRING_ATTR_BYTES;

    // m[2] is integer array
    item = &evalmem->data[ 2 ];
    item->type = STACK_ITEM_TYPE_CSTRING;
    item->CSTR__str = iString.New( NULL, "000011112222" );
    CStringAttributes( (CString_t*)item->CSTR__str ) = CSTRING_ATTR_ARRAY_INT;

    // m[3] is float array
    item = &evalmem->data[ 3 ];
    item->type = STACK_ITEM_TYPE_CSTRING;
    item->CSTR__str = iString.New( NULL, "000011112222" );
    CStringAttributes( (CString_t*)item->CSTR__str ) = CSTRING_ATTR_ARRAY_FLOAT;

    // m[4] is map
    item = &evalmem->data[ 4 ];
    item->type = STACK_ITEM_TYPE_CSTRING;
    item->CSTR__str = iString.New( NULL, "000011112222" );
    CStringAttributes( (CString_t*)item->CSTR__str ) = CSTRING_ATTR_ARRAY_MAP;



    __test math[] = {
      INTEGER( 0,       "0"   ),
      // unary negation
      INTEGER( -1,            "-1"          ),
      INTEGER( -1,            "- 1"         ),
      INTEGER( 1,             "--1"         ),
      INTEGER( 1,             "-- 1"        ),
      INTEGER( 1,             "- -1"        ),
      INTEGER( 1,             "- - 1"       ),
      INTEGER( -1,            "---1"        ),
      INTEGER( -1,            "--- 1"       ),
      INTEGER( -1,            "-- -1"       ),
      INTEGER( -1,            "-- - 1"      ),
      INTEGER( -1,            "- --1"       ),
      INTEGER( -1,            "- -- 1"      ),
      INTEGER( -1,            "- - -1"      ),
      INTEGER( -1,            "- - - 1"     ),
      INTEGER( -2,            "-(1+1)"      ),
      INTEGER( -2,            "- (1+1)"     ),
      INTEGER( 2,             "--(1+1)"     ),
      INTEGER( 2,             "-- (1+1)"    ),
      INTEGER( 2,             "- -(1+1)"    ),
      INTEGER( 2,             "- - (1+1)"   ),

      REAL( -1.5,             "-1.5"        ),
      REAL( -1.5,             "- 1.5"       ),
      REAL( 1.5,              "--1.5"       ),
      REAL( 1.5,              "-- 1.5"      ),
      REAL( 1.5,              "- -1.5"      ),
      REAL( 1.5,              "- - 1.5"     ),
      REAL( -1.5,             "---1.5"      ),
      REAL( -1.5,             "--- 1.5"     ),
      REAL( -1.5,             "-- -1.5"     ),
      REAL( -1.5,             "-- - 1.5"    ),
      REAL( -1.5,             "- --1.5"     ),
      REAL( -1.5,             "- -- 1.5"    ),
      REAL( -1.5,             "- - -1.5"    ),
      REAL( -1.5,             "- - - 1.5"   ),
      REAL( -2.5,             "-(1+1.5)"    ),
      REAL( -2.5,             "- (1+1.5)"   ),
      REAL( 2.5,              "--(1+1.5)"   ),
      REAL( 2.5,              "-- (1+1.5)"  ),
      REAL( 2.5,              "- -(1+1.5)"  ),
      REAL( 2.5,              "- - (1+1.5)" ),

      INTEGER( -(int64_t)arc->tail,      "-vertex" ),
      ASSERT(                     "-vertex.id == vertex.id" ),
      ASSERT(                     "--vertex.id == vertex.id" ),
      ASSERT(                     "vertex.id == --vertex.id" ),

      ASSERT(                     "isvector( -vertex.vector )" ),
      REAL( -1.0,                 "cosine( -vertex.vector, vertex.vector )" ),
      REAL( 1.0,                  "cosine( -vertex.vector, -vertex.vector )" ),
      REAL( -1.0,                 "cosine( --vertex.vector, -vertex.vector )" ),
      REAL( 1.0,                  "cosine( --vertex.vector, --vertex.vector )" ),

      ASSERT(                     "-'hello' == 'hello'" ),
      ASSERT(                     "--'hello' == 'hello'" ),
      ASSERT(                     "'hello' == --'hello'" ),

      // isnan
      ASSERT(         "isnan( null )"           ),
      UNTRUE(         "isnan( 0 )"              ),  // INT
      UNTRUE(         "isnan( 1 )"              ),  // INT
      UNTRUE(         "isnan( -1 )"             ),  // INT
      UNTRUE(         "isnan( 0.0 )"            ),  // REA
      UNTRUE(         "isnan( 1.0 )"            ),  // REA
      UNTRUE(         "isnan( -1.0 )"           ),  // REA
      UNTRUE(         "isnan( inf )"            ),  // REA
      ASSERT(         "isnan( nan )"            ),
      ASSERT(         "isnan( vertex )"         ),
      ASSERT(         "isnan( 'string' )"       ),
      ASSERT(         "isnan( vertex.vector )"  ),
      UNTRUE(         "isnan( 0b0 )"            ),  // BTV
      UNTRUE(         "isnan( 0b101010 )"       ),  // BTV
      ASSERT(         "isnan( keyval(0,0.0) )"  ),
      ASSERT(         "isnan( keyval(1,2.0) )"  ),
      ASSERT(         "isnan( vertex.id )"      ),

      // isinf
      UNTRUE(         "isinf( null )"           ),
      UNTRUE(         "isinf( 0 )"              ),
      UNTRUE(         "isinf( 1 )"              ),
      UNTRUE(         "isinf( -1 )"             ),
      UNTRUE(         "isinf( 0.0 )"            ),
      UNTRUE(         "isinf( 1.0 )"            ),
      UNTRUE(         "isinf( -1.0 )"           ),
      ASSERT(         "isinf( inf )"            ),  // REAL INFINITY
      ASSERT(         "isinf( -inf )"           ),  // REAL INFINITY
      UNTRUE(         "isinf( nan )"            ),
      UNTRUE(         "isinf( vertex )"         ),
      UNTRUE(         "isinf( 'string' )"       ),
      UNTRUE(         "isinf( vertex.vector )"  ),
      UNTRUE(         "isinf( 0b0 )"            ),
      UNTRUE(         "isinf( 0b101010 )"       ),
      UNTRUE(         "isinf( keyval(0,0.0) )"  ),
      UNTRUE(         "isinf( keyval(1,2.0) )"  ),
      UNTRUE(         "isinf( vertex.id )"      ),
      ASSERT(         "isinf( -1/-0.0 )"        ), // inf
      ASSERT(         "isinf( 1/-0.0 )"         ), // -inf
      UNTRUE(         "isinf( asin(2) )"        ), // nan
      UNTRUE(         "isinf( 1e300 )"          ),
      UNTRUE(         "isinf( -1e300 )"         ),
      UNTRUE(         "isinf( 1e-300 )"         ),
      UNTRUE(         "isinf( -1e-300 )"        ),
      UNTRUE(         "isinf( 0x7fffffffffffffff )" ),
      UNTRUE(         "isinf( 0xffffffffffffffff )" ),

      // isint
      UNTRUE(         "isint( null )"           ),
      ASSERT(         "isint( 0 )"              ),  // INT
      ASSERT(         "isint( 1 )"              ),  // INT
      ASSERT(         "isint( -1 )"             ),  // INT
      UNTRUE(         "isint( 0.0 )"            ),  // REA
      UNTRUE(         "isint( 1.0 )"            ),  // REA
      UNTRUE(         "isint( -1.0 )"           ),  // REA
      UNTRUE(         "isint( inf )"            ),  // REA
      UNTRUE(         "isint( nan )"            ),
      UNTRUE(         "isint( vertex )"         ),
      UNTRUE(         "isint( 'string' )"       ),
      UNTRUE(         "isint( vertex.vector )"  ),
      UNTRUE(         "isint( 0b0 )"            ),  // BTV
      UNTRUE(         "isint( 0b101010 )"       ),  // BTV
      UNTRUE(         "isint( keyval(0,0.0) )"  ),
      UNTRUE(         "isint( keyval(1,2.0) )"  ),
      UNTRUE(         "isint( vertex.id )"      ),

      // isreal
      UNTRUE(         "isreal( null )"           ),
      UNTRUE(         "isreal( 0 )"              ),  // INT
      UNTRUE(         "isreal( 1 )"              ),  // INT
      UNTRUE(         "isreal( -1 )"             ),  // INT
      ASSERT(         "isreal( 0.0 )"            ),  // REA
      ASSERT(         "isreal( 1.0 )"            ),  // REA
      ASSERT(         "isreal( -1.0 )"           ),  // REA
      ASSERT(         "isreal( inf )"            ),  // REA
      ASSERT(         "isreal( -inf )"           ),  // REA
      UNTRUE(         "isreal( nan )"            ),
      UNTRUE(         "isreal( vertex )"         ),
      UNTRUE(         "isreal( 'string' )"       ),
      UNTRUE(         "isreal( vertex.vector )"  ),
      UNTRUE(         "isreal( 0b0 )"            ),  // BTV
      UNTRUE(         "isreal( 0b101010 )"       ),  // BTV
      UNTRUE(         "isreal( keyval(0,0.0) )"  ),
      UNTRUE(         "isreal( keyval(1,2.0) )"  ),
      UNTRUE(         "isreal( vertex.id )"      ),
      ASSERT(         "isreal( -1/-0.0 )"        ), // inf
      ASSERT(         "isreal( 1/-0.0 )"         ), // -inf
      UNTRUE(         "isreal( asin(2) )"        ), // nan

      // isbitvector
      UNTRUE(         "isbitvector( null )"           ),
      UNTRUE(         "isbitvector( 0 )"              ),  // INT
      UNTRUE(         "isbitvector( 1 )"              ),  // INT
      UNTRUE(         "isbitvector( -1 )"             ),  // INT
      UNTRUE(         "isbitvector( 0.0 )"            ),  // REA
      UNTRUE(         "isbitvector( 1.0 )"            ),  // REA
      UNTRUE(         "isbitvector( -1.0 )"           ),  // REA
      UNTRUE(         "isbitvector( inf )"            ),  // REA
      UNTRUE(         "isbitvector( nan )"            ),
      UNTRUE(         "isbitvector( vertex )"         ),
      UNTRUE(         "isbitvector( 'string' )"       ),
      UNTRUE(         "isbitvector( vertex.vector )"  ),
      ASSERT(         "isbitvector( 0b0 )"            ),  // BTV
      ASSERT(         "isbitvector( 0b101010 )"       ),  // BTV
      UNTRUE(         "isbitvector( keyval(0,0.0) )"  ),
      UNTRUE(         "isbitvector( keyval(1,2.0) )"  ),
      UNTRUE(         "isbitvector( vertex.id )"      ),

      // iskeyval
      UNTRUE(         "iskeyval( null )"           ),
      UNTRUE(         "iskeyval( 0 )"              ),  // INT
      UNTRUE(         "iskeyval( 1 )"              ),  // INT
      UNTRUE(         "iskeyval( -1 )"             ),  // INT
      UNTRUE(         "iskeyval( 0.0 )"            ),  // REA
      UNTRUE(         "iskeyval( 1.0 )"            ),  // REA
      UNTRUE(         "iskeyval( -1.0 )"           ),  // REA
      UNTRUE(         "iskeyval( inf )"            ),  // REA
      UNTRUE(         "iskeyval( nan )"            ),
      UNTRUE(         "iskeyval( vertex )"         ),
      UNTRUE(         "iskeyval( 'string' )"       ),
      UNTRUE(         "iskeyval( vertex.vector )"  ),
      UNTRUE(         "iskeyval( 0b0 )"            ),  // BTV
      UNTRUE(         "iskeyval( 0b101010 )"       ),  // BTV
      ASSERT(         "iskeyval( keyval(0,0.0) )"  ),
      ASSERT(         "iskeyval( keyval(1,2.0) )"  ),
      UNTRUE(         "iskeyval( vertex.id )"      ),

      // isstr
      UNTRUE(         "isstr( null )"           ),
      UNTRUE(         "isstr( 0 )"              ),  // INT
      UNTRUE(         "isstr( 1 )"              ),  // INT
      UNTRUE(         "isstr( -1 )"             ),  // INT
      UNTRUE(         "isstr( 0.0 )"            ),  // REA
      UNTRUE(         "isstr( 1.0 )"            ),  // REA
      UNTRUE(         "isstr( -1.0 )"           ),  // REA
      UNTRUE(         "isstr( inf )"            ),  // REA
      UNTRUE(         "isstr( nan )"            ),
      UNTRUE(         "isstr( vertex )"         ),
      ASSERT(         "isstr( 'string' )"       ),
      ASSERT(         "isstr( 'a' + 'a' )"      ),
      ASSERT(         "isstr( 'a' + 1 )"        ),
      UNTRUE(         "isstr( vertex.vector )"  ),
      UNTRUE(         "isstr( 0b0 )"            ),  // BTV
      UNTRUE(         "isstr( 0b101010 )"       ),  // BTV
      UNTRUE(         "isstr( keyval(0,0.0) )"  ),
      UNTRUE(         "isstr( keyval(1,2.0) )"  ),
      UNTRUE(         "isstr( vertex.id )"      ),

      // isvector
      UNTRUE(         "isvector( null )"           ),
      UNTRUE(         "isvector( 0 )"              ),  // INT
      UNTRUE(         "isvector( 1 )"              ),  // INT
      UNTRUE(         "isvector( -1 )"             ),  // INT
      UNTRUE(         "isvector( 0.0 )"            ),  // REA
      UNTRUE(         "isvector( 1.0 )"            ),  // REA
      UNTRUE(         "isvector( -1.0 )"           ),  // REA
      UNTRUE(         "isvector( inf )"            ),  // REA
      UNTRUE(         "isvector( nan )"            ),
      UNTRUE(         "isvector( vertex )"         ),
      UNTRUE(         "isvector( 'string' )"       ),
      ASSERT(         "isvector( vertex.vector )"  ),
      UNTRUE(         "isvector( 0b0 )"            ),  // BTV
      UNTRUE(         "isvector( 0b101010 )"       ),  // BTV
      UNTRUE(         "isvector( keyval(0,0.0) )"  ),
      UNTRUE(         "isvector( keyval(1,2.0) )"  ),
      UNTRUE(         "isvector( vertex.id )"      ),

      // isbytearray
      UNTRUE(         "isbytearray( null )"           ),
      UNTRUE(         "isbytearray( 0 )"              ),
      UNTRUE(         "isbytearray( 1 )"              ),
      UNTRUE(         "isbytearray( -1 )"             ),
      UNTRUE(         "isbytearray( 0.0 )"            ),
      UNTRUE(         "isbytearray( 1.0 )"            ),
      UNTRUE(         "isbytearray( -1.0 )"           ),
      UNTRUE(         "isbytearray( inf )"            ),
      UNTRUE(         "isbytearray( nan )"            ),
      UNTRUE(         "isbytearray( vertex )"         ),
      UNTRUE(         "isbytearray( 'string' )"       ),
      UNTRUE(         "isbytearray( 'a' + 'a' )"      ),
      UNTRUE(         "isbytearray( 'a' + 1 )"        ),
      UNTRUE(         "isbytearray( vertex.vector )"  ),
      UNTRUE(         "isbytearray( 0b0 )"            ),
      UNTRUE(         "isbytearray( 0b101010 )"       ),
      UNTRUE(         "isbytearray( keyval(0,0.0) )"  ),
      UNTRUE(         "isbytearray( keyval(1,2.0) )"  ),
      UNTRUE(         "isbytearray( vertex.id )"      ),
      ASSERT(         "isbytearray( load(0) )"        ), // BYTEARRAY
      UNTRUE(         "isbytearray( load(1) )"        ),
      UNTRUE(         "isbytearray( load(2) )"        ),
      UNTRUE(         "isbytearray( load(3) )"        ),
      UNTRUE(         "isbytearray( load(4) )"        ),

      // isbytes
      UNTRUE(         "isbytes( null )"           ),
      ASSERT(         "isbytes( bytes() )"        ),
      ASSERT(         "isbytes( bytes(1,2,3) )"   ),
      UNTRUE(         "isbytes( 0 )"              ),
      UNTRUE(         "isbytes( 1 )"              ),
      UNTRUE(         "isbytes( -1 )"             ),
      UNTRUE(         "isbytes( 0.0 )"            ),
      UNTRUE(         "isbytes( 1.0 )"            ),
      UNTRUE(         "isbytes( -1.0 )"           ),
      UNTRUE(         "isbytes( inf )"            ),
      UNTRUE(         "isbytes( nan )"            ),
      UNTRUE(         "isbytes( vertex )"         ),
      ASSERT(         "isbytes( b'string' )"      ),
      UNTRUE(         "isbytes( 'string' )"       ),
      UNTRUE(         "isbytes( 'a' + 'a' )"      ),
      UNTRUE(         "isbytes( 'a' + b'a' )"     ),
      ASSERT(         "isbytes( b'a' + 'a' )"     ),
      ASSERT(         "isbytes( b'a' + b'a' )"    ),
      ASSERT(         "isbytes( b'a' + 1 )"       ),
      UNTRUE(         "isbytes( 'a' + 1 )"        ),
      UNTRUE(         "isbytes( vertex.vector )"  ),
      UNTRUE(         "isbytes( 0b0 )"            ),
      UNTRUE(         "isbytes( 0b101010 )"       ),
      UNTRUE(         "isbytes( keyval(0,0.0) )"  ),
      UNTRUE(         "isbytes( keyval(1,2.0) )"  ),
      UNTRUE(         "isbytes( vertex.id )"      ),
      UNTRUE(         "isbytes( load(0) )"        ),
      ASSERT(         "isbytes( load(1) )"        ), // BYTES
      UNTRUE(         "isbytes( load(2) )"        ),
      UNTRUE(         "isbytes( load(3) )"        ),
      UNTRUE(         "isbytes( load(4) )"        ),

      // isutf8
      UNTRUE(         "isutf8( null )"            ),
      UNTRUE(         "isutf8( 0 )"               ),
      UNTRUE(         "isutf8( 1 )"               ),
      UNTRUE(         "isutf8( -1 )"              ),
      UNTRUE(         "isutf8( 0.0 )"             ),
      UNTRUE(         "isutf8( 1.0 )"             ),
      UNTRUE(         "isutf8( -1.0 )"            ),
      UNTRUE(         "isutf8( inf )"             ),
      UNTRUE(         "isutf8( nan )"             ),
      UNTRUE(         "isutf8( vertex )"          ),
      ASSERT(         "isutf8( b'string' )"       ),
      UNTRUE(         "isutf8( 'string' )"        ),
      UNTRUE(         "isutf8( vertex.vector )"   ),
      UNTRUE(         "isutf8( 0b0 )"             ),
      UNTRUE(         "isutf8( 0b101010 )"        ),
      UNTRUE(         "isutf8( keyval(0,0.0) )"   ),
      UNTRUE(         "isutf8( keyval(1,2.0) )"   ),
      UNTRUE(         "isutf8( vertex.id )"       ),
      UNTRUE(         "isutf8( load(0) )"         ),
      ASSERT(         "isutf8( load(1) )"         ),
      UNTRUE(         "isutf8( load(2) )"         ),
      UNTRUE(         "isutf8( load(3) )"         ),
      UNTRUE(         "isutf8( load(4) )"         ),
      UNTRUE(         "isutf8( '' )"              ),
      ASSERT(         "isutf8( b'' )"             ),
      ASSERT(         "isutf8( b'\\x00' )"        ),
      ASSERT(         "isutf8( b'\\x7f' )"        ),
      UNTRUE(         "isutf8( b'\\x80' )"        ),
      UNTRUE(         "isutf8( b'\\xce' )"        ),
      ASSERT(         "isutf8( b'\\xce\\xb2' )"   ),

      // isarray
      UNTRUE(         "isarray( null )"           ),
      UNTRUE(         "isarray( 0 )"              ),
      UNTRUE(         "isarray( 1 )"              ),
      UNTRUE(         "isarray( -1 )"             ),
      UNTRUE(         "isarray( 0.0 )"            ),
      UNTRUE(         "isarray( 1.0 )"            ),
      UNTRUE(         "isarray( -1.0 )"           ),
      UNTRUE(         "isarray( inf )"            ),
      UNTRUE(         "isarray( nan )"            ),
      UNTRUE(         "isarray( vertex )"         ),
      UNTRUE(         "isarray( 'string' )"       ),
      UNTRUE(         "isarray( 'a' + 'a' )"      ),
      UNTRUE(         "isarray( 'a' + 1 )"        ),
      UNTRUE(         "isarray( vertex.vector )"  ),
      UNTRUE(         "isarray( 0b0 )"            ),
      UNTRUE(         "isarray( 0b101010 )"       ),
      UNTRUE(         "isarray( keyval(0,0.0) )"  ),
      UNTRUE(         "isarray( keyval(1,2.0) )"  ),
      UNTRUE(         "isarray( vertex.id )"      ),
      UNTRUE(         "isarray( load(0) )"        ),
      UNTRUE(         "isarray( load(1) )"        ),
      ASSERT(         "isarray( load(2) )"        ), // INT ARRAY
      ASSERT(         "isarray( load(3) )"        ), // FLT ARRAY
      UNTRUE(         "isarray( load(4) )"        ),

      // ismap
      UNTRUE(         "ismap( null )"           ),
      UNTRUE(         "ismap( 0 )"              ),
      UNTRUE(         "ismap( 1 )"              ),
      UNTRUE(         "ismap( -1 )"             ),
      UNTRUE(         "ismap( 0.0 )"            ),
      UNTRUE(         "ismap( 1.0 )"            ),
      UNTRUE(         "ismap( -1.0 )"           ),
      UNTRUE(         "ismap( inf )"            ),
      UNTRUE(         "ismap( nan )"            ),
      UNTRUE(         "ismap( vertex )"         ),
      UNTRUE(         "ismap( 'string' )"       ),
      UNTRUE(         "ismap( 'a' + 'a' )"      ),
      UNTRUE(         "ismap( 'a' + 1 )"        ),
      UNTRUE(         "ismap( vertex.vector )"  ),
      UNTRUE(         "ismap( 0b0 )"            ),
      UNTRUE(         "ismap( 0b101010 )"       ),
      UNTRUE(         "ismap( keyval(0,0.0) )"  ),
      UNTRUE(         "ismap( keyval(1,2.0) )"  ),
      UNTRUE(         "ismap( vertex.id )"      ),
      UNTRUE(         "ismap( load(0) )"        ),
      UNTRUE(         "ismap( load(1) )"        ),
      UNTRUE(         "ismap( load(2) )"        ),
      UNTRUE(         "ismap( load(3) )"        ),
      ASSERT(          "ismap( load(4) )"        ), // MAP

      // anynan
      UNTRUE(         "anynan()"                  ),
      ASSERT(         "anynan( null )"            ), // null
      ASSERT(         "anynan( 1, null )"         ),
      ASSERT(         "anynan( null, 1 )"         ),
      ASSERT(         "anynan( 1, null, 1 )"      ),
      UNTRUE(         "anynan( 0 )"               ), // INT
      UNTRUE(         "anynan( 0, 1 )"            ),
      UNTRUE(         "anynan( 0, 1, -1 )"        ),
      UNTRUE(         "anynan( 0.0 )"             ), // REA
      UNTRUE(         "anynan( 0.0, 1.0 )"        ),
      UNTRUE(         "anynan( 0.0, 1.0, -1.0 )"  ),
      UNTRUE(         "anynan( 0b0 )"             ), // BTV
      UNTRUE(         "anynan( 0b0, 0b1 )"        ),
      UNTRUE(         "anynan( 0b1, 0b0 )"        ),
      ASSERT(         "anynan( nan )"             ), // NAN
      ASSERT(         "anynan( 1, nan )"          ),
      ASSERT(         "anynan( nan, 1 )"          ),
      ASSERT(         "anynan( 1, nan, 1 )"       ),
      ASSERT(         "anynan( vertex )"          ), // VTX
      ASSERT(         "anynan( 1, vertex )"       ),
      ASSERT(         "anynan( vertex, 1 )"       ),
      ASSERT(         "anynan( 1, vertex, 1 )"    ),
      ASSERT(         "anynan( 'hello' )"         ), // STR
      ASSERT(         "anynan( 1, 'hello' )"      ),
      ASSERT(         "anynan( 'hello', 1 )"      ),
      ASSERT(         "anynan( 1, 'hello', 1 )"   ),
      ASSERT(         "anynan( vertex.vector )"         ), // VEC
      ASSERT(         "anynan( 1, vertex.vector )"      ),
      ASSERT(         "anynan( vertex.vector, 1 )"      ),
      ASSERT(         "anynan( 1, vertex.vector, 1 )"   ),
      ASSERT(         "anynan( keyval(1,2) )"           ), // KYV
      ASSERT(         "anynan( 1, keyval(1,2) )"        ), // KYV
      ASSERT(         "anynan( keyval(1,2), 1 )"        ), // KYV
      ASSERT(         "anynan( 1, keyval(1,2), 1 )"     ), // KYV
      ASSERT(         "anynan( vertex.id )"             ), // VTX
      ASSERT(         "anynan( 1, vertex.id )"          ), // VID
      ASSERT(         "anynan( vertex.id, 1 )"          ), // VID
      ASSERT(         "anynan( 1, vertex.id, 1 )"       ), // VID
      ASSERT(         "anynan( nan, vertex )" ),
      ASSERT(         "anynan( nan, vertex, 'hello' )" ),
      ASSERT(         "anynan( nan, vertex, 'hello', 1 )" ),
      ASSERT(         "anynan( nan, vertex, 'hello', 1, 1.0 )" ),
      UNTRUE(         "anynan( 0, 1, 1.0, 0b1010 )"     ),
      UNTRUE(         "anynan( 0b1010, 1.0, 1, 0 )"     ),

      // allnan
      ASSERT(         "allnan()"                  ),
      ASSERT(         "allnan( null )"            ), // null
      UNTRUE(         "allnan( 1, null )"         ),
      UNTRUE(         "allnan( null, 1 )"         ),
      UNTRUE(         "allnan( 1, null, 1 )"      ),
      UNTRUE(         "allnan( 0 )"               ), // INT
      UNTRUE(         "allnan( 0, 1 )"            ),
      UNTRUE(         "allnan( 0, 1, -1 )"        ),
      UNTRUE(         "allnan( 0.0 )"             ), // REA
      UNTRUE(         "allnan( 0.0, 1.0 )"        ),
      UNTRUE(         "allnan( 0.0, 1.0, -1.0 )"  ),
      UNTRUE(         "allnan( 0b0 )"             ), // BTV
      UNTRUE(         "allnan( 0b0, 0b1 )"        ),
      UNTRUE(         "allnan( 0b1, 0b0 )"        ),
      ASSERT(         "allnan( nan )"             ), // NAN
      UNTRUE(         "allnan( 1, nan )"          ),
      UNTRUE(         "allnan( nan, 1 )"          ),
      UNTRUE(         "allnan( 1, nan, 1 )"       ),
      ASSERT(         "allnan( vertex )"          ), // VTX
      UNTRUE(         "allnan( 1, vertex )"       ),
      UNTRUE(         "allnan( vertex, 1 )"       ),
      UNTRUE(         "allnan( 1, vertex, 1 )"    ),
      ASSERT(         "allnan( 'hello' )"         ), // STR
      UNTRUE(         "allnan( 1, 'hello' )"      ),
      UNTRUE(         "allnan( 'hello', 1 )"      ),
      UNTRUE(         "allnan( 1, 'hello', 1 )"   ),
      ASSERT(         "allnan( vertex.vector )"         ), // VEC
      UNTRUE(         "allnan( 1, vertex.vector )"      ),
      UNTRUE(         "allnan( vertex.vector, 1 )"      ),
      UNTRUE(         "allnan( 1, vertex.vector, 1 )"   ),
      ASSERT(         "allnan( keyval(1,2) )"           ), // KYV
      UNTRUE(         "allnan( 1, keyval(1,2) )"        ), // KYV
      UNTRUE(         "allnan( keyval(1,2), 1 )"        ), // KYV
      UNTRUE(         "allnan( 1, keyval(1,2), 1 )"     ), // KYV
      ASSERT(         "allnan( vertex.id )"             ), // VTX
      UNTRUE(         "allnan( 1, vertex.id )"          ), // VID
      UNTRUE(         "allnan( vertex.id, 1 )"          ), // VID
      UNTRUE(         "allnan( 1, vertex.id, 1 )"       ), // VID
      ASSERT(         "allnan( nan, vertex )" ),
      ASSERT(         "allnan( nan, vertex, 'hello' )" ),
      UNTRUE(         "allnan( nan, vertex, 'hello', 1 )" ),
      UNTRUE(         "allnan( nan, vertex, 'hello', 1, 1.0 )" ),
      UNTRUE(         "allnan( 0, 1, 1.0, 0b1010 )"     ),
      UNTRUE(         "allnan( 0b1010, 1.0, 1, 0 )"     ),

      // inv
      ASSERT(         "inv(0) > 3.4e38"   ),
      UNTRUE(         "inv(0) < 3.4e38"   ),
      ASSERT(         "inv(0.0) > 3.4e38" ),
      UNTRUE(         "inv(0.0) < 3.4e38" ),
      REAL( 1.0,              "inv(1)"            ),
      REAL( -1.0,             "inv(-1)"           ),
      REAL( 1.0,              "-inv(-1)"          ),
      REAL( 1.0,              "inv(1.0)"          ),
      REAL( -1.0,             "inv(-1.0)"         ),
      REAL( 1.0,              "-inv(-1.0)"        ),
      REAL( 0.5,              "inv(2)"            ),
      REAL( -0.5,             "inv(-2)"           ),
      REAL( 0.5,              "-inv(-2)"          ),
      REAL( 0.5,              "inv(2.0)"          ),
      REAL( -0.5,             "inv(-2.0)"         ),
      REAL( 0.5,              "-inv(-2.0)"        ),
      ASSERT(          "inv(vertex) == vertex"               ),
      ASSERT(          "inv(vertex.id) == vertex.id"         ),
      ASSERT(          "inv(vertex.vector) == vertex.vector" ),
      ASSERT(          "inv('hello') == 'hello'"             ),

      // log2
      REAL( -1074,           "log2(0)"            ),
      REAL( -1074,           "log2(-1)"           ),
      REAL( -1074,           "log2(0.0)"          ),
      REAL( -1074,           "log2(-1.1)"         ),
      XREAL(                  log2(0.01)          ),
      XREAL(                  log2(0.99)          ),
      XREAL(                  log2(1)             ),
      XREAL(                  log2(1.01)          ),
      XREAL(                  log2(1.99)          ),
      XREAL(                  log2(2)             ),
      XREAL(                  log2(2.01)          ),
      XREAL(                  log2(2.99)          ),
      XREAL(                  log2(3)             ),
      XREAL(                  log2(3.01)          ),
      XREAL(                  log2(1000)          ),
      XREAL(                  log2(1e10)          ),
      XREAL(                  log2(1.7e100)       ),
      REAL( log2((double)(int64_t)arc->tail),   "log2( vertex )"        ),
      REAL( 0.0,                                "log2( vertex.id )"     ),
      REAL( 0.0,                                "log2( vertex.vector )" ),
      REAL( 0.0,                                "log2( 'hello' )"       ),


      // log
      REAL( -745,            "log(0)"             ),
      REAL( -745,            "log(-1)"            ),
      REAL( -745,            "log(0.0)"           ),
      REAL( -745,            "log(-1.1)"          ),
      XREAL(                  log(0.01)           ),
      XREAL(                  log(0.99)           ),
      XREAL(                  log(1)              ),
      XREAL(                  log(1.01)           ),
      XREAL(                  log(1.99)           ),
      XREAL(                  log(2)              ),
      XREAL(                  log(2.01)           ),
      XREAL(                  log(2.99)           ),
      XREAL(                  log(3)              ),
      XREAL(                  log(3.01)           ),
      XREAL(                  log(1000)           ),
      XREAL(                  log(1e10)           ),
      XREAL(                  log(1.7e100)        ),
      REAL( log((double)(int64_t)arc->tail),    "log( vertex )"        ),
      REAL( 0.0,                                "log( vertex.id )"     ),
      REAL( 0.0,                                "log( vertex.vector )" ),
      REAL( 0.0,                                "log( 'hello' )"       ),


      // log10
      REAL( -324,            "log10(0)"            ),
      REAL( -324,            "log10(-1)"           ),
      REAL( -324,            "log10(0.0)"          ),
      REAL( -324,            "log10(-1.1)"         ),
      XREAL(                  log10(0.01)          ),
      XREAL(                  log10(0.99)          ),
      XREAL(                  log10(1)             ),
      XREAL(                  log10(1.01)          ),
      XREAL(                  log10(1.99)          ),
      XREAL(                  log10(2)             ),
      XREAL(                  log10(2.01)          ),
      XREAL(                  log10(2.99)          ),
      XREAL(                  log10(3)             ),
      XREAL(                  log10(3.01)          ),
      XREAL(                  log10(1000)          ),
      XREAL(                  log10(1e10)          ),
      XREAL(                  log10(1.7e100)       ),
      REAL( log10((double)(int64_t)arc->tail),  "log10( vertex )"        ),
      REAL( 0.0,                                "log10( vertex.id )"     ),
      REAL( 0.0,                                "log10( vertex.vector )" ),
      REAL( 0.0,                                "log10( 'hello' )"       ),

      // rad / deg
      REAL( 0.0,              "rad( 0 )"           ),
      REAL( 0.0,              "rad( 0.0 )"         ),
      REAL( 0.0,              "rad( 'hello' )"     ),
      ASSERT(          "rad( 1 ) > 0"       ),
      ASSERT(          "rad( 90 ) == pi/2"  ),
      ASSERT(          "rad( 180 ) == pi"   ),

      REAL( 0.0,              "deg( 0 )"           ),
      REAL( 0.0,              "deg( 0.0 )"         ),
      REAL( 0.0,              "deg( 'hello' )"     ),
      ASSERT(          "deg( 1 ) > 0"       ),
      ASSERT(          "deg( pi/2 ) == 90"  ),
      ASSERT(          "deg( pi ) == 180"   ),

      // sin / asin 
      XREAL(                  sin(0)               ),
      XREAL(                  sin(1)               ),
      XREAL(                  sin(-1)              ),
      XREAL(                  sin(3.14)            ),
      XREAL(                  sin(-3.14)           ),
      XREAL(                  sin(7)               ),
      XREAL(                  sin(-7)              ),
      XREAL(                  asin( sin(0) )       ),
      XREAL(                  asin( sin(1) )       ),
      XREAL(                  asin( sin(-1) )      ),
      XREAL(                  asin( sin(3.14) )    ),
      XREAL(                  asin( sin(-3.14) )   ),
      XREAL(                  asin( sin(7) )       ),
      XREAL(                  asin( sin(-7) )      ),
      ASSERT(          "sin( vertex ) == vertex"                 ),
      ASSERT(          "asin( vertex ) == vertex"                ),
      ASSERT(          "sin( vertex.id ) == vertex.id"           ),
      ASSERT(          "asin( vertex.id ) == vertex.id"          ),
      ASSERT(          "sin( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "asin( vertex.vector ) == vertex.vector"  ),
      ASSERT(          "sin( 'hello' ) == 'hello'"               ),
      ASSERT(          "asin( 'hello' ) == 'hello'"              ),


      // sinh / asinh
      XREAL(                  sinh(0)              ),
      XREAL(                  sinh(1)              ),
      XREAL(                  sinh(-1)             ),
      XREAL(                  sinh(3.14)           ),
      XREAL(                  sinh(-3.14)          ),
      XREAL(                  sinh(7)              ),
      XREAL(                  sinh(-7)             ),
      XREAL(                  asinh( sinh(0) )     ),
      XREAL(                  asinh( sinh(1) )     ),
      XREAL(                  asinh( sinh(-1) )    ),
      XREAL(                  asinh( sinh(3.14) )  ),
      XREAL(                  asinh( sinh(-3.14) ) ),
      XREAL(                  asinh( sinh(7) )     ),
      XREAL(                  asinh( sinh(-7) )    ),
      ASSERT(          "sinh( vertex ) == vertex"                  ),
      ASSERT(          "asinh( vertex ) == vertex"                 ),
      ASSERT(          "sinh( vertex.id ) == vertex.id"            ),
      ASSERT(          "asinh( vertex.id ) == vertex.id"           ),
      ASSERT(          "sinh( vertex.vector ) == vertex.vector"    ),
      ASSERT(          "asinh( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "sinh( 'hello' ) == 'hello'"                ),
      ASSERT(          "asinh( 'hello' ) == 'hello'"               ),


      // cos / acos 
      XREAL(                  cos(0)               ),
      XREAL(                  cos(1)               ),
      XREAL(                  cos(-1)              ),
      XREAL(                  cos(3.14)            ),
      XREAL(                  cos(-3.14)           ),
      XREAL(                  cos(7)               ),
      XREAL(                  cos(-7)              ),
      XREAL(                  acos( cos(0) )       ),
      XREAL(                  acos( cos(1) )       ),
      XREAL(                  acos( cos(-1) )      ),
      XREAL(                  acos( cos(3.14) )    ),
      XREAL(                  acos( cos(-3.14) )   ),
      XREAL(                  acos( cos(7) )       ),
      XREAL(                  acos( cos(-7) )      ),
      ASSERT(          "cos( vertex ) == vertex"                 ),
      ASSERT(          "acos( vertex ) == vertex"                ),
      ASSERT(          "cos( vertex.id ) == vertex.id"           ),
      ASSERT(          "acos( vertex.id ) == vertex.id"          ),
      ASSERT(          "cos( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "acos( vertex.vector ) == vertex.vector"  ),
      ASSERT(          "cos( 'hello' ) == 'hello'"               ),
      ASSERT(          "acos( 'hello' ) == 'hello'"              ),


      // cosh / acosh
      XREAL(                  cosh(0)              ),
      XREAL(                  cosh(1)              ),
      XREAL(                  cosh(-1)             ),
      XREAL(                  cosh(3.14)           ),
      XREAL(                  cosh(-3.14)          ),
      XREAL(                  cosh(7)              ),
      XREAL(                  cosh(-7)             ),
      XREAL(                  acosh( cosh(0) )     ),
      XREAL(                  acosh( cosh(1) )     ),
      XREAL(                  acosh( cosh(-1) )    ),
      XREAL(                  acosh( cosh(3.14) )  ),
      XREAL(                  acosh( cosh(-3.14) ) ),
      XREAL(                  acosh( cosh(7) )     ),
      XREAL(                  acosh( cosh(-7) )    ),
      ASSERT(          "cosh( vertex ) == vertex"                ),
      ASSERT(          "acosh( vertex ) == vertex"               ),
      ASSERT(          "cosh( vertex.id ) == vertex.id"          ),
      ASSERT(          "acosh( vertex.id ) == vertex.id"         ),
      ASSERT(          "cosh( vertex.vector ) == vertex.vector"  ),
      ASSERT(          "acosh( vertex.vector ) == vertex.vector" ),
      ASSERT(          "cosh( 'hello' ) == 'hello'"              ),
      ASSERT(          "acosh( 'hello' ) == 'hello'"             ),


      // tan / atan 
      XREAL(                  tan(0)               ),
      XREAL(                  tan(1)               ),
      XREAL(                  tan(-1)              ),
      XREAL(                  tan(3.14)            ),
      XREAL(                  tan(-3.14)           ),
      XREAL(                  tan(7)               ),
      XREAL(                  tan(-7)              ),
      XREAL(                  atan( tan(0) )       ),
      XREAL(                  atan( tan(1) )       ),
      XREAL(                  atan( tan(-1) )      ),
      XREAL(                  atan( tan(3.14) )    ),
      XREAL(                  atan( tan(-3.14) )   ),
      XREAL(                  atan( tan(7) )       ),
      XREAL(                  atan( tan(-7) )      ),
      ASSERT(          "tan( vertex ) == vertex"                 ),
      ASSERT(          "atan( vertex ) == vertex"                ),
      ASSERT(          "tan( vertex.id ) == vertex.id"           ),
      ASSERT(          "atan( vertex.id ) == vertex.id"          ),
      ASSERT(          "tan( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "atan( vertex.vector ) == vertex.vector"  ),
      ASSERT(          "tan( 'hello' ) == 'hello'"               ),
      ASSERT(          "atan( 'hello' ) == 'hello'"              ),


      // tanh / atanh
      XREAL(                  tanh(0)              ),
      XREAL(                  tanh(1)              ),
      XREAL(                  tanh(-1)             ),
      XREAL(                  tanh(3.14)           ),
      XREAL(                  tanh(-3.14)          ),
      XREAL(                  tanh(7)              ),
      XREAL(                  tanh(-7)             ),
      XREAL(                  atanh( tanh(0) )     ),
      XREAL(                  atanh( tanh(1) )     ),
      XREAL(                  atanh( tanh(-1) )    ),
      XREAL(                  atanh( tanh(3.14) )  ),
      XREAL(                  atanh( tanh(-3.14) ) ),
      XREAL(                  atanh( tanh(7) )     ),
      XREAL(                  atanh( tanh(-7) )    ),
      ASSERT(          "tanh( vertex ) == vertex"                  ),
      ASSERT(          "atanh( vertex ) == vertex"                 ),
      ASSERT(          "tanh( vertex.id ) == vertex.id"            ),
      ASSERT(          "atanh( vertex.id ) == vertex.id"           ),
      ASSERT(          "tanh( vertex.vector ) == vertex.vector"    ),
      ASSERT(          "atanh( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "tanh( 'hello' ) == 'hello'"                ),
      ASSERT(          "atanh( 'hello' ) == 'hello'"               ),


      // sinc
      REAL( 1.0,              "sinc(0)"                       ),
      ASSERT(          "sinc(1) < 1e-10"               ), // zero really
      ASSERT(          "sinc(0.1) > 0"                 ),
      ASSERT(          "sinc(2.1) > 0"                 ),
      ASSERT(          "sinc(4.1) > 0"                 ),
      ASSERT(          "sinc(6.1) > 0"                 ),
      ASSERT(          "sinc(0.1) > sinc(2.1)"         ),
      ASSERT(          "sinc(2.1) > sinc(4.1)"         ),
      ASSERT(          "sinc(4.1) > sinc(6.1)"         ),
      ASSERT(          "sinc(1.1) < 0"                 ),
      ASSERT(          "sinc(3.1) < 0"                 ),
      ASSERT(          "sinc(5.1) < 0"                 ),
      ASSERT(          "sinc(1.1) < sinc(3.1)"         ),
      ASSERT(          "sinc(3.1) < sinc(5.1)"         ),
      ASSERT(          "sinc( 1.23 ) == ( sin(pi*1.23) / (pi*1.23) )"  ),
      ASSERT(          "sinc( 12.3 ) == ( sin(pi*12.3) / (pi*12.3) )"  ),
      ASSERT(          "sinc( 123 ) == ( sin(pi*123) / (pi*123) )"  ),
      ASSERT(          "sinc( -1.23 ) == ( sin(pi*-1.23) / (pi*-1.23) )"  ),
      ASSERT(          "sinc( -12.3 ) == ( sin(pi*-12.3) / (pi*-12.3) )"  ),
      ASSERT(          "sinc( -123 ) == ( sin(pi*-123) / (pi*-123) )"  ),


      // exp
      XREAL(                  exp(-1e10)           ),
      XREAL(                  exp(-1.5)            ),
      XREAL(                  exp(-1)              ),
      XREAL(                  exp(-0.01)           ),
      XREAL(                  exp(0)               ),
      XREAL(                  exp(0.01)            ),
      XREAL(                  exp(1)               ),
      XREAL(                  exp(1.5)             ),
      XREAL(                  exp(100)             ),
      XREAL(                  exp(100.1)           ),
      ASSERT(          "exp( vertex ) == vertex"                 ),
      ASSERT(          "exp( vertex.id ) == vertex.id"           ),
      ASSERT(          "exp( vertex.vector ) == vertex.vector"   ),
      ASSERT(          "exp( 'hello' ) == 'hello'"               ),


      // abs
      INTEGER( 0,           "abs( 0 )"              ),
      INTEGER( 1,           "abs( -1 )"             ),
      INTEGER( 100,         "abs( -100 )"           ),
      INTEGER( 1,           "abs( 1 )"              ),
      INTEGER( 100,         "abs( 100 )"            ),
      REAL( 0.0,            "abs( 0.0 )"            ),
      REAL( 1.1,            "abs( -1.1 )"           ),
      REAL( 100.1,          "abs( -100.1 )"         ),
      REAL( 1.1,            "abs( 1.1 )"            ),
      REAL( 100.1,          "abs( 100.1 )"          ),
      ASSERT(        "abs( vertex ) == vertex"               ),
      ASSERT(        "abs( vertex.id ) == vertex.id"         ),
      ASSERT(        "isreal( abs( vertex.vector ) )"        ),
      ASSERT(        "abs( 'hello' ) == 'hello'"             ),


      // sqrt
      REAL( 0.0,            "sqrt(-100)"            ),
      REAL( 0.0,            "sqrt(-1.1)"            ),
      REAL( 0.0,            "sqrt(-1)"              ),  // sorry :(
      REAL( 0.0,            "sqrt(-0.001)"          ),
      REAL( 0.0,            "sqrt(0)"               ),
      REAL( 0.0,            "sqrt(0.0)"             ),
      XREAL(                 sqrt(0.1)              ),
      XREAL(                 sqrt(1)                ),
      XREAL(                 sqrt(1.1)              ),
      XREAL(                 sqrt(10)               ),
      XREAL(                 sqrt(64)               ),
      XREAL(                 sqrt(1e100)            ),
      ASSERT(        "sqrt( vertex ) == vertex"                ),
      ASSERT(        "sqrt( vertex.id ) == vertex.id"          ),
      ASSERT(        "sqrt( vertex.vector ) == vertex.vector"  ),
      ASSERT(        "sqrt( 'hello' ) == 'hello'"              ),


      // ceil / floor / round
      INTEGER( -100,        "ceil( -100 )"            ),
      REAL( -100.0,         "ceil( -100.0 )"          ),
      REAL( -100.0,         "ceil( -100.1 )"          ),
      REAL( -100.0,         "ceil( -100.5 )"          ),
      REAL( -100.0,         "ceil( -100.9 )"          ),
      INTEGER( 100,         "ceil( 100 )"             ),
      REAL( 100.0,          "ceil( 100.0 )"           ),
      REAL( 101.0,          "ceil( 100.1 )"           ),
      REAL( 101.0,          "ceil( 100.5 )"           ),
      REAL( 101.0,          "ceil( 100.9 )"           ),

      INTEGER( -100,        "floor( -100 )"           ),
      REAL( -100.0,         "floor( -100.0 )"         ),
      REAL( -101.0,         "floor( -100.1 )"         ),
      REAL( -101.0,         "floor( -100.5 )"         ),
      REAL( -101.0,         "floor( -100.9 )"         ),
      INTEGER( 100,         "floor( 100 )"            ),
      REAL( 100.0,          "floor( 100.0 )"          ),
      REAL( 100.0,          "floor( 100.1 )"          ),
      REAL( 100.0,          "floor( 100.5 )"          ),
      REAL( 100.0,          "floor( 100.9 )"          ),

      INTEGER( -100,        "round( -100 )"           ),
      REAL( -100.0,         "round( -100.0 )"         ),
      REAL( -100.0,         "round( -100.1 )"         ),
      REAL( -101.0,         "round( -100.5 )"         ),
      REAL( -101.0,         "round( -100.9 )"         ),
      INTEGER( 100,         "round( 100 )"            ),
      REAL( 100.0,          "round( 100.0 )"          ),
      REAL( 100.0,          "round( 100.1 )"          ),
      REAL( 101.0,          "round( 100.5 )"          ),
      REAL( 101.0,          "round( 100.9 )"          ),

      ASSERT(        "ceil( vertex ) == vertex"               ),
      ASSERT(        "ceil( vertex.id ) == vertex.id"         ),
      ASSERT(        "ceil( vertex.vector ) == vertex.vector" ),
      ASSERT(        "ceil( 'hello' ) == 'hello'"             ),
      ASSERT(        "floor( vertex ) == vertex"               ),
      ASSERT(        "floor( vertex.id ) == vertex.id"         ),
      ASSERT(        "floor( vertex.vector ) == vertex.vector" ),
      ASSERT(        "floor( 'hello' ) == 'hello'"             ),
      ASSERT(        "round( vertex ) == vertex"               ),
      ASSERT(        "round( vertex.id ) == vertex.id"         ),
      ASSERT(        "round( vertex.vector ) == vertex.vector" ),
      ASSERT(        "round( 'hello' ) == 'hello'"             ),


      // sign
      INTEGER( -1,          "sign( -100 )"      ),
      INTEGER( -1,          "sign( -1 )"        ),
      INTEGER( 0,           "sign( 0 )"         ),
      INTEGER( 1,           "sign( 1 )"         ),
      INTEGER( 1,           "sign( 100 )"       ),
      REAL( -1.0,           "sign( -100.1 )"    ),
      REAL( -1.0,           "sign( -1.0 )"      ),
      REAL( 0.0,            "sign( 0.0 )"       ),
      REAL( 1.0,            "sign( 1.0 )"       ),
      REAL( 1.0,            "sign( 100.1 )"     ),
      INTEGER( 1,           "sign( vertex )"        ),
      INTEGER( 1,           "sign( vertex.id )"     ),
      INTEGER( 1,           "sign( vertex.vector )" ),
      INTEGER( 1,           "sign( 'hello' )"       ),


      // fac
      REAL( 1.0,             "fac( -1 )"         ),
      REAL( 1.0,             "fac( -1.0 )"       ),
      REAL( 1.0,             "fac( 0 )"          ),
      REAL( 1.0,             "fac( 0.0 )"        ),
      REAL( 1.0,             "fac( 1 )"          ),
      REAL( 1.0,             "fac( 1.0 )"        ),
      REAL( 1.0,             "fac( 1.9 )"        ),
      REAL( 2.0,             "fac( 2 )"          ),
      REAL( 2.0,             "fac( 2.0 )"        ),
      REAL( 2.0,             "fac( 2.9 )"        ),
      REAL( 6.0,             "fac( 3 )"          ),
      REAL( 6.0,             "fac( 3.0 )"        ),
      REAL( 6.0,             "fac( 3.9 )"        ),
      REAL( 24.0,            "fac( 4 )"          ),
      REAL( 24.0,            "fac( 4.0 )"        ),
      REAL( 24.0,            "fac( 4.9 )"        ),
      REAL( 2.43290200817664e+18,   "fac( 20 )"  ),
      ASSERT(         "fac( 169 ) > fac( 168 )"  ),
      ASSERT(         "fac( 170 ) > fac( 169 )"  ),
      ASSERT(         "fac( 171 ) > fac( 170 )"  ),
      REAL( DBL_MAX,         "fac( 171 )"  ),
      ASSERT(         "fac( vertex ) == vertex"               ),
      ASSERT(         "fac( vertex.id ) == vertex.id"         ),
      ASSERT(         "fac( vertex.vector ) == vertex.vector" ),
      ASSERT(         "fac( 'hello' ) == 'hello'"             ),

      // popcnt
      INTEGER( 0,            "popcnt( 0 )" ),
      INTEGER( 1,            "popcnt( 1 )" ),
      INTEGER( 1,            "popcnt( 2 )" ),
      INTEGER( 2,            "popcnt( 3 )" ),
      INTEGER( 1,            "popcnt( 4 )" ),
      INTEGER( 16,           "popcnt( 0xffff )" ),
      INTEGER( 32,           "popcnt( 0xffff0000ffff0000 )" ),
      INTEGER( 64,           "popcnt( 0xffffffffffffffff )" ),

      // comb
      REAL( 1.0,             "comb(0, 0)"         ),
      REAL( 1.0,             "comb(1, 0)"         ),
      REAL( 1.0,             "comb(1, 1)"         ),
      REAL( 1.0,             "comb(2, 0)"         ),
      REAL( 2.0,             "comb(2, 1)"         ),
      REAL( 1.0,             "comb(2, 2)"         ),
      REAL( 1.0,             "comb(3, 0)"         ),
      REAL( 3.0,             "comb(3, 1)"         ),
      REAL( 3.0,             "comb(3, 2)"         ),
      REAL( 1.0,             "comb(3, 3)"         ),
      REAL( 1.0,             "comb(4, 0)"         ),
      REAL( 4.0,             "comb(4, 1)"         ),
      REAL( 6.0,             "comb(4, 2)"         ),
      REAL( 4.0,             "comb(4, 3)"         ),
      REAL( 1.0,             "comb(4, 4)"         ),
      REAL( 1.0,             "comb(5, 0)"         ),
      REAL( 5.0,             "comb(5, 1)"         ),
      REAL( 10.0,            "comb(5, 2)"         ),
      REAL( 10.0,            "comb(5, 3)"         ),
      REAL( 5.0,             "comb(5, 4)"         ),
      REAL( 1.0,             "comb(5, 5)"         ),
      REAL( 1.0,             "comb(5.0, 0)"       ),
      REAL( 5.0,             "comb(5, 1.0)"       ),
      REAL( 10.0,            "comb(5.1, 2.1)"     ),
      REAL( 10.0,            "comb(5, 3.7)"       ),
      REAL( 5.0,             "comb(5.8, 4.3)"     ),
      REAL( 1.0,             "comb(5.9, 5.1)"     ),
      XREAL(                 comb(20, 7)          ),
      XREAL(                 comb(50, 13)         ),
      INTEGER( 1,            "comb( 5, vertex )"          ),
      INTEGER( 1,            "comb( 5, next )"            ),
      INTEGER( 1,            "comb( 5, vertex.id )"       ),
      INTEGER( 1,            "comb( 5, vertex.vector )"   ),
      INTEGER( 1,            "comb( 5, 'hello' )"         ),
      INTEGER( 1,            "comb( vertex, vertex )"          ),
      INTEGER( 1,            "comb( next,   next )"            ),
      INTEGER( 1,            "comb( next,   vertex )"          ),
      INTEGER( 1,            "comb( vertex, next )"            ),
      INTEGER( 1,            "comb( vertex, vertex.id )"       ),
      INTEGER( 1,            "comb( vertex, vertex.vector )"   ),
      INTEGER( 1,            "comb( vertex, 'hello' )"         ),
      INTEGER( 1,            "comb( next,   'hello' )"         ),
      INTEGER( 1,            "comb( vertex, 5 )"          ),
      INTEGER( 1,            "comb( next, 5 )"            ),
      INTEGER( 1,            "comb( vertex.id, 5 )"       ),
      INTEGER( 1,            "comb( vertex.vector, 5 )"   ),
      INTEGER( 1,            "comb( 'hello', 5 )"         ),


      // add
      XINTEGER(                             0                 ),
      XINTEGER(                             1                 ),
      XINTEGER(                         0 + 0                 ),
      XINTEGER(                         0 + 1                 ),
      XINTEGER(                         2 + 0                 ),
      XINTEGER(                         2 + 1                 ),
      XINTEGER(                     0 + 0 + 0                 ),
      XINTEGER(                     0 + 0 + 1                 ),
      XINTEGER(                     0 + 2 + 0                 ),
      XINTEGER(                     0 + 2 + 1                 ),
      XINTEGER(                     3 + 0 + 0                 ),
      XINTEGER(                     3 + 0 + 1                 ),
      XINTEGER(                     3 + 2 + 0                 ),
      XINTEGER(                     3 + 2 + 1                 ),
      XINTEGER(                 0 + 0 + 0 + 0                 ),
      XINTEGER(                 0 + 0 + 0 + 1                 ),
      XINTEGER(                 0 + 0 + 2 + 0                 ),
      XINTEGER(                 0 + 0 + 2 + 1                 ),
      XINTEGER(                 0 + 3 + 0 + 0                 ),
      XINTEGER(                 0 + 3 + 0 + 1                 ),
      XINTEGER(                 0 + 3 + 2 + 0                 ),
      XINTEGER(                 0 + 3 + 2 + 1                 ),
      XINTEGER(                 4 + 0 + 0 + 0                 ),
      XINTEGER(                 4 + 0 + 0 + 1                 ),
      XINTEGER(                 4 + 0 + 2 + 0                 ),
      XINTEGER(                 4 + 0 + 2 + 1                 ),
      XINTEGER(                 4 + 3 + 0 + 0                 ),
      XINTEGER(                 4 + 3 + 0 + 1                 ),
      XINTEGER(                 4 + 3 + 2 + 0                 ),
      XINTEGER(                 4 + 3 + 2 + 1                 ),
      XREAL(                          0.0 + 0                 ),
      XREAL(                          0.0 + 1                 ),
      XREAL(                          2.2 + 0                 ),
      XREAL(                          2.2 + 1                 ),
      XREAL(                    0.0 + 0.0 + 0                 ),
      XREAL(                    0.0 + 0.0 + 1                 ),
      XREAL(                    0.0 + 2.2 + 0                 ),
      XREAL(                    0.0 + 2.2 + 1                 ),
      XREAL(                    3.3 + 0.0 + 0                 ),
      XREAL(                    3.3 + 0.0 + 1                 ),
      XREAL(                    3.3 + 2.2 + 0                 ),
      XREAL(                    3.3 + 2.2 + 1                 ),
      XREAL(                0 + 0.0 + 0.0 + 0                 ),
      XREAL(                0 + 0.0 + 0.0 + 1                 ),
      XREAL(                0 + 0.0 + 2.2 + 0                 ),
      XREAL(                0 + 0.0 + 2.2 + 1                 ),
      XREAL(                0 + 3.3 + 0.0 + 0                 ),
      XREAL(                0 + 3.3 + 0.0 + 1                 ),
      XREAL(                0 + 3.3 + 2.2 + 0                 ),
      XREAL(                0 + 3.3 + 2.2 + 1                 ),
      XREAL(                4 + 0.0 + 0.0 + 0                 ),
      XREAL(                4 + 0.0 + 0.0 + 1                 ),
      XREAL(                4 + 0.0 + 2.2 + 0                 ),
      XREAL(                4 + 0.0 + 2.2 + 1                 ),
      XREAL(                4 + 3.3 + 0.0 + 0                 ),
      XREAL(                4 + 3.3 + 0.0 + 1                 ),
      XREAL(                4 + 3.3 + 2.2 + 0                 ),
      XREAL(                4 + 3.3 + 2.2 + 1                 ),
      ASSERT(       "vertex + 1 == bitvector(vertex) + 1" ),
      ASSERT(       "1 + vertex == 1 + bitvector(vertex)" ),
      ASSERT(       "2 + vertex == 2 + bitvector(vertex)" ),
      ASSERT(       "vertex + vertex == 2 * vertex"    ),
      ASSERT(       "vertex + 1.0 == bitvector(vertex) + 1.0" ),
      ASSERT(       "1.0 + vertex == bitvector(vertex) + 1.0" ),
      REAL( 5.0,           "5.0 + vertex['nope']"             ),
      REAL( 5.0,           "vertex['nope'] + 5.0"             ),
      REAL( 10.0,          "5.0 + vertex['nope'] + 5.0"       ),
      ASSERT(       "vertex.id + 1 == bitvector(vertex.id) + 1" ),
      ASSERT(       "1 + vertex.id == bitvector(vertex.id) + 1" ),
      ASSERT(       "vertex.vector + 1 == bitvector(vertex.vector) + 1" ),
      ASSERT(       "1 + vertex.vector == bitvector(vertex.vector) + 1" ),
      ASSERT(       "isvector( vertex.vector + vertex.vector )" ),
      STRING( "hello1",    "'hello' + 1"                      ),



      // sub
      XINTEGER(                             0                 ),
      XINTEGER(                             1                 ),
      XINTEGER(                         0 - 0                 ),
      XINTEGER(                         0 - 1                 ),
      XINTEGER(                         2 - 0                 ),
      XINTEGER(                         2 - 1                 ),
      XINTEGER(                     0 - 0 - 0                 ),
      XINTEGER(                     0 - 0 - 1                 ),
      XINTEGER(                     0 - 2 - 0                 ),
      XINTEGER(                     0 - 2 - 1                 ),
      XINTEGER(                     3 - 0 - 0                 ),
      XINTEGER(                     3 - 0 - 1                 ),
      XINTEGER(                     3 - 2 - 0                 ),
      XINTEGER(                     3 - 2 - 1                 ),
      XINTEGER(                 0 - 0 - 0 - 0                 ),
      XINTEGER(                 0 - 0 - 0 - 1                 ),
      XINTEGER(                 0 - 0 - 2 - 0                 ),
      XINTEGER(                 0 - 0 - 2 - 1                 ),
      XINTEGER(                 0 - 3 - 0 - 0                 ),
      XINTEGER(                 0 - 3 - 0 - 1                 ),
      XINTEGER(                 0 - 3 - 2 - 0                 ),
      XINTEGER(                 0 - 3 - 2 - 1                 ),
      XINTEGER(                 4 - 0 - 0 - 0                 ),
      XINTEGER(                 4 - 0 - 0 - 1                 ),
      XINTEGER(                 4 - 0 - 2 - 0                 ),
      XINTEGER(                 4 - 0 - 2 - 1                 ),
      XINTEGER(                 4 - 3 - 0 - 0                 ),
      XINTEGER(                 4 - 3 - 0 - 1                 ),
      XINTEGER(                 4 - 3 - 2 - 0                 ),
      XINTEGER(                 4 - 3 - 2 - 1                 ),
      XREAL(                          0.0 - 0                 ),
      XREAL(                          0.0 - 1                 ),
      XREAL(                          2.2 - 0                 ),
      XREAL(                          2.2 - 1                 ),
      XREAL(                    0.0 - 0.0 - 0                 ),
      XREAL(                    0.0 - 0.0 - 1                 ),
      XREAL(                    0.0 - 2.2 - 0                 ),
      XREAL(                    0.0 - 2.2 - 1                 ),
      XREAL(                    3.3 - 0.0 - 0                 ),
      XREAL(                    3.3 - 0.0 - 1                 ),
      XREAL(                    3.3 - 2.2 - 0                 ),
      XREAL(                    3.3 - 2.2 - 1                 ),
      XREAL(                0 - 0.0 - 0.0 - 0                 ),
      XREAL(                0 - 0.0 - 0.0 - 1                 ),
      XREAL(                0 - 0.0 - 2.2 - 0                 ),
      XREAL(                0 - 0.0 - 2.2 - 1                 ),
      XREAL(                0 - 3.3 - 0.0 - 0                 ),
      XREAL(                0 - 3.3 - 0.0 - 1                 ),
      XREAL(                0 - 3.3 - 2.2 - 0                 ),
      XREAL(                0 - 3.3 - 2.2 - 1                 ),
      XREAL(                4 - 0.0 - 0.0 - 0                 ),
      XREAL(                4 - 0.0 - 0.0 - 1                 ),
      XREAL(                4 - 0.0 - 2.2 - 0                 ),
      XREAL(                4 - 0.0 - 2.2 - 1                 ),
      XREAL(                4 - 3.3 - 0.0 - 0                 ),
      XREAL(                4 - 3.3 - 0.0 - 1                 ),
      XREAL(                4 - 3.3 - 2.2 - 0                 ),
      XREAL(                4 - 3.3 - 2.2 - 1                 ),
      ASSERT(       "vertex - 1 == bitvector(vertex) - 1" ),
      INTEGER( 1,          "1 - vertex == 1 - bitvector(vertex)" ),
      ASSERT(       "vertex - vertex == 0"        ),
      ASSERT(       "vertex - 1.0 == bitvector(vertex) - 1.0" ),
      ASSERT(       "1.0 - vertex == 1.0 - bitvector(vertex)" ),
      REAL( 5.0,           "5.0 - vertex['nope']"             ),
      REAL( -5.0,          "vertex['nope'] - 5.0"             ),
      REAL( 0.0,           "5.0 - vertex['nope'] - 5.0"       ),
      ASSERT(       "vertex.id - 1 == bitvector(vertex.id) - 1" ),
      ASSERT(       "1 - vertex.id == 1 - bitvector(vertex.id)" ),
      ASSERT(       "vertex.id - vertex.id == 0" ),
      ASSERT(       "vertex.vector - 1 == bitvector(vertex.vector) - 1" ),
      ASSERT(       "1 - vertex.vector == 1 - bitvector(vertex.vector)" ),
      ASSERT(       "isvector( vertex.vector - vertex.vector )" ),
      ASSERT(       "'hello' - 'hello' != 0"     ),


      // mul
      XINTEGER(                             0                 ),
      XINTEGER(                             1                 ),
      XINTEGER(                         0 * 0                 ),
      XINTEGER(                         0 * 1                 ),
      XINTEGER(                         2 * 0                 ),
      XINTEGER(                         2 * 1                 ),
      XINTEGER(                     0 * 0 * 0                 ),
      XINTEGER(                     0 * 0 * 1                 ),
      XINTEGER(                     0 * 2 * 0                 ),
      XINTEGER(                     0 * 2 * 1                 ),
      XINTEGER(                     3 * 0 * 0                 ),
      XINTEGER(                     3 * 0 * 1                 ),
      XINTEGER(                     3 * 2 * 0                 ),
      XINTEGER(                     3 * 2 * 1                 ),
      XINTEGER(                 0 * 0 * 0 * 0                 ),
      XINTEGER(                 0 * 0 * 0 * 1                 ),
      XINTEGER(                 0 * 0 * 2 * 0                 ),
      XINTEGER(                 0 * 0 * 2 * 1                 ),
      XINTEGER(                 0 * 3 * 0 * 0                 ),
      XINTEGER(                 0 * 3 * 0 * 1                 ),
      XINTEGER(                 0 * 3 * 2 * 0                 ),
      XINTEGER(                 0 * 3 * 2 * 1                 ),
      XINTEGER(                 4 * 0 * 0 * 0                 ),
      XINTEGER(                 4 * 0 * 0 * 1                 ),
      XINTEGER(                 4 * 0 * 2 * 0                 ),
      XINTEGER(                 4 * 0 * 2 * 1                 ),
      XINTEGER(                 4 * 3 * 0 * 0                 ),
      XINTEGER(                 4 * 3 * 0 * 1                 ),
      XINTEGER(                 4 * 3 * 2 * 0                 ),
      XINTEGER(                 4 * 3 * 2 * 1                 ),
      XREAL(                          0.0 * 0                 ),
      XREAL(                          0.0 * 1                 ),
      XREAL(                          2.2 * 0                 ),
      XREAL(                          2.2 * 1                 ),
      XREAL(                    0.0 * 0.0 * 0                 ),
      XREAL(                    0.0 * 0.0 * 1                 ),
      XREAL(                    0.0 * 2.2 * 0                 ),
      XREAL(                    0.0 * 2.2 * 1                 ),
      XREAL(                    3.3 * 0.0 * 0                 ),
      XREAL(                    3.3 * 0.0 * 1                 ),
      XREAL(                    3.3 * 2.2 * 0                 ),
      XREAL(                    3.3 * 2.2 * 1                 ),
      XREAL(                0 * 0.0 * 0.0 * 0                 ),
      XREAL(                0 * 0.0 * 0.0 * 1                 ),
      XREAL(                0 * 0.0 * 2.2 * 0                 ),
      XREAL(                0 * 0.0 * 2.2 * 1                 ),
      XREAL(                0 * 3.3 * 0.0 * 0                 ),
      XREAL(                0 * 3.3 * 0.0 * 1                 ),
      XREAL(                0 * 3.3 * 2.2 * 0                 ),
      XREAL(                0 * 3.3 * 2.2 * 1                 ),
      XREAL(                4 * 0.0 * 0.0 * 0                 ),
      XREAL(                4 * 0.0 * 0.0 * 1                 ),
      XREAL(                4 * 0.0 * 2.2 * 0                 ),
      XREAL(                4 * 0.0 * 2.2 * 1                 ),
      XREAL(                4 * 3.3 * 0.0 * 0                 ),
      XREAL(                4 * 3.3 * 0.0 * 1                 ),
      XREAL(                4 * 3.3 * 2.2 * 0                 ),
      XREAL(                4 * 3.3 * 2.2 * 1                 ),
      // null * ...
      ASSERT(       "null * null == null"              ),    // null
      ASSERT(       "v = null * 5; isint(v) && v == 0" ),    // INT
      ASSERT(       "v = null * 5.5; isreal(v) && v == 0.0" ),    // REA
      ASSERT(       "isnan( null * nan )"              ),    // NAN
      ASSERT(       "isnan( null * vertex )"           ),    // VTX
      ASSERT(       "isnan( null * 'Fun' )"            ),    // STR
      ASSERT(       "isnan( null * vertex.vector )"    ),    // VEC
      ASSERT(       "v = null * 0b101; isbitvector(v) && v == 0b0" ),    // BTV
      ASSERT(       "isnan( null * keyval(2, 3.14) )"  ),    // KYV
      ASSERT(       "isnan( null * vertex.id )"        ),    // VID
      // INT * ...
      ASSERT(       "v = 5 * null; isint(v) && v == 0" ),    // INT
      ASSERT(       "5 * 5 == 25"                      ),    // INT
      ASSERT(       "5 * 5.5 == 27.5"                  ),    // REA
      ASSERT(       "isnan( 5 * nan )"                 ),    // NAN
      ASSERT(       "isbitvector( 5 * vertex )"        ),    // VTX
      ASSERT(       "isnan( 5 * 'Fun' )"               ),    // STR
      ASSERT(       "isnan( 5 * vertex.vector )"       ),    // VEC
      ASSERT(       "val = 5 * 0b101; isbitvector(val) && val == 0b11001" ), // BTV
      ASSERT(       "isnan( 5 * keyval(2, 3.14) ) "    ),    // KYV
      ASSERT(       "isnan( 5 * vertex.id )"           ),    // VID
      // REA * ...
      ASSERT(       "v = 5.5 * null; isreal(v) && v == 0.0" ), // null
      ASSERT(       "5.5 * 5 == 27.5"                  ),    // INT
      ASSERT(       "5.5 * 5.5 == 30.25"               ),    // REA
      ASSERT(       "isnan( 5.5 * nan )"               ),    // NAN
      ASSERT(       "isnan( 5.5 * vertex )"            ),    // VTX
      ASSERT(       "isnan( 5.5 * 'Fun' )"             ),    // STR
      ASSERT(       "isnan( 5.5 * vertex.vector )"     ),    // VEC
      ASSERT(       "isnan( 5.5 * 0b101 )"             ),    // BTV
      ASSERT(       "isnan( 5.5 * keyval(2, 3.14) )"   ),    // KYV
      ASSERT(       "isnan( 5.5 * vertex.id )"         ),    // VID
      // NAN * ...
      ASSERT(       "isnan( nan * null )"              ),    // null
      ASSERT(       "isnan( nan * 5 )"                 ),    // INT
      ASSERT(       "isnan( nan * 5.5 )"               ),    // REA
      ASSERT(       "isnan( nan * nan )"               ),    // NAN
      ASSERT(       "isnan( nan * vertex )"            ),    // VTX
      ASSERT(       "isnan( nan * 'Fun')"              ),    // STR
      ASSERT(       "isnan( nan * vertex.vector )"     ),    // VEC
      ASSERT(       "isnan( nan * 0b101 )"             ),    // BTV
      ASSERT(       "isnan( nan * keyval(2, 3.14) ) "  ),    // KYV
      ASSERT(       "isnan( nan * vertex.id )"         ),    // VID
      // VTX * ...
      ASSERT(       "vertex * null == null"            ),    // null
      ASSERT(       "isbitvector( vertex * 5 ) && vertex * 5 == 5 * vertex" ), // INT
      ASSERT(       "isnan( vertex * 5.5 )"            ),    // REA
      ASSERT(       "isnan( vertex * nan )"            ),    // NAN
      ASSERT(       "isbitvector( vertex * vertex )"   ),    // VTX
      ASSERT(       "isnan( vertex * 'Fun' )"          ),    // STR
      ASSERT(       "isnan( vertex * vertex.vector )"  ),    // VEC
      ASSERT(       "isbitvector( vertex * 0b101 ) && vertex * 0b101 == 0b101 * vertex" ), // BTV
      ASSERT(       "isnan( vertex * keyval(2, 3.14) )" ),   // KYV
      ASSERT(       "isnan( vertex * vertex.id )"      ),    // VID
      // STR * ...
      ASSERT(       "isnan( 'Fun' * null )"            ),    // null
      ASSERT(       "isnan( 'Fun' * 5 )"               ),    // INT
      ASSERT(       "isnan( 'Fun' * 5.5 )"             ),    // REA
      ASSERT(       "isnan( 'Fun' * nan )"             ),    // NAN
      ASSERT(       "isnan( 'Fun' * vertex )"          ),    // VTX
      ASSERT(       "isnan( 'Fun' * 'Fun')"            ),    // STR
      ASSERT(       "isnan( 'Fun' * vertex.vector )"   ),    // VEC
      ASSERT(       "isnan( 'Fun' * 0b101 )"           ),    // BTV
      ASSERT(       "isnan( 'Fun' * keyval(2, 3.14) )" ),    // KYV
      ASSERT(       "isnan( 'Fun' * vertex.id )"       ),    // VID
      // BTV * ...
      ASSERT(       "v = 0b101 * null; isbitvector(v) && v == 0b0" ), // null
      ASSERT(       "v = 0b101 * 5; isbitvector(v) && v == 0b11001" ), // INT
      ASSERT(       "isnan( 0b101 * 5.5 )"             ),    // REA
      ASSERT(       "isnan( 0b101 * nan )"             ),    // NAN
      ASSERT(       "v = 0b101 * vertex; isbitvector(v) && v == vertex * 0b101 && v != 0" ), // BTV
      ASSERT(       "isnan( 0b101 * 'Fun')"            ),    // STR
      ASSERT(       "isnan( 0b101 * vertex.vector )"   ),    // VEC
      ASSERT(       "v = 0b101 * 0b101; bitvector(v) && v == 0b11001" ),    // BTV
      ASSERT(       "isnan( 0b101 * keyval(2, 3.14) )" ),    // KYV
      ASSERT(       "isnan( 0b101 * vertex.id )"       ),    // VID
      // STR * ...
      ASSERT(       "isnan( keyval(2, 3.14) * null )"  ),    // null
      ASSERT(       "isnan( keyval(2, 3.14) * 5 )"     ),    // INT
      ASSERT(       "isnan( keyval(2, 3.14) * 5.5 )"   ),    // REA
      ASSERT(       "isnan( keyval(2, 3.14) * nan )"   ),    // NAN
      ASSERT(       "isnan( keyval(2, 3.14) * vertex )" ),    // VTX
      ASSERT(       "isnan( keyval(2, 3.14) * 'Fun')" ),    // STR
      ASSERT(       "isnan( keyval(2, 3.14) * vertex.vector )" ),    // VEC
      ASSERT(       "isnan( keyval(2, 3.14) * 0b101 )" ),    // BTV
      ASSERT(       "isnan( keyval(2, 3.14) * keyval(2, 3.14) )" ),    // KYV
      ASSERT(       "isnan( keyval(2, 3.14) * vertex.id )" ),    // VID
      ASSERT(       "isreal( vertex.vector * vertex.vector )" ),
      ASSERT(       "isvector( 2.5 * vertex.vector )" ),
      ASSERT(       "isvector( vertex.vector * 2.5 )" ),

      // div
      REAL( 0.0/FLT_MIN,               "0 / 0"                ),
      REAL( 1.0/FLT_MIN,               "1 / 0"                ),
      REAL( 1000.0/FLT_MIN,         "1000 / 0"                ),
      REAL( 0.0/FLT_MIN,             "0 / 0.0"                ),
      REAL( 1.0/FLT_MIN,             "1 / 0.0"                ),
      REAL( 1000.0/FLT_MIN,       "1000 / 0.0"                ),
      REAL( 0.0,                       "0 / 1"                ),
      REAL( 2.0,                       "2 / 1"                ),
      REAL( 0.0,                   "0 / 2 / 1"                ),
      REAL( 1.5,                   "3 / 2 / 1"                ),
      REAL( 0.0,               "0 / 3 / 2 / 1"                ),
      REAL( 2/3.0,             "4 / 3 / 2 / 1"                ),
      XREAL(                          0.0 / 1                 ),
      XREAL(                          2.2 / 1                 ),
      XREAL(                    0.0 / 2.2 / 1                 ),
      XREAL(                    3.3 / 2.2 / 1                 ),
      XREAL(                0 / 3.3 / 2.2 / 1                 ),
      XREAL(                4 / 3.3 / 2.2 / 1                 ),
      ASSERT(             "vertex / 2 == vertex"             ),
      INTEGER( 2,         "2 / vertex"                       ),
      ASSERT(             "vertex / vertex == vertex"        ),
      ASSERT(             "vertex / 2.2 == vertex"           ),
      REAL( 2.2,          "2.2 / vertex"                     ),
      ASSERT(             "vertex.id / 1 == vertex.id"       ),
      ASSERT(             "1 / vertex.id == 1"               ),
      ASSERT(             "vertex.id / vertex.id  == vertex.id" ),
      ASSERT(             "vertex.vector / 1 == vertex.vector"  ),
      ASSERT(             "1 / vertex.vector == 1"              ),
      ASSERT(             "vertex.vector / vertex.vector == vertex.vector"   ),
      ASSERT(             "'hello' / 1 == 'hello'"                           ),
      ASSERT(             "1 / 'hello' == 1"                                 ),
      ASSERT(             "'hello' / 'hello' == 'hello'"                     ),
        

      // mod
      INTEGER( 0,                      "0 % 1"                ),
      INTEGER( 0,                      "2 % 1"                ),
      INTEGER( 0,                  "0 % 2 % 1"                ),
      INTEGER( 0,                  "3 % 2 % 1"                ),
      INTEGER( 0,              "0 % 3 % 2 % 1"                ),
      INTEGER( 0,              "4 % 3 % 2 % 1"                ),
      REAL( 0.0,                     "0.0 % 1"                ),
      REAL( 0.2,                     "2.2 % 1"                ),
      REAL( 0.0,               "0.0 % 2.2 % 1"                ),
      REAL( 0.1,               "3.3 % 2.2 % 1"                ),
      REAL( 0.0,           "0 % 3.3 % 2.2 % 1"                ),
      REAL( 0.7,           "4 % 3.3 % 2.2 % 1"                ),
      ASSERT(               "vertex.id % 1 == vertex.id"      ),
      INTEGER( 1,           "1 % vertex.id"                   ),
      ASSERT(               "vertex.id % vertex.id == vertex.id" ),
      ASSERT(               "vertex.vector % 1 == vertex.vector" ),
      INTEGER( 1,           "1 % vertex.vector"               ),
      ASSERT(               "vertex.vector % vertex.vector == vertex.vector" ),
      ASSERT(               "'hello' % 1 == 'hello'"          ),
      INTEGER( 1,           "1 % 'hello'"                     ),
      ASSERT(               "'hello' % 'hello' == 'hello' "   ),


      // pow
      INTEGER( 1,           "0 ** 0"         ),
      INTEGER( 1,           "0 ** 0.0"       ),
      INTEGER( 1,           "1 ** 0"         ),
      INTEGER( 1,           "1 ** 0.0"       ),
      INTEGER( 1,           "(-1) ** 0"      ),
        
      INTEGER( 0,           "0   ** 2"       ),
      REAL( 0.0,            "0   ** 2.0"     ),
      REAL( 0.0,            "0.0 ** 2"       ),
      REAL( 0.0,            "0.0 ** 2.0"     ),
      INTEGER( LLONG_MIN,   "0   ** -2"      ),
      REAL( DBL_MAX,        "0   ** -2.0"    ),
      REAL( DBL_MAX,        "0.0 ** -2"      ),
      REAL( DBL_MAX,        "0.0 ** -2.0"    ),

      INTEGER( 1,           "1 ** 1"         ),
      REAL( 1.0,            "1 ** 1.0"       ),
      REAL( 1.0,            "1.0 ** 1"       ),
      REAL( 1.0,            "1.0 ** 1.0"     ),

      INTEGER( 1,           "1 ** 1"         ),
      REAL( 1.0,            "1 ** 2.1"       ),
      REAL( 2.1,            "2.1 ** 1"       ),
      REAL( 4.749638091742, "2.1 ** 2.1"     ),

      INTEGER( 1,           "vertex        ** 0"    ),
      INTEGER( 1,           "vertex        ** 0.0"  ),
      INTEGER( 1,           "vertex.id     ** 0"    ),
      INTEGER( 1,           "vertex.id     ** 0.0"  ),
      INTEGER( 1,           "vertex.vector ** 0"    ),
      INTEGER( 1,           "vertex.vector ** 0.0"  ),
      INTEGER( 1,           "'hello'       ** 0"    ),
      INTEGER( 1,           "'hello'       ** 0.0"  ),

      INTEGER( 5,           "5 ** vertex"                     ),
      INTEGER( 5,           "5 ** vertex.id"                  ),
      INTEGER( 5,           "5 ** vertex.vector"              ),
      INTEGER( 5,           "5 ** 'hello'"                    ),
      REAL( 5.1,            "5.1 ** vertex"                   ),
      REAL( 5.1,            "5.1 ** vertex.id"                ),
      REAL( 5.1,            "5.1 ** vertex.vector"            ),
      REAL( 5.1,            "5.1 ** 'hello'"                  ),
      ASSERT(        "vertex ** vertex == vertex"      ),
      ASSERT(        "vertex ** vertex.id == vertex"             ),
      ASSERT(        "vertex ** vertex.vector == vertex"         ),
      ASSERT(        "vertex ** 'hello' == vertex"               ),
      ASSERT(        "vertex.id ** vertex == vertex.id"             ),
      ASSERT(        "vertex.id ** vertex.id == vertex.id"          ),
      ASSERT(        "vertex.id ** vertex.vector == vertex.id"      ),
      ASSERT(        "vertex.id ** 'hello' == vertex.id"            ),
      ASSERT(        "vertex.vector ** vertex == vertex.vector"         ),
      ASSERT(        "vertex.vector ** vertex.id == vertex.vector"      ),
      ASSERT(        "vertex.vector ** vertex.vector == vertex.vector"  ),
      ASSERT(        "vertex.vector ** 'hello' == vertex.vector"        ),
      ASSERT(        "'hello' ** vertex == 'hello'"               ),
      ASSERT(        "'hello' ** vertex.id == 'hello'"            ),
      ASSERT(        "'hello' ** vertex.vector == 'hello'"        ),
      ASSERT(        "'hello' ** 'hello' == 'hello'"              ),

      // atan2
      XREAL(                atan2( 0, 0 )                     ),
      XREAL(                atan2( 0, 1 )                     ),
      XREAL(                atan2( 0, 2 )                     ),
      XREAL(                atan2( 0, 3 )                     ),
      XREAL(                atan2( 0, -1 )                    ),
      XREAL(                atan2( 0, -2 )                    ),
      XREAL(                atan2( 0, -3 )                    ),
      XREAL(                atan2( 0, 0.1 )                   ),
      XREAL(                atan2( 0, 1.1 )                   ),
      XREAL(                atan2( 0, 2.1 )                   ),
      XREAL(                atan2( 0, 3.1 )                   ),
      XREAL(                atan2( 0, -1.1 )                  ),
      XREAL(                atan2( 0, -2.1 )                  ),
      XREAL(                atan2( 0, -3.1 )                  ),
      XREAL(                atan2( 0.2, 0 )                   ),
      XREAL(                atan2( 0.2, 1 )                   ),
      XREAL(                atan2( 0.2, 2 )                   ),
      XREAL(                atan2( 0.2, 3 )                   ),
      XREAL(                atan2( 0.2, -1 )                  ),
      XREAL(                atan2( 0.2, -2 )                  ),
      XREAL(                atan2( 0.2, -3 )                  ),
      XREAL(                atan2( 0.2, 0.1 )                 ),
      XREAL(                atan2( 0.2, 1.1 )                 ),
      XREAL(                atan2( 0.2, 2.1 )                 ),
      XREAL(                atan2( 0.2, 3.1 )                 ),
      XREAL(                atan2( 0.2, -1.1 )                ),
      XREAL(                atan2( 0.2, -2.1 )                ),
      XREAL(                atan2( 0.2, -3.1 )                ),


      // max / min
      INTEGER( 0,            "max( 0, 0 )"                    ),
      INTEGER( 1,            "max( 0, 1 )"                    ),
      INTEGER( 1,            "max( 1, 0 )"                    ),
      INTEGER( 1,            "max( 1, 1 )"                    ),
      INTEGER( 0,            "min( 0, 0 )"                    ),
      INTEGER( 0,            "min( 0, 1 )"                    ),
      INTEGER( 0,            "min( 1, 0 )"                    ),
      INTEGER( 1,            "min( 1, 1 )"                    ),

      INTEGER( 0,            "max( 0, 0.0 )"                  ),
      REAL( 1.0,             "max( 0, 1.0 )"                  ),
      INTEGER( 1,            "max( 1, 0.0 )"                  ),
      INTEGER( 1,            "max( 1, 1.0 )"                  ),
      INTEGER( 0,            "min( 0, 0.0 )"                  ),
      INTEGER( 0,            "min( 0, 1.0 )"                  ),
      REAL( 0.0,             "min( 1, 0.0 )"                  ),
      INTEGER( 1,            "min( 1, 1.0 )"                  ),

      REAL( 0.0,             "max( 0.0, 0 )"                  ),
      INTEGER( 1,            "max( 0.0, 1 )"                  ),
      REAL( 1.0,             "max( 1.0, 0 )"                  ),
      REAL( 1.0,             "max( 1.0, 1 )"                  ),
      REAL( 0.0,             "min( 0.0, 0 )"                  ),
      REAL( 0.0,             "min( 0.0, 1 )"                  ),
      INTEGER( 0,            "min( 1.0, 0 )"                  ),
      REAL( 1.0,             "min( 1.0, 1 )"                  ),

      ASSERT(         "max( 'hello', 'hell'  ) == 'hello'"    ),
      ASSERT(         "max( 'hello', 'helln' ) == 'hello'"    ),
      ASSERT(         "max( 'hello', 'hellp' ) == 'hellp'"    ),
      ASSERT(         "max( 'hell',  'hello' ) == 'hello'"    ),
      ASSERT(         "max( 'helln', 'hello' ) == 'hello'"    ),
      ASSERT(         "max( 'hellp', 'hello' ) == 'hellp'"    ),

      ASSERT(         "min( 'hello', 'hell'  ) == 'hell'"     ),
      ASSERT(         "min( 'hello', 'helln' ) == 'helln'"    ),
      ASSERT(         "min( 'hello', 'hellp' ) == 'hello'"    ),
      ASSERT(         "min( 'hell',  'hello' ) == 'hell'"     ),
      ASSERT(         "min( 'helln', 'hello' ) == 'helln'"    ),
      ASSERT(         "min( 'hellp', 'hello' ) == 'hello'"    ),

      ASSERT(         "max( vertex, next ) == next"    ), // because B was created after ROOT
      ASSERT(         "max( next, vertex ) == next"    ),
      ASSERT(         "min( vertex, next ) == vertex"  ),
      ASSERT(         "min( next, vertex ) == vertex"  ),


      // prox
      REAL( 1.0,             "prox(    0,      0   )"             ),
      REAL( 1.0,             "prox(  100,    100   )"             ),
      REAL( 1.0,             "prox( -100,   -100   )"             ),
      REAL( 1.0,             "prox(    0,      0.0 )"             ),
      REAL( 1.0,             "prox(  100,    100.0 )"             ),
      REAL( 1.0,             "prox( -100,   -100.0 )"             ),
      REAL( 1.0,             "prox(    0.0,    0.0 )"             ),
      REAL( 1.0,             "prox(  100.0,  100.0 )"             ),
      REAL( 1.0,             "prox( -100.0, -100.0 )"             ),
      REAL( 1.0,             "prox(    0.0,    0   )"             ),
      REAL( 1.0,             "prox(  100.0,  100   )"             ),
      REAL( 1.0,             "prox( -100.0, -100   )"             ),

      ASSERT(         "prox( 1, 2 ) < 1"                   ),
      ASSERT(         "prox( 2, 1 ) < 1"                   ),
      ASSERT(         "prox( 2, 1 ) == prox( 1, 2 )"       ),
      ASSERT(         "prox( 2, 1 ) > prox( 3, 1 )"        ),
      ASSERT(         "prox( 3, 1 ) > prox( 4, 1 )"        ),
      ASSERT(         "prox( 1.1e10, 1.0e10 ) > prox( 0.8e10, 1.0e10 )" ),
      ASSERT(         "prox( -1, -1.0e10 ) > prox( 1, -1.0e10 )" ),
      ASSERT(         "prox( -1, -1.0e10 ) < prox( -2, -1.0e10 )" ),

      ASSERT(         "prox( 'hello', 'hello' ) < 1.0"     ),
      ASSERT(         "prox( 'hello', 'hello' ) > 0.1"     ),
      ASSERT(         "prox( 'hello', 'hi' ) < 1.0"        ),
      ASSERT(         "prox( 'hello', 'hi' ) > 0.1"        ),

      ASSERT(         "prox( vertex, vertex ) == 1.0"      ),
      ASSERT(         "prox( vertex, next ) < 1.0"         ),
      ASSERT(         "prox( vertex, next ) > 0.1"         ),

      // approx
      ASSERT(         "approx( 49.999, 50.0, 0.001 )"      ),
      ASSERT(         "approx( 49.999, 50.0, 0.0001 )"     ),
      UNTRUE(         "approx( 49.999, 50.0, 0.00001 )"    ),
      ASSERT(         "approx( -49.999, -50.0, 0.001 )"    ),
      ASSERT(         "approx( -49.999, -50.0, 0.0001 )"   ),
      UNTRUE(         "approx( -49.999, -50.0, 0.00001 )"  ),
      ASSERT(         "approx( 49999, 50000, 2 )"          ),
      ASSERT(         "approx( 49999, 50000, 1 )"          ),
      UNTRUE(         "approx( 49999, 50000, 0 )"          ),

      // firstval
      ASSERT(         "isnan( firstval() )"                ),
      ASSERT(         "firstval( 1 ) == 1"                 ),
      ASSERT(         "firstval( 1, 2 ) == 1"              ),
      ASSERT(         "firstval( 2, 1 ) == 2"              ),
      ASSERT(         "firstval( 1, 2, 3 ) == 1"           ),
      ASSERT(         "firstval( 3, 2, 1 ) == 3"           ),
      ASSERT(         "firstval( vertex['nix'], 2, 1 ) == 2"                 ),
      ASSERT(         "firstval( vertex['nix'], vertex['nope'], 1 ) == 1"    ),
      ASSERT(         "firstval( 4, vertex['nix'], vertex['nope'], 1 ) == 4" ),
      ASSERT(         "isnan( firstval( vertex['nix'], vertex['nope'] ) )"   ),

      // lastval
      ASSERT(         "isnan( lastval() )"                ),
      ASSERT(         "lastval( 1 ) == 1"                 ),
      ASSERT(         "lastval( 1, 2 ) == 2"              ),
      ASSERT(         "lastval( 2, 1 ) == 1"              ),
      ASSERT(         "lastval( 1, 2, 3 ) == 3"           ),
      ASSERT(         "lastval( 3, 2, 1 ) == 1"           ),
      ASSERT(         "lastval( vertex['nix'], 2, 1 ) == 1"                 ),
      ASSERT(         "lastval( vertex['nix'], vertex['nope'], 1 ) == 1"    ),
      ASSERT(         "lastval( vertex['nix'], 2, vertex['nope'] ) == 2"    ),
      ASSERT(         "lastval( 3, vertex['nix'], 2, vertex['nope'] ) == 2" ),
      ASSERT(         "lastval( 4, vertex['nix'], vertex['nope'], 1 ) == 1" ),
      ASSERT(         "isnan( lastval( vertex['nix'], vertex['nope'] ) )"   ),

      // sum
      INTEGER( 0,            "sum()"                              ),
      INTEGER( 0,            "sum( 0 )"                           ),
      INTEGER( 1,            "sum( 1 )"                           ),
      INTEGER( 3,            "sum( 1, 2 )"                        ),
      INTEGER( 6,            "sum( 1, 2, 3 )"                     ),
      REAL( 6.0,             "sum( 1, 2.0, 3 )"                   ),
      REAL( 0.0,             "sum( -1.0, 2, -1 )"                 ),
      REAL( 1.1,             "sum( 1, vertex, 0.1 )"              ),
      REAL( 2.1,             "sum( 1, vertex, 0.1, '\x01' )"      ),
      REAL( 4.1,             "sum( 1, vertex, 0.1, '\x01\x02' )"  ),
      REAL( 533.1,           "sum( 1, vertex, 0.1, 'hello' )"     ),
      INTEGER( 0,            "sum( vertex, vertex )"              ),


      // sumsqr
      INTEGER( 0,            "sumsqr()"                             ),
      INTEGER( 0,            "sumsqr( 0 )"                          ),
      INTEGER( 1,            "sumsqr( 1 )"                          ),
      INTEGER( 5,            "sumsqr( 1, 2 )"                       ),
      INTEGER( 14,           "sumsqr( 1, 2, 3 )"                    ),
      REAL( 14.0,            "sumsqr( 1, 2.0, 3 )"                  ),
      REAL( 6.0,             "sumsqr( -1.0, 2, -1 )"                ),
      REAL( 65.01,           "sumsqr( 1, vertex, 0.1, '\x08' )"     ),
      INTEGER( 0,            "sumsqr( vertex, vertex )"             ),


      // invsum
      REAL( 0.0,             "invsum()"                             ),
      REAL( 1.0/FLT_MIN,     "invsum( 0 )"                          ),
      REAL( 1.0,             "invsum( 1 )"                          ),
      REAL( 1.5,             "invsum( 1, 2 )"                       ),
      REAL( 1.833333333333,  "invsum( 1, 2, 3 )"                    ),
      REAL( 1.833333333333,  "invsum( 1, 2.0, 3 )"                  ),
      REAL( -1.5,            "invsum( -1.0, 2, -1 )"                ),
      REAL( 11.5,            "invsum( 1, vertex, 0.1, '\x02' )"     ),
      REAL( 0.0,             "invsum( vertex, vertex )"             ),


      // prod
      INTEGER( 0,            "prod()"                              ),
      INTEGER( 0,            "prod( 0 )"                           ),
      INTEGER( 1,            "prod( 1 )"                           ),
      INTEGER( 2,            "prod( 1, 2 )"                        ),
      INTEGER( 6,            "prod( 1, 2, 3 )"                     ),
      REAL( 6.0,             "prod( 1, 2.0, 3 )"                   ),
      REAL( 2.0,             "prod( -1.0, 2, -1 )"                 ),
      REAL( 1.0,             "prod( 1, vertex, 0.1, '\x02\x05' )"  ),
      INTEGER( 1,            "prod( vertex, vertex )"              ),
      ASSERT(         "prod( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 ) == fac( 20 )" ),
      ASSERT(         "prod( 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 9, 8, 7, 6, 5, 4, 3, 2, 1 ) == fac( 20 )" ),


      // mean
      REAL( 0.0,             "mean()"                              ),
      REAL( 0.0,             "mean( 0 )"                           ),
      REAL( 1.0,             "mean( 1 )"                           ),
      REAL( 1.5,             "mean( 1, 2 )"                        ),
      REAL( 2.0,             "mean( 1, 2, 3 )"                     ),
      REAL( 2.0,             "mean( 1, 2.0, 3 )"                   ),
      REAL( 0.0,             "mean( -1.0, 2, -1 )"                 ),
      REAL( 4.1/4,           "mean( 1, vertex, 0.1, '\x03' )"      ),
      REAL( 0.0,             "mean( vertex, vertex )"              ),


      // harmmean
      REAL( 0.0,             "harmmean()"                             ),
      REAL( FLT_MIN,         "harmmean( 0 )"                          ),
      REAL( 1.0,             "harmmean( 1 )"                          ),
      REAL( 2/1.5,           "harmmean( 1, 2 )"                       ),
      REAL( 1.636363636363,  "harmmean( 1, 2, 3 )"                    ),
      REAL( 1.636363636363,  "harmmean( 1, 2.0, 3 )"                  ),
      REAL( -2.0,            "harmmean( -1.0, 2, -1 )"                ),
      REAL( 4/(11.0+1.0/9),  "harmmean( 1, vertex, 0.1, '\x09' )"     ),
      REAL( 2/FLT_MIN,       "harmmean( vertex, vertex )"             ),


      // geomean
      REAL( 0.0,             "geomean()"                              ),
      REAL( 0.0,             "geomean( 0 )"                           ),
      REAL( 1.0,             "geomean( 1 )"                           ),
      REAL( sqrt(2),         "geomean( 1, 2 )"                        ),
      REAL( pow(6,1.0/3),    "geomean( 1, 2, 3 )"                     ),
      REAL( pow(6,1.0/3),    "geomean( 1, 2.0, 3 )"                   ),
      REAL( pow(2,1.0/3),    "geomean( -1.0, 2, -1 )"                 ),
      REAL( pow(0.5,1.0/4),  "geomean( 1, vertex, 0.1 ,  '\x05' )"    ),
      REAL( 1.0,             "geomean( vertex, vertex )"              ),
      REAL( -NAN,            "geomean( 2, -1 )"                       ),

      // do
      INTEGER( 1,            "do()"                                 ),
      INTEGER( 1,            "do( 0 )"                              ),
      INTEGER( 1,            "do( 1 )"                              ),
      INTEGER( 1,            "do( 0, 1 )"                           ),
      INTEGER( 1,            "do( 1, 0 )"                           ),
      INTEGER( 1,            "do( mean() )"                         ),
      INTEGER( 1,            "do( mean(), mean() )"                 ),
      INTEGER( 1,            "do( mean(0), mean() )"                ),
      INTEGER( 1,            "do( mean(), mean(0) )"                ),
      INTEGER( 1,            "do( mean(-1,0,1), mean(-2, 2) )"      ),
      INTEGER( 1,            "do( mean(1,2,3), mean(4, 5) )"        ),

      // void
      ASSERT(         "isnan( void() )"                          ),
      ASSERT(         "isnan( void( 0 ) )"                       ),
      ASSERT(         "isnan( void( 1 ) )"                       ),
      ASSERT(         "isnan( void( void() ) )"                  ),
      ASSERT(         "isnan( void( void(), void() ) )"          ),
      ASSERT(         "isnan( void( void(), void( void() ) ) )"  ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, math, 0.0, evalmem, NULL ) >= 0,   "Math" );
    iString.Discard( (CString_t**)&evalmem->data[0].CSTR__str );
    iString.Discard( (CString_t**)&evalmem->data[1].CSTR__str );
    iString.Discard( (CString_t**)&evalmem->data[2].CSTR__str );
    iString.Discard( (CString_t**)&evalmem->data[3].CSTR__str );
    iString.Discard( (CString_t**)&evalmem->data[4].CSTR__str );
    iEvaluator.DiscardMemory( &evalmem );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Object
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Object" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    __test object[] = {
      INTEGER( 0,     "len('')"       ),
      INTEGER( 1,     "len('x')"      ),
      INTEGER( 2,     "len('xy')"     ),
      INTEGER( 3,     "len('xyz')"    ),

      INTEGER( 0,     "len( b'' )"    ),
      INTEGER( 1,     "len( b'x' )"   ),
      INTEGER( 2,     "len( b'xy' )"  ),
      INTEGER( 3,     "len( b'xyz' )" ),

      INTEGER( 1,     "len( '\\x00' )" ),
      INTEGER( 0,     "len( '\\x80' )" ), // invalid utf-8
      INTEGER( 0,     "len( '\\x80\\x00' )" ), // invalid utf-8
      INTEGER( 2,     "len( '\\x00\\x00' )" ),
      INTEGER( 2,     "len( '\\x00\\x00' )" ),
      INTEGER( 1,     "len( '\\xce\\xb2' )" ), // valid utf-8 for greek beta
      INTEGER( 2,     "strlen( '\\xce\\xb2' )" ), // valid utf-8 for greek beta
      INTEGER( 0,     "len( '\\xce\\xff' )" ), // invalid utf-8

      INTEGER( 70,    "len('0123456789012345678901234567890123456789012345678901234567890123456789')"  ),
      INTEGER( 70,    "strlen('0123456789012345678901234567890123456789012345678901234567890123456789')"  ),
      INTEGER( 32,    "len( next.vector )"     ),
      INTEGER( 32,    "len( vertex.vector )"   ),
      INTEGER( 4,     "len( vertex.id )" ),
      INTEGER( 4,     "strlen( vertex.id )" ),
      INTEGER( 1,     "len( next.id )"   ),
      INTEGER( 1,     "strlen( next.id )"   ),


      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, object, 0.0, NULL, NULL ) >= 0,   "Object" );

    SELECTED_TEST_ARC = &ARC_ROOT_to_C;
    __test null_vector[] = {
      INTEGER( 0,     "len( next.vector )"     ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, null_vector, 0.0, NULL, NULL ) >= 0,   "Object" );


  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Memory
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Memory" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    vgx_ExpressEvalMemory_t *evalmem = iEvaluator.NewMemory( -1 );
    __test memory[] = {
      INTEGER( 0,       "load( R1 )"          ),
      
      INTEGER( 101,     "store( R1, 101 )"    ),
      INTEGER( 102,     "store( R2, 102 )"    ),
      REAL( 103.5,      "store( R3, 103.5 )"  ),
      INTEGER( 104,     "store( R4, 104 )"    ),
      
      REAL( 103.5,      "load( R3 )"          ),
      INTEGER( 102,     "load( R2 )"          ),
      INTEGER( 101,     "load( R1 )"          ),
      INTEGER( 104,     "load( R4 )"          ),

      REAL( 103.5,      "r3"                  ),
      INTEGER( 102,     "r2"                  ),
      INTEGER( 101,     "r1"                  ),
      INTEGER( 104,     "r4"                  ),

      INTEGER( 102,     "inc( R1 )"           ),
      REAL( 104.5,      "inc( R3 )"           ),

      INTEGER( 101,     "dec( R2 )"           ),
      INTEGER( 100,     "dec( R2 )"           ),
      INTEGER( 99,      "dec( R2 )"           ),
      REAL( 103.5,      "dec( R3 )"           ),

      INTEGER( 150,     "add( R1, 48 )"       ),
      REAL( 105.0,      "add( R3, 1.5 )"      ),
      
      INTEGER( 100,     "sub( R1, 50 )"       ),
      REAL( 5.0,        "sub( R3, 100 )"      ),
      REAL( -5.0,       "sub( R3, 10 )"       ),

      INTEGER( 200,     "mul( R1, 2 )"        ),
      REAL( 198.0,      "mul( R2, 2.0 )"      ),
      REAL( -10.0,      "mul( R3, 2 )"        ),
      REAL( 55.0,       "mul( R3, -5.5 )"     ),

      INTEGER( 100,     "div( R1, 2 )"        ),
      INTEGER( 33,      "div( R1, 3 )"        ),
      REAL( 5.5,        "div( R3, 10 )"       ),

      INTEGER( 3,       "mod( R1, 10 )"       ),
      INTEGER( 0,       "mod( R1, 3 )"        ),
      REAL( 0.5,        "mod( R3, 2.5 )"      ),

      INTEGER( 0,       "indexed( vertex )"   ),
      INTEGER( 1,       "index( vertex )"     ),
      INTEGER( 1,       "indexed( vertex )"   ),
      INTEGER( 0,       "index( vertex )"     ),
      INTEGER( 1,       "indexed( vertex )"   ),
      INTEGER( 0,       "unindex( vertex )"   ),
      INTEGER( 0,       "indexed( vertex )"   ),

      INTEGER( 1,           "store( R1, 1 )"      ),
      INTEGER( 2,           "shl( R1, 1 )"        ),
      INTEGER( 4,           "shl( R1, 1 )"        ),
      INTEGER( 16,          "shl( R1, 2 )"        ),
      INTEGER( 128,         "shl( R1, 3 )"        ),
      INTEGER( (128LL<<40), "shl( R1, 40 )"       ),
      INTEGER( 128,         "shr( R1, 40 )"       ),
      INTEGER( 64,          "shr( R1, 1 )"        ),
      INTEGER( 65,          "or( R1, 1 )"         ),
      INTEGER( 65,          "or( R1, 1 )"         ),
      INTEGER( 67,          "or( R1, 2 )"         ),
      INTEGER( 3,           "and( R1, 7 )"        ),
      INTEGER( 4,           "xor( R1, 7 )"        ),

      INTEGER( 1,       "store( R1, 1 )"          ),  // R1 = 1
      REAL( 2.2,        "store( R2, 2.2 )"        ),  // R2 = 2.2
      INTEGER( 3,       "store( R3, 3 )"          ),  // R3 = 3
      INTEGER( 0,       "store( R4, 0 )"          ),  // R4 = 0
      REAL( 2.2,        "mov( R4, R2 )"           ),  // R2 -> R4 = 2.2
      REAL( 2.2,        "load( R2 )"              ),  //
      REAL( 2.2,        "load( R4 )"              ),  //
      INTEGER( 1,       "mov( R4, R1 )"           ),  // R1 -> R4 = 1
      INTEGER( 1,       "load( R1 )"              ),  //
      REAL( 2.2,        "load( R2 )"              ),  //
      INTEGER( 1,       "load( R4 )"              ),  //
      INTEGER( 3,       "xchg( R2, R3 )"          ),  // R2 <-> R3, R2=3, R3=2.2
      REAL( 2.2,        "load( R3 )"              ),  //
      INTEGER( 3,       "load( R2 )"              ),  //
      REAL( 2.2,        "xchg( R3, R3 )"          ),  // noop
      REAL( 2.2,        "load( R3 )"              ),  //

      // return
      INTEGER( 1,            "return(1); 2"                       ),
      INTEGER( 2,            "5; return(1+1)"                     ),
      INTEGER( 10,           "returnif( 2>1, 10); 20"             ),
      INTEGER( 20,           "returnif( 2<1, 10); 20"             ),
      INTEGER( 100,          "store( R1, 100 )"                   ),
      INTEGER( 10,           "returnif( 2>1, 10); inc( R1 ); 20"  ),
      INTEGER( 100,          "load( R1 )"                         ),
      INTEGER( 20,           "returnif( 2<1, 10); inc( R1 ); 20"  ),
      INTEGER( 101,          "load( R1 )"                         ),
      INTEGER( 25,           "require( true ); 25"                ),
      INTEGER( 0,            "require( false ); 25"               ),


      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, memory, 0.0, evalmem, NULL ) >= 0,   "Memory Default" );
    iEvaluator.DiscardMemory( &evalmem );


    vgx_ExpressEvalMemory_t *evalmem_1k_basic = iEvaluator.NewMemory( 10 );
    __test memory_1k_basic[] = {
      // Test memory stack
      INTEGER( 17,      "push( 17 )" ),
      INTEGER( 17,      "load( -5 )" ),
      INTEGER( 42,      "push( 42 )" ),
      INTEGER( 17,      "load( -5 )" ),
      INTEGER( 42,      "load( -6 )" ),
      INTEGER( 313,     "push( 313 )" ),
      INTEGER( 17,      "load( -5 )" ),
      INTEGER( 42,      "load( -6 )" ),
      INTEGER( 313,     "load( -7 )" ),
      INTEGER( 313,     "get()" ),
      INTEGER( 313,     "get()" ),
      INTEGER( 313,     "pop()" ),
      INTEGER( 42,      "get()" ),
      INTEGER( 42,      "pop()" ),
      INTEGER( 17,      "pop()" ),
      INTEGER( 1004,    "store( R4, 1004 )"),
      INTEGER( 0,       "store( R3, 0 )"),
      INTEGER( 0,       "store( R2, 0 )"),
      INTEGER( 1,       "store( R1, 1 )"),
      INTEGER( 777,     "store( 0, 777 )"),
      INTEGER( 1004,    "pop()" ),
      INTEGER( 0,       "pop()" ),
      INTEGER( 0,       "pop()" ),
      INTEGER( 1,       "pop()" ),
      INTEGER( 777,     "pop()" ),
      INTEGER( 888,     "push( 888 )" ),
      INTEGER( 888,     "load( 0 )" ),
      INTEGER( 1,       "push( 1 )" ),
      INTEGER( 2,       "push( 2 )" ),
      INTEGER( 3,       "push( 3 )" ),
      INTEGER( 4,       "push( 4 )" ),
      INTEGER( 2,       "load( R2 )" ),
      INTEGER( 4,       "load( R4 )" ),
      INTEGER( 3,       "load( R3 )" ),
      INTEGER( 1,       "load( R1 )" ),

      INTEGER( 0,       "store( 0, 0 )"           ),  // m[0] = 0
      INTEGER( 100,     "store( 1, 100 )"         ),  // m[1] = 100
      INTEGER( 50,      "store( 2, 50 )"          ),  // m[2] = 50
      INTEGER( 33,      "store( 3, 33 )"          ),  // m[3] = 33
      INTEGER( 100,     "store( 4, 100 )"         ),  // m[1] = 100

      ASSERT(   "equ( 1, 1 )"             ),  // 100 == 100 ?
      UNTRUE(   "equ( 1, 2 )"             ),  // 100 == 50  ?
      UNTRUE(   "equ( 2, 1 )"             ),  // 50  == 100 ?
      ASSERT(   "equ( 1, 4 )"             ),  // 100 == 100 ?

      UNTRUE(   "neq( 1, 1 )"             ),  // 100 != 100 ?
      ASSERT(   "neq( 1, 2 )"             ),  // 100 != 50  ?
      ASSERT(   "neq( 2, 1 )"             ),  // 50  != 100 ?
      UNTRUE(   "neq( 1, 4 )"             ),  // 100 != 100 ?

      UNTRUE(   "gt( 1, 1 )"              ),  // 100 > 100 ?
      ASSERT(   "gt( 1, 2 )"              ),  // 100 > 50  ?
      UNTRUE(   "gt( 2, 1 )"              ),  // 50  > 100 ?
      UNTRUE(   "gt( 1, 4 )"              ),  // 100 > 100 ?

      ASSERT(   "gte( 1, 1 )"             ),  // 100 >= 100 ?
      ASSERT(   "gte( 1, 2 )"             ),  // 100 >= 50  ?
      UNTRUE(   "gte( 2, 1 )"             ),  // 50  >= 100 ?
      ASSERT(   "gte( 1, 4 )"             ),  // 100 >= 100 ?

      UNTRUE(   "lt( 1, 1 )"              ),  // 100 < 100 ?
      UNTRUE(   "lt( 1, 2 )"              ),  // 100 < 50  ?
      ASSERT(   "lt( 2, 1 )"              ),  // 50  < 100 ?
      UNTRUE(   "lt( 1, 4 )"              ),  // 100 < 100 ?

      ASSERT(   "lte( 1, 1 )"             ),  // 100 <= 100 ?
      UNTRUE(   "lte( 1, 2 )"             ),  // 100 <= 50  ?
      ASSERT(   "lte( 2, 1 )"             ),  // 50  <= 100 ?
      ASSERT(   "lte( 1, 4 )"             ),  // 100 <= 100 ?

      INTEGER( 1,       "store( R1, 1 )"          ),  // R1 = 1
      INTEGER( 2,       "store( R2, 2 )"          ),  // R2 = 2
      INTEGER( 3,       "store( R3, 3 )"          ),  // R3 = 3
      INTEGER( 4,       "store( R4, 4 )"          ),  // R4 = 4

      ASSERT(   "requ( R1, R1 )"             ),  // 100 == 100 ?
      UNTRUE(   "requ( R1, R2 )"             ),  // 100 == 50  ?
      UNTRUE(   "requ( R2, R1 )"             ),  // 50  == 100 ?
      ASSERT(   "requ( R1, R4 )"             ),  // 100 == 100 ?

      UNTRUE(   "rneq( R1, R1 )"             ),  // 100 != 100 ?
      ASSERT(   "rneq( R1, R2 )"             ),  // 100 != 50  ?
      ASSERT(   "rneq( R2, R1 )"             ),  // 50  != 100 ?
      UNTRUE(   "rneq( R1, R4 )"             ),  // 100 != 100 ?

      UNTRUE(   "rgt( R1, R1 )"              ),  // 100 > 100 ?
      ASSERT(   "rgt( R1, R2 )"              ),  // 100 > 50  ?
      UNTRUE(   "rgt( R2, R1 )"              ),  // 50  > 100 ?
      UNTRUE(   "rgt( R1, R4 )"              ),  // 100 > 100 ?

      ASSERT(   "rgte( R1, R1 )"             ),  // 100 >= 100 ?
      ASSERT(   "rgte( R1, R2 )"             ),  // 100 >= 50  ?
      UNTRUE(   "rgte( R2, R1 )"             ),  // 50  >= 100 ?
      ASSERT(   "rgte( R1, R4 )"             ),  // 100 >= 100 ?

      UNTRUE(   "rlt( R1, R1 )"              ),  // 100 < 100 ?
      UNTRUE(   "rlt( R1, R2 )"              ),  // 100 < 50  ?
      ASSERT(   "rlt( R2, R1 )"              ),  // 50  < 100 ?
      UNTRUE(   "rlt( R1, R4 )"              ),  // 100 < 100 ?

      ASSERT(   "rlte( R1, R1 )"             ),  // 100 <= 100 ?
      UNTRUE(   "rlte( R1, R2 )"             ),  // 100 <= 50  ?
      ASSERT(   "rlte( R2, R1 )"             ),  // 50  <= 100 ?
      ASSERT(   "rlte( R1, R4 )"             ),  // 100 <= 100 ?

      // Test write
      INTEGER( 4,       "mset( 200, 203, 0 )" ),
      INTEGER( 1,       "write( 200, 10 )" ),
      ASSERT(           "load( 200 ) == 10 && load( 201 ) == 0" ),
      INTEGER( 2,       "write( 200, 10, 11 )" ),
      ASSERT(           "load( 200 ) == 10 && load( 201 ) == 11 && load( 202 ) == 0" ),
      INTEGER( 3,       "write( 200, 10, 11, 12 )" ),
      ASSERT(           "load( 200 ) == 10 && load( 201 ) == 11 && load( 202 ) == 12 && load( 203 ) == 0" ),
      INTEGER( 4,       "write( 200, 10, 11, 12, 13 )" ),
      ASSERT(           "load( 200 ) == 10 && load( 201 ) == 11 && load( 202 ) == 12 && load( 203 ) == 13" ),
      INTEGER( 4,       "write( 200, 10, 11.1, str('hello'), next )" ),
      ASSERT(           "load( 200 ) == 10 && load( 201 ) == 11.1 && load( 202 ) == 'hello' && load( 203 ) == next" ),

      // Test writeif
      INTEGER( 3,       "mset( 300, 302, 0 )" ),
      UNTRUE(           "writeif( false, 300, 777, 888 )" ),
      ASSERT(           "load( 300 ) == 0 && load( 301 ) == 0 && load( 302 ) == 0" ),
      ASSERT(           "writeif( true, 300, 777, 888 )" ),
      ASSERT(           "load( 300 ) == 777 && load( 301 ) == 888 && load( 302 ) == 0" ),
        
      // Test rwrite
      INTEGER( 10,      "mset( 400, 409, 0 )" ),
      INTEGER( 400,     "store( R1, 400 )" ),
      INTEGER( 1,       "rwrite( R1, 10 )" ),
      INTEGER( 2,       "rwrite( R1, 10, 11 )" ),
      INTEGER( 3,       "rwrite( R1, 10, 11, 12 )" ),
      INTEGER( 4,       "rwrite( R1, 10, 11, 12, 13 )" ),
      INTEGER( 410,     "load( R1 )" ),
      ASSERT(           "load( 400 ) == 10 && load( 401 ) == 10 && load( 402 ) == 11 && load( 403 ) == 10 && load( 404 ) == 11 && load( 405 ) == 12 && load( 406 ) == 10 && load( 407 ) == 11 && load( 408 ) == 12 && load( 409 ) == 13" ),
      // Test rwriteif
      INTEGER( 3,       "mset( 500, 502, 0 )" ),
      INTEGER( 500,     "store( R1, 500 )" ),
      UNTRUE(           "rwriteif( false, R1, 777, 888 )" ),
      ASSERT(           "load( 500 ) == 0 && load( 501 ) == 0 && load( 502 ) == 0" ),
      ASSERT(           "rwriteif( true, R1, 777, 888 )" ),
      ASSERT(           "load( 500 ) == 777 && load( 501 ) == 888 && load( 502 ) == 0" ),
      INTEGER( 502,     "load( R1 )" ),
      

      // Test conditional push
      INTEGER( 1000,    "push( 1000 )" ),
      INTEGER( 1000,    "get()" ),
      ASSERT(           "pushif( 2 > 1, 2000 ) " ),
      INTEGER( 2000,    "get()" ),
      UNTRUE(           "pushif( 1 > 2, 3000 ) " ),
      INTEGER( 2000,    "get()" ),
      INTEGER( 2000,    "pop()" ),
      INTEGER( 1000,    "pop()" ),
      ASSERT(           "pushif( 2 > 1, 1.5 ) " ),
      REAL( 1.5,        "get()" ),
      UNTRUE(           "pushif( 1 > 2, 111.555 ) " ),
      REAL( 1.5,        "get()" ),
      REAL( 1.5,        "pop()" ),

      // Test conditional store
      INTEGER( 5000,    "store( 50, 5000 )" ),
      INTEGER( 5000,    "load( 50 )" ),
      INTEGER( 5001,    "store( 50, 5001 )" ),
      INTEGER( 5001,    "load( 50 )" ),
      ASSERT(           "storeif( 2 > 1, 50, 5002 )" ),
      INTEGER( 5002,    "load( 50 )" ),
      UNTRUE(           "storeif( 1 > 2, 50, 5555 )" ),
      INTEGER( 5002,    "load( 50 )" ),
      ASSERT(           "storeif( 2 > 1, 70, 5.002 )" ),
      REAL( 5.002,      "load( 70 )" ),
      UNTRUE(           "storeif( 1 > 2, 70, 5.555 )" ),
      REAL( 5.002,      "load( 70 )" ),

      // Test conditional inc/dec
      ASSERT(           "incif( 2 > 1, 50 )" ),
      INTEGER( 5003,    "load( 50 )" ),
      UNTRUE(           "incif( 1 > 2, 50 )" ),
      INTEGER( 5003,    "load( 50 )" ),
      ASSERT(           "decif( 2 > 1, 50 )" ),
      INTEGER( 5002,    "load( 50 )" ),
      UNTRUE(           "decif( 1 > 2, 50 )" ),
      INTEGER( 5002,    "load( 50 )" ),
      ASSERT(           "incif( 2 > 1, 70 )" ),
      REAL( 6.002,      "load( 70 )" ),
      UNTRUE(           "incif( 1 > 2, 70 )" ),
      REAL( 6.002,      "load( 70 )" ),
      ASSERT(           "decif( 2 > 1, 70 )" ),
      REAL( 5.002,      "load( 70 )" ),
      UNTRUE(           "decif( 1 > 2, 70 )" ),
      REAL( 5.002,      "load( 70 )" ),

      // Test conditional add/sub/mul/div/mod
      ASSERT(           "addif( 2 > 1, 50, 100 )" ),
      INTEGER( 5102,    "load( 50 )" ),
      UNTRUE(           "addif( 1 > 2, 50, 100 )" ),
      INTEGER( 5102,    "load( 50 )" ),
      ASSERT(           "subif( 2 > 1, 50, 102 )" ),
      INTEGER( 5000,    "load( 50 )" ),
      UNTRUE(           "subif( 1 > 2, 50, 7777 )" ),
      INTEGER( 5000,    "load( 50 )" ),
      ASSERT(           "mulif( 2 > 1, 50, 2.5 )" ),
      REAL( 12500.0,    "load( 50 )" ),
      UNTRUE(           "mulif( 1 > 2, 50, 1.33 )" ),
      REAL( 12500.0,    "load( 50 )" ),
      ASSERT(           "divif( 2 > 1, 50, 1250.0 )" ),
      REAL( 10.0,       "load( 50 )" ),
      UNTRUE(           "divif( 1 > 2, 50, 1.33 )" ),
      REAL( 10.0,       "load( 50 )" ),
      ASSERT(           "modif( 2 > 1, 50, 4.5 )" ),
      REAL( 1.0,        "load( 50 )" ),
      UNTRUE(           "modif( 1 > 2, 50, 0.4 )" ),
      REAL( 1.0,        "load( 50 )" ),
      ASSERT(           "addif( 2 > 1, 50, 1 )" ),
      REAL( 2.0,        "load( 50 )" ),
      ASSERT(           "subif( 2 > 1, 50, 1 )" ),
      REAL( 1.0,        "load( 50 )" ),


      // Test conditional bitwise
      INTEGER( 3,       "store( 50, 3 )" ),
      ASSERT(           "shlif( 2 > 1, 50, 1)" ), // 3 << 1 = 6
      ASSERT(           "shlif( 2 > 1, 50, 3)" ), // 6 << 3 = 48
      UNTRUE(           "shlif( 1 > 2, 50, 3)" ), // noop
      INTEGER( 48,      "load( 50 )" ),
      ASSERT(           "shrif( 2 > 1, 50, 1)" ), // 48 >> 1 = 24
      ASSERT(           "shrif( 2 > 1, 50, 2)" ), // 24 >> 2 = 6
      UNTRUE(           "shrif( 1 > 2, 50, 1)" ), // noop
      INTEGER( 6,       "load( 50 )" ),
      ASSERT(           "shlif( 2 > 1, 50, 60)" ), // 6 << 60
      ASSERT(           "shrif( 2 > 1, 50, 61)" ), // 6 >> 61
      INTEGER( 3,       "load( 50 )" ),            // 6 >> 1 = 3
      INTEGER( 6,       "store( 50, 6 )" ),
      ASSERT(           "andif( 2 > 1, 50, 0xf)" ), // 6 & 0xf = 6
      ASSERT(           "andif( 2 > 1, 50, 0xc)" ), // 6 & 0xc = 4
      INTEGER( 4,       "load( 50 )" ),
      UNTRUE(           "andif( 1 > 2, 50, 1)" ), // noop
      INTEGER( 4,       "load( 50 )" ),
      ASSERT(           "orif( 2 > 1, 50, 3)" ), // 4 | 3 = 7
      INTEGER( 7,       "load( 50 )" ),
      ASSERT(           "orif( 2 > 1, 50, 3)" ), // 7 | 3 = 7
      INTEGER( 7,       "load( 50 )" ),
      UNTRUE(           "orif( 1 > 2, 50, 0xff)" ), // noop
      INTEGER( 7,       "load( 50 )" ),
      ASSERT(           "orif( 2 > 1, 50, 0xff)" ), // 7 & 0xff = 0xff
      INTEGER( 255,     "load( 50 )" ),
      ASSERT(           "xorif( 2 > 1, 50, 0x0f)" ), // 0xff ^ 0x0f = 0xf0
      INTEGER( 0xf0,    "load( 50 )" ),
      ASSERT(           "xorif( 2 > 1, 50, 0x0f)" ), // 0xf0 ^ 0x0f = 0xff
      INTEGER( 0xff,    "load( 50 )" ),
      UNTRUE(           "xorif( 1 > 2, 50, 0x17)" ), // 0xff ^ 0x17 = 0xff
      INTEGER( 0xff,    "load( 50 )" ),


      // Test conditional mov
      INTEGER( 5002,    "store( 50, 5002 )"   ),
      REAL( 5.002,      "store( 70, 5.002 )"  ),
      INTEGER( 0,       "store( 80, 0 )"      ),
      INTEGER( 0,       "store( 81, 0 )"      ),
      INTEGER( 0,       "store( 90, 0 )"      ),
      INTEGER( 0,       "store( 91, 0 )"      ),
      INTEGER( 0,       "load( 80 )"          ),
      INTEGER( 0,       "load( 81 )"          ),
      INTEGER( 0,       "load( 90 )"          ),
      INTEGER( 0,       "load( 91 )"          ),
      INTEGER( 5002,    "mov( 80, 50 )"       ),
      INTEGER( 5002,    "load( 80 )"          ),
      INTEGER( 0,       "load( 81 )"          ),
      INTEGER( 5002,    "load( 50 )"          ),
      REAL( 5.002,      "mov( 90, 70 )"       ),
      REAL( 5.002,      "load( 90 )"          ),
      INTEGER( 0,       "load( 91 )"          ),
      REAL( 5.002,      "load( 70 )"          ),
      UNTRUE(           "movif( 1 > 2, 81, 80 )" ),
      INTEGER( 5002,    "load( 80 )"          ),
      INTEGER( 0,       "load( 81 )"          ),
      ASSERT(           "movif( 2 > 1, 81, 80 )" ),
      INTEGER( 5002,    "load( 80 )"          ),
      INTEGER( 5002,    "load( 81 )"          ),
      UNTRUE(           "movif( 1 > 2, 91, 90 )" ),
      REAL( 5.002,      "load( 90 )"          ),
      INTEGER( 0,       "load( 91 )"          ),
      ASSERT(           "movif( 2 > 1, 91, 90 )" ),
      REAL( 5.002,      "load( 90 )"          ),
      REAL( 5.002,      "load( 91 )"          ),

      // Test conditional xchg
      UNTRUE(           "xchgif( 1 > 2, 91, 81 )" ),
      REAL( 5.002,      "load( 91 )"              ),
      INTEGER( 5002,    "load( 81 )"              ),
      ASSERT(           "xchgif( 2 > 1, 91, 81 )" ),
      INTEGER( 5002,    "load( 91 )"              ),
      REAL( 5.002,      "load( 81 )"              ),

      // Test smooth
      REAL( 100.0,      "store( 50, 100.0 )" ),
      ASSERT(           "do( store( R1, load(50) ), smooth( 50, 200.0, 0.5 ) ) && load(50) > load(R1) && load(50) > 100.0 && load(50) < 190.0" ),
      ASSERT(           "do( store( R1, load(50) ), smooth( 50, 200.0, 0.5 ) ) && load(50) > load(R1) && load(50) > 100.0 && load(50) < 190.0" ),
      ASSERT(           "do( store( R1, load(50) ), smooth( 50, 200.0, 0.5 ) ) && load(50) > load(R1) && load(50) > 100.0 && load(50) < 190.0" ),
      UNTRUE(           "do( store( R1, load(50) ), smooth( 50, 200.0, 0.5 ) ) && load(50) > load(R1) && load(50) > 100.0 && load(50) < 190.0" ),

      // Test count
      INTEGER( 0,       "store( R1, 0 )" ),
      INTEGER( 1,       "count( R1 )" ),
      INTEGER( 2,       "count( R1, 'hello' )" ),
      INTEGER( 3,       "count( R1, 'hello again' )" ),
      INTEGER( 4,       "count( R1, 1+2, load( R1 ), 'hello', 1, 1, 2 )" ),
      INTEGER( 101,     "count( R1, store( R1, 100 ) )" ),

      // Test countif
      INTEGER( 0,       "store( R1, 0 )" ),
      ASSERT(           "countif( false, R1 ) == false && load( R1 ) == 0" ),
      ASSERT(           "countif( false, R1, 'hello' ) == false && load( R1 ) == 0" ),
      ASSERT(           "countif( true, R1, 'hello' ) == true && load( R1 ) == 1" ),
      ASSERT(           "countif( true, R1 ) == true && load( R1 ) == 2" ),

      // Test modindex
      INTEGER( 0,       "modindex( 0, 0, 0 )"  ),
      INTEGER( 1,       "modindex( 1, 0, 0 )"  ),
      INTEGER( 0,       "modindex( 0, 1, 0 )"  ),
      INTEGER( 0,       "modindex( 1, 1, 0 )"  ),
      INTEGER( 15,      "modindex( 15, 17, 0 )"  ),
      INTEGER( 16,      "modindex( 16, 17, 0 )"  ),
      INTEGER( 0,       "modindex( 17, 17, 0 )"  ),
      INTEGER( 1,       "modindex( 18, 17, 0 )"  ),
      INTEGER( 16,      "modindex( 33, 17, 0 )"  ),
      INTEGER( 0,       "modindex( 34, 17, 0 )"  ),
      INTEGER( 1015,    "modindex( 15, 17, 1000 )"  ),
      INTEGER( 1016,    "modindex( 16, 17, 1000 )"  ),
      INTEGER( 1000,    "modindex( 17, 17, 1000 )"  ),
      INTEGER( 1001,    "modindex( 18, 17, 1000 )"  ),
      INTEGER( 1016,    "modindex( 33, 17, 1000 )"  ),
      INTEGER( 1000,    "modindex( 34, 17, 1000 )"  ),

      // Test double indirection
      INTEGER( 1024,    "mset( 0, -1, 0 )" ),
      INTEGER( 10,      "store( R1, 10 )" ),
      INTEGER( 20,      "store( R2, 20 )" ),
      INTEGER( 30,      "store( R3, 30 )" ),
      INTEGER( 8888,    "rstore( R1, 8888 )" ),
      INTEGER( 8888,    "load( 10 )" ),
      INTEGER( 8888,    "rload( R1 )" ),
      INTEGER( 8888,    "rmov( R2, R1 )" ),
      INTEGER( 8888,    "load( 20 )" ),
      INTEGER( 0,       "rxchg( R2, R3 )" ),
      INTEGER( 8888,    "load( 30 )" ),
      INTEGER( 1,       "rinc( R2 )" ),
      INTEGER( 2,       "rinc( R2 )" ),
      INTEGER( 3,       "rinc( R2 )" ),
      INTEGER( 2,       "rdec( R2 )" ),
      INTEGER( 2,       "rload( R2 )" ),
      INTEGER( 2,       "load( 20 )" ),
      UNTRUE(           "rstoreif( 0, R1, 9999 )" ),
      INTEGER( 8888,    "rload( R1 )" ),
      ASSERT(           "rstoreif( 1, R1, 9999 )" ),
      INTEGER( 9999,    "rload( R1 )" ),
      UNTRUE(           "rmovif( 0, R1, R3 )" ),
      INTEGER( 9999,    "rload( R1 )" ),
      ASSERT(           "rmovif( 1, R1, R3 )" ),
      INTEGER( 8888,    "rload( R1 )" ),
      UNTRUE(           "rxchgif( 0, R1, R2 )" ),
      INTEGER( 8888,    "rload( R1 )" ),
      INTEGER( 2,       "rload( R2 )" ),
      ASSERT(           "rxchgif( 1, R1, R2 )" ),
      INTEGER( 2,       "rload( R1 )" ),
      INTEGER( 8888,    "rload( R2 )" ),
      UNTRUE(           "rincif( 0, R2 )" ),
      INTEGER( 8888,    "rload( R2 )" ),
      ASSERT(           "rincif( 1, R2 )" ),
      INTEGER( 8889,    "rload( R2 )" ),
      UNTRUE(           "rdecif( 0, R2 )" ),
      INTEGER( 8889,    "rload( R2 )" ),
      ASSERT(           "rdecif( 1, R2 )" ),
      INTEGER( 8888,    "rload( R2 )" ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, memory_1k_basic, 0.0, evalmem_1k_basic, NULL ) >= 0,   "Memory 1kB Basic" );
    iEvaluator.DiscardMemory( &evalmem_1k_basic );


    vgx_ExpressEvalMemory_t *evalmem_1k_array = iEvaluator.NewMemory( 10 );
    __test memory_1k_array[] = {

      // mset
      INTEGER( 1,       "mset( 0, 0, 10 )" ),
      INTEGER( 1,       "mset( 1, 1, 11 )" ),
      INTEGER( 2,       "mset( 2, 3, 12 )" ),
      INTEGER( 0,       "mset( 3, 2, 13 )" ),
      INTEGER( 10,      "load( 0 )"     ),
      INTEGER( 11,      "load( 1 )"     ),
      INTEGER( 12,      "load( 2 )"     ),
      INTEGER( 12,      "load( 3 )"     ),
      INTEGER( 1024,    "mset( 0, -1, 1000 )" ),
      INTEGER( 1000,    "load( 0 )"           ),
      INTEGER( 1000,    "load( 1 )"           ),
      INTEGER( 1000,    "load( 2 )"           ),
      INTEGER( 1000,    "load( 1021 )"        ),
      INTEGER( 1000,    "load( 1022 )"        ),
      INTEGER( 1000,    "load( 1023 )"        ),
      INTEGER( 1023,    "mset( 0, -2, 2000 )" ),
      INTEGER( 2000,    "load( 0 )"           ),
      INTEGER( 2000,    "load( 1 )"           ),
      INTEGER( 2000,    "load( 1022 )"        ),
      INTEGER( 1000,    "load( 1023 )"        ),
      INTEGER( 1022,    "mset( 1, -2, 3000 )" ),
      INTEGER( 2000,    "load( 0 )"           ),
      INTEGER( 3000,    "load( 1 )"           ),
      INTEGER( 3000,    "load( 1022 )"        ),
      INTEGER( 1000,    "load( 1023 )"        ),

      INTEGER( 1024,    "mset( 0, 1023, 1 )" ),
      INTEGER( 1,       "load( 0 )"     ),
      INTEGER( 1,       "load( 1 )"     ),
      INTEGER( 1,       "load( 2 )"     ),
      INTEGER( 1,       "load( 50 )"    ),
      INTEGER( 1,       "load( 70 )"    ),
      INTEGER( 1,       "load( R1 )"    ),
      INTEGER( 1,       "load( R2 )"    ),
      INTEGER( 1,       "load( R3 )"    ),
      INTEGER( 1,       "load( R4 )"    ),
      INTEGER( 1,       "get()"         ),
      INTEGER( 11,      "mset( 10, 20, 5.0 )" ),
      INTEGER( 1,       "load( 9 )"     ),
      REAL( 5.0,        "load( 10 )"    ),
      REAL( 5.0,        "load( 11 )"    ),
      REAL( 5.0,        "load( 20 )"    ),
      INTEGER( 1,       "load( 21 )"    ),
      INTEGER( 1,       "load( R1 )"    ),

      // mreset
      INTEGER( 1,       "store(0, 1)"       ),
      INTEGER( 2,       "store(1, 2)"       ),
      INTEGER( 1022,    "store(-2, 1022)"   ),
      INTEGER( 1023,    "store(-1, 1023)"   ),
      INTEGER( 1024,    "mreset()"          ),
      ASSERT(           "load(0) == 0 && load(1) == 0 && load(-2) == 0 && load(-1) == 0" ),

      // M[]
      INTEGER( 111,     "store(0, 111)"     ),
      REAL( 22.2,       "store(7, 22.2)"    ),
      INTEGER( 99,      "store(-1, 99)"     ),
      REAL( 88.8,       "store(-7, 88.8)"   ),
      INTEGER( 111,     "M[ 0 ]"            ),
      REAL( 22.2,       "M[ 7 ]"            ),
      INTEGER( 99,      "M[ -1 ]"           ),
      REAL( 88.8,       "M[ -7 ]"           ),

      // mrandomize
      INTEGER( 1024,    "mrandomize( 0, 1023 )" ),
      ASSERT(           "x = load(0); isreal(x) && x != load(1) && x != load(2)"  ),
      ASSERT(           "x = load(1); isreal(x) && x != load(2) && x != load(3)"  ),
      ASSERT(           "x = load(2); isreal(x) && x != load(3) && x != load(4)"  ),
      ASSERT(           "x = load(-2); isreal(x) && x != load(-3) && x != load(-4)"  ),
      ASSERT(           "x = load(-1); isreal(x) && x != load(-2) && x != load(-3)"  ),

      // mrandbits
      INTEGER( 1024,    "mrandbits( 0, 1023 )" ),
      ASSERT(           "x = load(0); isbitvector(x) && x != load(1) && x != load(2)"  ),
      ASSERT(           "x = load(1); isbitvector(x) && x != load(2) && x != load(3)"  ),
      ASSERT(           "x = load(2); isbitvector(x) && x != load(3) && x != load(4)"  ),
      ASSERT(           "x = load(-2); isbitvector(x) && x != load(-3) && x != load(-4)"  ),
      ASSERT(           "x = load(-1); isbitvector(x) && x != load(-2) && x != load(-3)"  ),

      // mcopy / mcmp
      INTEGER( 3,       "mcopy( 10, 0, 3 )" ),
      ASSERT(           "load(10) == load(0) && load(11) == load(1) && load(12) == load(2)" ),
      ASSERT(           "mcmp( 10, 0, 3 ) == 0" ),
      INTEGER( 2,       "mcopy( 1022, 0, 2 )" ),
      INTEGER( 2,       "mcopy( -2, 0, 2 )" ),
      INTEGER( 2,       "mcopy( 0, 1022, 2 )" ),
      INTEGER( 2,       "mcopy( 0, -2, 2 )" ),
      INTEGER( 0,       "mcopy( 1022, 0, 3 )  /* dest beyond end */ " ),
      INTEGER( 0,       "mcopy( -2, 0, 3 )    /* dest beyond end */ " ),
      INTEGER( 0,       "mcopy( 0, 1022, 3 )  /* src beyond end */" ),
      INTEGER( 0,       "mcopy( 0, -2, 3 )    /* src beyond end */" ),
      INTEGER( 0,       "mcopy( 0, 10, 0 )    /* n=0 */" ),
      INTEGER( 1,       "mcopy( 0, 10, 1 )    /* n=1 */" ),
      INTEGER( 2,       "mcopy( 0, 10, 2 )    /* n=2 */" ),
      INTEGER( 0,       "mcopy( 0, 10, -1 )   /* n<0 */" ),
      INTEGER( 100,     "mrandomize( 200, 299 )" ),
      INTEGER( 100,     "mcopy( 400, 200, 100 )" ),
      ASSERT(           "mcmp( 400, 200, 100 ) == 0" ),
      INTEGER( 2,       "store( 450, 2 )" ),
      ASSERT(           "mcmp( 400, 200, 50 ) == 0" ),
      ASSERT(           "mcmp( 400, 200, 51 ) > 0" ),
      ASSERT(           "mcmp( 400, 200, 100 ) > 0" ),
      INTEGER( 2,       "store( 240, 2 )" ),
      ASSERT(           "mcmp( 400, 200, 100 ) < 0" ),

      // mpwrite
      INTEGER( 100,     "store( R1, 100 )" ),
      INTEGER( 4,       "write( 10,   11,  22,  33,  44 )" ),
      INTEGER( 4,       "write( 20,  555, 666, 777, 888 )" ),
      UNTRUE(           "mcmp( 10, 100, 4 ) == 0" ),
      INTEGER( 4,       "mpwrite( R1, 10, 4 )" ),
      INTEGER( 104,     "load( R1 )" ),
      ASSERT(           "mcmp( 10, 100, 4 ) == 0" ),
      UNTRUE(           "mcmp( 20, 104, 4 ) == 0" ),
      INTEGER( 4,       "mpwrite( R1, 20, 4 )" ),
      INTEGER( 108,     "load( R1 )" ),
      ASSERT(           "mcmp( 20, 104, 4 ) == 0" ),


      // mcmpa
      INTEGER( 5,       "/* A      */      write( 10,  1,  -1.0,          1.0, 1,            5 )" ),
      INTEGER( 5,       "/* B      */      write( 20,  1,  -1.0000000001,   1, 1.0000000001, 5 )" ),
      INTEGER( 5,       "/* C      */      write( 30,  1,  -1.0000000001, 1.0, 1.0000000001, 6 )" ),
      INTEGER( 5,       "/* D      */      write( 40,  1,  -1.0000000001, 1.0, 1.0001,       5 )" ),
      ASSERT(           "/* A==A   */  mcmpa( 10, 10, 5 ) == 0" ),
      ASSERT(           "/* A==B   */  mcmpa( 10, 20, 5 ) == 0" ),
      ASSERT(           "/* A<C    */  mcmpa( 10, 30, 5 ) < 0" ),
      ASSERT(           "/* C>A    */  mcmpa( 30, 10, 5 ) > 0" ),
      ASSERT(           "/* A==C(4)*/  mcmpa( 10, 30, 4 ) == 0" ), // skip 5/6 cmp
      ASSERT(           "/* A<D    */  mcmpa( 10, 40, 5 ) < 0" ),
      ASSERT(           "/* D>A    */  mcmpa( 40, 10, 5 ) > 0" ),

      // mcopyobj
      INTEGER( 24,      "mcopyobj( 0, next )" ),

      // mterm / mlen
      INTEGER( 1024,    "mreset()" ),
      INTEGER( 1024,    "mlen(0)" ),
      INTEGER( 1023,    "mlen(1)" ),
      INTEGER( 1,       "mlen(1023)" ),
      INTEGER( 1,       "mlen(-1)" ),
      INTEGER( 1024,    "mlen(1024)" ),
      INTEGER( 1024,    "mrandomize(0,1023)" ),
      INTEGER( 1024,    "mlen(0)" ),
      INTEGER( 1023,    "mlen(1)" ),
      INTEGER( 1000,    "mterm(1000)" ),
      INTEGER( 1000,    "mlen(0)" ),
      INTEGER( 100,     "mterm(100)" ),
      INTEGER( 100,     "mlen(0)" ),
      INTEGER( 10,      "mterm(10)" ),
      INTEGER( 10,      "mlen(0)" ),
      INTEGER( 1,       "mterm(1)" ),
      INTEGER( 1,       "mlen(0)" ),
      INTEGER( 0,       "mterm(0)" ),
      INTEGER( 0,       "mlen(0)" ),

      // mheapinit
      INTEGER( 1024,    "mreset()" ),
      ASSERT(           "A=10; k=12; store(R3,A); store(R4,k); true /* HEAP AT A=10 with k=12 elements */" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize     */      mheapinit( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* All null       */      mcount( A, A+k-1, null ) == k" ),

      // heapify
      //   basics
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymin( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* All null       */      mcount( A, A+k-1, null ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* All null       */      mcount( A, A+k-1, null ) == k" ),
      //   5 out of 12, top element should remain null
      ASSERT(           "A=load(R3); k=load(R4);  /* Write 5 ints   */      write( A, 111, 222, 333, 444, 555 ) == 5" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 5 less nulls   */      mcount( A, A+k-1, null ) == (k-5)" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymin( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop is null */      load(A) == null" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 5 less nulls   */      mcount( A, A+k-1, null ) == (k-5)" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop is null */      load(A) == null" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 5 less nulls   */      mcount( A, A+k-1, null ) == (k-5)" ),
      //   11 out of 12, top element should remain null
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize     */      mheapinit( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* All null       */      mcount( A, A+k-1, null ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Write 11 nums  */      write( A, 111, 222, 333, 444, 555, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0 ) == 11" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 11 less nulls  */      mcount( A, A+k-1, null ) == (k-11)" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymin( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop is null */      load(A) == null" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 11 less nulls  */      mcount( A, A+k-1, null ) == (k-11)" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop is null */      load(A) == null" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 11 less nulls  */      mcount( A, A+k-1, null ) == (k-11)" ),
      //   12 out of 12, top element should be either min or max
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize     */      mheapinit( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* All null       */      mcount( A, A+k-1, null ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Write 12 nums  */      write( A, 111, 222, 333, 444, 555, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0 ) == 12" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 0 nulls        */      mcount( A, A+k-1, null ) == 0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymin( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop is min  */      load(A) == 6.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 0 nulls        */      mcount( A, A+k-1, null ) == 0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Return k       */      mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop is max  */      load(A) == 555" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* 0 nulls        */      mcount( A, A+k-1, null ) == 0" ),

      // heappush
      //   min
      ASSERT(           "A=load(R3); k=load(R4);  /* Make MinHeap       */    mheapifymin( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop is min      */    load(A) == 6.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 777 yank 6.0  */    mheappushmin( A, k, 777 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop now 7.0     */    load(A) == 7.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 6.0 too small */    mheappushmin( A, k, 6.0 ) == 0" ),
      //   max
      ASSERT(           "A=load(R3); k=load(R4);  /* Make MaxHeap       */    mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop is max      */    load(A) == 777" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 6.0 yank 777  */    mheappushmax( A, k, 6.0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 555     */    load(A) == 555" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 777 too large */    mheappushmax( A, k, 777 ) == 0" ),
      //   max
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 5 yank 555    */    mheappushmax( A, k, 5 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 444     */    load(A) == 444" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 6 yank 444    */    mheappushmax( A, k, 6 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 333     */    load(A) == 333" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 5 yank 333    */    mheappushmax( A, k, 5 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 222     */    load(A) == 222" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 6 yank 222    */    mheappushmax( A, k, 6 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 111     */    load(A) == 111" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 6 yank 111    */    mheappushmax( A, k, 6 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 12.0    */    load(A) == 12.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push -1 yank 12.0  */    mheappushmax( A, k, -1 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 11.0    */    load(A) == 11.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push -3 yank 11.0  */    mheappushmax( A, k, -3 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 10.0    */    load(A) == 10.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push -2 yank 10.0  */    mheappushmax( A, k, -2 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 9.0     */    load(A) == 9.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push -1 yank 9.0   */    mheappushmax( A, k, -1 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 8.0     */    load(A) == 8.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 8.0    */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 7.0     */    load(A) == 7.0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 7.0    */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 6 or 6.0*/    load(A) == 6" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 4 6s         */    mcount( A, A+k-1, 6 ) == 4" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 6/6.0  */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop still 6/6.0 */    load(A) == 6" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 3 6s         */    mcount( A, A+k-1, 6 ) == 3" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 6/6.0  */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop still 6/6.0 */    load(A) == 6" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 6s         */    mcount( A, A+k-1, 6 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 6/6.0  */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop still 6/6.0 */    load(A) == 6" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 6s         */    mcount( A, A+k-1, 6 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 6/6.0  */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 5       */    load(A) == 5" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 5s         */    mcount( A, A+k-1, 5 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 5      */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop still 5     */    load(A) == 5" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 5s         */    mcount( A, A+k-1, 5 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Push 0 yank 5      */    mheappushmax( A, k, 0 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 0       */    load(A) == 0" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 8 0s         */    mcount( A, A+k-1, 0 ) == 8" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 -1s        */    mcount( A, A+k-1, -1 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 -2s        */    mcount( A, A+k-1, -2 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 -3s        */    mcount( A, A+k-1, -3 ) == 1" ),

      // heapwrite
      //   min
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize         */    mheapinit( A, k ) == k" ),
      INTEGER( 0,       "A=load(R3); k=load(R4);  /* Write nothing      */    mheapwritemin()" ),
      INTEGER( 0,       "A=load(R3); k=load(R4);  /* Write nothing      */    mheapwritemin( A )" ),
      INTEGER( 0,       "A=load(R3); k=load(R4);  /* Write nothing      */    mheapwritemin( A, k )" ),
      INTEGER( 1,       "A=load(R3); k=load(R4);  /* Write 1            */    mheapwritemin( A, k, 1 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop stillnull   */    load(A) == null" ),
      INTEGER( 2,       "A=load(R3); k=load(R4);  /* Write 2, 3         */    mheapwritemin( A, k, 2, 3 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop still null  */    load(A) == null" ),
      INTEGER( 3,       "A=load(R3); k=load(R4);  /* Write 4, 5, 6      */    mheapwritemin( A, k, 4, 5, 6 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop still null  */    load(A) == null" ),
      INTEGER( 4,       "A=load(R3); k=load(R4);  /* Write 7, 8, 9, 10  */    mheapwritemin( A, k, 7, 8, 9, 10 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop still null  */    load(A) == null" ),
      INTEGER( 5,       "A=load(R3); k=load(R4);  /* Write 11,12,...,15 */    mheapwritemin( A, k, 11, 12, 13, 14, 15 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MinTop now 4       */    load(A) == 4" ),
      //   max
      //                                                                      NOW: 4,5,6,7,8,9,10,11,12,13,14,15
      ASSERT(           "A=load(R3); k=load(R4);  /* Make MaxHeap       */    mheapifymax( A, k ) == k" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 15      */    load(A) == 15" ),
      INTEGER( 0,       "A=load(R3); k=load(R4);  /* Try 16,17,...,21   */    mheapwritemax( A, k, 16, 17, 18, 19, 20, 21 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop still 15    */    load(A) == 15" ),
      INTEGER( 4,       "A=load(R3); k=load(R4);  /* Write 8,9,10,11    */    mheapwritemax( A, k, 8, 9, 10, 11, 12, 13, 14 )" ),
      //                                                                      NOW: 4,5,6,7,8,8,9,9,10,10,11,11 
      ASSERT(           "A=load(R3); k=load(R4);  /* MaxTop now 11      */    load(A) == 11" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 4          */    mcount( A, A+k-1, 4 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 5          */    mcount( A, A+k-1, 5 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 6          */    mcount( A, A+k-1, 6 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 1 7          */    mcount( A, A+k-1, 7 ) == 1" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 8          */    mcount( A, A+k-1, 8 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 9          */    mcount( A, A+k-1, 9 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 10         */    mcount( A, A+k-1, 10 ) == 2" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Count 2 11         */    mcount( A, A+k-1, 11 ) == 2" ),

      // heapsift
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize         */    mheapinit( A, k ) == k" ),
      INTEGER( 900,     "A=load(R3); k=load(R4);  /* Make random data   */    mrandomize( 100, 999 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* [R1]=smallest val  */    m = mmin( R1, 100, 999 ); load(R1) == load(m); load(R1) < 0.5" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* [R2]=largest val   */    m = mmax( R2, 100, 999 ); load(R2) == load(m); load(R2) > 0.5" ),
      INTEGER( 900,     "A=load(R3); k=load(R4);  /* Sift away small    */    mheapsiftmin( A, k, 100, 999 )" ),
      UNTRUE(           "A=load(R3); k=load(R4);  /* smallest not in A  */    mcontains( A, A+k-1, load(R1) )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* largest in A       */    mcontains( A, A+k-1, load(R2) )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* Initialize         */    mheapinit( A, k ) == k" ),
      INTEGER( 900,     "A=load(R3); k=load(R4);  /* Sift away large    */    mheapsiftmax( A, k, 100, 999 )" ),
      ASSERT(           "A=load(R3); k=load(R4);  /* smallest in A      */    mcontains( A, A+k-1, load(R1) )" ),
      UNTRUE(           "A=load(R3); k=load(R4);  /* largest not in A   */    mcontains( A, A+k-1, load(R2) )" ),

      // msort / msortrev
      INTEGER( 1024,    "mreset()" ),
      INTEGER( 100,     "mrandomize( 0, 99 )" ),
      INTEGER( 898,     "mrandomize( 102, 999 )" ),
      ASSERT(           "store( 100, 0.5 ) == 0.5" ),
      ASSERT(           "store( 101, 0.5 ) == 0.5" ),
      ASSERT(           "m = mmin( R1, 0, 999 ); load(R1) == load(m); load(R1) < 0.5" ),
      ASSERT(           "m = mmax( R2, 0, 999 ); load(R2) == load(m); load(R2) > 0.5" ),
      INTEGER( 100,     "mterm( 100 ); /* Trick to get null into location 100 */  " ),
      ASSERT(           "store( 101, 42 ) == 42" ),
      //    asc
      INTEGER( 999,     "msort( 0, 1000 )" ),
      ASSERT(           "load(0) <= load(1) && load(1) <= load(2) && load(2) <= load(3) && load(99) <= load(100) && load(100) <= load(101)" ),
      ASSERT(           "load( 998 ) == 42" ),
      ASSERT(           "load( 999 ) == null" ),
      //    desc
      INTEGER( 999,     "msortrev( 0, 1000 )" ),
      ASSERT(           "load( 0 ) == 42" ),
      ASSERT(           "load(0) >= load(1) && load(1) >= load(2) && load(2) >= load(3) && load(99) >= load(100) && load(100) >= load(101)" ),
      ASSERT(           "load( 999 ) == null" ),

      // mrsort / mrsortrev
      INTEGER( 1000,    "mrandomize( 0, 999 )" ),
      //    asc
      INTEGER( 1000,    "mrsort( 0, 1000 )" ),
      ASSERT(           "load(0) <= load(1) && load(1) <= load(2) && load(2) <= load(3) && load(99) <= load(100) && load(100) <= load(101) && load(101) <= load(102) && load(998) <= load(999)" ),
      //    desc
      INTEGER( 1000,    "mrsortrev( 0, 1000 )" ),
      ASSERT(           "load(0) >= load(1) && load(1) >= load(2) && load(2) >= load(3) && load(99) >= load(100) && load(100) >= load(101) && load(101) >= load(102) && load(998) >= load(999)" ),
      
      // mreverse
      INTEGER( 1024,    "mreset()" ),
      INTEGER( 2,       "write( 0, 1, 2 )" ),
      ASSERT(           "mreverse( 0, 2 ) == 2" ),
      ASSERT(           "load(0) == 2 && load(1) == 1" ),
      ASSERT(           "mreverse( 0, 1 ) == 1" ),
      ASSERT(           "load(0) == 2 && load(1) == 1" ),
      ASSERT(           "mreverse( 1, 1 ) == 1" ),
      ASSERT(           "load(0) == 2 && load(1) == 1" ),

      INTEGER( 6,       "write( 10, 222, 444.4, str('hello'), next, 333, 555 )" ),
      ASSERT(           "mcopy( 20, 10, 6 ) == 6" ),
      ASSERT(           "mcmp( 10, 20, 6) == 0" ),
      ASSERT(           "mreverse( 10, 6 ) == 6" ),
      ASSERT(           "mcmp( 10, 20, 6 ) != 0" ),
      ASSERT(           "load(10) == load(25)" ),
      ASSERT(           "load(11) == load(24)" ),
      ASSERT(           "load(12) == load(23)" ),
      ASSERT(           "load(13) == load(22)" ),
      ASSERT(           "load(14) == load(21)" ),
      ASSERT(           "load(15) == load(20)" ),


      // mint
      INTEGER( 1000,    "mrandomize( 0, 999 )" ),
      INTEGER( 1000,    "mint( 0, 999 )" ),
      ASSERT(           "isint( load(0) ) && isint( load(1) ) && isint( load(999) )" ),
      ASSERT(           "msum( R1, 0, 999 ) == 0.0" ),

      // mintr
      INTEGER( 1000,    "mrandomize( 0, 999 )" ),
      INTEGER( 1000,    "mintr( 0, 999 )" ),
      ASSERT(           "isint( load(0) ) && isint( load(1) ) && isint( load(999) )" ),
      ASSERT(           "s = msum( R1, 0, 999 );  s > 400.0 && s < 600.0" ),

      // mbits
      INTEGER( 1000,    "mrandomize( 0, 999 )" ),
      INTEGER( 1000,    "mbits( 0, 999 )" ),
      ASSERT(           "isbitvector( load(0) ) && isbitvector( load(1) ) && isbitvector( load(999) )" ),

      // mreal
      INTEGER( 1000,    "mrandomize( 0, 999 )" ),
      INTEGER( 1000,    "mintr( 0, 999 )" ),
      INTEGER( 1000,    "mreal( 0, 999 )" ),
      ASSERT(           "isreal( load(0) ) && isreal( load(1) ) && isreal( load(999) )" ),
      ASSERT(           "s = msum( R1, 0, 999 );  s > 400.0 && s < 600.0" ),

      // minc / mdec / miinc / midec
      INTEGER( 4,       "/* A     */  write( 10, 111, 222, 333, 444 )" ),
      INTEGER( 4,       "/* B     */  write( 20, 112, 223, 334, 445 )" ),
      INTEGER( 4,       "/* W=A   */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* inc W */  minc( 0, 3 )" ),
      ASSERT(           "/* W==B  */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* dec W */  mdec( 0, 3 )" ),
      ASSERT(           "/* W==A  */  mcmp( 0, 10, 4 ) == 0" ),
      INTEGER( 4,       "/* inc W */  miinc( 0, 3 )" ),
      ASSERT(           "/* W==B  */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* dec W */  midec( 0, 3 )" ),
      ASSERT(           "/* W==A  */  mcmp( 0, 10, 4 ) == 0" ),

      // mrinc / mrdec
      INTEGER( 4,       "/* A     */  write( 10, 11.1, 22.2, 33.3, 44.4 )" ),
      INTEGER( 4,       "/* B     */  write( 20, 12.1, 23.2, 34.3, 45.4 )" ),
      INTEGER( 4,       "/* W=A   */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* inc W */  mrinc( 0, 3 )" ),
      ASSERT(           "/* W==B  */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* dec W */  mrdec( 0, 3 )" ),
      ASSERT(           "/* W==A  */  mcmp( 0, 10, 4 ) == 0" ),


      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */  write( 10,  1,    2,    3,    4 )" ),
      INTEGER( 4,       "/* B      */  write( 20, 11,   12,   13,   14 )" ),
      INTEGER( 4,       "/* C      */  write( 30, 12.5, 13.5, 14.5, 15.5 )" ),
      INTEGER( 4,       "/* D      */  write( 40, 15.0, 16.0, 17.0, 18.0 )" ),

      // madd / msub
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W+=10  */  madd( 0, 3, 10 )" ),
      ASSERT(           "/* W==B   */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W+=1.5 */  madd( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==C   */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=1.5 */  msub( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==B   */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=10  */  msub( 0, 3, 10 )" ),
      ASSERT(           "/* W==A   */  mcmp( 0, 10, 4 ) == 0" ),

      // miadd / misub
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W+=10  */  miadd( 0, 3, 10 )" ),
      ASSERT(           "/* W==B   */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=10  */  misub( 0, 3, 10 )" ),
      ASSERT(           "/* W==A   */  mcmp( 0, 10, 4 ) == 0" ),

      // mradd / mrsub
      INTEGER( 4,       "/* W=C    */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W+=2.5 */  mradd( 0, 3, 2.5 )" ),
      ASSERT(           "/* W==D   */  mcmp( 0, 40, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=2.5 */  mrsub( 0, 3, 2.5 )" ),
      ASSERT(           "/* W==C   */  mcmp( 0, 30, 4 ) == 0" ),

      // mvadd / mvsub
      INTEGER( 4,       "/* E      */  write( 50, 12, 14, 16, 18 )" ),
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W+=B   */  mvadd( 0, 20, 4 )" ),
      ASSERT(           "/* W==E   */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=A   */  mvsub( 0, 10, 4 )" ),
      ASSERT(           "/* W==B   */  mcmp( 0, 20, 4 ) == 0" ),

      INTEGER( 4,       "/* E      */  write( 50, 13.5, 15.5, 17.5, 19.5 )" ),
      INTEGER( 4,       "/* F      */  write( 60, -11.5, -11.5, -11.5, -11.5  )" ),
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W+=C   */  mvadd( 0, 30, 4 )" ),
      ASSERT(           "/* W==E   */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=A   */  mvsub( 0, 10, 4 )" ),
      ASSERT(           "/* W==E   */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B    */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W+=A   */  mvadd( 0, 10, 4 )" ),
      ASSERT(           "/* W==E   */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W-=C   */  mvsub( 0, 30, 4 )" ),
      ASSERT(           "/* W==F   */  mcmp( 0, 60, 4 ) == 0" ),

      INTEGER( 4,       "/* E      */  write( 50, 27.5, 29.5, 31.5, 33.5 )" ),
      INTEGER( 4,       "/* W=C    */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W+=D   */  mvadd( 0, 40, 4 )" ),
      ASSERT(           "/* W==E   */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W-=C   */  mvsub( 0, 30, 4 )" ),
      ASSERT(           "/* W==D   */  mcmp( 0, 40, 4 ) == 0" ),


      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,  1,    2,    3,    4 )" ),
      INTEGER( 4,       "/* B      */         write( 20,  5,   10,   15,   20 )" ),
      INTEGER( 4,       "/* C      */  f=1.5; write( 30,  5*f, 10*f, 15*f, 20*f )" ),

      // mmul / mdiv
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W*=5    */  mmul( 0, 3, 5 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W*=1.5  */  mmul( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W/=1.5  */  mdiv( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W/=5    */  mdiv( 0, 3, 5 )" ),
      ASSERT(           "/* W==A    */  mcmp( 0, 10, 4 ) == 0" ),

      // mimul / midiv
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W*=5    */  mimul( 0, 3, 5 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* W/=5    */  midiv( 0, 3, 5 )" ),
      ASSERT(           "/* W==A    */  mcmp( 0, 10, 4 ) == 0" ),

      // mrmul/ mrdiv
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* real(W) */  mreal( 0, 3 )" ),
      INTEGER( 4,       "/* W*=1.5  */  mrmul( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W/=1.5  */  mrdiv( 0, 3, 1.5 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),

      // mvmul / mvdiv
      INTEGER( 4,       "/* D      */         write( 40,  2.0,         3.0,           4.0,           5.0 )" ),
      INTEGER( 4,       "/* E      */         write( 50,  5,           20,            45,            80 )" ),
      INTEGER( 4,       "/* F      */  f=1.5; write( 60,  5*5*f,       20*10*f,       45*15*f,       80*20*f )" ),
      INTEGER( 4,       "/* G      */  f=1.5; write( 70,  5*5*f*1,     20*10*f*2,     45*15*f*3,     80*20*f*4 )" ),
      INTEGER( 4,       "/* H      */  f=1.5; write( 80,  5*5*f*1*2.0, 20*10*f*2*3.0, 45*15*f*3*4.0, 80*20*f*4*5.0 )" ),
      INTEGER( 4,       "/* I      */         write( 90,  5/2.0,       20/3.0,        45/4.0,        80/5.0 )" ),
      INTEGER( 4,       "/* J      */         write( 100, 5/1.0,       20/2.0,        45/3.0,        80/4.0 )" ),
      INTEGER( 4,       "/* W=A    */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W*=B   */  mvmul( 0, 20, 4 )" ), // int * int
      ASSERT(           "/* W==E   */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W*=C   */  mvmul( 0, 30, 4 )" ), // int * real
      ASSERT(           "/* W==F   */  mcmp( 0, 60, 4 ) == 0" ),
      INTEGER( 4,       "/* W*=A   */  mvmul( 0, 10, 4 )" ), // real * int
      ASSERT(           "/* W==G   */  mcmp( 0, 70, 4 ) == 0" ),
      INTEGER( 4,       "/* W*=D   */  mvmul( 0, 40, 4 )" ), // real * real
      ASSERT(           "/* W==H   */  mcmp( 0, 80, 4 ) == 0" ),

      INTEGER( 4,       "/* W/=D   */  mvdiv( 0, 40, 4 )" ), // real / real
      ASSERT(           "/* W==G   */  load(0)==load(70) && load(1)==load(71) && load(2) == load(72) && load(3) == load(73)" ),
      INTEGER( 4,       "/* W/=A   */  mvdiv( 0, 10, 4 )" ), // real / int
      ASSERT(           "/* W==F   */  load(0)==load(60) && load(1)==load(61) && load(2) == load(62) && load(3) == load(63)" ),
      INTEGER( 4,       "/* W=E    */  mcopy( 0, 50, 4 )" ),
      INTEGER( 4,       "/* W/=D   */  mvdiv( 0, 40, 4 )" ), // int / real
      ASSERT(           "/* W==I   */  load(0)==load(90) && load(1)==load(91) && load(2) == load(92) && load(3) == load(93)" ),
      INTEGER( 4,       "/* W=E    */  mcopy( 0, 50, 4 )" ),
      INTEGER( 4,       "/* W/=A   */  mvdiv( 0, 10, 4 )" ), // int / int
      ASSERT(           "/* W==J   */  load(0)==load(100) && load(1)==load(101) && load(2) == load(102) && load(3) == load(103)" ),

      // mmod
      INTEGER( 4,       "/* E       */  write( 50,  1, 0, 1, 0 )" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* W%=2    */  mmod( 0, 3, 2 )" ),
      ASSERT(           "/* W==E    */  mcmp( 0, 50, 4 ) == 0" ),

      // mimod
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* W%=2    */  mimod( 0, 3, 2 )" ),
      ASSERT(           "/* W==E    */  mcmp( 0, 50, 4 ) == 0" ),

      // mrmod
      INTEGER( 4,       "/* F       */  write( 60,  0.5, 1.0, 1.5, 2.0 )" ),
      INTEGER( 4,       "/* W=C     */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W%=3.5  */  mrmod( 0, 3, 3.5 )" ),
      ASSERT(           "/* W==F    */  mcmp( 0, 60, 4 ) == 0" ),

      // mvmod
      INTEGER( 4,       "/* A       */         write( 10,    11,       22,     33.0,       44.0 )" ),
      INTEGER( 4,       "/* B       */         write( 20,     2,      3.0,        4,        5.0 )" ),
      INTEGER( 4,       "/* C       */         write( 30,  11%2,   22%3.0,   33.0%4,   44.0%5.0 )" ),
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W%B     */  mvmod( 0, 20, 4 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),


      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,     1,    2,    3,    4 )" ),
      INTEGER( 4,       "/* B      */         write( 20,     1,  1/2,  1/3,  1/4 )" ),

      // minv
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* inv(W)  */  minv( 0, 3 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* inv(W)  */  minv( 0, 3 )" ),
      ASSERT(           "/* W==A    */  mcmp( 0, 10, 4 ) == 0" ),

      // mrinv
      INTEGER( 4,       "/* inv(W)  */  mrinv( 0, 3 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),
      INTEGER( 4,       "/* inv(W)  */  mrinv( 0, 3 )" ),
      ASSERT(           "/* W==A    */  mcmp( 0, 10, 4 ) == 0" ),

      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,     1,    2,    3,    4 )" ),
      INTEGER( 4,       "/* B      */   x=5;  write( 20,     1**x, 2**x, 3**x, 4**x)" ),
      INTEGER( 4,       "/* C      */         write( 30,     1.0,    2.0,    3.0,    4.0 )" ),
      INTEGER( 4,       "/* B      */   x=5;  write( 40,     1.0**x, 2.0**x, 3.0**x, 4.0**x)" ),

      // mpow
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W**5    */  mpow( 0, 3, 5 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),

      // mrpow
      INTEGER( 4,       "/* W=C     */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W**5    */  mpow( 0, 3, 5 )" ),
      ASSERT(           "/* W==D    */  mcmp( 0, 40, 4 ) == 0" ),

      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,     1,    2,    3,    4 )" ),
      INTEGER( 4,       "/* B      */         write( 20,     1**2, 2**2, 3**2, 4**2 )" ),
      INTEGER( 4,       "/* C      */         write( 30,     1.0,    2.0,    3.0,    4.0 )" ),
      INTEGER( 4,       "/* B      */         write( 40,     1.0**2, 2.0**2, 3.0**2, 4.0**2 )" ),

      // msq
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* W**2    */  msq( 0, 3 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),

      // mrsq
      INTEGER( 4,       "/* W=C     */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* W**2    */  msq( 0, 3 )" ),
      ASSERT(           "/* W==D    */  mcmp( 0, 40, 4 ) == 0" ),

      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,       9,    16,   25.0,   36.0 )" ),
      INTEGER( 4,       "/* B      */         write( 20,     3.0,   4.0,    5.0,    6.0 )" ),
      INTEGER( 4,       "/* C      */         write( 30,     9.0,  16.0,   25.0,   36.0 )" ),

      // msqrt
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* sqrt(W) */  msqrt( 0, 3 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),

      // mrsqrt
      INTEGER( 4,       "/* W=C     */  mcopy( 0, 30, 4 )" ),
      INTEGER( 4,       "/* sqrt(W) */  mrsqrt( 0, 3 )" ),
      ASSERT(           "/* W==B    */  mcmp( 0, 20, 4 ) == 0" ),

      // (Setup for arithmetic)
      INTEGER( 4,       "/* A      */         write( 10,     1,    1.3,   1.6,  2.0 )" ),
      INTEGER( 4,       "/* B      */         write( 20,     1.0,  1.3,   1.6,  2.0 )" ),
      INTEGER( 4,       "/* C      */         write( 30,     1.0,  2.0,   2.0,  2.0 )" ),
      INTEGER( 4,       "/* D      */         write( 40,     1.0,  1.0,   1.0,  2.0 )" ),
      INTEGER( 4,       "/* E      */         write( 50,     1.0,  1.0,   2.0,  2.0 )" ),

      // mceil / mfloor
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* ceil(W) */  mceil( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* floor(W)*/  mfloor( 0, 3 )" ),
      ASSERT(           "/* W==D    */  mcmp( 0, 40, 4 ) == 0" ),

      // mrceil / mrfloor
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* ceil(W) */  mrceil( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* floor(W)*/  mrfloor( 0, 3 )" ),
      ASSERT(           "/* W==D    */  mcmp( 0, 40, 4 ) == 0" ),

      // mround / mrround
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* round(W)*/  mround( 0, 3 )" ),
      ASSERT(           "/* W==E    */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* round(W)*/  mrround( 0, 3 )" ),
      ASSERT(           "/* W==E    */  mcmp( 0, 50, 4 ) == 0" ),

      // (Setup for arithmetic)
      INTEGER( 4,       "/* A       */        write( 10,     -1,    1,   -1.5,  1.5 )" ),
      INTEGER( 4,       "/* B       */        write( 20,   -1.0,  1.0,   -1.5,  1.5 )" ),
      INTEGER( 4,       "/* C       */        write( 30,      1,    1,    1.5,  1.5 )" ),
      INTEGER( 4,       "/* D       */        write( 40,    1.0,  1.0,    1.5,  1.5 )" ),

      // mabs / mrabs
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* abs(W)  */  mabs( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmp( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* abs(W)  */  mrabs( 0, 3 )" ),
      ASSERT(           "/* W==D    */  mcmp( 0, 40, 4 ) == 0" ),

      INTEGER( 4,       "/* E       */        write( 50,     -1,    1,   -1.0,   1.0)" ),
      INTEGER( 4,       "/* F       */        write( 60,   -1.0,  1.0,   -1.0,   1.0)" ),

      // msign
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* abs(W)  */  msign( 0, 3 )" ),
      ASSERT(           "/* W==E    */  mcmp( 0, 50, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* abs(W)  */  mrsign( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmp( 0, 60, 4 ) == 0" ),

      // (Setup for mlog2 and mexp2)
      INTEGER( 4,       "/* A       */        write( 10,        1,      2,    256.0,     12345.6789       )" ),
      INTEGER( 4,       "/* B       */        write( 20,      1.0,    2.0,    256.0,     12345.6789       )" ),
      INTEGER( 4,       "/* C       */        write( 30,      0.0,    1.0,      8.0,       log2(12345.6789) )" ),
      INTEGER( 4,       "/* D       */        write( 40,       -1,      0,        1,       16.0             )" ),
      INTEGER( 4,       "/* E       */        write( 50,     -1.0,    0.0,      1.0,       16.0             )" ),
      INTEGER( 4,       "/* F       */        write( 60,  2.0**-1, 2.0**0,   2.0**1,  2.0**16.0             )" ),

      // mlog2, mrlog2, mexp2, mrexp2
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* log2(W) */  mlog2( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* log2(W) */  mrlog2( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=D     */  mcopy( 0, 40, 4 )" ),
      INTEGER( 4,       "/* exp2(W) */  mexp2( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),
      INTEGER( 4,       "/* W=E     */  mcopy( 0, 50, 4 )" ),
      INTEGER( 4,       "/* exp2(W) */  mrexp2( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),

      INTEGER( 4,       "/* W=A               */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* exp2( log2(W) )   */  mlog2( 0, 3 ); mexp2( 0, 3 )" ),
      ASSERT(           "/* W==A              */  mcmpa( 0, 10, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B               */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* exp2( log2(W) )   */  mrlog2( 0, 3 ); mrexp2( 0, 3 )" ),
      ASSERT(           "/* W==B              */  mcmpa( 0, 20, 4 ) == 0" ),


      // (Setup for mlog and mexp)
      INTEGER( 4,       "/* A       */        write( 10,        1,      2,      e,       12345.6789      )" ),
      INTEGER( 4,       "/* B       */        write( 20,      1.0,    2.0,      e,       12345.6789      )" ),
      INTEGER( 4,       "/* C       */        write( 30,   log(1), log(2),    1.0,   log(12345.6789)     )" ),
      INTEGER( 4,       "/* D       */        write( 40,       -1,      0,      1,        pi             )" ),
      INTEGER( 4,       "/* E       */        write( 50,     -1.0,    0.0,    1.0,        pi             )" ),
      INTEGER( 4,       "/* F       */        write( 60,  exp(-1), exp(0), exp(1),    exp(pi)            )" ),

      // mlog, mrlog, mexp, mrexp
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* log(W)  */  mlog( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* log(W)  */  mrlog( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=D     */  mcopy( 0, 40, 4 )" ),
      INTEGER( 4,       "/* exp(W)  */  mexp( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),
      INTEGER( 4,       "/* W=E     */  mcopy( 0, 50, 4 )" ),
      INTEGER( 4,       "/* exp(W)  */  mrexp( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),

      INTEGER( 4,       "/* W=A               */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* exp( log(W) )     */  mlog( 0, 3 ); mexp( 0, 3 )" ),
      ASSERT(           "/* W==A              */  mcmpa( 0, 10, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B               */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* exp( log(W) )     */  mrlog( 0, 3 ); mrexp( 0, 3 )" ),
      ASSERT(           "/* W==B              */  mcmpa( 0, 20, 4 ) == 0" ),

      // (Setup for mlog10 and mexp10)
      INTEGER( 4,       "/* A       */        write( 10,        1,        2,      1000.0,        12345.6789     )" ),
      INTEGER( 4,       "/* B       */        write( 20,      1.0,      2.0,      1000.0,        12345.6789     )" ),
      INTEGER( 4,       "/* C       */        write( 30, log10(1), log10(2),         3.0,  log10(12345.6789)    )" ),
      INTEGER( 4,       "/* D       */        write( 40,       -1,        0,          1,         pi             )" ),
      INTEGER( 4,       "/* E       */        write( 50,     -1.0,      0.0,        1.0,         pi             )" ),
      INTEGER( 4,       "/* F       */        write( 60,      0.1,      1.0,       10.0,     10**pi             )" ),

      // mlog10, mrlog10, mexp10, mrexp10
      INTEGER( 4,       "/* W=A     */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* log10(W)*/  mlog10( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B     */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* log10(W)*/  mrlog10( 0, 3 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 4 ) == 0" ),
      INTEGER( 4,       "/* W=D     */  mcopy( 0, 40, 4 )" ),
      INTEGER( 4,       "/* exp10(W)*/  mexp10( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),
      INTEGER( 4,       "/* W=E     */  mcopy( 0, 50, 4 )" ),
      INTEGER( 4,       "/* exp10(W)*/  mrexp10( 0, 3 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 4 ) == 0" ),

      INTEGER( 4,       "/* W=A               */  mcopy( 0, 10, 4 )" ),
      INTEGER( 4,       "/* exp10( log10(W) ) */  mlog10( 0, 3 ); mexp10( 0, 3 )" ),
      ASSERT(           "/* W==A              */  mcmpa( 0, 10, 4 ) == 0" ),
      INTEGER( 4,       "/* W=B               */  mcopy( 0, 20, 4 )" ),
      INTEGER( 4,       "/* exp10( log10(W) ) */  mrlog10( 0, 3 ); mrexp10( 0, 3 )" ),
      ASSERT(           "/* W==B              */  mcmpa( 0, 20, 4 ) == 0" ),

      // (Setup for mrad / mdeg)
      INTEGER( 8,       "/* A       */        write( 10,    -180,    -90,    0,    90,    180,     270,    360,     450  )" ),
      INTEGER( 8,       "/* B       */        write( 20,     -pi,  -pi/2,  0.0,  pi/2,     pi,  3*pi/2,   2*pi,  5*pi/2  )" ),
      INTEGER( 8,       "/* C       */        write( 30,  -180.0,  -90.0,  0.0,  90.0,  180.0,   270.0,  360.0,   450.0  )" ),

      // mrad / mdeg / mrrad / mrdeg
      INTEGER( 8,       "/* W=A     */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* rad(W)  */  mrad( 0, 7 )" ),
      ASSERT(           "/* W==B    */  mcmpa( 0, 20, 8 ) == 0" ),
      INTEGER( 8,       "/* deg(W)  */  mdeg( 0, 7 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 8 ) == 0" ),
      INTEGER( 8,       "/* rad(W)  */  mrrad( 0, 7 )" ),
      ASSERT(           "/* W==B    */  mcmpa( 0, 20, 8 ) == 0" ),
      INTEGER( 8,       "/* deg(W)  */  mrdeg( 0, 7 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 8 ) == 0" ),

      // (Setup for trig)
      INTEGER( 6,       "/* A       */        write( 10,              -5,              -0.1,               0,               1,              pi/2,             123.4    )" ),
      INTEGER( 6,       "/* B       */        write( 20,          sin(-5),         sin(-0.1),          sin(0),          sin(1),         sin(pi/2),        sin(123.4)   )" ),
      INTEGER( 6,       "/* C       */        write( 30,     asin(sin(-5)),   asin(sin(-0.1)),    asin(sin(0)),    asin(sin(1)),   asin(sin(pi/2)),  asin(sin(123.4))  )" ),
      INTEGER( 6,       "/* D       */        write( 40,          cos(-5),         cos(-0.1),          cos(0),          cos(1),         cos(pi/2),        cos(123.4)   )" ),
      INTEGER( 6,       "/* E       */        write( 50,     acos(cos(-5)),   acos(cos(-0.1)),    acos(cos(0)),    acos(cos(1)),   acos(cos(pi/2)),  acos(cos(123.4))  )" ),
      INTEGER( 6,       "/* F       */        write( 60,          tan(-5),         tan(-0.1),          tan(0),          tan(1),         tan(pi/2),        tan(123.4)   )" ),
      INTEGER( 6,       "/* G       */        write( 70,     atan(tan(-5)),   atan(tan(-0.1)),    atan(tan(0)),    atan(tan(1)),   atan(tan(pi/2)),  atan(tan(123.4))  )" ),

      INTEGER( 6,       "/* H       */        write( 80,               -pi/2,              -pi/4,               0.0,               pi/4,              pi/2,             123.4    )" ),
      INTEGER( 6,       "/* I       */        write( 90,           sin(-pi/2),         sin(-pi/4),          sin(0.0),          sin(pi/4),         sin(pi/2),        sin(123.4)   )" ),
      INTEGER( 6,       "/* J       */        write( 100,     asin(sin(-pi/2)),   asin(sin(-pi/4)),    asin(sin(0.0)),    asin(sin(pi/4)),   asin(sin(pi/2)),  asin(sin(123.4))  )" ),
      INTEGER( 6,       "/* K       */        write( 110,          cos(-pi/2),         cos(-pi/4),          cos(0.0),          cos(pi/4),         cos(pi/2),        cos(123.4)   )" ),
      INTEGER( 6,       "/* L       */        write( 120,     acos(cos(-pi/2)),   acos(cos(-pi/4)),    acos(cos(0.0)),    acos(cos(pi/4)),   acos(cos(pi/2)),  acos(cos(123.4))  )" ),
      INTEGER( 6,       "/* M       */        write( 130,          tan(-pi/2),         tan(-pi/4),          tan(0.0),          tan(pi/4),         tan(pi/2),        tan(123.4)   )" ),
      INTEGER( 6,       "/* N       */        write( 140,     atan(tan(-pi/2)),   atan(tan(-pi/4)),    atan(tan(0.0)),    atan(tan(pi/4)),   atan(tan(pi/2)),  atan(tan(123.4))  )" ),

      // msin / masin / mrsin / mrasin
      INTEGER( 6,       "/* W=A     */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* sin(W)  */  msin( 0, 5 )" ),
      ASSERT(           "/* W==B    */  mcmpa( 0, 20, 6 ) == 0" ),
      INTEGER( 6,       "/* asin(W) */  masin( 0, 5 )" ),
      ASSERT(           "/* W==C    */  mcmpa( 0, 30, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H     */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* sin(W)  */  mrsin( 0, 5 )" ),
      ASSERT(           "/* W==I    */  mcmpa( 0, 90, 6 ) == 0" ),
      INTEGER( 6,       "/* asin(W) */  mrasin( 0, 5 )" ),
      ASSERT(           "/* W==J    */  mcmpa( 0, 100, 6 ) == 0" ),

      // mcos / acos / mrcos / mracos
      INTEGER( 6,       "/* W=A     */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* cos(W)  */  mcos( 0, 5 )" ),
      ASSERT(           "/* W==D    */  mcmpa( 0, 40, 6 ) == 0" ),
      INTEGER( 6,       "/* acos(W) */  macos( 0, 5 )" ),
      ASSERT(           "/* W==E    */  mcmpa( 0, 50, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H     */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* cos(W)  */  mrcos( 0, 5 )" ),
      ASSERT(           "/* W==K    */  mcmpa( 0, 110, 6 ) == 0" ),
      INTEGER( 6,       "/* acos(W) */  mracos( 0, 5 )" ),
      ASSERT(           "/* W==L    */  mcmpa( 0, 120, 6 ) == 0" ),

      // mtan / matan / mrtan / mratan
      INTEGER( 6,       "/* W=A     */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* tan(W)  */  mtan( 0, 5 )" ),
      ASSERT(           "/* W==F    */  mcmpa( 0, 60, 6 ) == 0" ),
      INTEGER( 6,       "/* atan(W) */  matan( 0, 5 )" ),
      ASSERT(           "/* W==G    */  mcmpa( 0, 70, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H     */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* tan(W)  */  mrtan( 0, 5 )" ),
      ASSERT(           "/* W==M    */  mcmpa( 0, 130, 6 ) == 0" ),
      INTEGER( 6,       "/* atan(W) */  mratan( 0, 5 )" ),
      ASSERT(           "/* W==N    */  mcmpa( 0, 140, 6 ) == 0" ),


      // (Setup for hyperbolic)
      INTEGER( 6,       "/* A       */        write( 10,                -3,                -0.1,                 0,                 1,                e,               pi    )" ),
      INTEGER( 6,       "/* B       */        write( 20,           sinh(-3),          sinh(-0.1),           sinh(0),           sinh(1),          sinh(e),         sinh(pi)   )" ),
      INTEGER( 6,       "/* C       */        write( 30,     asinh(sinh(-3)),   asinh(sinh(-0.1)),    asinh(sinh(0)),    asinh(sinh(1)),   asinh(sinh(e)),  asinh(sinh(pi))  )" ),
      INTEGER( 6,       "/* D       */        write( 40,           cosh(-3),          cosh(-0.1),           cosh(0),           cosh(1),          cosh(e),         cosh(pi)   )" ),
      INTEGER( 6,       "/* E       */        write( 50,     acosh(cosh(-3)),   acosh(cosh(-0.1)),    acosh(cosh(0)),    acosh(cosh(1)),   acosh(cosh(e)),  acosh(cosh(pi))  )" ),
      INTEGER( 6,       "/* F       */        write( 60,           tanh(-3),          tanh(-0.1),           tanh(0),           tanh(1),          tanh(e),         tanh(pi)   )" ),
      INTEGER( 6,       "/* G       */        write( 70,     atanh(tanh(-3)),   atanh(tanh(-0.1)),    atanh(tanh(0)),    atanh(tanh(1)),   atanh(tanh(e)),  atanh(tanh(pi))  )" ),

      INTEGER( 6,       "/* H       */        write( 80,                 -pi,                -e,                 0.0,                 1.0,                e,               pi    )" ),
      INTEGER( 6,       "/* I       */        write( 90,            sinh(-pi),          sinh(-e),           sinh(0.0),           sinh(1.0),          sinh(e),         sinh(pi)   )" ),
      INTEGER( 6,       "/* J       */        write( 100,     asinh(sinh(-pi)),   asinh(sinh(-e)),    asinh(sinh(0.0)),    asinh(sinh(1.0)),   asinh(sinh(e)),  asinh(sinh(pi))  )" ),
      INTEGER( 6,       "/* K       */        write( 110,           cosh(-pi),          cosh(-e),           cosh(0.0),           cosh(1.0),          cosh(e),         cosh(pi)   )" ),
      INTEGER( 6,       "/* L       */        write( 120,     acosh(cosh(-pi)),   acosh(cosh(-e)),    acosh(cosh(0.0)),    acosh(cosh(1.0)),   acosh(cosh(e)),  acosh(cosh(pi))  )" ),
      INTEGER( 6,       "/* M       */        write( 130,           tanh(-pi),          tanh(-e),           tanh(0.0),           tanh(1.0),          tanh(e),         tanh(pi)   )" ),
      INTEGER( 6,       "/* N       */        write( 140,     atanh(tanh(-pi)),   atanh(tanh(-e)),    atanh(tanh(0.0)),    atanh(tanh(1.0)),   atanh(tanh(e)),  atanh(tanh(pi))  )" ),

      // msinh / masinh / mrsinh / mrasinh
      INTEGER( 6,       "/* W=A      */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* sinh(W)  */  msinh( 0, 5 )" ),
      ASSERT(           "/* W==B     */  mcmpa( 0, 20, 6 ) == 0" ),
      INTEGER( 6,       "/* asinh(W) */  masinh( 0, 5 )" ),
      ASSERT(           "/* W==C     */  mcmpa( 0, 30, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H      */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* sinh(W)  */  mrsinh( 0, 5 )" ),
      ASSERT(           "/* W==I     */  mcmpa( 0, 90, 6 ) == 0" ),
      INTEGER( 6,       "/* asinh(W) */  mrasinh( 0, 5 )" ),
      ASSERT(           "/* W==J     */  mcmpa( 0, 100, 6 ) == 0" ),

      // mcosh / acosh / mrcosh / mracosh
      INTEGER( 6,       "/* W=A      */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* cosh(W)  */  mcosh( 0, 5 )" ),
      ASSERT(           "/* W==D     */  mcmpa( 0, 40, 6 ) == 0" ),
      INTEGER( 6,       "/* acosh(W) */  macosh( 0, 5 )" ),
      ASSERT(           "/* W==E     */  mcmpa( 0, 50, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H      */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* cosh(W)  */  mrcosh( 0, 5 )" ),
      ASSERT(           "/* W==K     */  mcmpa( 0, 110, 6 ) == 0" ),
      INTEGER( 6,       "/* acosh(W) */  mracosh( 0, 5 )" ),
      ASSERT(           "/* W==L     */  mcmpa( 0, 120, 6 ) == 0" ),

      // mtanh / matanh / mrtanh / mratanh
      INTEGER( 6,       "/* W=A      */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* tanh(W)  */  mtanh( 0, 5 )" ),
      ASSERT(           "/* W==F     */  mcmpa( 0, 60, 6 ) == 0" ),
      INTEGER( 6,       "/* atanh(W) */  matanh( 0, 5 )" ),
      ASSERT(           "/* W==G     */  mcmpa( 0, 70, 6 ) == 0" ),
      INTEGER( 6,       "/* W=H      */  mcopy( 0, 80, 6 )" ),
      INTEGER( 6,       "/* tanh(W)  */  mrtanh( 0, 5 )" ),
      ASSERT(           "/* W==M     */  mcmpa( 0, 130, 6 ) == 0" ),
      INTEGER( 6,       "/* atanh(W) */  mratanh( 0, 5 )" ),
      ASSERT(           "/* W==N     */  mcmpa( 0, 140, 6 ) == 0" ),

      // (Setup for sinc)
      INTEGER( 6,       "/* A       */        write( 10,          -3,          -0.1,          0,            1,            pi/2,         123.4    )" ),
      INTEGER( 6,       "/* B       */        write( 20,     sinc(-3),    sinc(-0.1),    sinc(0),      sinc(1),      sinc(pi/2),   sinc(123.4)   )" ),
      INTEGER( 6,       "/* C       */        write( 30,          -pi,         -0.1,          0.0,          1.0,          pi/2,         123.4    )" ),
      INTEGER( 6,       "/* D       */        write( 40,     sinc(-pi),   sinc(-0.1),    sinc(0.0),    sinc(1.0),    sinc(pi/2),   sinc(123.4)   )" ),

      // msinc / mrsinc
      INTEGER( 6,       "/* W=A     */  mcopy( 0, 10, 6 )" ),
      INTEGER( 6,       "/* sinc(W) */  msinc( 0, 5 )" ),
      ASSERT(           "/* W==B    */  mcmpa( 0, 20, 6 ) == 0" ),
      INTEGER( 6,       "/* W=C     */  mcopy( 0, 30, 6 )" ),
      INTEGER( 6,       "/* sinc(W) */  mrsinc( 0, 5 )" ),
      ASSERT(           "/* W==D    */  mcmpa( 0, 40, 6 ) == 0" ),


      // (Setup for bitwise)
      INTEGER( 8,       "/* A           */        write( 10,    0x0000,   0x0001,   0x0002,   0x0003,   0x000F,   0x00FF,      0xFFFF,           0xF000  )" ),
      INTEGER( 8,       "/* B   = A<<2  */        write( 20,    0x0000,   0x0004,   0x0008,   0x000C,   0x003C,   0x03FC,     0x3FFFC,          0x3C000  )" ),
      INTEGER( 8,       "/* C           */        write( 30,         1,        1,        1,        4,       12,        8,          16,               32  )" ),
      INTEGER( 8,       "/* D   = A<<C  */        write( 40,    0x0000,   0x0002,   0x0004,   0x0030,   0xF000,   0xFF00,  0xFFFF0000,   0xF00000000000  )" ),

      INTEGER( 8,       "/* E = A&0xF0  */        write( 50,    0x0000,   0x0000,   0x0000,   0x0000,   0x0000,   0x00F0,      0x00F0,           0x0000  )" ),
      INTEGER( 8,       "/* F = A|0xF0  */        write( 60,    0x00F0,   0x00F1,   0x00F2,   0x00F3,   0x00FF,   0x00FF,      0xFFFF,           0xF0F0  )" ),
      INTEGER( 8,       "/* G = A^0xF0  */        write( 70,    0x00F0,   0x00F1,   0x00F2,   0x00F3,   0x00FF,   0x000F,      0xFF0F,           0xF0F0  )" ),

      INTEGER( 8,       "/* H           */        write( 80,    0x0001,   0x0001,   0x0001,   0x0FFF,   0x0FFF,   0x0FFF,      0x0FF0,           0xF00F  )" ),
      INTEGER( 8,       "/* I = A&H     */        write( 90,    0x0000,   0x0001,   0x0000,   0x0003,   0x000F,   0x00FF,      0x0FF0,           0xF000  )" ),
      INTEGER( 8,       "/* J = A|H     */        write( 100,   0x0001,   0x0001,   0x0003,   0x0FFF,   0x0FFF,   0x0FFF,      0xFFFF,           0xF00F  )" ),
      INTEGER( 8,       "/* K = A^H     */        write( 110,   0x0001,   0x0000,   0x0003,   0x0FFC,   0x0FF0,   0x0F00,      0xF00F,           0x000F  )" ),

      // mshl / mshr / mvshl / mvshr
      INTEGER( 8,       "/* W=A         */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mshl(W,2)   */  mshl( 0, 7, 2 )" ),
      ASSERT(           "/* W==B        */  mcmp( 0, 20, 8 ) == 0" ),
      INTEGER( 8,       "/* mshr(W,2)   */  mshr( 0, 7, 2 )" ),
      ASSERT(           "/* W==A        */  mcmp( 0, 10, 8 ) == 0" ),
      INTEGER( 8,       "/* mvshl(W,C)  */  mvshl( 0, 30, 8 )" ),
      ASSERT(           "/* W==D        */  mcmp( 0, 40, 8 ) == 0" ),
      INTEGER( 8,       "/* mvshr(W,C)  */  mvshr( 0, 30, 8 )" ),
      ASSERT(           "/* W==A        */  mcmp( 0, 10, 8 ) == 0" ),

      // mand / mor / mxor
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mand(W,0xF0) */  mand( 0, 7, 0xF0 )" ),
      ASSERT(           "/* W==E         */  mcmp( 0, 50, 8 ) == 0" ),
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mor(W,0xF0)  */  mor( 0, 7, 0xF0 )" ),
      ASSERT(           "/* W==F         */  mcmp( 0, 60, 8 ) == 0" ),
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mxor(W,0xF0) */  mxor( 0, 7, 0xF0 )" ),
      ASSERT(           "/* W==G         */  mcmp( 0, 70, 8 ) == 0" ),

      // mvand / mvor / mvxor
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mvand(W,H)   */  mvand( 0, 80, 8 )" ),
      ASSERT(           "/* W==I         */  mcmp( 0, 90, 8 ) == 0" ),
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mvor(W,H)    */  mvor( 0, 80, 8 )" ),
      ASSERT(           "/* W==J         */  mcmp( 0, 100, 8 ) == 0" ),
      INTEGER( 8,       "/* W=A          */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* mvxor(W,H)   */  mvxor( 0, 80, 8 )" ),
      ASSERT(           "/* W==K         */  mcmp( 0, 110, 8 ) == 0" ),

      // (Setup for mpopcnt)
      INTEGER( 9,       "/* source val  */        write(  0, 0, 1, 2, 3, 4, 0xffff, 0xffff0000, 0xffff0000ffff0000, 0xffffffffffffffff )" ),
      INTEGER( 9,       "/* expected    */        write( 10, 0, 1, 1, 2, 1, 16,     16,         32,                 64 )" ),

      // mpopcnt
      INTEGER( 9,       "/* mpopcnt()   */        mpopcnt( 0, 8 )" ),
      ASSERT(           "/* compare     */        mcmp( 0, 10, 9 ) == 0" ),

      // (Setup for hash)
      INTEGER( 8,       "/* A           */        write( 10,       0,       0.0,       1,       -1,       e,        str('hello'),        null,       next   )" ),
      INTEGER( 8,       "/* B           */        write( 20,  hash(0), hash(0.0), hash(1), hash(-1), hash(e), hash( str('hello') ), hash(null), hash(next)  )" ),

      // mhash
      INTEGER( 8,       "/* W=A         */  mcopy( 0, 10, 8 )" ),
      INTEGER( 8,       "/* hash(W)     */  mhash( 0, 7 )" ),
      ASSERT(           "/* W==B        */  mcmp( 0, 20, 8 ) == 0" ),
      INTEGER( 1024,    "mreset()" ),

      // (Setup for aggregation)
      INTEGER( 8,       "/* A           */        write( 10,  1,    2,    3,    4,    5.0,  6.0,  7.0,  8.0 )" ),
      INTEGER( 8,       "/* B           */        write( 20,  1.0,  2.0,  3.0,  4.0,  5.0,  6.0,  7.0,  8.0 )" ),
      INTEGER( 8,       "/* C           */        write( 30,  1.0,  2.0,  3.0,  4.0,  5.0,  6.0,  7.0,  8.0 )" ),
      ASSERT(           "/* 0=sum       */        do( store( 0, real( 1+2+3+4+5+6+7+8 ) ) )" ),
      ASSERT(           "/* 1=sumsqr    */        do( store( 1, real( 1+4+9+16+25+36+49+64 ) ) )" ),
      ASSERT(           "/* 2=invsum    */        do( store( 2, 1/1 + 1/2 + 1/3 + 1/4 + 1/5 + 1/6 + 1/7 + 1/8 ) )" ),
      ASSERT(           "/* 3=prod      */        do( store( 3, real( 1*2*3*4*5*6*7*8 ) ) )" ),
      ASSERT(           "/* 4=mean      */        do( store( 4, load(0)/8 ) )" ),
      ASSERT(           "/* 5=harmmean  */        do( store( 5, 8/load(2) ) )" ),
      ASSERT(           "/* 6=geomean   */        do( store( 6, load(3) ** (1/8) ) )" ),
      ASSERT(           "/* 7=stdev     */        m=load(4); s = (1-m)**2 + (2-m)**2 + (3-m)**2 + (4-m)**2 + (5-m)**2 + (6-m)**2 + (7-m)**2 + (8-m)**2; store( 7, sqrt( s/8 ) ); true" ),
      ASSERT(           "/* 8=geostdev  */        m=load(6); s = log(1/m)**2 + log(2/m)**2 + log(3/m)**2 + log(4/m)**2 + log(5/m)**2 + log(6/m)**2 + log(7/m)**2 + log(8/m)**2; store( 8, exp(sqrt(s/8)) ); true" ),

      // msum / mrsum
      ASSERT(           "/* R1=sum(A)   */  msum( R1, 10, 17 ) == load(0)" ),
      ASSERT(           "/* [R1]==[0]   */  load(R1) == load(0)" ),
      ASSERT(           "/* R2=sum(A)   */  mrsum( R2, 20, 27 ) == load(0)" ),
      ASSERT(           "/* [R2]==[0]   */  load(R2) == load(0)" ),

      // msumsqr / mrsumsqr
      ASSERT(           "/* R1=sumsqr(A)   */  msumsqr( R1, 10, 17 ) == load(1)" ),
      ASSERT(           "/* [R1]==[1]      */  load(R1) == load(1)" ),
      ASSERT(           "/* R2=sumsqr(A)   */  mrsumsqr( R2, 20, 27 ) == load(1)" ),
      ASSERT(           "/* [R2]==[1]      */  load(R2) == load(1)" ),

      // minvsum // mrinvsum
      ASSERT(           "/* R1=invsum(A)   */  minvsum( R1, 10, 17 ) == load(2)" ),
      ASSERT(           "/* [R1]==[2]      */  load(R1) == load(2)" ),
      ASSERT(           "/* R2=invsum(A)   */  mrinvsum( R2, 20, 27 ) == load(2)" ),
      ASSERT(           "/* [R2]==[2]      */  load(R2) == load(2)" ),

      // mprod / mrprod
      ASSERT(           "/* R1=prod(A)     */  mprod( R1, 10, 17 ) == load(3)" ),
      ASSERT(           "/* [R1]==[3]      */  load(R1) == load(3)" ),
      ASSERT(           "/* R2=prod(A)     */  mrprod( R2, 20, 27 ) == load(3)" ),
      ASSERT(           "/* [R2]==[3]      */  load(R2) == load(3)" ),

      // mmean / mrmean
      ASSERT(           "/* R1=mean(A)     */  mmean( R1, 10, 17 ) == load(4)" ),
      ASSERT(           "/* [R1]==[4]      */  load(R1) == load(4)" ),
      ASSERT(           "/* R2=mean(A)     */  mrmean( R2, 20, 27 ) == load(4)" ),
      ASSERT(           "/* [R2]==[4]      */  load(R2) == load(4)" ),

      // mharmmean / mrharmmean
      ASSERT(           "/* R1=harmmean(A) */  mharmmean( R1, 10, 17 ) == load(5)" ),
      ASSERT(           "/* [R1]==[5]      */  load(R1) == load(5)" ),
      ASSERT(           "/* R2=harmmean(A) */  mrharmmean( R2, 20, 27 ) == load(5)" ),
      ASSERT(           "/* [R2]==[5]      */  load(R2) == load(5)" ),

      // mgeomean / mrgeomean
      ASSERT(           "/* R1=mgeomean(A) */  mgeomean( R1, 10, 17 ) == load(6)" ),
      ASSERT(           "/* [R1]==[6]      */  load(R1) == load(6)" ),
      ASSERT(           "/* R2=mgeomean(A) */  mrgeomean( R2, 20, 27 ) == load(6)" ),
      ASSERT(           "/* [R2]==[6]      */  load(R2) == load(6)" ),

      // mstdev / mrsdtev
      ASSERT(           "/* R1=stdev(A)    */  mstdev( R1, 10, 17 ) == load(7)" ),
      ASSERT(           "/* [R1]==[7]      */  load(R1) == load(7)" ),
      ASSERT(           "/* R2=stdev(A)    */  mrstdev( R2, 20, 27 ) == load(7)" ),
      ASSERT(           "/* [R2]==[7]      */  load(R2) == load(7)" ),

      // mgeostdev / mrgeostdev
      ASSERT(           "/* R1=geostdev(A) */  mgeostdev( R1, 10, 17 ) == load(8)" ),
      ASSERT(           "/* [R1]==[8]      */  load(R1) == load(8)" ),
      ASSERT(           "/* R2=geostdev(A) */  mrgeostdev( R2, 20, 27 ) == load(8)" ),
      ASSERT(           "/* [R2]==[8]      */  load(R2) == load(8)" ),

      // (Setup for probing)
      INTEGER( 8,       "/* A           */        write( 1,  -100, -pi, -1, 0, 1, 100, 1, 100 )" ),

      // mmax / mmin
      ASSERT(           "/* max(A) at 6 */     mmax( R1, 1, 8 ) == 6 " ),
      ASSERT(           "/* [R1]==100   */     load(R1) == 100" ),
      ASSERT(           "/* min(A) at 1 */     mmin( R1, 1, 8 ) == 1 " ),
      ASSERT(           "/* [R1]==-100  */     load(R1) == -100" ),

      // mcontains
      ASSERT(           "/* -100 in A   */     mcontains( 1, 8, -100 ) " ),
      ASSERT(           "/* -pi in A    */     mcontains( 1, 8, -pi ) " ),
      ASSERT(           "/* -1 in A     */     mcontains( 1, 8, -1 ) " ),
      ASSERT(           "/* 0 in A      */     mcontains( 1, 8, 0 ) " ),
      ASSERT(           "/* 1 in A      */     mcontains( 1, 8, 1 ) " ),
      ASSERT(           "/* 100 in A    */     mcontains( 1, 8, 100 ) " ),
      UNTRUE(           "/* pi not in A */     mcontains( 1, 8, pi ) " ),

      // mcount
      ASSERT(           "/* -100 (1)    */     mcount( 1, 8, -100 ) == 1 " ),
      ASSERT(           "/* -pi (1)     */     mcount( 1, 8, -pi ) == 1 " ),
      ASSERT(           "/* -1 (1)      */     mcount( 1, 8, -1 ) == 1 " ),
      ASSERT(           "/* 0 (1)       */     mcount( 1, 8, 0 ) == 1 " ),
      ASSERT(           "/* 1 (2)       */     mcount( 1, 8, 1 ) == 2 " ),
      ASSERT(           "/* 100 (2)     */     mcount( 1, 8, 100 ) == 2 " ),
      ASSERT(           "/* pi (0)      */     mcount( 1, 8, pi ) == 0 " ),

      // mindex
      ASSERT(           "/* -100 at 1   */     mindex( 1, 8, -100 ) == 1 " ),
      ASSERT(           "/* -pi at 2    */     mindex( 1, 8, -pi ) == 2 " ),
      ASSERT(           "/* -1 at 3     */     mindex( 1, 8, -1 ) == 3 " ),
      ASSERT(           "/* 0 at 4      */     mindex( 1, 8, 0 ) == 4 " ),
      ASSERT(           "/* 1 at 5      */     mindex( 1, 8, 1 ) == 5 " ),
      ASSERT(           "/* 100 at 6    */     mindex( 1, 8, 100 ) == 6 " ),
      ASSERT(           "/* pi not found*/     mindex( 1, 8, pi ) == -1 " ),

      // (Setup for subset)
      INTEGER( 3,       "/* A           */        write( 0,          11, 22, 33 )" ),
      INTEGER( 7,       "/* B           */        write( 10,  9, 10, 11, 22, 33, 34, 35 )" ),
      INTEGER( 4,       "/* C           */        write( 20,  9, 10, 11, 22  )" ),

      // msubset
      ASSERT(           "/* A in B      */     msubset( 0, 2, 10, 16 )" ),
      UNTRUE(           "/* B not in A  */     msubset( 10, 16, 0, 2 )" ),
      UNTRUE(           "/* A not in C  */     msubset( 0, 2, 20, 23 )" ),
      UNTRUE(           "/* C not in A  */     msubset( 20, 23, 0, 2 )" ),
      ASSERT(           "/* A in A      */     msubset( 0, 2, 0, 2 )" ),
      ASSERT(           "/* B in B      */     msubset( 10, 16, 10, 16 )" ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, memory_1k_array, 0.0, evalmem_1k_array, NULL ) >= 0,   "Memory 1kB Array" );
    iEvaluator.DiscardMemory( &evalmem_1k_array );




    QWORD altarray[] = {
      (1111ULL << 32) | 11, 
      (2222ULL << 32) | 22, 
      (3333ULL << 32) | 33, 
      (4444ULL << 32) | 33
    };
    const int64_t sz = qwsizeof( altarray );
    vgx_VertexProperty_t *altarray_prop = iVertexProperty.NewIntArray( graph, "altarray", sz, altarray ); 
    vgx_Vertex_t *V = arc->head.vertex;
    CALLABLE( V )->SetProperty( V, altarray_prop );

    vgx_ExpressEvalMemory_t *evalmem_probes = iEvaluator.NewMemory( 10 );
    __test memory_probes[] = {
      // Probe
      INTEGER( 100,      "store( 1, 100 )" ),
      INTEGER( 200,      "store( 2, 200 )" ),
      INTEGER( 300,      "store( 3, 300 )" ),
      INTEGER( 400,      "store( 4, 400 )" ),
      INTEGER( 500,      "store( 5, 500 )" ),
      INTEGER( 600,      "store( 6, 600 )" ),

      // Set 1 
      INTEGER( 300,      "store( 10, 300 )" ),
      INTEGER( 100,      "store( 11, 100 )" ),
      INTEGER( 400,      "store( 12, 400 )" ),
      INTEGER( 200,      "store( 13, 200 )" ),
      INTEGER( 500,      "store( 14, 500 )" ),

      // Set 2 
      INTEGER( 9300,     "store( 20, 9300 )" ),
      INTEGER( 9100,     "store( 21, 9100 )" ),
      INTEGER( 9400,     "store( 22, 9400 )" ),
      INTEGER( 9200,     "store( 23, 9200 )" ),
      INTEGER( 9500,     "store( 24, 9500 )" ),
      INTEGER( 9600,     "store( 25, 9600 )" ),

      // Set 3
      REAL(    301.1,    "store( 30, 301.1 )" ),
      INTEGER( 100,      "store( 31, 100 )" ),
      INTEGER( 400,      "store( 32, 400 )" ),
      INTEGER( 200,      "store( 33, 200 )" ),
      REAL(    499.9,    "store( 34, 499.9 )" ),


      // Test
      ASSERT(     "mcmp( 11, 31, 1 ) == 0" ),
      ASSERT(     "mcmp( 11, 31, 2 ) == 0" ),
      ASSERT(     "mcmp( 11, 31, 3 ) == 0" ),
      ASSERT(     "mcmp( 11, 31, 4 ) > 0" ),
      ASSERT(     "mcmp( 12, 32, 3 ) > 0" ),
      ASSERT(     "mcmp( 13, 33, 2 ) > 0" ),
      ASSERT(     "mcmp( 14, 34, 1 ) > 0" ),
      ASSERT(     "mcmp( 14, 34, 0 ) == 0" ),
      ASSERT(     "mcmp( 10, 30, 0 ) == 0" ),
      ASSERT(     "mcmp( 10, 30, 1 ) < 0" ),
      ASSERT(     "mcmp( 10, 30, 2 ) < 0" ),
      ASSERT(     "mcmp( 10, 30, 3 ) < 0" ),
      ASSERT(     "mcmp( 10, 30, 4 ) < 0" ),
      ASSERT(     "mcmp( 10, 30, 5 ) < 0" ),
      ASSERT(     "mcmp( 30, 30, 0 ) == 0" ),
      ASSERT(     "mcmp( 30, 30, 1 ) == 0" ),
      ASSERT(     "mcmp( 30, 30, 2 ) == 0" ),
      ASSERT(     "mcmp( 30, 30, 3 ) == 0" ),
      ASSERT(     "mcmp( 30, 30, 4 ) == 0" ),
      ASSERT(     "mcmp( 30, 30, 5 ) == 0" ),


      // Test
      ASSERT(     "msubset( 1, 1, 10, 14 )" ),
      ASSERT(     "msubset( 1, 2, 10, 14 )" ),
      ASSERT(     "msubset( 2, 2, 10, 14 )" ),
      ASSERT(     "msubset( 1, 3, 10, 14 )" ),
      ASSERT(     "msubset( 1, 4, 10, 14 )" ),
      ASSERT(     "msubset( 1, 5, 10, 14 )" ),
      UNTRUE(     "msubset( 1, 6, 10, 14 )" ),
      UNTRUE(     "msubset( 2, 6, 10, 14 )" ),
      UNTRUE(     "msubset( 3, 6, 10, 14 )" ),
      UNTRUE(     "msubset( 4, 6, 10, 14 )" ),
      UNTRUE(     "msubset( 5, 6, 10, 14 )" ),
      UNTRUE(     "msubset( 6, 6, 10, 14 )" ),

      // Test
      UNTRUE(     "msubset( 1, 1, 20, 25 )" ),
      UNTRUE(     "msubset( 2, 2, 20, 25 )" ),
      UNTRUE(     "msubset( 3, 3, 20, 25 )" ),
      UNTRUE(     "msubset( 4, 4, 20, 25 )" ),
      UNTRUE(     "msubset( 5, 5, 20, 25 )" ),
      UNTRUE(     "msubset( 1, 5, 20, 25 )" ),

      // TODO: msubsetobj


      // TODO: msumprodobj



      /*  next['altarray']: [ (1111ULL << 32) | 11, 
                              (2222ULL << 32) | 22, 
                              (3333ULL << 32) | 33, 
                              (4444ULL << 32) | 33  ]
      */

      // Altarray probe
      INTEGER( 1111ULL<<32,     "store( 1, 1111 << 32 )"  ),
      INTEGER( 2222ULL<<32,     "store( 2, 2222 << 32 )"  ),
      INTEGER( 3333ULL<<32,     "store( 3, 3333 << 32 )"  ),
      INTEGER( 4444ULL<<32,     "store( 4, 4444 << 32 )"  ),
      INTEGER( 5555ULL<<32,     "store( 5, 5555 << 32 )"  ),
      // Prefix match, primary regions
      REAL( 100.0,              "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )  // 1"  ),
      REAL( 100.0,              "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )  // 2"  ),
      REAL( 100.0,              "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )  // 3"  ),
      REAL( 100.0,              "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )  // 4"  ),
      REAL( 0.0,                "probesuperarray( 1, 5, next['altarray'], 100.0, 0.5 )  // 5"  ), // miss
      // Infix match, primary regions
      REAL( 1.0,                "probesuperarray( 2, 2, next['altarray'], 100.0, 0.5 )  // 6"  ),
      REAL( 1.0,                "probesuperarray( 2, 3, next['altarray'], 100.0, 0.5 )  // 7"  ),
      REAL( 1.0,                "probesuperarray( 2, 4, next['altarray'], 100.0, 0.5 )  // 8"  ),
      REAL( 0.0,                "probesuperarray( 2, 5, next['altarray'], 100.0, 0.5 )  // 9"  ), // miss
      REAL( 1.0,                "probesuperarray( 3, 3, next['altarray'], 100.0, 0.5 )  // 10"  ),
      REAL( 1.0,                "probesuperarray( 3, 4, next['altarray'], 100.0, 0.5 )  // 11"  ),
      REAL( 0.0,                "probesuperarray( 3, 5, next['altarray'], 100.0, 0.5 )  // 12"  ), // miss
      REAL( 1.0,                "probesuperarray( 4, 4, next['altarray'], 100.0, 0.5 )  // 13"  ),
      REAL( 0.0,                "probesuperarray( 4, 5, next['altarray'], 100.0, 0.5 )  // 14"  ), // miss
      REAL( 0.0,                "probesuperarray( 5, 5, next['altarray'], 100.0, 0.5 )  // 15"  ), // miss

      // Altarray probe
      INTEGER( 11,              "store( 1, 11 )"  ),
      INTEGER( 22,              "store( 2, 22 )"  ),
      INTEGER( 33,              "store( 3, 33 )"  ),
      INTEGER( 33,              "store( 4, 33 )"  ),
      INTEGER( 33,              "store( 5, 33 )"  ),
      // Prefix match, secondary regions
      REAL( 50.0,                "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )   // 16"  ),
      REAL( 50.0,                "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )   // 17"  ),
      REAL( 50.0,                "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )   // 18"  ),
      REAL( 50.0,                "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )   // 19"  ),
      REAL( 0.0,                 "probesuperarray( 1, 5, next['altarray'], 100.0, 0.5 )   // 20"  ), // miss (too many 33)
      // Infix match, secondary regions
      REAL( 0.5,                 "probesuperarray( 2, 2, next['altarray'], 100.0, 0.5 )   // 21"  ),
      REAL( 0.5,                 "probesuperarray( 2, 3, next['altarray'], 100.0, 0.5 )   // 22"  ),
      REAL( 0.5,                 "probesuperarray( 2, 4, next['altarray'], 100.0, 0.5 )   // 23"  ),
      REAL( 0.0,                 "probesuperarray( 2, 5, next['altarray'], 100.0, 0.5 )   // 24"  ), // miss (too many 33)
      REAL( 0.5,                 "probesuperarray( 3, 3, next['altarray'], 100.0, 0.5 )   // 25"  ),
      REAL( 0.5,                 "probesuperarray( 3, 4, next['altarray'], 100.0, 0.5 )   // 26"  ),
      REAL( 0.0,                 "probesuperarray( 3, 5, next['altarray'], 100.0, 0.5 )   // 27"  ), // miss (too many 33)
      REAL( 0.5,                 "probesuperarray( 4, 4, next['altarray'], 100.0, 0.5 )   // 28"  ),
      REAL( 0.5,                 "probesuperarray( 4, 5, next['altarray'], 100.0, 0.5 )   // 29"  ), // hit (two 33 ok)
      REAL( 0.5,                 "probesuperarray( 5, 5, next['altarray'], 100.0, 0.5 )   // 30"  ), // hit (one 33 ok)

      // Altarray probe
      INTEGER( 1111ULL<<32,      "store( 1, 1111 << 32 )"  ),
      INTEGER( 2222ULL<<32,      "store( 2, 2222 << 32 )"  ),
      INTEGER( 3333ULL<<32,      "store( 3, 3333 << 32 )"  ),
      INTEGER( (3333ULL<<32)+33, "store( 4, (3333 << 32)+33 )"  ),
      // Prefix match, primary regions
      REAL( 100.0,              "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 31"  ),
      REAL( 100.0,              "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )    // 32"  ),
      REAL( 100.0,              "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )    // 33"  ),
      // Prefix match, one region secondary
      REAL( 50.0,               "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )    // 34"  ),


      // Altarray bitvector probe
      ASSERT(                   "store( 1, bitvector( 0x000F0000 ) ) == 0x000F0000"    ), // assert all positions primary
      REAL( 100.0,              "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 35"  ),

      // Altarray bitvector probe
      ASSERT(                   "store( 1, bitvector( 0x0000000F ) ) == 0x0000000F"    ), // assert all positions secondary
      REAL( 50.0,               "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 36"  ),
      
      // Altarray bitvector probes
      ASSERT(                   "store( 1, bitvector( 0x00010000 ) ) == 0x00010000"    ),
      ASSERT(                   "store( 2, bitvector( 0x00020000 ) ) == 0x00020000"    ),
      ASSERT(                   "store( 3, bitvector( 0x00040000 ) ) == 0x00040000"    ),
      ASSERT(                   "store( 4, bitvector( 0x00080000 ) ) == 0x00080000"    ),
      ASSERT(                   "store( 4, bitvector( 0x000F0000 ) ) == 0x000F0000"    ), // too much
      REAL( 100.0,              "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 37"  ),
      REAL( 100.0,              "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )    // 38"  ),
      REAL( 100.0,              "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )    // 39"  ),
      REAL( 100.0,              "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )    // 40"  ),
      REAL( 0.0,                "probesuperarray( 1, 5, next['altarray'], 100.0, 0.5 )    // 41"  ), // too much

      // Altarray bitvector probes out of order
      ASSERT(                   "store( 1, bitvector( 0x00080000 ) ) == 0x00080000"    ),
      ASSERT(                   "store( 2, bitvector( 0x00040000 ) ) == 0x00040000"    ),
      ASSERT(                   "store( 3, bitvector( 0x00020000 ) ) == 0x00020000"    ),
      ASSERT(                   "store( 4, bitvector( 0x00010000 ) ) == 0x00010000"    ),
      REAL( 100.0,              "probesuperarray( 4, 4, next['altarray'], 100.0, 0.5 )    // 42"  ), // single (last)
      REAL( 1.0,                "probesuperarray( 3, 4, next['altarray'], 100.0, 0.5 )    // 43"  ),
      REAL( 1.0,                "probesuperarray( 2, 4, next['altarray'], 100.0, 0.5 )    // 44"  ),
      REAL( 1.0,                "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )    // 45"  ),

      // Altarray bitvector probes out of order, with secondary
      ASSERT(                   "store( 1, bitvector( 0x00000008 ) ) == 0x00000008"    ),
      ASSERT(                   "store( 2, bitvector( 0x00040000 ) ) == 0x00040000"    ),
      ASSERT(                   "store( 3, bitvector( 0x00020000 ) ) == 0x00020000"    ),
      ASSERT(                   "store( 4, bitvector( 0x00010000 ) ) == 0x00010000"    ),
      REAL( 100.0,              "probesuperarray( 4, 4, next['altarray'], 100.0, 0.5 )    // 46"  ), // single (last)
      REAL( 1.0,                "probesuperarray( 3, 4, next['altarray'], 100.0, 0.5 )    // 47"  ),
      REAL( 1.0,                "probesuperarray( 2, 4, next['altarray'], 100.0, 0.5 )    // 48"  ),
      REAL( 0.5,                "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )    // 49"  ), // secondary match

      // Altarray mixed
      INTEGER( (2222ULL<<32),   "store( 1, 2222 << 32)"                                ),
      INTEGER( 33,              "store( 2, 33 )"                                       ),
      ASSERT(                   "store( 3, bitvector( 0x00080004 ) ) == 0x00080004"    ), // pos 3 (sec) and 4 (pri)

      REAL( 1.0,                "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 50"  ), // infix on 2222
      REAL( 0.5,                "probesuperarray( 2, 2, next['altarray'], 100.0, 0.5 )    // 51"  ), // infix on 33
      REAL( 1.0,                "probesuperarray( 3, 3, next['altarray'], 100.0, 0.5 )    // 52"  ), // infix bitvector on 3333
      REAL( 0.5,                "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )    // 53"  ), // infix on 2222 and 33
      REAL( 0.5,                "probesuperarray( 2, 3, next['altarray'], 100.0, 0.5 )    // 54"  ), // infix on 33 and bitvector on 3333
      REAL( 0.5,                "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )    // 55"  ), // infix on 2222, 33, and bitvector on 3333

      // Altarray mixed
      INTEGER( (1111ULL<<32)+11,"store( 1, (1111 << 32)+11)"                           ), // pos 0 1111
      INTEGER( (7777ULL<<32)+22,"store( 2, (7777 << 32)+22)"                           ), // pos 1 22
      ASSERT(                   "store( 3, bitvector( 0x000C0000 ) ) == 0x000C0000"    ), // assert pos 3 (lsb) primary
      ASSERT(                   "store( 4, bitvector( 0x0000000C ) ) == 0x0000000C"    ), // assert pos 4 (lsb) primary
      INTEGER( (3333ULL<<32)+33,"store( 5, (3333 << 32)+33)"                           ), // positions used up, no match

      REAL( 100.0,              "probesuperarray( 1, 1, next['altarray'], 100.0, 0.5 )    // 56"  ), // prefix on 1111
      REAL( 50.0,               "probesuperarray( 1, 2, next['altarray'], 100.0, 0.5 )    // 57"  ), // prefix on 22, secondary
      REAL( 50.0,               "probesuperarray( 1, 3, next['altarray'], 100.0, 0.5 )    // 58"  ), // prefix on bitvector for pos 3=3333, primary
      REAL( 50.0,               "probesuperarray( 1, 4, next['altarray'], 100.0, 0.5 )    // 59"  ), // prefix on bitvector for pos 4=4444, secondary
      REAL( 0.0,                "probesuperarray( 1, 5, next['altarray'], 100.0, 0.5 )    // 60"  ), // all positions used up

      REAL( 0.5,                "probesuperarray( 2, 2, next['altarray'], 100.0, 0.5 )    // 61"  ), // infix on 22, secondary
      REAL( 0.5,                "probesuperarray( 2, 3, next['altarray'], 100.0, 0.5 )    // 62"  ), // infix on bitvector for pos 3=3333, primary
      REAL( 0.5,                "probesuperarray( 2, 4, next['altarray'], 100.0, 0.5 )    // 63"  ), // infix on bitvector for pos 4=4444, secondary
      REAL( 0.0,                "probesuperarray( 2, 5, next['altarray'], 100.0, 0.5 )    // 64"  ), // all positions for matching this used up

      REAL( 1.0,                "probesuperarray( 3, 3, next['altarray'], 100.0, 0.5 )    // 65"  ), // infix on bitvector for pos 3=3333, primary
      REAL( 0.5,                "probesuperarray( 3, 4, next['altarray'], 100.0, 0.5 )    // 66"  ), // infix on bitvector for pos 4=4444, secondary
      REAL( 0.0,                "probesuperarray( 3, 5, next['altarray'], 100.0, 0.5 )    // 67"  ), // all positions for matching this used up

      REAL( 0.5,                "probesuperarray( 4, 4, next['altarray'], 100.0, 0.5 )    // 68"  ), // infix on bitvector for pos 4=4444, secondary
      REAL( 0.5,                "probesuperarray( 4, 5, next['altarray'], 100.0, 0.5 )    // 69"  ), // infix match on pos 3=3333

      REAL( 1.0,                "probesuperarray( 5, 5, next['altarray'], 100.0, 0.5 )    // 70"  ), // infix match on pos 3=3333

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, memory_probes, 0.0, evalmem_probes, NULL ) >= 0,   "Memory Probes" );
    iEvaluator.DiscardMemory( &evalmem_probes );

    CALLABLE( V )->RemoveProperty( V, altarray_prop );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Random
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Random" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test random[] = {

      ASSERT(    "isreal( random() )"          ),
      ASSERT(    "random() != random()"        ),
      ASSERT(    "random() >= 0.0"             ),
      ASSERT(    "random() <= 1.0"             ),

      ASSERT(    "isint( randint( 0, 100 ) )"  ),
      ASSERT(    "(randint( -10, 11 ) + 10) / 20.0 <= 1.0"   ),
      ASSERT(    "(randint( 100, 1101 ) - 100) / 1000.0 <= 1.0"   ),

      ASSERT(    "isbitvector( randbits() )"   ),
      ASSERT(    "randbits() != randbits()"    ),

      ASSERT(    "hash( random()  ) != hash( random()  )"),
      ASSERT(    "hash( randint(-0x7fffffffffffffff,+0x7fffffffffffffff) ) != hash( randint(-0x7fffffffffffffff,+0x7fffffffffffffff) )"),
      ASSERT(    "hash( 10000 ) == hash( 10000 )" ),
      ASSERT(    "hash( -10000 ) == hash( -10000 )" ),
      ASSERT(    "hash( 10000 ) != hash( -10000 )" ),
      ASSERT(    "hash( 'hello' ) == hash( 'hello' )" ),
      ASSERT(    "hash( 'hello ' ) != hash( 'hello' )" ),
      ASSERT(    "hash( ' hello' ) != hash( 'hello' )" ),
      ASSERT(    "hash( ' hello ' ) != hash( 'hello' )" ),
      ASSERT(    "hash( '' ) != hash( ' ' )" ),
      {0}
    };
    for( int n = 0; n < 10000; n++ ) {
      TEST_ASSERTION( __test_expressions( graph, random, 0.0, NULL, NULL ) >= 0,   "Random" );
    }
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Rank
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Rank" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    vgx_Vertex_t *head = arc->head.vertex;

    // Recall: B.rank.c1 = 2.0, B.rank.c0 = 10.0
    __test rank[] = {
      REAL( vgx_RankGetC0( &head->rank ),                                       "rank()"          ),
      REAL( vgx_RankGetC0( &head->rank ),                                       "rank(0)"         ),
      REAL( vgx_RankGetC0( &head->rank ),                                       "rank(0,0)"       ),
      REAL( vgx_RankGetC0( &head->rank ) + 1.0 * vgx_RankGetC1( &head->rank ),  "rank(1)"         ),
      REAL( vgx_RankGetC0( &head->rank ) + 1.5 * vgx_RankGetC1( &head->rank ),  "rank(1.5)"       ),
      REAL( vgx_RankGetC0( &head->rank ) + 2.5 * vgx_RankGetC1( &head->rank ),  "rank(1.5,1)"     ),
      REAL( vgx_RankGetC0( &head->rank ) + 2.5 * vgx_RankGetC1( &head->rank ),  "rank(1,1.5)"     ),
      REAL( vgx_RankGetC0( &head->rank ) + 9.8 * vgx_RankGetC1( &head->rank ),  "rank(1.3,1.5,7)" ),
      /*
      TODO:
      static void __eval_variadic_rank( vgx_Evaluator_t *self );
      */
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, rank, 0.0, NULL, NULL ) >= 0,   "Rank" );
    

    // Boston
    vgx_RankSetLatLon( &head->rank, 42.3601f, 71.0589f );

    // Sydney
    vgx_RankSetLatLon( &arc->tail->rank, -33.8688f, -151.2093f );

    __test georank[] = {
      //             BOSTON -    T O K Y O           BOSTON -  SAN FRANCISCO
      ASSERT(        "georank( 35.6762, -139.6503 ) < georank( 37.7749, 122.4194 )"    ),  // Tokyo farther from Boston (lower rank) than San Francisco from Boston (higher rank)
      //             BOSTON -   SAN FRANCISCO       BOSTON -  N E W  Y O R K
      ASSERT(        "georank( 37.7749, 122.4194 ) < georank( 40.7128, 74.0060 )"      ),  // San Francisco farther from Boston (lower rank) then New York from Boston (higher rank)
      //             BOSTON - SYDNEY     BOSTON -      T O K Y O
      ASSERT(        "georank( vertex ) < georank( 35.6762, -139.6503 )"),                 // Sydney-Boston is farther (lower rank) than Sydney-Tokyo (higher rank)
      //             BOSTON - SYDNEY     BOSTON -   M E L B O U R N E
      ASSERT(        "georank( vertex ) > georank( -37.8136, -144.9631 )"),                // Boston-Sydney is closer (higher rank) than Boston-Melbourne (lower rank)
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, georank, 0.0, NULL, NULL ) >= 0,   "Georank" );




  } END_TEST_SCENARIO


    
  /*******************************************************************//**
   * Stack
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Stack" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test stack[] = {
      INTEGER( 1,     "(1)"        ),
      INTEGER( 2,     "(2,1)"      ),
      INTEGER( 3,     "(3,2,1)"    ),
      INTEGER( 4,     "(4,3,2,1)"  ),

      ASSERT(         "(4,3,2,1) == 4"  ),

      ASSERT(         "(  'hello '  ,  vertex,  'hi'  ,  next.id  )  ==  'hello ' "  ),
      ASSERT(         "(  ' hello'  ,  vertex,  'hi'  ,  next.id  )  ==  ' hello' "  ),
      ASSERT(         "(  vertex,  'hi'  ,  next.id  )               ==  vertex "    ),
      ASSERT(         "('hi','hello',next.id)                        ==  'hi'"       ),
      ASSERT(         "(  7,'hello'  )                               ==  7 "         ),
      ASSERT(         "(  7.1,'hello'  )                             ==  7.1 "       ),
      ASSERT(         "(  7.1e10,'hello'  )                          ==  7.1e10 "    ),

      SET( 4,         "{'name', 'value', 3.14, vertex}"                              ),
      SET( 4,         "myset4 := {'name', 'value', 3.14, vertex}"                    ),
      INTEGER( 1,     "mytest1 := 'value' in {'name', 'value', 3.14, vertex}"        ),
      INTEGER( 1,     "mytest0 := 'nix' notin {'name', 'value', 3.14, vertex}"       ),

      SET( 4,         "myset4"  ),
      INTEGER( 1,     "mytest1" ),
      INTEGER( 1,     "mytest0" ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, stack, 0.0, NULL, NULL ) >= 0,   "Stack" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Euclidean Vector
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Euclidean Vector" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    float tail_mag = CALLABLE( arc->tail->vector )->Magnitude( arc->tail->vector );
    float head_mag = CALLABLE( arc->head.vertex->vector )->Magnitude( arc->head.vertex->vector );

    // Vectors should not match with a high sim threshold
    graph->similarity->params.threshold.similarity = 0.99f;
    graph->similarity->params.threshold.hamming = 1;
    __test vector_miss[] = {
      // ROOT vector
      ASSERT(         "vertex.vector != 0"               ),
      INTEGER( 32,    "asint( vertex.vector )"           ),
      REAL( tail_mag, "asreal( vertex.vector )"          ),
      // HEAD vector
      ASSERT(         "next.vector != 0"                 ),
      INTEGER( 32,    "asint( next.vector )"             ),
      REAL( head_mag, "asreal( next.vector )"            ),
      // SIM
      ASSERT(         "vertex.vector != next.vector"     ),
      ASSERT(         "next.vector   != vertex.vector"   ),
      UNTRUE(         "vertex.vector == next.vector"     ),
      UNTRUE(         "next.vector   == vertex.vector"   ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vector_miss, 0.0, NULL, NULL ) >= 0,   "Vertex Vectors Miss" );

    // Vectors should match with a lower sim threshold
    graph->similarity->params.threshold.similarity = 0.6f;
    __test vector_match[] = {
      ASSERT(    "len( vertex.vector ) == 32"      ),
      ASSERT(    "len( next.vector ) == 32"        ),
      ASSERT(    "vertex.vector == next.vector"    ),
      ASSERT(    "next.vector   == vertex.vector"  ),
      UNTRUE(    "vertex.vector != next.vector"    ),
      UNTRUE(    "next.vector   != vertex.vector"  ),
      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vector_match, 0.0, NULL, NULL ) >= 0,   "Vertex Vectors Match" );

  } END_TEST_SCENARIO




  /*******************************************************************//**
   * Vertex
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Vertex" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;

    vgx_Vertex_t *ROOT = arc->tail;
    vgx_Vertex_t *A = ARC_ROOT_to_A.head.vertex;
    vgx_Vertex_t *B = ARC_ROOT_to_B.head.vertex;
    vgx_Vertex_t *C = ARC_ROOT_to_C.head.vertex;
    vgx_Vertex_t *D = ARC_ROOT_rel_D.head.vertex;

    CString_t *CSTR__tail_addr_ROOT  = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "vertex == %lld", (uint64_t)ROOT );
    CString_t *CSTR__tail_addr_A = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "vertex == %lld", (uint64_t)A );

    CString_t *CSTR__head_addr_B  = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "next == %lld", (uint64_t)B );
    CString_t *CSTR__head_addr_C = CStringNewFormatAlloc( graph->ephemeral_string_allocator_context, "next == %lld", (uint64_t)C );

    // High sim threshold
    graph->similarity->params.threshold.similarity = 0.99f;
    graph->similarity->params.threshold.hamming = 1;

    __test vertex[] = {
      // Address
      nolineINTEGER( true,   CStringValue( CSTR__tail_addr_ROOT ) ),
      nolineINTEGER( false,  CStringValue( CSTR__tail_addr_A )    ),
      nolineINTEGER( true,   CStringValue( CSTR__head_addr_B )    ),
      nolineINTEGER( false,  CStringValue( CSTR__head_addr_C )    ),

      // ID
      ASSERT(   "vertex.id == 'ROOT'"     ),
      ASSERT(   "vertex.id == 'R*'"       ),
      ASSERT(   "vertex.id == 'RO*'"      ),
      ASSERT(   "vertex.id == 'ROO*'"     ),
      ASSERT(   "vertex.id == 'ROOT*'"    ),
      ASSERT(   "vertex.id != 'ROOTx*'"   ),
      ASSERT(   "vertex.id != 'A'"     ),
      ASSERT(   "vertex.id != 'A*'"    ),
      ASSERT(   "vertex.id != 'B'"     ),
      ASSERT(   "vertex.id != 'B*'"    ),
      ASSERT(   "vertex.id != 'C'"     ),
      ASSERT(   "vertex.id != 'C*'"    ),
      ASSERT(   "vertex.id != 'D'"     ),
      ASSERT(   "vertex.id != 'D*'"    ),

      ASSERT(   "next.id == 'B'"     ),
      ASSERT(   "next.id == 'B*'"    ),
      ASSERT(   "next.id != 'Bx*'"   ),
      ASSERT(   "next.id != 'ROOT'"  ),
      ASSERT(   "next.id != 'R*'"    ),
      ASSERT(   "vertex.id != 'A'"     ),
      ASSERT(   "vertex.id != 'A*'"    ),
      ASSERT(   "vertex.id != 'C'"     ),
      ASSERT(   "vertex.id != 'C*'"    ),
      ASSERT(   "vertex.id != 'D'"     ),
      ASSERT(   "vertex.id != 'D*'"    ),

      ASSERT(   "next.arc.type == 'to'  && next.id == 'B'" ),
      UNTRUE(   "next.arc.type == 'to'  && next.id == 'C'" ),
      UNTRUE(   "next.arc.type == 'rel' && next.id == 'B'" ),
      UNTRUE(   "next.arc.type == 'rel' && next.id == 'C'" ),

      ASSERT(   "next.arc.type in { 'to' } && next.id == 'B'" ),
      ASSERT(   "next.id == 'B' && next.arc.type in { 'to' }" ),
      ASSERT(   "next.arc.type in { 'to' } && next.id in { 'B' }" ),
      ASSERT(   "next.id in { 'B' } && next.arc.type in { 'to' }" ),

      UNTRUE(   "next.arc.type in { 'rel' } && next.id == 'B'" ),
      UNTRUE(   "next.id == 'B' && next.arc.type in { 'rel' }" ),
      UNTRUE(   "next.arc.type in { 'rel' } && next.id in { 'B' }" ),
      UNTRUE(   "next.id in { 'B' } && next.arc.type in { 'rel' }" ),

      ASSERT(   "next.arc.type in { 'to', 'rel' } && next.id in { 'C', 'B' }" ),
      ASSERT(   "next.id in { 'B', 'C' } && next.arc.type in { 'rel', 'to' }" ),

      // Internalid
      ASSERT(   "next.internalid == next.internalid" ),
      ASSERT(   "next.internalid == next"            ),
      ASSERT(   "next == next.internalid"            ),
      ASSERT(   "next == next"                       ),
      ASSERT(   "next != vertex"                     ),

      // Type
      UNTRUE(   "vertex.type == typeenc('node')" ),
      ASSERT(   "vertex.type != typeenc('node')" ),
      ASSERT(   "vertex.type == typeenc('root')" ),
      ASSERT(   "typedec( typeenc('node') ) == 'node'" ),
      ASSERT(   "typedec( typeenc('node') ) != 'root'" ),
      ASSERT(   "typedec( typeenc('root') ) == 'root'" ),
      ASSERT(   "typedec( typeenc('root') ) != 'node'" ),
      UNTRUE(   "vertex.type == 'node'"          ),
      ASSERT(   "vertex.type != 'node'"          ),
      ASSERT(   "vertex.type == 'root'"          ),
      UNTRUE(   "'node' == vertex.type"          ),
      ASSERT(   "'node' != vertex.type"          ),
      ASSERT(   "'root' == vertex.type"          ),

      ASSERT(   "vertex.type in { 'root' }" ),
      ASSERT(   "vertex.type notin { 'node' }" ),
      ASSERT(   "vertex.type in { 'thing', 'node', 'root' }" ),
      UNTRUE(   "vertex.type notin { 'thing', 'node', 'root' }" ),
      UNTRUE(   "vertex.type in { 'thing', 'node' }" ),
      ASSERT(   "vertex.type notin { 'thing', 'node' }" ),
      
      ASSERT(   "next.type == typeenc('node')" ),
      UNTRUE(   "next.type != typeenc('node')" ),
      UNTRUE(   "next.type == typeenc('root')" ),
      ASSERT(   "next.type == 'node'"          ),
      UNTRUE(   "next.type != 'node'"          ),
      UNTRUE(   "next.type == 'root'"          ),
      ASSERT(   "'node' == next.type"          ),
      UNTRUE(   "'node' != next.type"          ),
      UNTRUE(   "'root' == next.type"          ),

      ASSERT(   "next.type in { 'node' }" ),
      ASSERT(   "next.type notin { 'root' }" ),
      ASSERT(   "next.type in { 'thing', 'node', 'root' }" ),
      UNTRUE(   "next.type notin { 'thing', 'node', 'root' }" ),
      UNTRUE(   "next.type in { 'thing', 'root' }" ),
      ASSERT(   "next.type notin { 'thing', 'root' }" ),

      ASSERT(   "vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'node' }" ),
      UNTRUE(   "vertex.type in { 'thing', 'node' } && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'node' }" ),
      UNTRUE(   "vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'root' }" ),
      UNTRUE(   "vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'rel' } && next.type in { 'stuff', 'node' }" ),
      ASSERT(   "vertex.type in { 'thing', 'root' } || next.arc.type in { 'maybe', 'rel' } || next.type in { 'stuff', 'root' }" ),
      ASSERT(   "vertex.type in { 'thing', 'node' } || next.arc.type in { 'maybe', 'to'  } || next.type in { 'stuff', 'root' }" ),
      ASSERT(   "vertex.type in { 'thing', 'node' } || next.arc.type in { 'maybe', 'rel' } || next.type in { 'stuff', 'node' }" ),
      UNTRUE(   "vertex.type in { 'thing', 'node' } || next.arc.type in { 'maybe', 'rel' } || next.type in { 'stuff', 'root' }" ),

      ASSERT(   "next.id == 'B' && vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'node' }" ),
      ASSERT(   "vertex.type in { 'thing', 'root' } && next.id == 'B' && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'node' }" ),
      ASSERT(   "vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'to'  } && next.id == 'B' && next.type in { 'stuff', 'node' }" ),
      ASSERT(   "vertex.type in { 'thing', 'root' } && next.arc.type in { 'maybe', 'to'  } && next.type in { 'stuff', 'node' } && next.id == 'B'" ),
      
      // Properties
      ASSERT(   "'propR' in vertex"        ),
      UNTRUE(   "'prop1' in vertex"        ),
      UNTRUE(   "'prop2' in vertex"        ),
      UNTRUE(   "'prop3' in vertex"        ),
      UNTRUE(   "'propS' in vertex"        ),
      UNTRUE(   "'nix' in vertex"          ),

      INTEGER( 1,     "vertex.propcount"                 ),
      ASSERT(         "len(vertex) == vertex.propcount"  ),
      INTEGER( 4,     "next.propcount"                   ),
      ASSERT(         "len(next) == next.propcount"      ),

      INTEGER( 0xA,   "vertex.property( 'propR', 0xF )" ),
      INTEGER( 0xF,   "vertex.property( 'nope', 0xF )"  ),
      INTEGER( 0xA,   "vertex.property( 'propR', 0xF, true )" ),
      INTEGER( 0xF,   "vertex.property( 'propR', 0xF, false )" ),
      INTEGER( 0xF,   "vertex.property( 'nope', 0xF, true )" ),
      INTEGER( 0xF,   "vertex.property( 'nope', 0xF, false )" ),

      ASSERT(         "'prop1' in next"        ),
      UNTRUE(         "'nix' in next"          ),
      ASSERT(         "'nix' notin next"       ),
      ASSERT(         "!('nix' in next)"       ),
      ASSERT(         "'nix' notin next"       ),
      ASSERT(         "isnan( next['nix'] )"   ),
      INTEGER( 100,   "firstval( next['nix'], 100 )" ),
      REAL(    3.14,  "firstval( next['nix'], 3.14 )" ),
      ASSERT(   "next['prop1'] == 1"     ),
      ASSERT(   "next['prop1'] != 2"     ),
      ASSERT(   "next['prop2'] == 2"     ),
      ASSERT(   "next['prop2'] != 3"     ),
      ASSERT(   "next['prop3'] == 3"     ),
      ASSERT(   "next['prop3'] != 4"     ),
      ASSERT(   "next['propS'] != ' head property'" ),
      ASSERT(   "next['propS'] != 'head propert'"   ),
      ASSERT(   "next['propS'] != 'ead property'"   ),
      ASSERT(   "next['propS'] == 'head property'"  ),

      // Hit should not be cached for tail
      UNTRUE(  "prev['propS'] == 'head property'"  ),

      // Miss should not be cached for head
      ASSERT(   "next['propS'] == 'head property'"  ),

      // Wildcards
      ASSERT(   "next['propS'] == 'head property*'"  ),
      ASSERT(   "next['propS'] == 'head propert*'"  ),
      ASSERT(   "next['propS'] == 'head proper*'"  ),
      ASSERT(   "next['propS'] == 'head prope*'"  ),
      ASSERT(   "next['propS'] == 'head prop*'"  ),
      ASSERT(   "next['propS'] == 'head pro*'"  ),
      ASSERT(   "next['propS'] == 'head pr*'"  ),
      ASSERT(   "next['propS'] == 'head p*'"  ),
      ASSERT(   "next['propS'] == 'head *'"  ),
      ASSERT(   "next['propS'] == 'head*'"  ),
      ASSERT(   "next['propS'] == 'hea*'"  ),
      ASSERT(   "next['propS'] == 'he*'"  ),
      ASSERT(   "next['propS'] == 'h*'"  ),
      ASSERT(   "next['propS'] == '*'"  ),
      ASSERT(   "'*' == next['propS']"  ),
      ASSERT(   "'he*' == next['propS']"  ),
      ASSERT(   "'head prop*' == next['propS']"  ),
      ASSERT(   "'head property*' == next['propS']"  ),
      ASSERT(   "next['propS'] != 'H*'"  ),
      ASSERT(   "next['propS'] != ' h*'"  ),
      ASSERT(   "next['propS'] != 'e*'"  ),
      ASSERT(   "next['propS'] != 'ead*'"  ),

      ASSERT(   "next['propS'] == '*y'"  ),
      ASSERT(   "next['propS'] == '*ty'"  ),
      ASSERT(   "next['propS'] == '*rty'"  ),
      ASSERT(   "next['propS'] != '*rt'"  ),
      ASSERT(   "next['propS'] != '*t'"  ),
      ASSERT(   "next['propS'] == '*rt*'"  ),
      ASSERT(   "next['propS'] == '*rty*'"  ),
      ASSERT(   "next['propS'] == '*ty*'"  ),
      ASSERT(   "next['propS'] == '*y*'"  ),
      ASSERT(   "next['propS'] == '*ad pr*'"  ),
      ASSERT(   "next['propS'] != '*adpr*'"  ),
      ASSERT(   "next['propS'] == '*ead*'"  ),
      ASSERT(   "next['propS'] == '*head*'"  ),
      ASSERT(   "next['propS'] == 'he*ad property'"  ),
      ASSERT(   "next['propS'] == 'h*ad property'"  ),
      ASSERT(   "next['propS'] == 'h*d property'"  ),
      ASSERT(   "next['propS'] == 'h* property'"  ),
      ASSERT(   "next['propS'] == 'h*property'"  ),
      ASSERT(   "next['propS'] == 'h*roperty'"  ),
      ASSERT(   "next['propS'] == 'h*operty'"  ),
      ASSERT(   "next['propS'] == 'h*perty'"  ),
      ASSERT(   "next['propS'] == 'h*erty'"  ),
      ASSERT(   "next['propS'] == 'h*rty'"  ),
      ASSERT(   "next['propS'] == 'h*ty'"  ),
      ASSERT(   "next['propS'] == 'h*y'"  ),
      ASSERT(   "next['propS'] == 'he*y'"  ),
      ASSERT(   "next['propS'] == 'hea*y'"  ),
      ASSERT(   "next['propS'] == 'head*y'"  ),
      ASSERT(   "next['propS'] == 'head *y'"  ),
      ASSERT(   "next['propS'] == 'head p*y'"  ),
      ASSERT(   "next['propS'] == 'head pr*y'"  ),
      ASSERT(   "next['propS'] == 'head pro*y'"  ),
      ASSERT(   "next['propS'] == 'head prop*y'"  ),
      ASSERT(   "next['propS'] == 'head prope*y'"  ),
      ASSERT(   "next['propS'] == 'head proper*y'"  ),
      ASSERT(   "next['propS'] == 'head propert*y'"  ),
      ASSERT(   "'*y' == next['propS']"  ),
      ASSERT(   "'*y*' == next['propS']"  ),
      ASSERT(   "'*ad pr*' == next['propS']"  ),
      ASSERT(   "'*ad pr' != next['propS']"  ),
      ASSERT(   "'*adpr*' != next['propS']"  ),
      ASSERT(   "'*t' != next['propS']"  ),
      ASSERT(   "'*adpr*' != next['propS']"  ),
      ASSERT(   "'h*y' == next['propS']"  ),

      // Manifestation
      ASSERT(   "vertex.virtual == false"    ),
      ASSERT(   "vertex.virtual != true"     ),
      UNTRUE(   "vertex.virtual != false"    ),
      UNTRUE(   "vertex.virtual == true"     ),

      ASSERT(   "next.virtual == false"      ),
      ASSERT(   "next.virtual != true"       ),
      UNTRUE(   "next.virtual != false"      ),
      UNTRUE(   "next.virtual == true"       ),

      // Degree
      INTEGER( 4,      "vertex.odeg"                ),
      INTEGER( 0,      "vertex.ideg"                ),
      INTEGER( 4,      "vertex.deg"                 ),

      INTEGER( 0,      "next.odeg"                ),
      INTEGER( 1,      "next.ideg"                ),
      INTEGER( 1,      "next.deg"                 ),


      // Time
      INTEGER( arc->tail->TMC,                    "vertex.tmc"  ),
      INTEGER( arc->tail->TMM,                    "vertex.tmm"  ),
      INTEGER( arc->tail->TMX.vertex_ts,          "vertex.tmx"  ),

      INTEGER( arc->head.vertex->TMC,             "next.tmc"  ),
      INTEGER( arc->head.vertex->TMM,             "next.tmm"  ),
      INTEGER( arc->head.vertex->TMX.vertex_ts,   "next.tmx"  ),


      // Rank
      REAL( vgx_RankGetC1( &arc->tail->rank ),    "vertex.c1" ),
      REAL( vgx_RankGetC0( &arc->tail->rank ),    "vertex.c0" ),
      REAL( vgx_RankGetC1( &arc->head.vertex->rank ),  "next.c1" ),
      REAL( vgx_RankGetC0( &arc->head.vertex->rank ),  "next.c0" ),

      // Vector
      ASSERT(    "vertex.vector == vertex.vector"  ),
      ASSERT(    "vertex.vector != next.vector"    ),
      ASSERT(    "next.vector != vertex.vector"    ),
      ASSERT(    "next.vector == next.vector"      ),
      ASSERT(    "hamdist( vertex.vector, next.vector ) < 24 " ),
      ASSERT(    "abs(sim( vertex.vector, next.vector ) - 0.7) < 0.005 " ),
      ASSERT(    "abs(cosine( vertex.vector, next.vector ) - 0.70) < 0.005 " ),


      // Operation
      ASSERT(    "vertex.op > next.op"             ),  // since ROOT was modified after B


      // Refcount
      INTEGER( _cxmalloc_linehead_from_object( arc->tail )->data.refc,          "vertex.refc"   ),
      INTEGER( _cxmalloc_linehead_from_object( arc->head.vertex )->data.refc,   "next.refc"   ),


      // Allocator block index
      INTEGER( _cxmalloc_linehead_from_object( arc->tail )->data.bidx,          "vertex.bidx"   ),
      INTEGER( _cxmalloc_linehead_from_object( arc->head.vertex )->data.bidx,   "next.bidx"   ),
      

      // Allocator block offset
      INTEGER( _cxmalloc_linehead_from_object( arc->tail )->data.offset,        "vertex.oidx"   ),
      INTEGER( _cxmalloc_linehead_from_object( arc->head.vertex )->data.offset, "next.oidx"   ),

      // Locked
      ASSERT(    "vertex.locked == false" ),
      ASSERT(    "next.locked == false" ),
      ASSERT(    ".locked == false" ),
      // TODO: Open a vertex in another thread and verify .locked = true

      // Address
      ASSERT(    "vertex.address == vertex"            ),
      ASSERT(    "vertex.address == vertex.address"    ),
      ASSERT(    "vertex.address == vertex.internalid" ),
      UNTRUE(    "next.address == vertex"              ),
      UNTRUE(    "next.address == vertex.address"      ),
      UNTRUE(    "next.address == vertex.internalid"   ),

      // Index
      //ASSERT(    "vertex.index     ==       ( int( vertex.address & 0xFFFFFFFF ) * 0x55555556) >> 38" ),
      ASSERT(    "vertex.index     ==       int( (vertex.address-32) / 192 )" ),
      //ASSERT(    "vertex.bitindex  ==       ( int( vertex.address & 0xFFFFFFFF ) * 0x55555556) >> 44" ),
      ASSERT(    "vertex.bitindex  ==       int( (vertex.address-32) / (192*64) )" ),
      //ASSERT(    "vertex.bitvector == 1 << (((int( vertex.address & 0xFFFFFFFF ) * 0x55555556) >> 38) & 0x3f)" ),
      ASSERT(    "vertex.bitvector == 1 << (int( (vertex.address-32) / 192 ) & 0x3f)" ),
      //ASSERT(    "next.index       ==       ( int( next.address   & 0xFFFFFFFF ) * 0x55555556) >> 38" ),
      ASSERT(    "next.index       ==       int( (next.address-32) / 192 )" ),
      //ASSERT(    "next.bitindex    ==       ( int( next.address   & 0xFFFFFFFF ) * 0x55555556) >> 44" ),
      ASSERT(    "next.bitindex    ==       int( (next.address-32) / (192*64) )"),
      //ASSERT(    "next.bitvector   == 1 << (((int( next.address   & 0xFFFFFFFF ) * 0x55555556) >> 38) & 0x3f)" ),
      ASSERT(    "next.bitvector   == 1 << (int( (next.address-32) / 192 ) & 0x3f)" ),

      // Shorthand equivalents
      ASSERT(    "next.c1 == .c1"                    ),
      ASSERT(    "next.c0 == .c0"                    ),
      ASSERT(    "next.virtual == .virtual"          ),
      ASSERT(    "next.id == .id"                    ),
      ASSERT(    "next.internalid == .internalid"    ),
      ASSERT(    "next.type == .type"                ),
      ASSERT(    "next.deg == .deg"                  ),
      ASSERT(    "next.ideg == .ideg"                ),
      ASSERT(    "next.odeg == .odeg"                ),
      ASSERT(    "next.tmc == .tmc"                  ),
      ASSERT(    "next.tmm == .tmm"                  ),
      ASSERT(    "next.tmx == .tmx"                  ),
      ASSERT(    "next.vector == .vector"            ),
      ASSERT(    "next.op == .op"                    ),
      ASSERT(    "next.refc == .refc"                ),
      ASSERT(    "next.bidx == .bidx"                ),
      ASSERT(    "next.oidx == .oidx"                ),
      ASSERT(    "next.address == .address"          ),
      ASSERT(    "next.address != .bidx"             ),
      ASSERT(    "next.index == .index"              ),
      ASSERT(    "next.bitindex == .bitindex"        ),
      ASSERT(    "next.bitvector == .bitvector"      ),


      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, vertex, 0.0, NULL, NULL ) >= 0,   "Vertex" );


    vgx_EvalStackItem_t default_prop = {
      .type = STACK_ITEM_TYPE_CSTRING,
      .CSTR__str = NewEphemeralCString( graph, "Default" )
    };

    __test test_default_prop[] = {
      ASSERT(   "'prop1' in next"          ),
      UNTRUE(   "'nix' in next"            ),
      ASSERT(   "next['prop1'] == 1"       ),
      ASSERT(   "next['prop1'] != 2"       ),
      ASSERT(   "next['nix'] == 'Default'" ),
      UNTRUE(   "next['nix'] == 0"         ),
      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, test_default_prop, 0.0, NULL, &default_prop ) >= 0,   "Vertex" );
    CStringDelete( default_prop.CSTR__str );


    SELECTED_TEST_ARC = &ARC_ROOT_to_A;
    __test test_empty_prop[] = {
      ASSERT(   "'propR' in vertex"        ),
      UNTRUE(   "'propR' notin vertex"     ),
      ASSERT(   "vertex.propcount == 1"    ),
      ASSERT(   "len(vertex) == 1"         ),
      UNTRUE(   "'propR' in next"          ),
      ASSERT(   "'propR' notin next"       ),
      ASSERT(   "next.propcount == 0"      ),
      ASSERT(   ".propcount == 0"          ),
      ASSERT(   "len(next) == 0"           ),
      {0}
    };

    TEST_ASSERTION( __test_expressions( graph, test_empty_prop, 0.0, NULL, NULL ) >= 0,   "Vertex" );

    iString.Discard( &CSTR__tail_addr_ROOT );
    iString.Discard( &CSTR__tail_addr_A );

    iString.Discard( &CSTR__head_addr_B );
    iString.Discard( &CSTR__head_addr_C );

  } END_TEST_SCENARIO


    
  /*******************************************************************//**
   * Collect
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Collect" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test collect[] = {
      ASSERT(  "stage() == 0"                  ),
      ASSERT(  "stage( pi ) == 0"              ),
      ASSERT(  "stage( null ) == 0"            ),
      ASSERT(  "stage( pi, 1 ) == 0"           ),
      ASSERT(  "stage( null, 1 ) == 0"         ),
      ASSERT(  "stageif( 1 ) == 1"             ),
      ASSERT(  "stageif( 0 ) == 0"             ),
      ASSERT(  "stageif( 1, pi ) == 1"         ),
      ASSERT(  "stageif( 1, pi, 1 ) == 1"      ),

      ASSERT(  "commit() == 0"                 ),
      ASSERT(  "commit( 1 ) == 0"              ),
      ASSERT(  "commitif( 1 ) == 1"            ),
      ASSERT(  "commitif( 0 ) == 0"            ),
      ASSERT(  "commitif( 1, 1 ) == 1"         ),

      ASSERT(  "unstage() == 0"                ),
      ASSERT(  "unstage( 1 ) == 0"             ),
      ASSERT(  "unstageif( 1 ) == 1"           ),
      ASSERT(  "unstageif( 0 ) == 0"           ),
      ASSERT(  "unstageif( 1, 1 ) == 1"        ),

      ASSERT(  "collect() == 0"                ),
      ASSERT(  "collect( pi ) == 0"            ),
      ASSERT(  "collect( null ) == 0"          ),
      ASSERT(  "collectif( 1 ) == 1"           ),
      ASSERT(  "collectif( 0 ) == 0"           ),
      ASSERT(  "collectif( 1, pi ) == 1"       ),
      ASSERT(  "collectif( 1, null ) == 1"     ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, collect, 0.0, NULL, NULL ) >= 0,   "Collect" );
  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Cull
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Cull" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test cull[] = {
      ASSERT(          "true" ),
      ASSERT_CONTINUE( "mcull( 0.6, 1 ) == 1" ), // 6
      ASSERT_CONTINUE( "mcull( 0.4, 1 ) == 0" ), // 6
      ASSERT_CONTINUE( "mcull( 0.8, 1 ) == 1" ), // 8
      ASSERT_CONTINUE( "mcull( 0.2, 1 ) == 0" ), // 8
      ASSERT_CONTINUE( "mcull( 0.5, 1 ) == 0" ), // 8
      ASSERT_CONTINUE( "mcull( 0.7, 1 ) == 0" ), // 8
      ASSERT_CONTINUE( "mcull( 0.9, 1 ) == 1" ), // 9

      ASSERT(          "true" ),
      ASSERT_CONTINUE( "mcull( 0.6, 2 ) == 1" ), // 6 -
      ASSERT_CONTINUE( "mcull( 0.4, 2 ) == 1" ), // 6 4
      ASSERT_CONTINUE( "mcull( 0.8, 2 ) == 1" ), // 8 6
      ASSERT_CONTINUE( "mcull( 0.2, 2 ) == 0" ), // 8 6
      ASSERT_CONTINUE( "mcull( 0.5, 2 ) == 0" ), // 8 6
      ASSERT_CONTINUE( "mcull( 0.7, 2 ) == 1" ), // 8 7
      ASSERT_CONTINUE( "mcull( 0.9, 2 ) == 1" ), // 8 9

      ASSERT(          "true" ),
      ASSERT_CONTINUE( "mcull( 0.6, 3 ) == 1" ), // 6 - -
      ASSERT_CONTINUE( "mcull( 0.4, 3 ) == 1" ), // 6 4 -
      ASSERT_CONTINUE( "mcull( 0.8, 3 ) == 1" ), // 8 6 4
      ASSERT_CONTINUE( "mcull( 0.2, 3 ) == 0" ), // 8 6 4
      ASSERT_CONTINUE( "mcull( 0.5, 3 ) == 1" ), // 8 6 5
      ASSERT_CONTINUE( "mcull( 0.7, 3 ) == 1" ), // 8 7 6
      ASSERT_CONTINUE( "mcull( 0.9, 3 ) == 1" ), // 9 8 7

      ASSERT(          "true" ),
      ASSERT_CONTINUE( "mcull( 0.6, 4 ) == 1" ), // 6 - - -
      ASSERT_CONTINUE( "mcull( 0.4, 4 ) == 1" ), // 6 4 - -
      ASSERT_CONTINUE( "mcull( 0.8, 4 ) == 1" ), // 8 6 4 -
      ASSERT_CONTINUE( "mcull( 0.2, 4 ) == 1" ), // 8 6 4 2
      ASSERT_CONTINUE( "mcull( 0.5, 4 ) == 1" ), // 8 6 5 4
      ASSERT_CONTINUE( "mcull( 0.7, 4 ) == 1" ), // 8 7 6 5
      ASSERT_CONTINUE( "mcull( 0.9, 4 ) == 1" ), // 9 8 7 6

      ASSERT(          "true" ),
      ASSERT_CONTINUE( "mcullif( true, 0.6, 1 ) == true" ), // 6
      ASSERT_CONTINUE( "mcullif( true, 0.4, 1 ) == true" ), // 6
      ASSERT_CONTINUE( "mcullif( false, 0.8, 1 ) == false" ), // 6
      ASSERT_CONTINUE( "mcullif( true, 0.2, 1 ) == true" ), // 6
      ASSERT_CONTINUE( "mcull( 0.5, 1 ) == 0" ), // 6
      ASSERT_CONTINUE( "mcull( 0.7, 1 ) == 1" ), // 7
      ASSERT_CONTINUE( "mcull( 0.9, 1 )" ), // 9

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, cull, 0.0, NULL, NULL ) >= 0,   "Cull" );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Sets
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Sets" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    vgx_ExpressEvalMemory_t *evalmem = iEvaluator.NewMemory( 5 );
    __test sets[] = {

      // -----------
      // iset
      // -----------

      // Empty
      ASSERT(         "iset.len() == 0"          ),
      ASSERT(         "iset.has( 123 ) == 0"     ),
      ASSERT(         "iset.has( 124 ) == 0"     ),

      // Add first
      ASSERT(         "iset.add( 123 ) == 1"     ),
      ASSERT(         "iset.add( 123 ) == 0"     ),

      // Check one
      ASSERT(         "iset.len() == 1"          ),
      ASSERT(         "iset.has( 123 ) == 1"     ),
      ASSERT(         "iset.has( 124 ) == 0"     ),

      // Add second
      ASSERT(         "iset.add( 124 ) == 1"     ),
      ASSERT(         "iset.add( 124 ) == 0"     ),

      // Check two
      ASSERT(         "iset.len() == 2"          ),
      ASSERT(         "iset.has( 123 ) == 1"     ),
      ASSERT(         "iset.has( 124 ) == 1"     ),

      // Remove one
      ASSERT(         "iset.del( 123 ) == 1"     ),

      // Check one remain
      ASSERT(         "iset.len() == 1"          ),
      ASSERT(         "iset.has( 123 ) == 0"     ),
      ASSERT(         "iset.has( 124 ) == 1"     ),

      // Remove another
      ASSERT(         "iset.del( 124 ) == 1"     ),

      // Check empty
      ASSERT(         "iset.len() == 0"          ),
      ASSERT(         "iset.has( 123 ) == 0"     ),
      ASSERT(         "iset.has( 124 ) == 0"     ),

      // Add several and check
      ASSERT(         "iset.add( 101 ) == 1"     ),
      ASSERT(         "iset.add( 102 ) == 1"     ),
      ASSERT(         "iset.add( 103 ) == 1"     ),
      ASSERT(         "iset.add( 104 ) == 1"     ),
      ASSERT(         "iset.add( 105 ) == 1"     ),
      ASSERT(         "iset.len() == 5"          ),
      ASSERT(         "iset.has( 101 ) == 1"     ),
      ASSERT(         "iset.has( 105 ) == 1"     ),

      // Clear and check
      ASSERT(         "iset.clr() == 5"          ),
      ASSERT(         "iset.len() == 0"          ),
      ASSERT(         "iset.has( 101 ) == 0"     ),
      ASSERT(         "iset.has( 105 ) == 0"     ),

      // -----------
      // vset
      // -----------

      // Non-vertex
      ASSERT(         "vset.add( 123 ) == 0     /* must be vertex instance */"     ),
      ASSERT(         "vset.has( 123 ) == 0     /* must be vertex instance */"     ),
      ASSERT(         "vset.del( 123 ) == 0     /* must be vertex instance */"     ),

      // Check empty
      ASSERT(         "vset.has( vertex ) == 0"   ),
      ASSERT(         "vset.has( next ) == 0"     ),
      ASSERT(         "vset.len() == 0"           ),

      // Add first vertex
      ASSERT(         "vset.add( vertex ) == 1"   ),

      // Check one
      ASSERT(         "vset.len() == 1"           ),
      ASSERT(         "vset.has( vertex ) == 1"   ),
      ASSERT(         "vset.add( vertex ) == 0"   ),
      ASSERT(         "vset.has( next ) == 0"     ),

      // Add second vertex
      ASSERT(         "vset.add( next ) == 1"     ),

      // Check two
      ASSERT(         "vset.len() == 2"           ),
      ASSERT(         "vset.has( vertex ) == 1"   ),
      ASSERT(         "vset.has( next ) == 1"     ),
      ASSERT(         "vset.add( next ) == 0"     ),

      // Remove one and check
      ASSERT(         "vset.del( next ) == 1"     ),
      ASSERT(         "vset.len() == 1"           ),
      ASSERT(         "vset.has( next ) == 0"     ),
      ASSERT(         "vset.del( next ) == 0"     ),

      // Remove another and check
      ASSERT(         "vset.del( vertex ) == 1"     ),
      ASSERT(         "vset.len() == 0"           ),
      ASSERT(         "vset.has( vertex ) == 0"     ),
      ASSERT(         "vset.del( vertex ) == 0"     ),

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, sets, 0.0, evalmem, NULL ) >= 0,   "Sets" );
    iEvaluator.DiscardMemory( &evalmem );
  } END_TEST_SCENARIO




  /*******************************************************************//**
   * Misc. expressions
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Composite Expressions" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    __test misc[] = {
      // BASIC PRECEDENCE
      XREAL(    1   +   2   -   3   *   4   /   5.0   *   6   -   7   +   8  ),
      XREAL(    1   +   2   -   3   *   4   /   5.0   *   6   -  (7   +   8) ),
      XREAL(    1   +   2   -   3   *   4   /   5.0   *  (6   -   7)  +   8  ),
      XREAL(    1   +   2   -   3   *   4   /  (5.0   *   6)  -   7   +   8  ),
      XREAL(    1   +   2   -   3   *  (4   /   5.0)  *   6   -   7   +   8  ),
      XREAL(    1   +   2   -  (3   *   4)  /   5.0   *   6   -   7   +   8  ),
      XREAL(    1   +  (2   -   3)  *   4   /   5.0   *   6   -   7   +   8  ),
      XREAL(   (1   +   2)  -   3   *   4   /   5.0   *   6   -   7   +   8  ),

      XREAL(    1   +   2   -   3   *   4   /   5.0   *  (6   -   7   +   8) ),
      XREAL(    1   +   2   -   3   *   4   /  (5.0   *   6   -   7)  +   8  ),
      XREAL(    1   +   2   -   3   *  (4   /   5.0   *   6)  -   7   +   8  ),
      XREAL(    1   +   2   -  (3   *   4   /   5.0)  *   6   -   7   +   8  ),
      XREAL(    1   +  (2   -   3   *   4)  /   5.0   *   6   -   7   +   8  ),
      XREAL(   (1   +   2   -   3)  *   4   /   5.0   *   6   -   7   +   8  ),

      XREAL(    1   +   2   -   3   *   4   /  (5.0   *   6   -   7   +   8) ),
      XREAL(    1   +   2   -   3   *  (4   /   5.0   *   6   -   7)  +   8  ),
      XREAL(    1   +   2   -  (3   *   4   /   5.0   *   6)  -   7   +   8  ),
      XREAL(    1   +  (2   -   3   *   4   /   5.0)  *   6   -   7   +   8  ),
      XREAL(   (1   +   2   -   3   *   4)  /   5.0   *   6   -   7   +   8  ),

      XREAL(    1   +   2   -   3   *  (4   /   5.0   *   6   -   7   +   8) ),
      XREAL(    1   +   2   -  (3   *   4   /   5.0   *   6   -   7)  +   8  ),
      XREAL(    1   +  (2   -   3   *   4   /   5.0   *   6)  -   7   +   8  ),
      XREAL(   (1   +   2   -   3   *   4   /   5.0)  *   6   -   7   +   8  ),

      XREAL(    1   +   2   -  (3   *   4   /   5.0   *   6   -   7   +   8) ),
      XREAL(    1   +  (2   -   3   *   4   /   5.0   *   6   -   7)  +   8  ),
      XREAL(   (1   +   2   -   3   *   4   /   5.0   *   6)  -   7   +   8  ),

      XREAL(    1   +  (2   -   3   *   4   /   5.0   *   6   -   7   +   8) ),
      XREAL(   (1   +   2   -   3   *   4   /   5.0   *   6   -   7)  +   8  ),

      XREAL(   (1   +   2   -   3   *   4   /   5.0   *   6   -   7   +   8) ),

      XREAL(    1   +   2   -   3   *   4   /  (5.0   *   6   -  (7   +   8))),
      XREAL(    1   +   2   -   3   *  (4   /   5.0   *  (6   -   7)) +   8  ),
      XREAL(    1   +   2   -  (3   *   4   /  (5.0   *   6)) -   7   +   8  ),
      XREAL(    1   +  (2   -   3   *  (4   /   5.0)) *   6   -   7   +   8  ),
      XREAL(   (1   +   2   -  (3   *   4)) /   5.0   *   6   -   7   +   8  ),

      XREAL(    1   +   2   -   3   *  (4   /   5.0   *  (6   -   7)  +   8) ),
      XREAL(    1   +   2   -  (3   *   4   /  (5.0   *   6)  -   7)  +   8  ),
      XREAL(    1   +  (2   -   3   *  (4   /   5.0)  *   6)  -   7   +   8  ),
      XREAL(   (1   +   2   -  (3   *   4)  /   5.0)  *   6   -   7   +   8  ),

      XREAL(    1   +   2   - ((3   *   4   /   5.0)  *   6   -   7   +   8) ),
      XREAL(    1   + ((2   -   3   *   4)  /   5.0   *   6   -   7)  +   8  ),
      XREAL(  ((1   +   2   -   3)  *   4   /   5.0   *   6)  -   7   +   8  ),

      XREAL(       1.5 - log2( 3 - ( 2.0 / 5 ) ) + log2( ( 3 - 2.0 ) / 5  )     ),
      XREAL(       1.5 - (log2( 3 - ( 2.0 / 5 ) ) + log2( ( 3 - 2.0 ) / 5  ))   ),
      XREAL(      (1.5 - log2( 3 - ( 2.0 / 5 ) )) + log2( ( 3 - 2.0 ) / 5  )    ),

      // TODO: MORE

      {0}
    };
    TEST_ASSERTION( __test_expressions( graph, misc, 0.0, NULL, NULL ) >= 0,   "Composite" );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Syntax Errors
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Syntax Errors" ) {
    TEST_ASSERTION( __is_syntax_error( graph, " +                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, " 1 + 2 !* 5              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, " next / .                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "  878df7 hgsdg8738hs     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, " ....................    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "(,(,(,(,(                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "(((((((((((((((((((((((( " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, ")))))))))))))))))))))))) " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, " @$%&$%^@#$%#$%%^$^&$%^& " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "2-2-2-2-2-2-2-2-2-2-     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "('xxxxx) + 7             " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "(((((((((((((((((((((((((((((true]asdf [8 ,.sin/next.type " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "1?1?1?1?1?1?1?1?1?1?1?1?1" ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "((next[((                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "((1,1,1,1,1,1,1,1,1,1,,, " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1,(1" ), "Syntax Error" );

    // EXPECT OPERAND / EXPECT ANY
    TEST_ASSERTION( __is_syntax_error( graph, "{}                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "{)                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "()                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "{)                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "+)                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "+,                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "+[                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "+]                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "+ ==                     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "-)                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "-,                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "-[                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "-]                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "- ==                     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5 * )                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5 * ,                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5 * [                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5 * ]                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5 * ==                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "! )                      " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "! ,                      " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "! [                      " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "! ]                      " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "! ==                     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "myfunc :=)               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "myfunc :=,               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "myfunc :=[               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "myfunc :=]               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "myfunc := ==             " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin()                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin(,                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin([                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin(]                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin( ==                  " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 2, )                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 2, ,                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 2, [                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 2, ]                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 2, ==               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ )                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ ,                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ [                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ ]                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ ==               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "1 + nope( 13 )           " ), "Syntax Error" );

    // EXPECT ANY
    TEST_ASSERTION( __is_syntax_error( graph, "sum( ,                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sum( [                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sum( ]                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "1 + {2,3,4}              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "2 in {2,{3,4},5}         " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "log({2,3,4})             " ), "Syntax Error" );

    // EXPECT INFIX
    TEST_ASSERTION( __is_syntax_error( graph, "pi max                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex prox              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vector min               " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 1, 2 ) min          " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ 'prop' ] max     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "123 max                  " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "0xabc max                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "123.4 max                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "next.arc.type == 'x' max " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "'x' == next.arc.type max " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "'hello' max              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "17 range( 10, 20 )       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "17 + range( 10, 20 )     " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "17 in range( 10 )        " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "17 in range( 10, 20, 30 )" ), "Syntax Error" );
    
    // EXPECT ASSIGNMENT
    TEST_ASSERTION( __is_syntax_error( graph, "func == 1 + 1            " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "5unc := 1 + 1            " ), "Syntax Error" );

    // EXPECT OPEN PAREN
    TEST_ASSERTION( __is_syntax_error( graph, "sin 1 + 1                " ), "Syntax Error" );

    // EXPECT CX_CLOSE PAREN
    TEST_ASSERTION( __is_syntax_error( graph, "random( 5 )              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "relenc( 'one', 'two' )   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "typeenc( 'one', 'two')   " ), "Syntax Error" );

    // ARG COUNT
    TEST_ASSERTION( __is_syntax_error( graph, "sin()                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin( 1, 2 )              " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 1, 2, 3 )           " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max( 1 )                 " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "geodist(1,2,3)           " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "geodist(1,2,3,4,5)       " ), "Syntax Error" );

    // VARIADIC ARG COUNT
    TEST_ASSERTION( __is_syntax_error( graph, "stage( 100, 0, 999 )         " ), "Syntax Error" ); // too many args
    TEST_ASSERTION( __is_syntax_error( graph, "stageif()                    " ), "Syntax Error" ); // too few args
    TEST_ASSERTION( __is_syntax_error( graph, "stageif( true, 100, 0, 999)  " ), "Syntax Error" ); // too many args

    TEST_ASSERTION( __is_syntax_error( graph, "unstage( 0, 999 )            " ), "Syntax Error" ); // too many args
    TEST_ASSERTION( __is_syntax_error( graph, "unstageif()                  " ), "Syntax Error" ); // too few args
    TEST_ASSERTION( __is_syntax_error( graph, "unstageif( true, 0, 999 )    " ), "Syntax Error" ); // too many args
    
    TEST_ASSERTION( __is_syntax_error( graph, "commit( 0, 999 )             " ), "Syntax Error" ); // too many args
    TEST_ASSERTION( __is_syntax_error( graph, "commitif()                   " ), "Syntax Error" ); // too few args
    TEST_ASSERTION( __is_syntax_error( graph, "commitif( true, 0, 999 )     " ), "Syntax Error" ); // too many args

    TEST_ASSERTION( __is_syntax_error( graph, "collect( 100, 999 )          " ), "Syntax Error" ); // too many args
    TEST_ASSERTION( __is_syntax_error( graph, "collectif()                  " ), "Syntax Error" ); // too few args
    TEST_ASSERTION( __is_syntax_error( graph, "collectif( true, 100, 999 )  " ), "Syntax Error" ); // too many args

    // GROUP
    TEST_ASSERTION( __is_syntax_error( graph, "1 + 2)                   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "()                       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "(1 2)                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "sin(1 2)                 " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max(1 2)                 " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max(,1 2)                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max(1 2,)                " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "max(1,2,)                " ), "Syntax Error" );

    // EXPECT OPEN BRACKET
    TEST_ASSERTION( __is_syntax_error( graph, "vertex 'prop'            " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "1 + rel 'knows'          " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "1 + type 'node'          " ), "Syntax Error" );

    // EXPECT CX_CLOSE BRACKET
    TEST_ASSERTION( __is_syntax_error( graph, "vertex[ 'prop' + 1       " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "rel[ 'knows' + 1         " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "type[ 'node' + 1         " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex.id == 'thing**'   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex.id == 't**ing'    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex.id == 't*in*g*'   " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "vertex.id == '*hin*g'    " ), "Syntax Error" );

    // EXPECT OPEN BRACE
    TEST_ASSERTION( __is_syntax_error( graph, "5 in 1,2,3,4,5}          " ), "Syntax Error" );

    // EXPEXT CX_CLOSE BRACE
    TEST_ASSERTION( __is_syntax_error( graph, "5 in {1,2,3,4,5          " ), "Syntax Error" );

    // EXPECT LITERAL
    TEST_ASSERTION( __is_syntax_error( graph, "12x                      " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "0x12g                    " ), "Syntax Error" );
    TEST_ASSERTION( __is_syntax_error( graph, "'unterminated            " ), "Syntax Error" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Chaos
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Chaos" ) {
    vgx_LockableArc_t *arc = SELECTED_TEST_ARC = &ARC_ROOT_to_B;
    char buffer[0x10001] = {' '};
    buffer[ 0x10000 ] = '\0';

#define SET_ARRAY_SIZE( Array ) (Array)[0] = (void*)( qwsizeof( Array ) - 1 )

    const char *symbols[] = {
      0,
      "!", "!", "!", "!", "!", "!", "!", "!", "!",        
      "@", "#", "$", "%", "^", "&", "*",
      "(", ")", "-", "_", "=", "+", "|", "~",
      "'", "{", "}", "[", "]", "<", ">", ",",
      ".", "/", ":", ":", "?",
      ">=", "<=", "==", "!=", ":=", "<<", ">>", "&&",
      "===", "==<", "==>",  "<==",  "=!=",   
      "||", "**",
      "in", "in", "in", "in", "in", "in", "in", 
      "notin", "notin", "notin", "notin", "notin", "notin", "notin", 
      "^^", "***", "<>", "%%", "::", "?:", "&|", "|&", ":&:", "?&?",
      ",", ",", ",", ",", ",", ",", ",",
      "(", "(", "(", "(", "(", ")", ")", ")", ")"
    };
    SET_ARRAY_SIZE( symbols );


    const char *functions[] = {
      0,
      "int", "real", "hash", "bitvector", "keyval", "neg",
      "isnan", "isinf", "isint", "isreal", "isbitvector", "iskeyval", "isstr", "isvector", "isarray", "ismap",
      "inv", "log2", "log", "log10",
      "rad", "deg",
      "sin", "sinh", "asin", "asinh",
      "cos", "cosh", "acos", "acosh",
      "tan", "tanh", "atan", "atanh",
      "exp", "abs", "sqrt", "ceil", "floor", "round", "sign", "fac",
      "atan2", "max", "min", "prox", "sim", "comb",
      "sim", "cosine", "jaccard", "hamdist"
      "geodist", "geoprox",
      "relenc", "typeenc", "reldec", "typedec", "modtostr", "dirtostr",
      "len",
      "range", "range", "range", "range", "range",
      "rank", "georank",
      "sum", "sumsqr", "invsum", "prod", "mean", "harmmean", "geomean",
      "sum", "sumsqr", "invsum", "prod", "mean", "harmmean", "geomean",
      "sum", "sumsqr", "invsum", "prod", "mean", "harmmean", "geomean",
      "rel", "type",
      "rel", "type",
      "rel", "type",
      "random", "randint",
      "random", "randint",
      "random", "randint",
      "firstval", "lastval", "do", "do", "void",
      "store", "load", "push", "pop", "mov", "xchg",
      "store", "load", "push", "pop", "mov", "xchg",
      "store", "load", "push", "pop", "mov", "xchg",
      "equ", "neq", "gt", "lt", "gte", "lte", 
      "equ", "neq", "gt", "lt", "gte", "lte", 
      "equ", "neq", "gt", "lt", "gte", "lte", 
      "requ", "rneq", "rgt", "rlt", "rgte", "rlte", 
      "requ", "rneq", "rgt", "rlt", "rgte", "rlte", 
      "requ", "rneq", "rgt", "rlt", "rgte", "rlte", 
      "store", "load", "push", "pop", "get", "count", "inc", "dec", "add", "sub", "mul", "div", "mod", "shr", "shl", "and", "or", "xor",
      "storeif", "pushif", "incif", "decif", "addif", "subif", "mulif", "divif", "modif", "shrif", "shlif", "andif", "orif", "xorif",
      "rstore", "rstoreif", "rload", "rmov", "rmovif", "rxchg", "rxchgif", "rinc", "rincif", "rdec", "rdecif",
      "smooth",
      "index", "indexed", "unindex",
      "mint", "mreal", "mround", "minc", "mdec", "madd", "msub", "mmul", "mdiv", "mmod",
      "miinc", "midec", "miadd", "misub", "mimul", "midiv", "mimod",
      "mrinc", "mrdec", "mradd", "mrsub", "mrmul", "mrdiv", "mrmod",
      "mshl", "mshr", "mand", "mor", "mxor", "mset", "mrandomize", "mhash",
      "msum", "msumsrq", "mstdev", "minvsum", "mprod", "mmean", "mharmmean", "mgeomean", "mgeostdev",
      "mmax", "mmin"
      "mmax", "mmin"
      "mmax", "mmin"
    };
    SET_ARRAY_SIZE( functions );


    const char *attributes[] = {
      0,
      "prev", "prev.c1", "prev.c0", "prev.virtual", "prev.id", "prev.internalid",
      "prev.type", "prev.deg", "prev.ideg", "prev.odeg", "prev.tmc", "prev.tmm", "prev.tmx",
      "prev.vector", "prev.op", "prev.refc", "prev.bidx", "prev.oidx", "prev.address",
      "prev.index", "prev.bitindex", "prev.bitvector",

      "prev.arc.value", "prev.arc.dist", "prev.arc.type", "prev.arc.dir", "prev.arc.mod", "prev.arc",

      "vertex", "vertex.c1", "vertex.c0", "vertex.virtual", "vertex.id", "vertex.internalid",
      "vertex.type", "vertex.deg", "vertex.ideg", "vertex.odeg", "vertex.tmc", "vertex.tmm", "vertex.tmx",
      "vertex.vector", "vertex.op", "vertex.refc", "vertex.bidx", "vertex.oidx", "vertex.address",
      "vertex.index", "vertex.bitindex", "vertex.bitvector",

      "next.arc.value", "next.arc.dist", "next.arc.type", "next.arc.dir", "next.arc.mod", "next.arc",
      ".arc.value", ".arc.dist", ".arc.type", ".arc.dir", ".arc.mod", ".arc",

      "next", "next.c1", "next.c0", "next.virtual", "next.id", "next.internalid",
      "next.type", "next.deg", "next.ideg", "next.odeg", "next.tmc", "next.tmm", "next.tmx",
      "next.vector", "next.op", "next.refc", "next.bidx", "next.oidx", "next.address",
      "next.index", "next.bitindex", "next.bitvector",

      ".c1", ".c0", ".virtual", ".id", ".internalid",
      ".type", ".deg", ".ideg", ".odeg", ".tmc", ".tmm", ".tmx",
      ".vector", ".op", ".refc", ".bidx", ".oidx", ".address",
      ".index", ".bitindex", ".bitvector",
    };
    SET_ARRAY_SIZE( attributes );
    

    const char *constants[] = {
      0,
      "true", "false",
      "graph.ts", "graph.age", "graph.order", "graph.size", "graph.op", "vector",
      "context.rank", ".rank", "sys.tick", "sys.uptime",
      "T_NEVER", "T_MAX", "T_MIN", "D_ANY", "D_IN", "D_OUT", "D_BOTH",
      "M_STAT", "M_SIM", "M_DIST", "M_LSH", "M_INT", "M_UINT", "M_FLT", "M_CNT", "M_ACC", "M_TMC", "M_TMM", "M_TMX",
      "pi", "e", "root2", "root3", "root5", "phi", "zeta3", "googol",
      "R1", "R2", "R3", "R4"
    };
    SET_ARRAY_SIZE( constants );


    const char *numbers[] = {
      0,
      "-1.5e10", "-1.5e9", "-1.5e8", "-1.5e7", "-1.5e6", "-1.5e5", "-1.5e4", "-1.5e3", "-1.5e2",
      "-30", "-29", "-28", "-27", "-26", "-25.5", "-24.5", "-23.5", "-22.5", "-21.5",
      "-20", "-19", "-18", "-17", "-16", "-15.5", "-14.5", "-13.5", "-12.5", "-11.5",
      "-10", "-9", "-8", "-7", "-6", "-5.5", "-4.5", "-3.5", "-2.5", "-1.5",
      "-0.5e-1", "-0.5e-2", "-0.5e-3", "-0.5e-4", "-0.5e-5",
      "0",
      "0.5e-5", "0.5e-4", "0.5e-3", "0.5e-2", "0.5e-1",
      "1", "2", "3", "4", "5", "6.5", "7.5", "8.5", "9.5", "10.5",
      "11", "12", "13", "14", "15", "16.5", "17.5", "18.5", "19.5", "20.5",
      "21", "22", "23", "24", "25", "26.5", "27.5", "28.5", "29.5", "30.5",
      "1.5e2", "1.5e3", "1.5e4", "1.5e5", "1.5e6", "1.5e7", "1.5e8", "1.5e9", "1.5e10"
    };
    SET_ARRAY_SIZE( numbers );


    const char *strings[] = {
      0,
      "'hello'", "'This is a longer string'", "''", "'x'", "'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'",
      "'prop1'", "'prop2'", "'first_dim'", "second_dim", "'third_dim'"
      "'jhsdjfhg(*&^#*(@&#^$*('", "'FSJKHSDBBCKJCK}}{{}{}}{SD}F}{{{{{'", "'(*&^#*(@&#^$*(  (*^& GJ{{{{'"
    };
    SET_ARRAY_SIZE( strings );


    const char *spaces[] = {
      0,
      "", " ", "  ", "   ", "    ", "     "
    };
    SET_ARRAY_SIZE( spaces );


    const char *junk[] = {
      0,
      "[x]", "['yy']", "[['zzz']]",
      "['zzz' 1]", "/1.2", "/0", "!(", "next[",
      "asdf", "$%@H$%@", "((()(Q*#", "(((((((((((((((((((((((((((((", "-+-+-+-+-+-+-+-+-+", "/////////////////////", 
      "'''''''''''''''''", "!!!!!!!!!!!!!!", "<< << >>", ",,,,,,,,,,,,,,,,",
      "{{{{{{{{{{{{{{{{{{{{{{", "{({({({({({({({({({({({({(1,'z',{", "{'x',('x',{'x'})}"
    };
    SET_ARRAY_SIZE( junk );


    const char *fragments[] = {
      0,
      " > 0 ? 'hello' :",
      "? 5 :",
      "in vertex",
      "in {",
      "** 2",
      "! ( 5 in",
      "! in {",
      "1,2,3}",
      "1,2,3,}",
      "in {-1, 0, +1}",
      "in range( -10, 10 )",
      "in {'to', 'maybe'}",
      "in {1,2,3,4,5,6,7}",
      "store(",
      "store( R1",
      "store( R2,",
      "store( R3, 1000 )",
      "load( R1 )",
      "load( R2 )",
      "load( R3 )"
    };
    SET_ARRAY_SIZE( fragments );


    const char **tokens[] = {
      0,
      symbols, symbols, symbols,
      functions, functions,
      attributes,
      constants, 
      numbers, numbers,
      spaces, spaces, spaces, spaces,
      junk,
      fragments
    };
    SET_ARRAY_SIZE( tokens );


    int good[30] = {0};
    int bad = 0;
    int wow = 0;

    for( int n=0; n<3000000; n++ ) {
      int64_t n_tokens = 1 + rand63() % 30;
      char *p = buffer;
      for( int64_t t=0; t<n_tokens; t++ ) {
        // Pick token category
        int64_t catx = 1 + rand63() % (int64_t)tokens[0];
        // Pick a token
        const char **cat = tokens[ catx ];
        int64_t tokx = 1 + rand63() % (int64_t)cat[0];
        const char *token = cat[ tokx ];
        // Write token to buffer
        while( (*p++ = *token++) != '\0' );
        --p; // next write will append to string
        // Put a single space sometimes
        if( rand16() < 10000  ) {
          *p++ = ' ';
          *p = '\0';
        }
      }

      if( __is_syntax_error( graph, buffer ) ) {
        ++bad;
      }
      else {
        if( n_tokens < 30 ) {
          if( n_tokens > 10 ) {
            ++wow; // wow
          }
          good[ n_tokens ]++;
        }
        else {
          good[0]++;
        }
      }
    }

    printf( "Chaos produced %d syntax error expressions\n", bad );
    printf( "Chaos produced valid expressions:\n" );
    for( int n=1; n<30; n++ ) {
      printf( "  %d tokens:  %d\n", n, good[n] );
    }

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY GRAPH
   ***********************************************************************
   */

  if( ARC_ROOT_to_A.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_A.tail );
  }
  if( ARC_ROOT_to_B.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_B.tail );
  }
  if( ARC_ROOT_to_C.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_C.tail );
  }
  if( ARC_ROOT_rel_D.tail ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_rel_D.tail );
  }
  if( ARC_ROOT_to_A.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_A.head.vertex );
  }
  if( ARC_ROOT_to_B.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_B.head.vertex );
  }
  if( ARC_ROOT_to_C.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_to_C.head.vertex );
  }
  if( ARC_ROOT_rel_D.head.vertex ) {
    CALLABLE( graph )->simple->CloseVertex( graph, &ARC_ROOT_rel_D.head.vertex );
  }

  CALLABLE( graph )->advanced->CloseOpenVertices( graph );

  CALLABLE( graph )->simple->Truncate( graph, NULL );
  
  __DESTROY_GRAPH_FACTORY( INITIALIZED );


  iString.Discard( &CSTR__error );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );

} END_UNIT_TEST
RESUME_WARNINGS


#endif
