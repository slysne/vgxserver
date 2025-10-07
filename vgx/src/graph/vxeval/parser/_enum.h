/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _enum.h
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

#ifndef _VGX_VXEVAL_PARSER_ENUM_H
#define _VGX_VXEVAL_PARSER_ENUM_H

static int                          __enum__is_string_context_rel( const __shunt_stack *shuntstack );
static int64_t                      __enum__key( vgx_Graph_t *graph, vgx_StackItemType_t type, const CString_t *CSTR__key );
static vgx_ExpressEvalOperation_t * __enum__emitted( vgx_Graph_t *graph, vgx_StackItemType_t type, vgx_ExpressEvalOperation_t *emitted );
static __rpn_operation *            __enum__conditional_args( vgx_Graph_t *graph, vgx_ExpressEvalProgram_t *program, __shunt_stack *shuntstack, __rpn_operation *current_op, vgx_EvalStackItem_t *current_item );



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int __enum__is_string_context_rel( const __shunt_stack *shuntstack )  {
  f_evaluator call;
  if( __shunt__top_is_call( shuntstack, &call ) ) {
    return call == __eval_synarc_decay     ||
           call == __eval_synarc_xdecay    ||
           call == __eval_synarc_hasrel    ||
           call == __eval_synarc_hasmod    ||
           call == __eval_synarc_hasrelmod ||
           call == __eval_synarc_arcvalue;
  }
  return 0;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int64_t __enum__key( vgx_Graph_t *graph, vgx_StackItemType_t itemtype, const CString_t *CSTR__key ) {
  // Interpret the subscript key and convert it to a suitable numeric code
  switch( itemtype ) {
  case __STACK_ITEM_PROP_STR:
    if( _vxenum__is_valid_storable_key( CStringValue( CSTR__key ) ) ) {
      return CStringHash64( CSTR__key );
    }
    else {
      return -1;
    }
  case __STACK_ITEM_REL_STR:
    if( graph ) {
      // WARNING: This assumes relationship encodings are persistent and permanent once assigned. For long-lived Evaluator instances
      //          this encoding will remain in effect even if the encoder somehow forgets the mapping. We assume the encoder will 
      //          never forget the mapping once assigned.
      // TODO:    Ensure the above assumption holds true.
      vgx_predicator_rel_enum relenc = (vgx_predicator_rel_enum)iEnumerator_OPEN.Relationship.GetEnum( graph, CSTR__key );
      if( relenc == VGX_PREDICATOR_REL_WILDCARD || ( relenc != VGX_PREDICATOR_REL_NONEXIST && __relationship_enumeration_valid( relenc ) ) ) {
        return relenc;
      }
      else {
        return VGX_PREDICATOR_REL_NO_MAPPING;
      }
    }
    else {
      return VGX_PREDICATOR_REL_WILDCARD;
    }

  case __STACK_ITEM_VTX_STR:
    if( graph ) {
      // WARNING: This assumes vertex type encodings are persistent and permanent once assigned. For long-lived Evaluator instances
      //          this encoding will remain in effect even if the encoder somehow forgets the mapping. We assume the encoder will 
      //          never forget the mapping once assigned.
      // TODO:    Ensure the above assumption holds true.
      vgx_vertex_type_t vtxenc = (vgx_vertex_type_t)iEnumerator_OPEN.VertexType.GetEnum( graph, CSTR__key );
      if( vtxenc == VERTEX_TYPE_ENUMERATION_WILDCARD || ( vtxenc != VERTEX_TYPE_ENUMERATION_NONEXIST && __vertex_type_enumeration_valid( vtxenc ) ) ) {
        return vtxenc;
      }
      else {
        return VERTEX_TYPE_ENUMERATION_NO_MAPPING;
      }
    }
    else {
      return VERTEX_TYPE_ENUMERATION_WILDCARD;
    }

  case __STACK_ITEM_DIM_STR:
    if( !igraphfactory.EuclideanVectors() ) {
      if( graph ) {
        feature_vector_dimension_t dim = iEnumerator_OPEN.Dimension.EncodeChars( graph->similarity, CStringValue( CSTR__key ), NULL, false );
        if( dim >= FEATURE_VECTOR_DIMENSION_MIN ) {
          return dim;
        }
        else {
          return FEATURE_VECTOR_DIMENSION_NOENUM;
        }
      }
      else {
        return FEATURE_VECTOR_DIMENSION_NONE;
      }
    }

  // Can't enumerate
  default:
    return -1;
  }

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_ExpressEvalOperation_t * __enum__emitted( vgx_Graph_t *graph, vgx_StackItemType_t type, vgx_ExpressEvalOperation_t *emitted ) {
  if( (emitted->arg.integer = __enum__key( graph, type, emitted->arg.CSTR__str )) != 0 ) {
    emitted->arg.type = STACK_ITEM_TYPE_INTEGER;
    emitted->func = __stack_push_constant_integer;
  }
  else {
    // wildcard
    emitted->arg.type = STACK_ITEM_TYPE_WILD;
    emitted->func = __stack_push_constant_wild;
  }
  return emitted;
}



/*******************************************************************//**
 * 
 * <operand1> <cmp> <operand2>
 * 
 * <cmp>      :=  in  |  ==  |  !=  |  >  |  >=  |  <  |  <=
 *
 * For example:
 *   next.arc.type == "likes"
 *   vertex.type == "node"
 *   "likes" == next.arc.type   
 *   "node" == vertex.type
 *
 *   "name" in vertex
 *   "blue" in vector
 *   "x" in vertexID
 *
 *   next.arc.type in { "likes", "knows", "visits" }
 *   vertex.type in { "node", "thing" }
 *
 *
 * Returns:   1 : enumeration occurred
 *            0 : enumeration did not occur
 ***********************************************************************
 */
static __rpn_operation * __enum__conditional_args( vgx_Graph_t *graph, vgx_ExpressEvalProgram_t *program, __shunt_stack *shuntstack, __rpn_operation *current_op, vgx_EvalStackItem_t *current_item ) {

  // Already emitted operand which may have to be modified
  vgx_ExpressEvalOperation_t *emitted = program->parser._cursor-1;

  if( current_op->type == OP_TAIL_REL_OPERAND || current_op->type == OP_ARC_REL_OPERAND ) {
    program->parser._current_string_enum_mode = __STACK_ITEM_REL_STR;
  }
  // TODO: Find a better general way to detect this condition
  else if( __enum__is_string_context_rel( shuntstack ) ) {
    program->parser._current_string_enum_mode = __STACK_ITEM_REL_STR;
  }
  else if( __is_vertex_type_operand( current_op ) ) {
    program->parser._current_string_enum_mode = __STACK_ITEM_VTX_STR;
  }

  //  <operand1> in <operand2>
  //  <property> in <vertex>
  //  <dimension> in <vector>
  //
  if( __shunt__top_is_contains_operator( shuntstack ) ) {
    //
    // <operand2> is an object supporting the "in" operator (vertex, vector, vertexID)
    //
    if( __is_object_operand( current_op ) ) {
      // <operand1> is a string - we must enumerate <operand1> 
      if( emitted->func == __stack_push_constant_string ) {
        // Enumerate <operand1> as a vertex property
        if( __is_vertex_object_operand( current_op ) ) {
          __enum__emitted( graph, __STACK_ITEM_PROP_STR, emitted );
          return current_op;
        }
        // Enumerate <operand1> as a vector dimension
        else if( __is_vector_object_operand( current_op ) ) {
          __enum__emitted( graph, __STACK_ITEM_DIM_STR, emitted );
          return current_op;
        }
      }
    }
  }
  //
  //  <operand1> <cmp> <operand2>
  //
  //
  else if( program->parser._current_string_enum_mode != STACK_ITEM_TYPE_NONE ) {
    // <operand1>          <operand2>
    // <enumeration> <cmp> <string> ==> <enumeration> <cmp> <enumeration>
    if( __eq_operation( current_op, &RpnPushConstantString ) && current_item != NULL ) {
      // <operand1> is a relationship or vertex type enumeration - we must enumerate <operand2>
      // e.g. next.arc.type == 'likes'  ==>  next.arc.type == 12345
      // or   next.arc.type in { 'likes', 'knows', 'visits' }  ==>  next.arc.type in { 12345, 23456, 34567 }
      // or   next.type == 'node'  ==>  next.type == 45678
      // or   next.type in { 'node', 'thing' }  ==>  next.type in { 45678, 56789 }
      if( (current_item->integer = __enum__key( graph, program->parser._current_string_enum_mode, current_item->CSTR__str )) != 0 ) {
        current_item->type = STACK_ITEM_TYPE_INTEGER;
        return &RpnPushConstantInt;
      }
      else {
        current_item->type = STACK_ITEM_TYPE_WILD;
        return &RpnPushWild;
      }
    }

    // <operand1>     <operand2>
    // <string> <cmp> <enumeration> ==> <enumeration> <cmp> <enumeration>
    else if( emitted->func == __stack_push_constant_string && !__shunt__top_is_group_separator( shuntstack ) ) {
      // <operand1> is a string - we must enumerate <operand1> as relationship or vertex type
      // e.g. 'likes' == next.arc.type  ==>  12345 == next.arc.type
      // or   'node' == next.type  ==>  12345 == next.type
      __enum__emitted( graph, program->parser._current_string_enum_mode, emitted );
      return current_op;
    }
  }


  // No enumeration occurred
  return NULL;
}



#endif
