/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _rank.h
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

#ifndef _VGX_VXEVAL_MODULES_RANK_H
#define _VGX_VXEVAL_MODULES_RANK_H


/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_variadic_rank( vgx_Evaluator_t *self );
static void __eval_variadic_georank( vgx_Evaluator_t *self );
static void __stack_push_context_rank( vgx_Evaluator_t *self );



/*******************************************************************//**
 * rank( [a [, b [, ...]]] )
 *
 *
 * y = c1 * SUM(a,b,c,...) + c0
 *
 ***********************************************************************
 */
static void __eval_variadic_rank( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_Rank_t rank = self->context.HEAD->rank;
  double s = 0.0;
  // Repeat rank operation for all arguments
  while( nargs-- >= 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    switch( x.type ) {
    case STACK_ITEM_TYPE_INTEGER:
      s += (double)x.integer;
      continue;
    case STACK_ITEM_TYPE_REAL:
      s += x.real;
      continue;
    default:
      continue;
    }
  }
  // Linear rank score
  double slope = vgx_RankGetC1( &rank );
  double offset = vgx_RankGetC0( &rank );
  double y = slope * s +offset;
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_REAL_PITEM_VALUE( px, y );
}



/*******************************************************************//**
 * georank( lat, lon )
 * georank( vertex )
 *
 *
 ***********************************************************************
 */
static void __eval_variadic_georank( vgx_Evaluator_t *self ) {
  int64_t nargs = self->op->arg.integer;
  vgx_Rank_t rank = self->context.HEAD->rank;
  double lat = (double)NAN;
  double lon = (double)NAN;
  // Argument is another vertex with rank coefficients c1=lat, c0=lon
  if( nargs == 1 ) {
    vgx_EvalStackItem_t x = POP_ITEM( self );
    if( x.type == STACK_ITEM_TYPE_VERTEX ) {
      lat = vgx_RankGetLatitude( &x.vertex->rank );
      lon = vgx_RankGetLongitude( &x.vertex->rank );
    }
  }
  else if( nargs == 2 ) {
    // other lon
    vgx_EvalStackItem_t x_lon = POP_ITEM( self );
    if( x_lon.type == STACK_ITEM_TYPE_REAL ) {
      lon = x_lon.real;
    }
    else if( x_lon.type == STACK_ITEM_TYPE_INTEGER ) {
      lon = (double)x_lon.integer;
    }
    // other lat
    vgx_EvalStackItem_t x_lat = POP_ITEM( self );
    if( x_lat.type == STACK_ITEM_TYPE_REAL ) {
      lat = x_lat.real;
    }
    else if( x_lat.type == STACK_ITEM_TYPE_INTEGER ) {
      lat = (double)x_lat.integer;
    }
  }
  else {
    while( nargs-- >= 1 ) {
      POP_PITEM( self );
    }
  }
  double this_lat = vgx_RankGetLatitude( &rank );
  double this_lon = vgx_RankGetLongitude( &rank );
  double score = __geoproximity( this_lat, this_lon, lat, lon );
  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  SET_REAL_PITEM_VALUE( px, score );
}



/*******************************************************************//**
 * context.rank
 ***********************************************************************
 */
static void __stack_push_context_rank( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *item = NEXT_PITEM( self );
  double rankscore = self->context.rankscore;
  SET_REAL_PITEM_VALUE( item, rankscore );
}



#endif
