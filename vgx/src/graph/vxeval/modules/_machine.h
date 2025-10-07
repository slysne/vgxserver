/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _machine.h
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

#ifndef _VGX_VXEVAL_MODULES_MACHINE_H
#define _VGX_VXEVAL_MODULES_MACHINE_H




/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_cpukill( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = px->integer;
    double y = 0.0;
    int64_t i=0;
    while( i++ < x ) {
      y += (double)i;
    }
    SET_REAL_PITEM_VALUE( px, y );
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_memkill( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  if( px->type == STACK_ITEM_TYPE_INTEGER ) {
    int64_t x = px->integer;
    QWORD *data = calloc( x, sizeof( QWORD ) );
    if( data ) {
      int64_t i=0;
      while( i < x ) {
        data[i] = i;
        ++i;
      }
      i = 0;
      while( i < x ) {
        data[ ihash64( i + 1 ) % x ] = data[ ihash64( i + x ) % x ];
        ++i;
      }
      free( data );
    }
  }
}



#endif
