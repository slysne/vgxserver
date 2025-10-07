/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _varmap.h
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

#ifndef _VGX_VXEVAL_PARSER_VARMAP_H
#define _VGX_VXEVAL_PARSER_VARMAP_H



static __varmap *       __varmap__new( void );
static void             __varmap__delete( __varmap **varmap );
static __rpn_variable * __varmap__variable_new( const char *name, int index );
static void             __varmap__variable_delete( __rpn_variable **pvar );
static __rpn_variable * __varmap__variable_get( __varmap *map, const char *key );
static int              __varmap__variable_add( __varmap *map, __rpn_variable *variable );




/******************************************************************************
 *
 *
 ******************************************************************************
 */
static __varmap * __varmap__new( void ) {
  __varmap *varmap = calloc( 1, sizeof( __varmap ) );
  if( varmap ) {
    if( (varmap->list = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t )) == NULL ) {
      free( varmap );
      return NULL;
    }
  }
  return varmap;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __varmap__delete( __varmap **varmap ) {
  if( varmap && *varmap ) {
    if( (*varmap)->list ) {
      CQwordList_t *L = (*varmap)->list;
      int64_t sz = ComlibSequenceLength( L );
      QWORD addr;
      for( int64_t i=0; i<sz; i++ ) {
        CALLABLE( L )->Get( L, i, &addr );
        __rpn_variable *var = (__rpn_variable*)addr;
        __varmap__variable_delete( &var );
      }
      COMLIB_OBJECT_DESTROY( (*varmap)->list );
    }
    free( *varmap );
    *varmap = NULL;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static __rpn_variable * __varmap__variable_new( const char *name, int index ) {
  __rpn_variable *var = calloc( 1, sizeof( __rpn_variable ) );
  if( var ) {
    size_t sz = strlen( name );
    if( (var->name = malloc( sz + 1 )) == NULL ) {
      free( var );
      return NULL;
    }
    strcpy( var->name, name );
    var->subexpr_idx = index;
  }
  return var;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static void __varmap__variable_delete( __rpn_variable **pvar ) {
  if( pvar && *pvar ) {
    free( (*pvar)->name );
    free( *pvar );
    *pvar = NULL;
  }
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static __rpn_variable * __varmap__variable_get( __varmap *map, const char *key ) {
  QWORD *data = CALLABLE( map->list )->Cursor( map->list, 0 );
  QWORD *end = data + ComlibSequenceLength( map->list );
  QWORD *cursor = data;
  __rpn_variable *var;
  while( cursor < end ) {
    var = (__rpn_variable*)*cursor++;
    if( CharsEqualsConst( var->name, key ) ) {
      return var;
    }
  }
  return NULL;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
static int __varmap__variable_add( __varmap *map, __rpn_variable *variable ) {
  if( variable ) {
    if( __varmap__variable_get( map, variable->name ) == NULL ) {
      QWORD addr = (QWORD)variable;
      return CALLABLE( map->list )->Append( map->list, &addr );
    }
    else {
      return -1;
    }
  }
  else {
    return 0;
  }
}





#endif
