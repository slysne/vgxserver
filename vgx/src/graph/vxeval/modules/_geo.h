/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _geo.h
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

#ifndef _VGX_VXEVAL_MODULES_GEO_H
#define _VGX_VXEVAL_MODULES_GEO_H


#define EARTH_RADIUS 6371001
#define MAX_DISTANCE (EARTH_RADIUS * M_PI)


/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __geodistance( double lat1, double lon1, double lat2, double lon2 );
static double __geoproximity( double lat1, double lon1, double lat2, double lon2 );
static void __eval_quaternary_havdist( vgx_Evaluator_t *self );
static void __eval_quaternary_geoprox( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __geodistance( double lat1, double lon1, double lat2, double lon2 ) {
/*
To be more fancy, use R appropriate to the lat/longs in question:
Earth radius at sea level is 6378.137 km (3963.191 mi) at the equator. It is 6356.752 km (3949.903 mi) at the poles and 6371.001 km (3958.756 mi) on average.
latitude B, radius R, radius at equator r1, radius at pole r2
R = sqrt( [ (r1**2 * cos(B))**2 + (r2**2 * sin(B))**2 ] / [ (r1 * cos(B))**2 + (r2 * sin(B))**2 ] )
*/

  /*
  a = sin2(dX/2) * cos(x1) * cos(x2) * sin2(dY/2)
  c = atan2( sqrt(a), sqrt(1-a) )
  d = R * c
  */

  double r_lat1 = RADIANS( lat1 );
  double r_lon1 = RADIANS( lon1 );
  double r_lat2 = RADIANS( lat2 );
  double r_lon2 = RADIANS( lon2 );

  double sdlat2 = sin( ( r_lat2 - r_lat1 ) / 2.0 );
  double sdlon2 = sin( ( r_lon2 - r_lon1 ) / 2.0 );
  double a = sdlat2 * sdlat2 + cos( r_lat1 ) * cos( r_lat2 ) * sdlon2 * sdlon2;
  double c = 2.0 * atan2( sqrt( a ), sqrt( 1.0 - a ) ); 
  double distance = EARTH_RADIUS * c;
  if( isnan( distance ) ) {
    return MAX_DISTANCE;
  }
  else {
    return distance;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __geoproximity( double lat1, double lon1, double lat2, double lon2 ) {
  return (MAX_DISTANCE - __geodistance( lat1, lon1, lat2, lon2 )) / MAX_DISTANCE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_quaternary_havdist( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t item4 = POP_ITEM( self );
  vgx_EvalStackItem_t item3 = POP_ITEM( self );
  vgx_EvalStackItem_t item2 = POP_ITEM( self );
  vgx_EvalStackItem_t *pitem1 = GET_PITEM( self );

  vgx_StackPairType_t pair2_type = PAIR_TYPE( &item4, &item3 );
  vgx_StackPairType_t pair1_type = PAIR_TYPE( &item2, pitem1 );

  double distance = MAX_DISTANCE;

  if( __is_stack_pair_type_numeric( pair2_type ) && __is_stack_pair_type_numeric( pair1_type ) ) {
    double lon2 = item4.type   == STACK_ITEM_TYPE_INTEGER ? item4.integer   : item4.real;
    double lat2 = item3.type   == STACK_ITEM_TYPE_INTEGER ? item3.integer   : item3.real;
    double lon1 = item2.type   == STACK_ITEM_TYPE_INTEGER ? item2.integer   : item2.real;
    double lat1 = pitem1->type == STACK_ITEM_TYPE_INTEGER ? pitem1->integer : pitem1->real;
    distance = __geodistance( lat1, lon1, lat2, lon2 );
  }

  SET_REAL_PITEM_VALUE( pitem1, distance );

}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_quaternary_geoprox( vgx_Evaluator_t *self ) {
  __eval_quaternary_havdist( self );
  vgx_EvalStackItem_t *px = GET_PITEM( self );
  double score = (MAX_DISTANCE - px->real) / MAX_DISTANCE;
  SET_REAL_PITEM_VALUE( px, score );
}





#endif
