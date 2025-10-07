/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    demo_class.h
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

#ifndef COMLIB_DEMO_CLASS_H
#define COMLIB_DEMO_CLASS_H

#include "comlib.h"

struct s_DemoClass_t;

/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_DemoClass_constructor_args_t {
  QWORD a;
} DemoClass_constructor_args_t;


/*******************************************************************//**
 * STEP 1
 * Define the class methods
 ***********************************************************************
 */
typedef struct s_DemoClass_vtable_t {
  /* base methods */
  COMLIB_VTABLE_HEAD  /* This must always be the first entry in the struct */
  /* extended methods */
  QWORD (*SetValue)( struct s_DemoClass_t *self, QWORD x ); // Any number of additional methods,
  QWORD (*GetValue)( struct s_DemoClass_t *self);           // must take the class data structure as first argument
} DemoClass_vtable_t;


/*******************************************************************//**
 * STEP 2
 * Define the class data structure
 ***********************************************************************
 */
#define _DEMO_CLASS_OBJECT_ID_LENGTH 16
typedef struct s_DemoClass_t {
  /* base members */
  COMLIB_OBJECT_HEAD( DemoClass_vtable_t )  /* This must always be the first entry in the struct */
  /* extended members */
  char id[_DEMO_CLASS_OBJECT_ID_LENGTH];    // Any number of additional
  QWORD a;                                    // data members
} DemoClass_t;


/*******************************************************************//**
 * STEP 3
 * Declare the class registration functions
 ***********************************************************************
 */
void DemoClass_RegisterClass(void);     /* must be called somewhere at program startup to register the class in the framework */
void DemoClass_UnregisterClass(void);   /* may be called before program termination to perform proper clean-up */


DLL_EXPORT extern void DemoClass_DemonstrateUsage( void );

#endif 
