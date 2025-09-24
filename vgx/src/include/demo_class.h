/*
###################################################
#
# File:   demo_class.h
#
###################################################
*/

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

