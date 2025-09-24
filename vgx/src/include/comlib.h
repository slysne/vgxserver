/*
###################################################
#
# File:   comlib.h
#
###################################################
*/
#ifndef COMLIB_INCLUDED_H
#define COMLIB_INCLUDED_H

#if defined _COMLIB_EXPORT
#define DLL_COMLIB_PUBLIC DLL_EXPORT
#else
#define DLL_COMLIB_PUBLIC DLL_IMPORT
#endif


#include "objectmodel.h"
#include "cxcstring.h"
#include "cxtokenizer.h"
#include "cxthread.h"
#include "cxlog.h"
#include "messages.h"
#include "debug.h"
#include "utest.h"

#define SET_EXCEPTION_CONTEXT cxlib_set_exc_context( COMLIB_GetExceptionContext() );

#ifdef __cplusplus
extern "C" {
#endif

DLL_COMLIB_PUBLIC extern const char * comlib_version( bool ext );
DLL_COMLIB_PUBLIC extern int comlib_INIT( void );
DLL_COMLIB_PUBLIC extern int comlib_DESTROY( void );


DLL_COMLIB_PUBLIC extern char ** comlib_get_unit_test_names( void );
DLL_COMLIB_PUBLIC extern int comlib_unit_tests( const char *runonly[], const char *testdir );




#ifdef __cplusplus
}
#endif

#endif
