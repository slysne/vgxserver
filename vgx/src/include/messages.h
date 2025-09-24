/*
###################################################
#
# File:   messages.h
#
###################################################
*/
#ifndef COMLIB_MESSAGES_H
#define COMLIB_MESSAGES_H


#include "moduledefs.h"
#include "cxlib.h"


#define COMLIB_MAX_ERRCNT 100


#ifdef __cplusplus
extern "C" {
#endif

void COMLIB_InitializeMessages( void );

DLL_COMLIB_PUBLIC extern cxlib_exc_context_t * COMLIB_GetExceptionContext( void );
DLL_COMLIB_PUBLIC extern FILE * COMLIB_SetOutputStream( FILE *ostream );
DLL_COMLIB_PUBLIC extern FILE * COMLIB_GetOutputStream( void );
DLL_COMLIB_PUBLIC extern void COMLIB_MuteOutputStream( void );
DLL_COMLIB_PUBLIC extern void COMLIB_UnmuteOutputStream( void );
DLL_COMLIB_PUBLIC extern uint32_t COMLIB_GetMessage( void );
DLL_COMLIB_PUBLIC extern void COMLIB_Fatal( const char *message );

#ifdef __cplusplus
}
#endif

#endif

