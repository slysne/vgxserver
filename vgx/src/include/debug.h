/*
###################################################
#
# File:   debug.h
#
###################################################
*/

#ifndef COMLIB_DEBUG_H
#define COMLIB_DEBUG_H
#include "cxlib.h"


void comlib_set_pmesg_stream( FILE *stream );

DLL_COMLIB_PUBLIC extern FILE *comlib_null_sink;

#ifdef NDEBUG
/* macro with a variable number of arguments. 
   We use this extension here to preprocess pmesg away. */
#define pmesg(level, format, ...) ((void)0)
#else

DLL_COMLIB_PUBLIC extern void pmesg(int level, const char *format, ...);
/* print a message, if it is considered significant enough.
      Adapted from [K&R2], p. 174 */
#endif

#endif /* DEBUG_H */

