/*
###################################################
#
# File:   cxlib.h
#
###################################################
*/
#ifndef CXLIB_INCLUDED_H
#define CXLIB_INCLUDED_H



#include "cxid.h"
#include "cxmem.h"
#include "cxfileio.h"
#include "cxsock.h"
#include "cxaptr.h"
#include "lz4.h"

#define COMPILE_TIME_ASSERTION( expr ) {typedef char _[(expr)?1:-1];}

int sleep_nanoseconds( WAITABLE_TIMER Timer, int64_t ns );
void sleep_milliseconds( int32_t millisec );

char * uint64_to_bin( char * buf, uint64_t x );
char * uint32_to_bin( char * buf, uint32_t x );
char * uint16_to_bin( char * buf, uint16_t x );
char * uint8_to_bin( char * buf, uint8_t x );
int64_t qwstring_from_cstring( QWORD **dest, const char *src );
int64_t cstring_from_qwstring( char **dest, const QWORD *src );
bool string_list_contains( const char *probe, const char *list[] );


const char * cxlib_version( bool ext );

#endif




