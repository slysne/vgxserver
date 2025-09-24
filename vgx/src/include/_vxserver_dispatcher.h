/*
###################################################
#
# File:   _vxserver_dispatcher.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VXSERVER_DISPATCHER_H
#define _VXSERVER_DISPATCHER_H

#include "_vgx.h"
#include "_vxserver.h"


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

#define __VGX_SERVER_DISPATCHER_MESSAGE( LEVEL, Code, Format, ... ) LEVEL( Code, "IO::VGX::Dispatcher: " Format, ##__VA_ARGS__ )

#define VGX_SERVER_DISPATCHER_VERBOSE( Code, Format, ... )   __VGX_SERVER_DISPATCHER_MESSAGE( VERBOSE, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_DISPATCHER_INFO( Code, Format, ... )      __VGX_SERVER_DISPATCHER_MESSAGE( INFO, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_DISPATCHER_WARNING( Code, Format, ... )   __VGX_SERVER_DISPATCHER_MESSAGE( WARN, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_DISPATCHER_REASON( Code, Format, ... )    __VGX_SERVER_DISPATCHER_MESSAGE( REASON, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_DISPATCHER_CRITICAL( Code, Format, ... )  __VGX_SERVER_DISPATCHER_MESSAGE( CRITICAL, Code, Format, ##__VA_ARGS__ )
#define VGX_SERVER_DISPATCHER_FATAL( Code, Format, ... )     __VGX_SERVER_DISPATCHER_MESSAGE( FATAL, Code, Format, ##__VA_ARGS__ )







#endif
