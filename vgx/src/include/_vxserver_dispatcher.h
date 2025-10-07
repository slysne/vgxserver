/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxserver_dispatcher.h
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
