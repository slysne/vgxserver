/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    versiongen.h
 * Author:  Stian Lysne <...>
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

#ifndef VERSIONGEN_H_
#define VERSIONGEN_H_

#define VERSIONGEN_XSTR( s ) VERSIONGEN_STR( s )
#define VERSIONGEN_STR( s ) #s

#ifndef BUILT_BY
#define BUILT_BY local
#endif

#ifndef BUILD_NUMBER
#define BUILD_NUMBER N/A
#endif

#define GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) \
  LIB_NAME_STR " v" VERSION_STR

#define GENERATE_BUILD_TIME "" __DATE__ " " __TIME__

#define GENERATE_VERSION_INFO_EXT_STR( LIB_NAME_STR, VERSION_STR ) \
  GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) " [" GENERATE_BUILD_TIME "]"

#define GENERATE_BUILD_INFO "[date: " GENERATE_BUILD_TIME ", built by: " VERSIONGEN_XSTR( BUILT_BY ) ", build #: " VERSIONGEN_XSTR( BUILD_NUMBER ) "]"

#define GENERATE_VERSION_INFO_EXT_WITH_BUILD_STR( LIB_NAME_STR, VERSION_STR ) \
  GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) " " GENERATE_BUILD_INFO

#endif /* VERSIONGEN_H_ */
