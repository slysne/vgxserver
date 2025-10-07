/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    versiongen.h
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

#ifndef VERSIONGEN_H_
#define VERSIONGEN_H_

#define VERSIONGEN_XSTR( s ) VERSIONGEN_STR( s )
#define VERSIONGEN_STR( s ) #s

// Compiler detection and version
#if defined(__clang__)
    #define COMPILER_NAME "Clang"
    #define COMPILER_VERSION COMPILER_NAME " " VERSIONGEN_XSTR(__clang_major__) "." VERSIONGEN_XSTR(__clang_minor__) "." VERSIONGEN_XSTR(__clang_patchlevel__)

#elif defined(__GNUC__)
    #define COMPILER_NAME "GCC"
    #define COMPILER_VERSION COMPILER_NAME " " VERSIONGEN_XSTR(__GNUC__) "." VERSIONGEN_XSTR(__GNUC_MINOR__) "." VERSIONGEN_XSTR(__GNUC_PATCHLEVEL__)

#elif defined(_MSC_VER)
    #define COMPILER_NAME "MSVC"
    #define COMPILER_VERSION COMPILER_NAME " " VERSIONGEN_XSTR(_MSC_VER)

#else
    #define COMPILER_VERSION "Unknown Compiler"
#endif


#define GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) \
  LIB_NAME_STR " v" VERSION_STR

#define GENERATE_BUILD_TIME "" __DATE__ " " __TIME__

#define GENERATE_VERSION_INFO_EXT_STR( LIB_NAME_STR, VERSION_STR ) \
  GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) " [" GENERATE_BUILD_TIME "]"

#define GENERATE_BUILD_INFO GENERATE_BUILD_TIME " [" COMPILER_VERSION  "]"

#define GENERATE_VERSION_INFO_EXT_WITH_BUILD_STR( LIB_NAME_STR, VERSION_STR ) \
  GENERATE_VERSION_INFO_STR( LIB_NAME_STR, VERSION_STR ) " " GENERATE_BUILD_INFO

#endif /* VERSIONGEN_H_ */
