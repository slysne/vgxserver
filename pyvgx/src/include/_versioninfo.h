/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  pyvgx
 * File:    _versioninfo.h
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

#ifndef VERSIONINFO_H
#define VERSIONINFO_H

/* http://semver.org/ */
/* Find a better way to manage this than modifying source code */
#define PYVGX_VERSION_MAJOR "1"
#define PYVGX_VERSION_MINOR "0"
#define PYVGX_VERSION_PATCH "0"

#define PYVGX_VERSION_PRE_REL ""

#define PYVGX_VERSION_BUILD "+build.##PYVGX_VERSION_BUILD##"
#define PYVGX_VERSION_ENV "##PYVGX_VERSION_ENV##"

#define PYVGX_VERSION_MAX_CHARS 254


#endif
