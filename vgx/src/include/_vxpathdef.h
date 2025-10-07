/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxpathdef.h
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

#ifndef _VGX_VXPATHDEF_H
#define _VGX_VXPATHDEF_H

#define VGX_PATHDEF_EXT_DATA                          ".dat"
#define VGX_PATHDEF_EXT_ASCIIDOC                      ".adoc"

#define VGX_PATHDEF_REGISTRY                          "_registry"
#define VGX_PATHDEF_CLASSDEFS                         "vgxclassdefs" VGX_PATHDEF_EXT_DATA
#define VGX_PATHDEF_REGISTRY_PREFIX                   "vxgraphs"

#define VGX_PATHDEF_SYSTEM                            "_system"
#define VGX_PATHDEF_VECTOR_SETTINGS                   "vxvector" VGX_PATHDEF_EXT_DATA

#define VGX_PATHDEF_DURABLE_TXLOG                     "_txlog"

#define VGX_PATHDEF_GRAPHSTATE_FMT                    "vxgraph_[%s]"

#define VGX_PATHDEF_INSTANCE_GRAPH                    "graph"
#define VGX_PATHDEF_INSTANCE_PROPERTY                 "property"
#define VGX_PATHDEF_INSTANCE_SIMILARITY               "similarity"
#define VGX_PATHDEF_INSTANCE_VECTOR                   "vector"
#define VGX_PATHDEF_INSTANCE_DATA                     "data"
#define VGX_PATHDEF_INSTANCE_VERTEX                   "vertex"
#define VGX_PATHDEF_INSTANCE_STRING                   "string"
#define VGX_PATHDEF_INSTANCE_VIRTUAL                  "virtual"

#define VGX_PATHDEF_CODEC                             "codec"

#define VGX_PATHDEF_VIRTUAL_PROPERTIES_FMT            "prop_[%s][%08x]"

#define VGX_PATHDEF_CODEC_RELATIONSHIP_ENCODER_PREFIX "vxrelenc"
#define VGX_PATHDEF_CODEC_RELATIONSHIP_DECODER_PREFIX "vxreldec"
#define VGX_PATHDEF_CODEC_RELATIONSHIP_ENCODER_FMT    VGX_PATHDEF_CODEC_RELATIONSHIP_ENCODER_PREFIX "_[%s]"
#define VGX_PATHDEF_CODEC_RELATIONSHIP_DECODER_FMT    VGX_PATHDEF_CODEC_RELATIONSHIP_DECODER_PREFIX "_[%s]"

#define VGX_PATHDEF_CODEC_VXTYPE_ENCODER_PREFIX       "vxtypeenc"
#define VGX_PATHDEF_CODEC_VXTYPE_DECODER_PREFIX       "vxtypedec"
#define VGX_PATHDEF_CODEC_VXTYPE_ENCODER_FMT          VGX_PATHDEF_CODEC_VXTYPE_ENCODER_PREFIX "_[%s]"
#define VGX_PATHDEF_CODEC_VXTYPE_DECODER_FMT          VGX_PATHDEF_CODEC_VXTYPE_DECODER_PREFIX "_[%s]"


#define VGX_PATHDEF_CODEC_KEY_MAP_PREFIX              "vxkey"
#define VGX_PATHDEF_CODEC_VALUE_MAP_PREFIX            "vxval"

#define VGX_PATHDEF_INSTANCE_SIMILARITY_FMT           "vxsim_[%s]"

#define VGX_PATHDEF_CODEC_DIMENSION_ENCODER_PREFIX    "vxdimenc"
#define VGX_PATHDEF_CODEC_DIMENSION_DECODER_PREFIX    "vxdimdec"
#define VGX_PATHDEF_CODEC_DIMENSION_ENCODER_FMT       VGX_PATHDEF_CODEC_DIMENSION_ENCODER_PREFIX "_[%s]"
#define VGX_PATHDEF_CODEC_DIMENSION_DECODER_FMT       VGX_PATHDEF_CODEC_DIMENSION_DECODER_PREFIX "_[%s]"

#define VGX_PATHDEF_INSTANCE_DIMENSION                "dimension"
#define VGX_PATHDEF_INSTANCE_EXTERNAL                 "external"
#define VGX_PATHDEF_INSTANCE_INTERNAL                 "internal"
#define VGX_PATHDEF_INSTANCE_EUCLIDEAN                "euclidean"
#define VGX_PATHDEF_INSTANCE_MAP                      "map"


#define VGX_PATHDEF_INTERNAL_FEATURE_VECTOR_DIRNAME   (VGX_PATHDEF_INSTANCE_VECTOR "/" VGX_PATHDEF_INSTANCE_INTERNAL "/" VGX_PATHDEF_INSTANCE_MAP "/" VGX_PATHDEF_INSTANCE_DATA)
#define VGX_PATHDEF_EXTERNAL_FEATURE_VECTOR_DIRNAME   (VGX_PATHDEF_INSTANCE_VECTOR "/" VGX_PATHDEF_INSTANCE_EXTERNAL "/" VGX_PATHDEF_INSTANCE_MAP "/" VGX_PATHDEF_INSTANCE_DATA)

#define VGX_PATHDEF_INTERNAL_EUCLIDEAN_VECTOR_DIRNAME (VGX_PATHDEF_INSTANCE_VECTOR "/" VGX_PATHDEF_INSTANCE_INTERNAL "/" VGX_PATHDEF_INSTANCE_EUCLIDEAN "/" VGX_PATHDEF_INSTANCE_DATA)
#define VGX_PATHDEF_EXTERNAL_EUCLIDEAN_VECTOR_DIRNAME (VGX_PATHDEF_INSTANCE_VECTOR "/" VGX_PATHDEF_INSTANCE_EXTERNAL "/" VGX_PATHDEF_INSTANCE_EUCLIDEAN "/" VGX_PATHDEF_INSTANCE_DATA)


#define VGX_PATHDEF_VERTEX_DATA_DIRNAME               (VGX_PATHDEF_INSTANCE_GRAPH "/" VGX_PATHDEF_INSTANCE_VERTEX "/" VGX_PATHDEF_INSTANCE_DATA)

#define VGX_PATHDEF_STRING_PROPERTY_DATA_DIRNAME      (VGX_PATHDEF_INSTANCE_PROPERTY "/" VGX_PATHDEF_INSTANCE_STRING "/" VGX_PATHDEF_INSTANCE_DATA )


#endif
