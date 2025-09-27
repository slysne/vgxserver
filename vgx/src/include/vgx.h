/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx.h
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

#ifndef VGX_VGX_H
#define VGX_VGX_H

#if defined CXPLAT_WINDOWS_X64
#pragma comment( lib, "Ws2_32.lib" )
#elif defined __GCC__
#endif

#include "comlib.h"
#include "cxmalloc.h"
#include "vxdlldef.h"


#ifdef __cplusplus
extern "C" {
#endif

#include "vxbase.h"
#include "vxvertexdefs.h"
#include "vxgraph.h"
#include "vxapiservice.h"




DLL_VISIBLE extern vgx_context_t * vgx_INIT( const char *sysroot );
DLL_VISIBLE extern void vgx_DESTROY( vgx_context_t **context );


DLL_VISIBLE extern comlib_object_typeinfo_t _vgx_Graph_t_typeinfo;
DLL_VISIBLE extern comlib_object_typeinfo_t _vgx_Vertex_t_typeinfo;
DLL_VISIBLE extern comlib_object_typeinfo_t _vgx_Vector_t_typeinfo;

DLL_VISIBLE extern vgx_IOperation_t iOperation;

DLL_VISIBLE extern vgx_IVGXServer_t iVGXServer;

DLL_VISIBLE extern vgx_IArcVector_t iarcvector;

DLL_VISIBLE extern vgx_ArcFilterFunction_t arcfilterfunc;
DLL_VISIBLE extern vgx_VertexFilterFunction_t vertexfilterfunc;

DLL_VISIBLE extern vgx_PredicatorMatchFunction_t predmatchfunc;
DLL_VISIBLE extern vgx_VertexMatchFunction_t vtxmatchfunc;

DLL_VISIBLE extern vgx_IArcFilter_t iArcFilter;
DLL_VISIBLE extern vgx_IVertexFilter_t iVertexFilter;

DLL_VISIBLE extern vgx_IVGXProfile_t iVGXProfile;




DLL_VISIBLE extern vgx_IEnumerator_t iEnumerator_OPEN;
DLL_VISIBLE extern vgx_IEnumerator_t iEnumerator_CS;
DLL_VISIBLE extern vgx_IString_t iString;
DLL_VISIBLE extern vgx_IVertex_t iVertex;
DLL_VISIBLE extern vgx_ICache_t iCache;
DLL_VISIBLE extern vgx_IRelation_t iRelation;
DLL_VISIBLE extern vgx_IGraphInfo_t igraphinfo;
DLL_VISIBLE extern vgx_IGraphFactory_t igraphfactory;
DLL_VISIBLE extern vgx_ISystem_t iSystem;
DLL_VISIBLE extern vgx_IURI_t iURI;
DLL_VISIBLE extern vgx_IGraphEvent_t iGraphEvent;
DLL_VISIBLE extern vgx_IMapping_t iMapping;
DLL_VISIBLE extern vgx_IGraphQuery_t iGraphQuery;
DLL_VISIBLE extern vgx_IVertexCondition_t iVertexCondition;
DLL_VISIBLE extern vgx_IVertexProperty_t iVertexProperty;
DLL_VISIBLE extern vgx_IRankingCondition_t iRankingCondition;
DLL_VISIBLE extern vgx_IArcCondition_t iArcCondition;
DLL_VISIBLE extern vgx_IArcConditionSet_t iArcConditionSet;
DLL_VISIBLE extern vgx_IGraphResponse_t iGraphResponse;
DLL_VISIBLE extern vgx_IEvaluator_t iEvaluator;
DLL_VISIBLE extern int vgx_GRAPH_INIT( void );
DLL_VISIBLE extern void vgx_GRAPH_DESTROY( void );
DLL_VISIBLE extern int vgx_unit_tests( const char *runonly[], const char *testdir );
DLL_VISIBLE extern char ** vgx_get_unit_test_names( void );
DLL_VISIBLE extern test_descriptor_set_t * vgx_get_unit_test_definitions( void );




#define vgx_Graph_t_CheckExact( ObjectPtr )   COMLIB_OBJECT_TYPEMATCH( ObjectPtr, _vgx_Graph_t_typeinfo )
#define vgx_Vertex_t_CheckExact( ObjectPtr )  COMLIB_OBJECT_TYPEMATCH( ObjectPtr, _vgx_Vertex_t_typeinfo )
#define vgx_Vector_t_CheckExact( ObjectPtr )  COMLIB_OBJECT_TYPEMATCH( ObjectPtr, _vgx_Vector_t_typeinfo )

#define vgx_CheckVertex( ObjectPtr ) ((ObjectPtr) && vgx_Vertex_t_CheckExact( ObjectPtr ) ? (vgx_Vertex_t*)(ObjectPtr) : NULL)


#ifdef __cplusplus
}
#endif


#endif
