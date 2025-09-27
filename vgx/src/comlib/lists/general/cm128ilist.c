/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    cm128ilist.c
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

/*******************************************************************//**
 * Cm128iList_t
 * 
 ***********************************************************************
 */

#define _CSEQ_IMPLEMENTATION_LIST

#include "comlibsequence/_comlibsequence_header.h"

#define _CSEQ_IS_PRIMITIVE          0
#define _CSEQ_TYPENAME              X_COMLIB_SEQUENCE_TYPENAME( COMLIB_Cm128iList_TYPENAME )
#define _CSEQ_CONSTRUCTOR_ARGSTYPE  X_COMLIB_SEQUENCE_CONSTRUCTOR_ARGSTYPE( COMLIB_Cm128iList_TYPENAME )
#define _CSEQ_VTABLETYPE            X_COMLIB_SEQUENCE_VTABLETYPE( COMLIB_Cm128iList_TYPENAME )
#define _CSEQ_GLOBAL_VTABLE         X_COMLIB_SEQUENCE_GLOBAL_VTABLE( COMLIB_Cm128iList_TYPENAME )
#define _CSEQ_ELEMENT_TYPE          X_COMLIB_SEQUENCE_ELEMENT_TYPE( COMLIB_Cm128iList_ELEMENT_TYPE )
#define _CSEQ_REGISTER_CLASS        X_COMLIB_SEQUENCE_REGISTER_CLASS( COMLIB_Cm128iList_TYPENAME )
#define _CSEQ_UNREGISTER_CLASS      X_COMLIB_SEQUENCE_UNREGISTER_CLASS( COMLIB_Cm128iList_TYPENAME )

#define _CSEQ_DEFAULT_CAPACITY_EXP 8    // 2**8 = 256 __m128i = 1 PAGE
#define _CSEQ_MIN_CAPACITY_EXP     2    // 2**2 = 4 __m128i = 1 CL
#define _CSEQ_MAX_CAPACITY_EXP     28   // 2**28 __m128i = 4 GB arbitrary choice

#include "comlibsequence/_comlibsequence_m128i_ops.h"

#include "comlibsequence/_comlibsequence_code.h"

static int s = sizeof(Cm128iList_t);
