/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    delete.c
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

#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*******************************************************************//**
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __delete_item( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  return 0; // TODO!
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_delete__delete( framehash_t * const self ) {
  framehash_processing_context_t batch_delete = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __delete_item );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &batch_delete );
  return _framehash_processor__process( &batch_delete );
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_delete.h"

DLL_HIDDEN test_descriptor_t _framehash_delete_tests[] = {
  { "delete",   __utest_framehash_delete },
  {NULL}
};
#endif
