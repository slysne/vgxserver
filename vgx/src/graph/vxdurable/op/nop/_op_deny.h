/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_deny.h
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

#ifndef _VXDURABLE_OP_DENY_H
#define _VXDURABLE_OP_DENY_H



static int __execute_op_DENY( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __execute_op_DENY( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
    VERBOSE( 0x001, "DENY: %08X %s in tx=%016llx%016llx", op->op.code, op->op.name, parser->transid.H, parser->transid.L );
  } END_OPERATOR_RETURN;
}




#endif
