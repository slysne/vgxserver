/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _op_null.h
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

#ifndef _VXDURABLE_OP_NULL_H
#define _VXDURABLE_OP_NULL_H



static int __parse_op_null( vgx_OperationParser_t *parser );
static int __execute_op_null( vgx_OperationParser_t *parser );
static int __retire_op_null( vgx_OperationParser_t *parser );



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int __parse_op_null( vgx_OperationParser_t *parser ) {
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __execute_op_null( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __retire_op_null( vgx_OperationParser_t *parser ) {
  BEGIN_OPERATOR( op_none, parser ) {
  } END_OPERATOR_RETURN;
}



#endif
