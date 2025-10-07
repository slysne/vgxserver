/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxlog.h
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

#ifndef COMLIB_CXLOG_H
#define COMLIB_CXLOG_H


#include "_comlib.h"



typedef struct s_LogContext_t {
  // ------------------------------------------------
  // [Q1]
  CS_LOCK xlock;

  // ------------------------------------------------
  // [Q2.1.1]
  int fd;

  // [Q2.1.2]
  int init;
  
  // [Q2.2]
  int64_t sz;

  // [Q2.3]
  CString_t *CSTR__filename;

  // [Q2.4]
  comlib_task_t *task;

  // [Q2.5.1]
  int qready;

  // [Q2.5.2]
  int __rsv_2_5_2;

  // [Q2.6]
  QWORD __rsv_2_6;

  // [Q2.7]
  QWORD __rsv_2_7;

  // [Q2.8]
  QWORD __rsv_2_8;

  // ------------------------------------------------
  // [Q3]
  CS_LOCK qlock;

  // ------------------------------------------------
  // [Q4.1]
  Cx2tptrQueue_t *queue;

  // [Q4.2]
  QWORD __rsv_4_2;

  // [Q.3-8]
  CS_COND qwake;

  // ------------------------------------------------

} LogContext_t;



DLL_EXPORT LogContext_t * COMLIB__NewLogContext( void );
DLL_EXPORT int COMLIB__DeleteLogContext( LogContext_t **context );
DLL_EXPORT int COMLIB__OpenLog( const char *filepath, FILE **rfile, int *rfd, CString_t **CSTR__error );
DLL_EXPORT int COMLIB__LogRotate( LogContext_t *context, const char *filepath, CString_t **CSTR__error );
DLL_EXPORT int COMLIB__Log( LogContext_t *context, int64_t ns_1970, CString_t **CSTR__msg );



#endif
