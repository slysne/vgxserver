/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxfileio.h
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

#ifndef CXLIB_CXFILEIO_H
#define CXLIB_CXFILEIO_H

#include "cxplat.h"
#include <limits.h>

/* low level file IO */
#if defined CXPLAT_WINDOWS_X64
#  include <share.h>
#  include <io.h>
#  include <fcntl.h>
#  include <direct.h>
#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
#  include <fcntl.h>
#  include <unistd.h>
#  include <dirent.h>
#  include <stddef.h>
#  include <fnmatch.h>
typedef int errno_t;
#else
#  error "Unsupported platform"
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef enum e_file_attr_t {
  FILE_ATTR_UNKNOWN,  // File attributes unknown (used for error cases)
  FILE_ATTR_INVALID,  // File doesn't exist
  FILE_ATTR_FILE,     // File is a file
  FILE_ATTR_DIR       // File is a directory
} file_attr_t;


/* filesystem */
extern int create_dirs( const char *fullpath );
extern int delete_dir( const char *path );
extern  int delete_matching_files( const char *dirpath, const char *query, int64_t max_age_seconds );
extern file_attr_t get_file_attr( const char *path );
extern int dir_exists( const char *path );
extern int path_isabs( const char *path );
extern char * get_abspath( const char *filename );
extern int split_path( const char *path, char **dirname, char **filename );
extern int file_exists( const char *path );
extern int64_t file_readline( FILE *file, char *buf, size_t sz_buf );

extern errno_t OPEN_R_RAND( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_R_SEQ( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_W_RAND( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_W_SEQ( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_RW_RAND( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_RW_SEQ( int *FileDescriptor, const char *FileName );
extern errno_t OPEN_A_SEQ( int *FileDescriptor, const char *FileName );

#define OPEN_R    OPEN_R_RAND
#define OPEN_W    OPEN_W_RAND
#define OPEN_RW   OPEN_RW_RAND

extern size_t CX_READ( void *DestBuffer, size_t ElementSize, size_t Count, int FileDescriptor );
extern size_t CX_WRITE( const void *SrcBuffer, size_t ElementSize, size_t Count, int FileDescriptor );
extern size_t CX_CLOSE( int FileDescriptor );
extern int64_t CX_SEEK( int FileDescriptor, int64_t Offset, int Origin );
extern int64_t CX_TELL( int FileDescriptor );
extern errno_t CX_TRUNCATE( int FileDescriptor, int64_t EndOffset );


/* buffered file IO */
#if defined (_WIN64) || defined (_WIN32)
#  define CX_FOPEN fopen
#  define CX_FDOPEN _fdopen
#  define CX_FCLOSE fclose
#  define CX_FSEEK _fseeki64_nolock
#  define CX_FTELL _ftelli64_nolock
#  define CX_FWRITE _fwrite_nolock
#  define CX_FFLUSH _fflush_nolock
#  define CX_FREAD _fread_nolock
#  define _GETPID _getpid
#else
#  define CX_FOPEN fopen
#  define CX_FDOPEN fdopen
#  define CX_FCLOSE fclose
#  define CX_FSEEK  fseek
#  define CX_FTELL  ftell
#  define CX_FWRITE fwrite
#  define CX_FFLUSH fflush
#  define CX_FREAD  fread
#  define _GETPID getpid
#endif

#ifdef __cplusplus
}
#endif

#endif
