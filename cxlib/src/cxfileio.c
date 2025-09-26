/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxfileio.c
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

#include "cxfileio.h"
#include "cxexcept.h"

#include <string.h>

SET_EXCEPTION_MODULE( CXLIB_MSG_MOD_FILEIO );



/* low level file IO */
#if defined CXPLAT_WINDOWS_X64
#  define __OFLAG_READ_BINARY_RANDOM          (_O_BINARY | _O_RDONLY | _O_RANDOM                            )
#  define __OFLAG_READ_BINARY_SEQUENTIAL      (_O_BINARY | _O_RDONLY | _O_SEQUENTIAL                        )
#  define __OFLAG_WRITE_BINARY_RANDOM         (_O_BINARY | _O_WRONLY | _O_RANDOM     | _O_CREAT | _O_TRUNC  )
#  define __OFLAG_WRITE_BINARY_SEQUENTIAL     (_O_BINARY | _O_WRONLY | _O_SEQUENTIAL | _O_CREAT | _O_TRUNC  )
#  define __OFLAG_READWRITE_BINARY_RANDOM     (_O_BINARY | _O_RDWR   | _O_RANDOM                            )
#  define __OFLAG_READWRITE_BINARY_SEQUENTIAL (_O_BINARY | _O_RDWR   | _O_SEQUENTIAL                        )
#  define __OFLAG_APPEND_BINARY_SEQUENTIAL    (_O_BINARY | _O_RDWR   | _O_SEQUENTIAL | _O_CREAT | _O_APPEND )

#  define __SFLAG_DENY_WRITE                  _SH_DENYWR
#  define __SFLAG_DENY_READWRITE              _SH_DENYRW
#  define __OMODE_RW_PERM                     (_S_IWRITE | _S_IREAD )

#elif defined CXPLAT_LINUX_ANY
/* TODO: do we need O_EXLOCK ? */
/*       do we need O_FSYNC ?  */
#  define __OFLAG_READ       ( O_RDONLY                      | O_NOATIME )
#  define __OFLAG_WRITE      ( O_WRONLY | O_CREAT | O_TRUNC              )
#  define __OFLAG_READWRITE  ( O_RDWR   |                      O_NOATIME )
#  define __OFLAG_APPEND     ( O_RDWR   | O_CREAT | O_APPEND | O_NOATIME )
#  define __OMODE_RW_PERM    ( S_IRUSR  | S_IWUSR | S_IRGRP  | S_IWGRP | S_IROTH | S_IWOTH )

#elif defined CXPLAT_MAC_ARM64
#  define __OFLAG_READ       ( O_RDONLY                      )
#  define __OFLAG_WRITE      ( O_WRONLY | O_CREAT | O_TRUNC  )
#  define __OFLAG_READWRITE  ( O_RDWR                        )
#  define __OFLAG_APPEND     ( O_RDWR   | O_CREAT | O_APPEND )
#  define __OMODE_RW_PERM    ( S_IRUSR  | S_IWUSR | S_IRGRP  | S_IWGRP | S_IROTH | S_IWOTH )

#else
#error "Unsupported platform"
#endif




#if defined CXPLAT_WINDOWS_X64


/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_read_windows_random( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_READ_BINARY_RANDOM, __SFLAG_DENY_WRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_read_windows_sequential( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_READ_BINARY_SEQUENTIAL, __SFLAG_DENY_WRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_write_windows_random( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_WRITE_BINARY_RANDOM, __SFLAG_DENY_READWRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_write_windows_sequential( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_WRITE_BINARY_SEQUENTIAL, __SFLAG_DENY_READWRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_readwrite_windows_random( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_READWRITE_BINARY_RANDOM, __SFLAG_DENY_WRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_readwrite_windows_sequential( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_READWRITE_BINARY_SEQUENTIAL, __SFLAG_DENY_WRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_append_windows_sequential( int *fd, const char *fname ) {
  return _sopen_s( fd, fname, __OFLAG_APPEND_BINARY_SEQUENTIAL, __SFLAG_DENY_WRITE, __OMODE_RW_PERM );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static size_t __read_windows( void *destbuf, size_t elem_size, size_t elem_count, int fd ) {
  unsigned char *p = destbuf;   // current position in read buffer
  size_t target_bytes = elem_size * elem_count;
  size_t remain_bytes = target_bytes;
  unsigned int max_bytes = (unsigned int)((INT_MAX / elem_size) * elem_size);

  if( max_bytes == 0 ) {
    return 0; // elem_size is too large!
  }

  do {
    int actual;
    // Read a suitable number of bytes into buffer
    unsigned int bytes = remain_bytes > max_bytes ? max_bytes : (unsigned int)remain_bytes;
    if( (actual = _read( fd, p, bytes )) <= 0 ) {
      if( actual == 0 ) {
        break; // EOF
      }
      else {
        break; // other error
      }
    }
    //
    p += actual;
    remain_bytes -= actual;
  } while( remain_bytes != 0 );
  // Return the total number of completely read elements
  return (target_bytes - remain_bytes) / elem_size;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static size_t __write_windows( const void *srcbuf, size_t elem_size, size_t elem_count, int fd ) {
  const unsigned char *p = srcbuf;   // current position in write buffer
  size_t target_bytes = elem_size * elem_count;
  size_t remain_bytes = target_bytes;
  unsigned int max_bytes = (unsigned int)((INT_MAX / elem_size) * elem_size);

  if( max_bytes == 0 ) {
    return 0; // elem_size is too large!
  }

  do {
    int actual;
    // Write a suitable number of bytes from buffer to file
    unsigned int bytes = remain_bytes > max_bytes ? max_bytes : (unsigned int)remain_bytes;
    if( (actual = _write( fd, p, bytes )) <= 0 ) {
      break; // EOF or ERROR
    }
    //
    p += actual;
    remain_bytes -= actual;
  } while( remain_bytes != 0 );
  // Return the total number of completely written elements
  return (target_bytes - remain_bytes) / elem_size;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int __close_windows( int fd ) {
  return _close( fd );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int64_t __seek_windows( int fd, int64_t offset, int origin ) {
  return _lseeki64( fd, offset, origin );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int64_t __tell_windows( int fd ) {
  return _telli64( fd );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __truncate_windows( int fd, int64_t offset ) {
  return _chsize_s( fd, offset );
}




#define __open_read_random            __open_read_windows_random
#define __open_read_sequential        __open_read_windows_sequential
#define __open_write_random           __open_write_windows_random
#define __open_write_sequential       __open_write_windows_sequential
#define __open_readwrite_random       __open_readwrite_windows_random
#define __open_readwrite_sequential   __open_readwrite_windows_sequential
#define __open_append_sequential      __open_append_windows_sequential
#define __read                        __read_windows
#define __write                       __write_windows
#define __close                       __close_windows
#define __seek                        __seek_windows
#define __tell                        __tell_windows
#define __truncate                    __truncate_windows

#else

/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_read_unix( int *fd, const char *fname ) {
  return (*fd = open( fname, __OFLAG_READ, __OMODE_RW_PERM )) > 0 ? 0 : errno;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_write_unix( int *fd, const char *fname ) {
  return (*fd = open( fname, __OFLAG_WRITE, __OMODE_RW_PERM )) > 0 ? 0 : errno;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_readwrite_unix( int *fd, const char *fname ) {
  return (*fd = open( fname, __OFLAG_READWRITE, __OMODE_RW_PERM )) > 0 ? 0 : errno;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static errno_t __open_append_unix( int *fd, const char *fname ) {
  return (*fd = open( fname, __OFLAG_APPEND, __OMODE_RW_PERM )) > 0 ? 0 : errno;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static size_t __read_unix( void *destbuf, size_t elem_size, size_t elem_count, int fd ) {
  unsigned char *p = destbuf;   // current position in read buffer
  size_t target_bytes = elem_size * elem_count;
  size_t remain_bytes = target_bytes;
  size_t max_bytes = ((INT_MAX / elem_size) * elem_size);

  if( max_bytes == 0 ) {
    return 0; // elem_size is too large!
  }

  do {
    ssize_t actual;
    // Read a suitable number of bytes into buffer
    size_t bytes = remain_bytes > max_bytes ? max_bytes : remain_bytes;
    //TODO: push cancellation handler!
    if( (actual = read( fd, p, bytes )) <= 0 ) {
      if( actual == 0 ) {
        break; // EOF 
      }
      else {
        break; // other error
      }
    }
    // progress
    p += actual;
    remain_bytes -= actual;
  } while( remain_bytes != 0 );
  // Return the total number of completely read elements
  return (target_bytes - remain_bytes) / elem_size;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static size_t __write_unix( const void *srcbuf, size_t elem_size, size_t elem_count, int fd ) {
  const unsigned char *p = srcbuf;   // current position in write buffer
  size_t target_bytes = elem_size * elem_count;
  size_t remain_bytes = target_bytes;
  size_t max_bytes = ((INT_MAX / elem_size) * elem_size);

  if( max_bytes == 0 ) {
    return 0; // elem_size is too large!
  }

  do {
    ssize_t actual;
    // Write a suitable number of bytes from buffer to file
    size_t bytes = remain_bytes > max_bytes ? max_bytes : remain_bytes;
    //TODO: push cancellation handler!
    if( (actual = write( fd, p, bytes )) <= 0 ) {
      break; // EOF or ERROR
    }
    // progress
    p += actual;
    remain_bytes -= actual;
  } while( remain_bytes != 0 );
  // Return the total number of completely written elements
  return (target_bytes - remain_bytes) / elem_size;
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int __close_unix( int fd ) {
  return close( fd );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int64_t __seek_unix( int fd, int64_t offset, int origin ) {
  return lseek( fd, offset, origin );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int64_t __tell_unix( int fd ) {
  return lseek( fd, 0, SEEK_CUR );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
__inline static int64_t __truncate_unix( int fd, int64_t offset ) {
  return ftruncate( fd, offset ) < 0 ? errno : 0;
}


#define __open_read_random            __open_read_unix
#define __open_read_sequential        __open_read_unix
#define __open_write_random           __open_write_unix
#define __open_write_sequential       __open_write_unix
#define __open_readwrite_random       __open_readwrite_unix
#define __open_readwrite_sequential   __open_readwrite_unix
#define __open_append_sequential      __open_append_unix
#define __read                        __read_unix
#define __write                       __write_unix
#define __close                       __close_unix
#define __seek                        __seek_unix
#define __tell                        __tell_unix
#define __truncate                    __truncate_unix


#endif



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_R_RAND( int *FileDescriptor, const char *FileName ) {
  return __open_read_random( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_R_SEQ( int *FileDescriptor, const char *FileName ) {
  return __open_read_sequential( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_W_RAND( int *FileDescriptor, const char *FileName ) {
  return __open_write_random( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_W_SEQ( int *FileDescriptor, const char *FileName ) {
  return __open_write_sequential( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_RW_RAND( int *FileDescriptor, const char *FileName ) {
  return __open_readwrite_random( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_RW_SEQ( int *FileDescriptor, const char *FileName ) {
  return __open_readwrite_sequential( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t OPEN_A_SEQ( int *FileDescriptor, const char *FileName ) {
  return __open_append_sequential( FileDescriptor, FileName );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
size_t CX_READ( void *DestBuffer, size_t ElementSize, size_t Count, int FileDescriptor ) {
  return __read( DestBuffer, ElementSize, Count, FileDescriptor );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
size_t CX_WRITE( const void *SrcBuffer, size_t ElementSize, size_t Count, int FileDescriptor ) {
  return __write( SrcBuffer, ElementSize, Count, FileDescriptor );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
size_t CX_CLOSE( int FileDescriptor ) {
  return __close( FileDescriptor );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
int64_t CX_SEEK( int FileDescriptor, int64_t Offset, int Origin ) {
  return __seek( FileDescriptor, Offset, Origin );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
int64_t CX_TELL( int FileDescriptor ) {
  return __tell( FileDescriptor );
}



/*******************************************************************//**
 * 
 *
 ***********************************************************************
 */
errno_t CX_TRUNCATE( int FileDescriptor, int64_t EndOffset ) {
  return __truncate( FileDescriptor, EndOffset );
}



/*******************************************************************//**
 * Like mkdir -p.  Create the directory tree fullpath.
 * 
 * Returns  : 0 on success, -1 on failure
 ***********************************************************************
 */
int create_dirs( const char *fullpath ) {
  int retcode = 0;
  const char *cf = fullpath;
  char path[MAX_PATH+1];
  char *cp = path;
  char *prev_cp_pos = path;
  char c;
  file_attr_t file_attr;

  if( !fullpath ) {
    return 0;
  }

  XTRY {
    do {
      c = *cp = *cf++;
      if( c == '/' || c == '\\' || c == '\0' ) {
        if( cp - prev_cp_pos > 1 ) {
          *cp = '\0';  // ensure we terminate path temporarily so we can use the string
          file_attr = get_file_attr( path );

          if( file_attr == FILE_ATTR_FILE ) { // path points to a file
            CRITICAL( 0x021, "cannot create directory %s, file exists", path );
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x022 );
          }
          else if ( file_attr == FILE_ATTR_UNKNOWN ) {
            CRITICAL( 0x027, "Unknown error with path '%s'", path );
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x028 );
          }
          else if ( file_attr == FILE_ATTR_INVALID ) {
  #ifdef CXPLAT_WINDOWS_X64
            if( CreateDirectoryA( path, NULL ) == 0 ) { // error
              CRITICAL( 0x023, "CreateDirectory(%s) failed (%d)\n", path, errno );
              THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x024 );
            }
  #else
            if( mkdir( path, S_IRWXU | S_IRWXG | S_IRWXO ) == -1 ) { // error
              CRITICAL( 0x025, "mkdir(%s) failed (%d)\n", path, errno );
              THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x026 );
            }
  #endif
          }

          prev_cp_pos = cp;
          *cp = c; // restore end char to original (/ or \ if it wasn't nul)
        }
      }
      cp++;
    } while( c != '\0' );
  }
  XCATCH( errcode ) {
    retcode = -1;
  }
  XFINALLY {}

  return retcode;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
int delete_matching_files( const char *dirpath, const char *query, int64_t max_age_seconds ) {
  int ret = 0;

#ifdef CXPLAT_WINDOWS_X64

  char path[MAX_PATH+1] = {0};
  WIN32_FIND_DATA find_data = {0};
  HANDLE find_handle = NULL;

  // Current system time
  SYSTEMTIME st;
  GetSystemTime(&st);

  // Convert system time to filetime and subtract the number of cutoff seconds
  FILETIME cutoffTime;
  SystemTimeToFileTime(&st, &cutoffTime);
  ULARGE_INTEGER uli;
  uli.LowPart = cutoffTime.dwLowDateTime;
  uli.HighPart = cutoffTime.dwHighDateTime;
  uli.QuadPart -= max_age_seconds * 10'000'000ULL; // 100ns intervals
  cutoffTime.dwLowDateTime = uli.LowPart;
  cutoffTime.dwHighDateTime = uli.HighPart;


  XTRY {
    
    // Make sure the provided directory exists
    snprintf( path, MAX_PATH, "%s\\*", dirpath );
    path[MAX_PATH] = '\0'; // just in case
    if( (find_handle = FindFirstFile( path, &find_data )) == INVALID_HANDLE_VALUE ) {
      // Directory does not exist
      THROW_SILENT( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    FindClose( find_handle );
    find_handle = NULL;
    memset( &find_data, 0, sizeof( WIN32_FIND_DATA ) );

    // Set the file name filter and start iteration
    snprintf( path, MAX_PATH, "%s\\%s", dirpath, query );
    path[MAX_PATH] = '\0'; // just in case
    if( (find_handle = FindFirstFile( path, &find_data )) == INVALID_HANDLE_VALUE ) {
      // No matching files
      XBREAK;
    }

    do {
      // Skip directory match
      if( (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ) {
        continue;
      }
      // File is not old enough to be deleted
      if( CompareFileTime( &find_data.ftCreationTime, &cutoffTime ) > 0 ) {
        continue;
      }
      // Delete the file
      char delpath[MAX_PATH+1];
      snprintf( delpath, MAX_PATH, "%s\\%s", dirpath, find_data.cFileName );
      delpath[MAX_PATH] = '\0'; // just in case
      // Best effort
      DeleteFile( delpath );

    } while( FindNextFile( find_handle, &find_data ) );

    FindClose( find_handle );
    find_handle = NULL;
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( find_handle != NULL ) {
      FindClose( find_handle );
    }

  }

#else

#define SZ_ERRBUF 256
  DIR *d = NULL;
  struct dirent *current;

  // Current time in seconds
  time_t now = time(NULL);

  // Max file age in seconds
  time_t cutoff = now - max_age_seconds;

  XTRY {
    // Make sure the provided directory exists and can be accessed
    if( (d = opendir( dirpath )) == NULL ) {
      const char *error_str = NULL;
      char error_buf[ SZ_ERRBUF ] = {0};
      error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
      REASON( 0x000, "Unable to open directory '%s': %s", dirpath, error_str );
      THROW_SILENT( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    
    while( (current = readdir( d )) != NULL ) {
      // Skip unless regular file
      if( current->d_type != DT_REG ) {
        continue;
      }

      // Skip file unless matching query pattern
      if( !fnmatch( query, current->d_name, 0 ) ) {
        continue;
      }

      // Full path of file to consider
      char delpath[MAX_PATH+1];
      snprintf( delpath, MAX_PATH, "%s/%s", dirpath, current->d_name );
      delpath[MAX_PATH] = '\0'; // just in case

      // Get stat, ignore on error
      struct stat st;
      if( stat( delpath, &st ) < 0 ) {
        continue;
      }

      // File is not old enough to be deleted
      if( st.st_mtime > cutoff ) {
        continue;
      }

      // Delete the file (best effort)
      unlink( delpath );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( d != NULL ) {
      closedir( d );
    }
  }

#endif
  return ret;
}



/*******************************************************************//**
 * Platform-specific implementation of recursive directory deletion
 *
 * Returns  : 0 on success, -1 on failure
 ***********************************************************************
 */
int delete_dir_internal( const char *dirpath) {
#ifdef CXPLAT_WINDOWS_X64

  WIN32_FIND_DATA find_data;
  HANDLE find_handle = NULL;

  char buffer[2048];

  // All files in dir: *.* 
  sprintf( buffer, "%s\\*.*", dirpath );

  if( (find_handle = FindFirstFile( buffer, &find_data )) == INVALID_HANDLE_VALUE ) {
    printf( "Path not found: [%s]\n", dirpath );
    return -1;
  }

  do {
    // Find first file will always return "." and ".." as the first two directories.
    if( strcmp( find_data.cFileName, "." ) != 0 && strcmp( find_data.cFileName, ".." ) != 0 ) {
      // Build up file path using
      sprintf( buffer, "%s\\%s", dirpath, find_data.cFileName);

      // Dir found
      if( find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ) {
        if( delete_dir_internal( buffer ) < 0 ) {
          return -1;
        }
      }
      // File found
      else {
        if( remove( buffer ) != 0 ) {
          return -1;
        }
      }
    }
  } while( FindNextFile( find_handle, &find_data ) );

  FindClose( find_handle );

  if( _rmdir( dirpath ) != 0 ) {
    return -1;
  }

  return 0; // ok

#else // Linux
#define SZ_ERRBUF 256
  int retcode = 0;
  char file_path[ MAX_PATH + 1 ] = {0};
  char error_buf[ SZ_ERRBUF ] = {0};
  DIR *d = NULL;
  XTRY {
    if( (d = opendir( dirpath )) == NULL ) {
      // Unable to open directory
      const char* error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x031, "Unable to open directory '%s': %s", dirpath, error_str );
    }

    struct dirent *current;
    
    errno = 0;
    while( (current = readdir( d )) != NULL ) {
      // Skip "." and ".."
      if ( !strcmp( current->d_name, "." ) || !strcmp( current->d_name, ".." ) ) {
        continue;
      }

      snprintf( file_path, MAX_PATH, "%s/%s", dirpath, current->d_name );
     
      if( dir_exists( file_path ) ) {
        if( delete_dir_internal( file_path ) < 0 ) {
          THROW_CRITICAL_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x032, "Unable to delete directory '%s'", file_path );
        }
      }
      else if( unlink( file_path ) < 0 ) {
        const char* error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
        THROW_CRITICAL_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x033, "Unable to delete file '%s': %s", file_path, error_str );
      }
    }

    if( errno ) {
      const char* error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x034, "Unable to read directory '%s': %s", dirpath, error_str );
    }

    if( rmdir( dirpath ) < 0 && unlink( dirpath ) < 0 ) {
      const char* error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
      THROW_CRITICAL_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x035, "Unable to delete empty directory '%s': %s", dirpath, error_str);
    }
  }
  XCATCH( errcode ) {
    retcode = -1;
  }
  XFINALLY {
    if( d ) {
      closedir( d );
    }
  }

  return retcode;
#undef SZ_ERRBUF
#endif
}



/*******************************************************************//**
 * Like rm -rf.  Recursively deletes the directory.
 *
 * Returns  : 0 on success, -1 on failure
 ***********************************************************************
 */
int delete_dir( const char *path ) {
  int retcode = -1;

  // Check that the path points to a directory
  if( dir_exists( path ) ) {
    // Recursively delete the directory
    retcode = delete_dir_internal( path );
  }
  else {
    CRITICAL(0x041, "Cannot delete directory '%s', path is invalid", path);
  }

  return retcode;
}



/*******************************************************************//**
 * Platform-specific implementations of get_file_attr.
 *
 ***********************************************************************
 */
#ifdef CXPLAT_WINDOWS_X64
static file_attr_t get_file_attr_internal( const char *path ) {
  file_attr_t file_attr;
  DWORD file_attr_win;

  if ( path && strlen(path) ) {
    file_attr_win = GetFileAttributesA( path );
    if ( file_attr_win == INVALID_FILE_ATTRIBUTES ) {
      file_attr = FILE_ATTR_INVALID;
    }
    else if ( file_attr_win & FILE_ATTRIBUTE_DIRECTORY ) {
      file_attr = FILE_ATTR_DIR;
    }
    else {
      file_attr = FILE_ATTR_FILE;
    }
  }
  else {
    file_attr = FILE_ATTR_INVALID;
  }

  return file_attr;
}
#else
static file_attr_t get_file_attr_internal( const char *path ) {
  file_attr_t file_attr;
#if defined CXPLAT_LINUX_ANY
#define _STAT64 stat64
#else
#define _STAT64 stat
#endif

  struct _STAT64 fstat;

  XTRY {
    if( path && strlen(path) && _STAT64( path, &fstat ) == 0 ) {
      if ( fstat.st_mode & S_IFREG ) {
        file_attr = FILE_ATTR_FILE;
      }
      else if ( fstat.st_mode & S_IFDIR ) {
        file_attr = FILE_ATTR_DIR;
      }
      else {
        CRITICAL( 0x051, "get_file_attr_internal: unhandled mode %x", fstat.st_mode );
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x052 );
      }
    }
    else {
      file_attr = FILE_ATTR_INVALID;
    }
  }
  XCATCH( errcode ) {
    file_attr = FILE_ATTR_UNKNOWN;
  }
  XFINALLY {}

  return file_attr;
}
#endif



/*******************************************************************//**
 * Platform-independent call for getting a file's attributes.
 * Returns FILE_ATTR_UNKNOWN on error.
 ***********************************************************************
 */
file_attr_t get_file_attr( const char *path ) {
  return get_file_attr_internal( path );
}



/*******************************************************************//**
 * Return 1 if path exists and is a file, 0 otherwise.
 * 
 ***********************************************************************
 */
int file_exists( const char *path ) {
  file_attr_t file_attr = get_file_attr( path );
  if( file_attr == FILE_ATTR_FILE ) {
    return true;
  }
  else {
    return false;
  }
}



/*******************************************************************//**
 * Return 1 if path exists and is a directory, 0 otherwise.
 * 
 ***********************************************************************
 */
int dir_exists( const char *path ) {
  file_attr_t file_attr = get_file_attr( path );
  if( file_attr == FILE_ATTR_DIR ) {
    return true;
  }
  else {
    return false;
  }
}



#ifdef CXPLAT_WINDOWS_X64
#define IS_FILE_SEPARATOR( Char ) ((Char) == '/' || (Char) == '\\')
#else
#define IS_FILE_SEPARATOR( Char ) ((Char) == '/')
#endif



/*******************************************************************//**
 * Return 1 if path is absolute, 0 otherwise.
 * 
 ***********************************************************************
 */
int path_isabs( const char *path ) {
#ifdef CXPLAT_WINDOWS_X64
  if( IS_FILE_SEPARATOR( path[0] ) ) {
    return 1;
  }
  else {
    const char *p = path;
    char c;
    while( (c = *p++) != '\0' ) {
      if( c == ':' ) {
        return 1;
      }
    }
  }
  return 0;
#else
  return path[0] == '/';
#endif
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
char * get_abspath( const char *filename ) {
  char *abspath = NULL;
#ifdef CXPLAT_WINDOWS_X64
  if( (abspath = calloc( 512, 1 )) != NULL ) {
    int ret = GetFullPathNameA( filename, 511, abspath, NULL );
    if( ret == 0 || ret > 511 ) {
      free( abspath );
      return NULL;
    }
  }
#else
  abspath = realpath( filename, NULL );
#endif
  return abspath;
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
int split_path( const char *path, char **dirname, char **filename ) {
  int err = 0;

  const char *cursor = path;
  char c;

  const char *pfname = NULL;

  // Find the filename
  while( (c = *cursor++) != '\0' ) {
    // Every time we encounter a separator we update the filename pointer to
    // what comes immediately after the separator
    if( IS_FILE_SEPARATOR( c ) ) {
      pfname = cursor; // cursor is pointing one beyond the separator
    }
  }

  // No filename determined, set filename to the entire string
  if( pfname == NULL ) {
    pfname = path;
  }

  // Return of filename requested
  if( filename ) {
    size_t sz_fname = cursor - pfname; // includes 0-term
    if( (*filename = calloc( sz_fname, 1 )) != NULL ) {
      strcpy( *filename, pfname );
    }
    else {
      --err;
    }
  }

  // Return of dirname requested
  if( !err && dirname ) {
    size_t sz_dirname = pfname - path; // includes separator (if one exists)
    if( sz_dirname > 0 ) {
      if( (*dirname = calloc( sz_dirname, 1 )) != NULL ) {
        strncpy( *dirname, path, sz_dirname );
      }
    }
    // Empty string
    else {
      *dirname = calloc( 1, 1 );
    }
    // Error
    if( *dirname == NULL ) {
      --err;
      if( filename && *filename ) {
        free( *filename );
        *filename = NULL;
      }
    }
  }

  return err;

}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
int64_t file_readline( FILE *file, char *buf, size_t sz_buf ) {
  int64_t offset = CX_FTELL( file );
  // Fill buffer from file
  if( fread( buf, 1, sz_buf-1, file ) == 0 ) {
    return 0;
  }
  // Find first newline
  char *p = buf;
  char *end = p + (sz_buf-1);
  while( p < end ) {
    if( *p++ == '\n' ) {
      break;
    }
  }
  *p = '\0';
  // Number of characters read from file
  int64_t sz_line = p - buf;
  // Adjust file offset
  CX_FSEEK( file, offset + sz_line, SEEK_SET );
  // Return number of characters read from file (may not be a full line terminated by newline)
  return sz_line;
}
