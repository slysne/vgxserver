/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_artifacts.c
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

#include "_vgx.h"
#include "_vxserver.h"
#include "generated/_vxhtml.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



static int64_t                      __decode_hexdata( BYTE **output, const char **input );

static const char *                 __subst_VERSION( CString_t **CSTR__line, const char *line );
static const char *                 __subst_CURRENTTIME( CString_t **CSTR__line, const char *line );
static const char *                 __subst_dynamic_html( char *buffer, int64_t sz_max, const char *line );
static BYTE *                       __render_artifact_lines( const char **lines, bool dynamic_subst, int64_t *rendered_sz );



DLL_HIDDEN framehash_t * vgx_server_artifacts__map = NULL;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __decode_hexdata( BYTE **output, const char **input ) {
  int64_t n_bytes = 0;

  // Count total number of output bytes represented by input hex data
  const char **cursor = input;
  while( *cursor ) {
    n_bytes += strlen( *cursor ) / 2;
    ++cursor;
  }

  // Allocate output
  if( *output ) {
    free( *output );
  }
  *output = malloc( n_bytes );
  if( *output == NULL ) {
    return -1;
  }

  // Prepare
  BYTE *dest = *output;
  BYTE *end = dest + n_bytes;

  // Decode all input strings into single output array
  cursor = input;
  while( *cursor ) {
    const char *src = *cursor++;
    BYTE b = 0;
    while( (src = hex_to_BYTE( src, &b )) != NULL && dest < end ) {
      *dest++ = b;
    }
  }

  return n_bytes;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __subst_VERSION( CString_t **CSTR__line, const char *line ) {
  if( *CSTR__line == NULL ) {
    *CSTR__line = CStringNew( line );
  }
  if( *CSTR__line != NULL ) {
    CString_t *CSTR__version = igraphinfo.Version( 1 );
    if( CSTR__version ) {
      CString_t *CSTR__version_line = CALLABLE( *CSTR__line )->Replace( *CSTR__line, "%VERSION%", CStringValue( CSTR__version ) );
      iString.Discard( CSTR__line );
      iString.Discard( &CSTR__version );
      if( CSTR__version_line ) {
        *CSTR__line = CSTR__version_line;
        return CStringValue( *CSTR__line );
      }
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __subst_CURRENTTIME( CString_t **CSTR__line, const char *line ) {
  if( *CSTR__line == NULL ) {
    *CSTR__line = CStringNew( line );
  }
  if( *CSTR__line != NULL ) {
    CString_t *CSTR__now = igraphinfo.CTime( -1, false );
    if( CSTR__now ) {
      CString_t *CSTR__now_line = CALLABLE( *CSTR__line )->Replace( *CSTR__line, "%CURRENTTIME%", CStringValue( CSTR__now ) );
      iString.Discard( CSTR__line );
      iString.Discard( &CSTR__now );
      if( CSTR__now_line ) {
        *CSTR__line = CSTR__now_line;
        return CStringValue( *CSTR__line );
      }
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __subst_dynamic_html( char *buffer, int64_t sz_max, const char *line ) {
  CString_t *CSTR__line = NULL;
  if( CharsContainsConst( line, "%VERSION%" ) ) {
    __subst_VERSION( &CSTR__line, line );
  }
  if( CharsContainsConst( line, "%CURRENTTIME%" ) ) {
    __subst_CURRENTTIME( &CSTR__line, line );
  }

  if( CSTR__line ) {
    strncpy( buffer, CStringValue( CSTR__line ), sz_max-1 );
    iString.Discard( &CSTR__line );
    return buffer;
  }
  else {
    return line;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static BYTE * __render_artifact_lines( const char **lines, bool dynamic_subst, int64_t *rendered_sz ) {
  BYTE *rendered = NULL;
  char buffer[1024];
  const char **pline = lines;
  const char *line;
  // Find out how much to malloc
  int64_t sz = 0;
  while( (line = *pline++) != NULL ) {
    if( dynamic_subst ) {
      line = __subst_dynamic_html( buffer, 1023, line );
    }
    sz += strlen( line ) + 1; // plus newline
  }
  // Populate memory
  if( (rendered = malloc( sz + 1 )) != NULL ) {
    BYTE *wp = rendered;
    pline = lines;
    while( (line = *pline++) != NULL ) {
      if( dynamic_subst ) {
        line = __subst_dynamic_html( buffer, 1023, line );
      }
      const char *rp = line;
      while( *rp != '\0' ) {
        *wp++ = *rp++;
      }
      *wp++ = '\n';
    }
    *wp = '\0';
  }
  *rendered_sz = sz;
  return rendered;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_artifacts__create( const char *prefix ) {
  int ret = 0;
  if( vgx_server_artifacts__map == NULL ) {

    CString_t *CSTR__map = NULL;

    XTRY {
      if( (CSTR__map = CStringNew( "server_artifacts" )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
      }
      vgx_mapping_spec_t map_spec = (unsigned)VGX_MAPPING_SYNCHRONIZATION_NONE | (unsigned)VGX_MAPPING_KEYTYPE_QWORD | (unsigned)VGX_MAPPING_OPTIMIZE_SPEED;
      if( (vgx_server_artifacts__map = iMapping.NewMap( NULL, CSTR__map, MAPPING_SIZE_UNLIMITED, 0, map_spec, CLASS_NONE )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
      }


      vgx_server_artifact_t *A = VGX_SERVER_ARTIFACTS;
      while( A->name != NULL && A->raw.data[0] != NULL ) {

        // Autodetect hexdata by looking at the first line
        bool is_hex = false;
        const char *first_line = A->raw.data[0];
        if( strlen(first_line) > 0 ) {
          const char *p = first_line;
          char c;
          while( (c = *p++) != '\0' ) {
            BYTE x;
            if( !hex_digit_byte( c, &x ) ) {
              break;
            }
          }
          if( c == '\0' ) {
            is_hex = true;
          }
        }

        if( is_hex ) {
          if( (A->servable.sz = __decode_hexdata( &A->servable.bytes, A->raw.data )) < 0 ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
          }
        }
        else {
          bool dynamic_subst = false;
          if( A->MediaType == MEDIA_TYPE__text_html ) {
            dynamic_subst = true;
          }
          if( (A->servable.bytes = __render_artifact_lines( A->raw.data, dynamic_subst, &A->servable.sz )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
          }
        }

        A->namehash = CharsHash64( A->name );
        if( CALLABLE( vgx_server_artifacts__map )->SetInt56( vgx_server_artifacts__map, CELL_KEY_TYPE_HASH64, A->namehash, (const int64_t)A ) != CELL_VALUE_TYPE_INTEGER ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }

        // For resources marked public also add path with prefix
        if( prefix && A->servable.public ) {
          CString_t *CSTR__prefix_path = CStringNewFormat( "%s%s", prefix, A->name );
          if( CSTR__prefix_path == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
          }
          QWORD public_hash = CStringHash64( CSTR__prefix_path );
          iString.Discard( &CSTR__prefix_path );
          if( CALLABLE( vgx_server_artifacts__map )->SetInt56( vgx_server_artifacts__map, CELL_KEY_TYPE_HASH64, public_hash, (const int64_t)A ) != CELL_VALUE_TYPE_INTEGER ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
          }
        }

        ++A;
      }

      // ok
      ret = 1;

    }
    XCATCH( errcode ) {
      vgx_server_artifacts__destroy();
      ret = -1;
    }
    XFINALLY {
    }
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_artifacts__destroy( void ) {
  if( vgx_server_artifacts__map ) {
    framehash_t *M = vgx_server_artifacts__map;
    CtptrList_t *entries = COMLIB_OBJECT_NEW_DEFAULT( CtptrList_t );
    if( entries ) {
      FRAMEHASH_DYNAMIC_PUSH_REF_LIST( &M->_dynamic, entries ) {
        int64_t n = CALLABLE( M )->GetValues( M );
        for( int64_t i=0; i<n; i++ ) {
          tptr_t tp;
          vgx_server_artifact_t *A;
          if( CALLABLE( entries )->Get( entries, i, &tp ) == 1 ) {
            if( (A = (vgx_server_artifact_t*)TPTR_GET_PTR56( &tp )) != NULL ) {
              free( A->servable.bytes );
              A->servable.bytes = NULL;
            }
          }
        }
      } FRAMEHASH_DYNAMIC_POP_REF_LIST;
      COMLIB_OBJECT_DESTROY( entries );
    }
    iMapping.DeleteMap( &vgx_server_artifacts__map );
  }
}
