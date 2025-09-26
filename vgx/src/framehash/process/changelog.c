/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  framehash
 * File:    changelog.c
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

#include "_framehash.h"
#include "_cxmalloc.h"

DISABLE_WARNING_NUMBER( 4206 )
#ifdef FRAMEHASH_CHANGELOG
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



static const QWORD g_start_delim = 0xCCCCCCCCCCCCCCCCULL;
static const QWORD g_end_delim   = 0xEEEEEEEEEEEEEEEEULL;


typedef union __u_change_persist_header_t {
  __m256i data[4]; // 16 qwords
  struct {
    // 1
    QWORD start_delim;  // START OF HEADER
    QWORD seq;          // Changelog sequence number
    QWORD t0;           // Creation time of changelog
    QWORD t1;           // End time of changelog
    QWORD __rsv1_5;
    QWORD __rsv1_6;
    QWORD __rsv1_7;
    QWORD __rsv1_8;
    // 2
    QWORD n_obj;        // Number of mapped objects after all operations in changelog
    QWORD n_ops;        // Number of operations in changelog
    QWORD n_add;        // Number of add/update operations in changelog
    QWORD n_del;        // Number of delete operations in changelog
    QWORD __rsv2_5;     //
    QWORD __rsv2_6;     //
    QWORD __rsv2_7;     //
    QWORD end_delim;    // END OF HEADER
  };
} __change_persist_header_t;



typedef union __u_change_persist_terminator_t {
  __m256i data; // 4 qwords
  struct {
    objectid_t obid;
    QWORD zero;
    QWORD end_delim;
  };
} __change_persist_terminator_t;



typedef struct __s_change_persist_t {
  __change_persist_header_t header;
  Cm256iQueue_t *queue;
  int64_t n_consumed;
  CString_t *CSTR__path;
  int fileno;
} __change_persist_t;




/**************************************************************************//**
 * GET_INIT_HEADER
 *
 ******************************************************************************
 */
static __change_persist_header_t GET_INIT_HEADER( framehash_t *self ) {
  __change_persist_header_t header = {0};
  header.start_delim = g_start_delim;
  header.end_delim = g_end_delim;
  DYNAMIC_LOCK( &self->_dynamic ) {
    header.t0 = __MILLISECONDS_SINCE_1970();
    header.seq = self->changelog.seq_end;
  } DYNAMIC_RELEASE;
  return header;
}




/**************************************************************************//**
 * GET_TERM
 *
 ******************************************************************************
 */
static __change_persist_terminator_t GET_TERM( framehash_t *self ) {
  __change_persist_terminator_t term = {0};
  idcpy( &term.obid, &self->obid );
  term.end_delim = g_end_delim;
 return term;
}



static int __start_changelog_monitor( framehash_t *self, bool async );
static int __stop_changelog_monitor( framehash_t *self, bool async );

static void __del_persist( framehash_t *self, __change_persist_t **ptrP );
static __change_persist_t * __new_persist( framehash_t *self );

static int64_t __consume( framehash_t *self, __change_persist_t **ptrP );
static int64_t __flush( __change_persist_t *P );
static int __stage( framehash_t *self, __change_persist_t **ptrP );


static const int64_t changelog_size = 65536; // 32*65536 = 2MB 

static const char *changelog_perm_suffix = "delta";
static const char *changelog_open_suffix = "delta~";



typedef union __u_change_operation_t {
  __m256i data;
  struct {
    union {
      __m128i bits;
      // for type HASH128
      objectid_t hash128;
      // for type HASH64
      struct {
        QWORD __na;
        shortid_t shortid;
      } hash64;
      // for type PLAIN64
      struct {
        QWORD plain;
        shortid_t shortid;
      } plain64;
    } key;
    union {
      QWORD qword;
      int64_t integer;
      uint64_t uinteger;
      double real;
      cxmalloc_handle_t handle;
    } value;
    framehash_keytype_t ktype;
    framehash_valuetype_t vtype;
  };
} __change_operation_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __new_changelog_name( const char *dirname, const char *basename, int seq, bool permanent ) {
  const char *suffix = permanent ? changelog_perm_suffix : changelog_open_suffix;
  return CStringNewFormat( "%s/%s_[%08x].%s", dirname, basename, seq, suffix );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * __get_permanent_name( CString_t *CSTR__delta_name, char *buffer ) {
  if( CALLABLE( CSTR__delta_name )->EndsWith( CSTR__delta_name, changelog_open_suffix ) ) {
    // The full temporary path with suffix
    const char *str = CStringValue( CSTR__delta_name );
    char *p = buffer;
    strcpy( p, str );
    // Advance pointer to the start of the suffix
    p += CStringLength( CSTR__delta_name ) - strlen( changelog_open_suffix );
    // Patch in the permanent suffix instead
    strcpy( p, changelog_perm_suffix );
    return buffer;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * _framehash_changelog__emit_operation
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_changelog__emit_operation( framehash_t *self, framehash_context_t *context ) {
  // Changelogged object type
  if( context->ktype == CELL_KEY_TYPE_HASH128 ) {
    __change_operation_t op = {
      .key.hash128 = *context->obid,
      .ktype = CELL_KEY_TYPE_HASH128,
      .vtype = context->vtype
    };
    // Create/Modify
    if( op.vtype == CELL_VALUE_TYPE_OBJECT128 ) {
      comlib_object_t *obj = context->value.pobject;
      uint8_t obclass = COMLIB_OBJECT_TYPEINFO( obj ).tp_class;
      if( obclass == self->changelog.obclass ) {
        op.value.handle = _cxmalloc_object_as_handle( obj );
        op.value.handle.objclass = obclass;
      }
      else {
        return -1;
      }
    }
    // Delete
    else {
      op.value.qword = 0;
    }


    DYNAMIC_LOCK( &self->_dynamic ) {
      Cm256iQueue_t *A = self->changelog.Qapi;
      CALLABLE( A )->AppendNolock( A, &op.data );
      if( !self->changelog.state.__running ) {
        __start_changelog_monitor( self, true );
      }
    } DYNAMIC_RELEASE;

  }
  // TODO: Add support for non-objects
  else {
    return -1;
  }

  return 0;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__init( framehash_t *self, object_class_t obclass ) {
  // Enable changelog for specific object class
  if( obclass != CLASS_NONE ) {
    // COMLIB object class to stream changes for
    self->changelog.obclass = obclass;
    // API queue
    self->changelog.Qapi = Cm256iQueueNew( changelog_size );
    // Internal queue
    self->changelog.Qmon = Cm256iQueueNew( changelog_size );
    // Enable
    if( self->changelog.Qapi && self->changelog.Qmon ) {
      self->changelog.enable = 1;
      return 1;
    }
    else {
      _framehash_changelog__destroy( self );
      return -1;
    }
  }
  else {
    return 0;
  }
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__destroy( framehash_t *self ) {
  if( self->changelog.Qapi ) {
    COMLIB_OBJECT_DESTROY( self->changelog.Qapi );
    self->changelog.Qapi = NULL;
  }
  if( self->changelog.Qmon ) {
    COMLIB_OBJECT_DESTROY( self->changelog.Qmon );
    self->changelog.Qmon = NULL;
  }
  self->changelog.obclass = CLASS_NONE;
  self->changelog.enable = 0;
  // Remove all uncommitted logs if any
  if( self->_CSTR__basename && self->_CSTR__dirname ) {
    const char *basename = CStringValue( self->_CSTR__basename );
    const char *dirname = CStringValue( self->_CSTR__dirname );
    for( int seq=self->changelog.seq_commit+1; seq<self->changelog.seq_end; seq++ ) {
      CString_t *CSTR__delta = __new_changelog_name( dirname, basename, seq, true );
      if( CSTR__delta ) {
        const char *delta_path = CStringValue( CSTR__delta );
        if( file_exists( delta_path ) ) {
          if( remove( delta_path ) != 0 ) {
            WARN( 0x001, "Failed to remove uncommitted changelog: %s", delta_path );
          }
        }
        CStringDelete( CSTR__delta );
      }
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__start( framehash_t *self ) {
  int ret = 0;

  XTRY {
    if( __start_changelog_monitor( self, false ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC01 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;

}


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__suspend( framehash_t *self ) {
  return __stop_changelog_monitor( self, false );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__resume( framehash_t *self ) {
  if( __start_changelog_monitor( self, false ) < 0 ) {
    return -1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__remove( framehash_t *self, int seq ) {
  const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;
  CString_t *CSTR__changelog = NULL;
  if( masterpath ) {
    const char *dirname = CStringValue( self->_CSTR__dirname );
    const char *basename = CStringValue( self->_CSTR__basename );
    if( (CSTR__changelog = __new_changelog_name( dirname, basename, seq, true )) == NULL ) {
      return -1;
    }
    const char *changelog = CStringValue( CSTR__changelog );
    if( file_exists( changelog ) ) {
      if( remove( changelog ) != 0 ) {
        return -1;
      }
    }
    CStringDelete( CSTR__changelog );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static comlib_object_t * __try_get_valid_object_from_handle( cxmalloc_family_t *family, __change_operation_t *op ) {
  comlib_object_t *obj = (comlib_object_t*)CALLABLE( family )->HandleAsObjectNolock( family, op->value.handle );
  if( obj && _cxmalloc_is_object_active( obj ) && idmatch( (objectid_t*)&CXMALLOC_META_FROM_OBJECT( obj )->M, &op->key.hash128 ) ) {
    return obj;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __decref_destructor( comlib_object_t *self ) {
  _cxmalloc_linehead_from_object( self )->data.refc--;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __process_delete_operation( framehash_t *self, __change_operation_t *op ) {
  // ---------------------
  // REMOVE INDEXED OBJECT
  // ---------------------
  comlib_object_t *obj;
  framehash_valuetype_t vtype = CALLABLE( self )->Get( self, op->ktype, &op->key.hash128, &obj );
  if( vtype == CELL_VALUE_TYPE_OBJECT128 || vtype == CELL_VALUE_TYPE_MEMBER ) {
    // Delete from index (SPECIAL DECREF DESTRUCTOR WILL BE CALLED BECAUSE WE PATCHED THE VTABLE EARLIER)
    // If item is membership it means the object does not exist in allocator and has been deleted, so we also delete here.
    if( CALLABLE( self )->Delete( self, op->ktype, &op->key.hash128 ) == vtype ) {
      return 1; // ok
    }
    else {
      return -1; // delete error
    }
  }
  // ------------------
  // OBJECT NOT INDEXED
  // ------------------
  else {
    // Count operation manually (since nothing is deleted when object not found in map)
    self->_opcnt++;
    return 0; // ok
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __process_set_operation( framehash_t *self, __change_operation_t *op, cxmalloc_family_t *allocator ) {
  comlib_object_t *obj128 = __try_get_valid_object_from_handle( allocator, op );
  // ------------
  // VALID OBJECT
  // ------------
  if( obj128 ) {
    // -----------
    // NOT INDEXED
    // -----------
    // Handle references a valid object with the correct identifier.
    // Own a reference and add to index if not already indexed.
    if( CALLABLE( self )->HasObj128Nolock( self, &op->key.hash128 ) != CELL_VALUE_TYPE_OBJECT128 ) {
      if( CALLABLE( self )->SetObj128Nolock( self, &op->key.hash128, obj128 ) == CELL_VALUE_TYPE_OBJECT128 ) {
        // LOW-LEVEL DIRECT INCREF EMULATING "NORMAL" DESERIALIZATION OF OBJECTS
        _cxmalloc_linehead_from_object( obj128 )->data.refc++;
        return 1; // ok
      }
      else {
        return -1; // set error
      }
    }
    // ---------------
    // ALREADY INDEXED
    // ---------------
    else {
      // Count operation manually (since we can't call the Set method)
      self->_opcnt++; 
      return 0; // ok
    }
  }
  // --------------
  // INVALID OBJECT
  // --------------
  // The handle for this operation is the reference that was current when this object
  // was set. It is not necessarily valid for the allocator end state which we are using here.
  // The handle could reference a non-existent object, or it could reference a different object.
  // Both cases are short-circuited here.
  else {
    // Count operation manually (since we can't call the Set method)
    self->_opcnt++; 
    return 0; // ok
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__apply( framehash_t *self ) {
  int ret = 0;

  int start = self->changelog.seq_start;
  int end = self->changelog.seq_end;

  const char *masterpath = self->_CSTR__masterpath ? CStringValue( self->_CSTR__masterpath ) : NULL;

  CString_t *CSTR__changelog = NULL;
  
  // Only a single object class is supported for changelog.
  // We extract vtable for this class and temporarily disable
  // object destructors since we have to manipulate the object
  // allocator and refcounts directly.
  // Object to be indexed
  // The (only) object class to expect in changelogs
  object_class_t obclass = self->changelog.obclass;
  // The object class vtable
  comlib_object_vtable_t *iobj = COMLIB_GetClassVTable( obclass );
  // Get the allocator family for this object class
  cxmalloc_family_t *obj_allocator = (cxmalloc_family_t*)iobj->vm_allocator( NULL );
  
  // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  // TEMPORARILY DETACH THE DESTRUCTOR FROM THIS CLASS TO PREVENT FRAMEHASH FROM RUNNING THE DESTRUCTOR WHEN DELETING
  // THE DESTRUCTOR WILL BE RE-ATTACHED BEFORE LEAVING THIS FUNCTION
  f_object_destructor_t obj_destroy = iobj->vm_destroy;
  // Temporarily replace the destructor with a local version that only updates the refcount
  iobj->vm_destroy = __decref_destructor;

  int fd = 0;
  errno_t err;

  __change_operation_t *operations = NULL;
  int64_t n_max_operations = 0;

  XTRY {
    if( masterpath ) {
      const char *dirname = CStringValue( self->_CSTR__dirname );
      const char *basename = CStringValue( self->_CSTR__basename );
      const char *changelog = NULL;
      __change_persist_header_t header;
      __change_persist_terminator_t term;
      int64_t delta_opcnt = 0;
      int64_t last_nobj = self->_nobj;

      // Allocate operations buffer
      if( (operations = calloc( changelog_size, sizeof( __change_operation_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0xC11 );
      }
      n_max_operations = changelog_size;

      // --------------------
      // Apply all changelogs
      // --------------------
      for( int seq=start; seq<end; seq++ ) {

        int64_t n_ops = 0;
        int64_t n_del = 0;
        int64_t n_add = 0;

        // Get changelog filepath
        if( (CSTR__changelog = __new_changelog_name( dirname, basename, seq, true )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0xC13 );
        }
        changelog = CStringValue( CSTR__changelog );

        // Verify changelog exists
        if( !file_exists( changelog ) ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0xC14, "Missing expected changelog: '%s'", changelog );
        }
        
        // Open changelog
        if( (err = OPEN_R_SEQ( &fd, changelog )) != 0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0xC15, "Changelog file error: %d %s", err, strerror( err ) );
        }

        // Read header
        if( CX_READ( &header, sizeof( __change_persist_header_t ), 1, fd ) != 1 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC16, "Changelog error: invalid header size" );
        }

        // Verify integrity of header
        if( header.start_delim != g_start_delim || header.end_delim != g_end_delim ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC17, "Changelog error: invalid header delimiters" );
        }
        if( header.seq != (QWORD)seq ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC18, "Changelog error: unexpected sequence number %llu (expected %d)", header.seq, seq );
        }

        // Number of change operations
        n_ops = header.n_ops;

        // Oversized changelog for some reason?
        if( n_ops > n_max_operations ) {
          free( operations );
          n_max_operations = n_ops;
          if( (operations = calloc( n_max_operations, sizeof( __change_operation_t ) )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_MEMORY, 0xC1A );
          }
        }

        // Read all operations from file into memory
        size_t rops;
        if( (rops = CX_READ( operations, sizeof( __change_operation_t ), header.n_ops, fd )) != header.n_ops ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC1B, "Changelog error: incorrect operation count %llu (expected %llu)", rops, header.n_ops );
        }

        // Read terminator
        if( CX_READ( &term, sizeof( __change_persist_terminator_t ), 1, fd ) != 1 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC1C, "Changelog error: invalid terminator size" );
        }

        // Verify terminator
        if( term.end_delim != g_end_delim || term.zero != 0 || !idmatch( &term.obid, &self->obid ) ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC1D, "Changelog error: invalid terminator delimiters" );
        }

        // Apply all changes in input to map
        // Assume all items are OBJECT128 of the same class
        INFO( 0xC1E, "Applying changelog: '%s'", changelog );
        __change_operation_t *op_cur = operations;
        __change_operation_t *op_end = operations + n_ops;
        __change_operation_t *op;
        while( (op = op_cur++) < op_end ) {
          // ======
          // Delete
          // ======
          if( op->value.qword == 0 ) {
            if( __process_delete_operation( self, op ) >= 0 ) {
              ++n_del;
              continue; // ok
            }
          }
          
          // ==========
          // Set/Update
          // ==========
          if( op->value.handle.objclass == obclass ) {
            if( __process_set_operation( self, op, obj_allocator ) >= 0 ) {
              ++n_add;
              continue;
            }
          }

          // Never here!
          THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0xC1F, "Changelog error: bad operation [%016llX%016llX %08X %08X %016llX]", op->key.hash128.H, op->key.hash128.L, op->ktype, op->vtype, op->value.qword );
        }

        // Verify operation counts
        if( header.n_add != (QWORD)n_add || header.n_del != (QWORD)n_del ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC20, "Changelog error: incorrect operation counts add=%lld (expected %llu) del=%lld (expected %llu)", n_add, header.n_add, n_del, header.n_del );
        }

        // Discard changelog name
        CStringDelete( CSTR__changelog );
        CSTR__changelog = NULL;

        // Close changelog
        CX_CLOSE( fd );
        fd = 0;

        // Update last object count expected by changelog header
        last_nobj = header.n_obj;
      }

      // ------------------------
      // Verify updated framehash
      // ------------------------

      // Verify state of map after applying all changelogs
      if( last_nobj != self->_nobj ) {
        THROW_ERROR_MESSAGE( CXLIB_ERR_CORRUPTION, 0xC21, "Changelog error: incorrect object count %lld after delta applied (expected %llu)", self->_nobj, last_nobj );
      }

      if( (delta_opcnt = _framehash_serialization__validate_applied_changelog( self )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0xC22 );
      }

      INFO( 0xC23, "Applied %llu incremental operations to '%s'", delta_opcnt, basename );

    }
    else {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC24 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    if( CSTR__changelog ) {
      CStringDelete( CSTR__changelog );
    }
    if( __FD_VALID( fd ) ) {
      CX_CLOSE( fd );
    }
    if( operations ) {
      free( operations );
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // RESTORE THE CLASS VTABLE TO ITS ORIGINAL STATE
    iobj->vm_destroy = obj_destroy;
  }


  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_changelog__end( framehash_t *self ) {
  return __stop_changelog_monitor( self, false );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __del_persist( framehash_t *self, __change_persist_t **ptrP ) {
  char staged_name[MAX_PATH+1];
  if( *ptrP ) {
    __change_persist_t *P = *ptrP;
    const char *path = P->CSTR__path ? CStringValue( P->CSTR__path ) : NULL;

    // Destroy the output queue
    if( P->queue ) {
      COMLIB_OBJECT_DESTROY( P->queue );
    }

    // Close the output file
    if( __FD_VALID( P->fileno ) ) {
      CX_CLOSE( P->fileno );
    }

    // Changelog is empty, delete the file from disk and roll back end seq point
    if( path && P->n_consumed == 0 ) {
      if( file_exists( path ) ) {
        if( remove( path ) != 0 ) {
          WARN( 0x001, "Failed to remove empty changelog: %s", path );
        }
        DYNAMIC_LOCK( &self->_dynamic ) {
          self->changelog.seq_end--;
        } DYNAMIC_RELEASE;
      }
    }

    // Clean up and convert file name if needed
    if( P->CSTR__path ) {
      // Rename temporary name to permanent if current path is temporary
      if( __get_permanent_name( P->CSTR__path, staged_name ) ) {
        // Remove any previous stale destination file
        if( file_exists( staged_name ) ) {
          remove( staged_name );
        }
        // Move the file to its staged name
        if( path && rename( path, staged_name ) != 0 ) {
          CRITICAL( 0x002, "Failed to stage changelog for commit: %s", path );
        }
      }
      CStringDelete( P->CSTR__path );
    }

    free( P );
    *ptrP = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __change_persist_t * __new_persist( framehash_t *self ) {
  __change_persist_t *P = NULL;
  errno_t err;
  XTRY {
    // Allocate persist
    if( (P = calloc( 1, sizeof( __change_persist_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0xC31 );
    }
    P->header = GET_INIT_HEADER( self );
    P->n_consumed = 0;

    // Advance end seq point
    DYNAMIC_LOCK( &self->_dynamic ) {
      self->changelog.seq_end++;
    } DYNAMIC_RELEASE;

    // Get changelong filename and open file
    const char *dirname = CStringValue( self->_CSTR__dirname );
    const char *basename = CStringValue( self->_CSTR__basename );
    if( (P->CSTR__path = __new_changelog_name( dirname, basename, (int)P->header.seq, false )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC32 );
    }
    const char *delta_path = CStringValue( P->CSTR__path );
    if( (err = OPEN_W_SEQ( &P->fileno, delta_path )) != 0 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0xC33 );
    }

    // Write initial header to output (will be completed on changelog close)
    if( CX_WRITE( &P->header, sizeof( __change_persist_header_t ), 1, P->fileno ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0xC34 );
    }

    // Create the output queue and attach changelog file
    if( (P->queue = Cm256iQueueNew( changelog_size )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC35 );
    }
    if( CALLABLE( P->queue )->AttachOutputDescriptor( P->queue, (short)P->fileno ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0xC36 );
    }
  }
  XCATCH( errcode ) {
    __del_persist( self, &P );
  }
  XFINALLY {
  }

  return P;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stage( framehash_t *self, __change_persist_t **ptrP ) {
  int ret = 0;
  __change_persist_t *P = *ptrP;

  XTRY {
    // Update header
    __change_persist_header_t *header = &P->header;
    DYNAMIC_LOCK( &self->_dynamic ) {
      header->t1 = __MILLISECONDS_SINCE_1970();
      header->n_obj = self->_nobj;
    } DYNAMIC_RELEASE;

    // Flush remaining changes to file
    Cm256iQueue_t *Q = P->queue;
    if( CALLABLE( Q )->FlushNolock( Q ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC41 );
    }

    // Write updated header to file
    int fd = P->fileno;
    CX_SEEK( fd, 0, SEEK_SET );
    if( CX_WRITE( header, sizeof( __change_persist_header_t ), 1, fd ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC42 );
    }

    // Terminate
    __change_persist_terminator_t term = GET_TERM( self );
    CX_SEEK( fd, 0, SEEK_END );
    if( CX_WRITE( &term, sizeof( __change_persist_terminator_t ), 1, fd ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0xC43 );
    }
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    // Detach output (close file), destroy queue and free all memory
    // If changelog was empty (no delta), the file is also deleted and end seq point rolled back
    __del_persist( self, ptrP );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __consume( framehash_t *self, __change_persist_t **ptrP ) {
  static const int64_t max_persist_size = 1LL << 21;
  Cm256iQueue_t *B;
  Cm256iQueue_vtable_t *iQ = (Cm256iQueue_vtable_t*)COMLIB_CLASS_VTABLE( Cm256iQueue_t );

  int64_t n_api = 0;
  int64_t n_mon = 0;

  DYNAMIC_LOCK( &self->_dynamic ) {
    n_api = iQ->Length( self->changelog.Qapi );
    n_mon = iQ->Length( self->changelog.Qmon );

    // Continue consuming non-empty mon queue (should never happen but be safe)
    if( n_mon > 0 ) {
      B = self->changelog.Qmon;
    }
    // Swap the api/mon queues so we can operate safely without lock
    else {
      // We're going to work on the current API queue below
      B = self->changelog.Qapi;
      // New API will be the previous (now empty) monitor queue
      self->changelog.Qapi = self->changelog.Qmon;
      // Complete the swap
      self->changelog.Qmon = B;
    }
  } DYNAMIC_RELEASE;

  if( n_mon + n_api == 0 ) {
    return 0;
  }
  else {
    int64_t n_remain = 0;
    do {
      // Current persist output
      __change_persist_t *P = *ptrP;
      
      // Establish new output buffer if needed
      if( P == NULL && (P = *ptrP = __new_persist( self )) == NULL ) {
        return -1;
      }

      // Upper number of changes that can fit in current persist output
      int64_t limit = max_persist_size - P->n_consumed;

      // Number of changes that need to be persisted
      int64_t sz_buffer = iQ->Length( B );

      // Number of changes to transfer to current persist output
      int64_t n_absorb = sz_buffer > limit ? limit : sz_buffer;

      // Number of changes that will not fit in current persist output
      n_remain = sz_buffer - n_absorb;

      // Now transfer recent changes to persist queue and update counts
      Cm256iQueue_t *Q = P->queue;
      __change_persist_header_t *header = &P->header;
      for( int64_t n=0; n<n_absorb; n++ ) {
        __change_operation_t op;
        if( iQ->NextNolock( B, &op.data ) == 1 ) {
          // Delete
          if( op.value.qword == 0 ) {
            header->n_del++;
          }
          // Add/Update
          else {
            header->n_add++;
          }
          if( iQ->AppendNolock( Q, &op.data ) == 1 ) {
            continue;
          }
        }
        // Never here
        return -1;
      }
      header->n_ops += n_absorb;
      P->n_consumed += n_absorb;

      // All recent changes consumed by current output
      if( n_remain == 0 ) {
        return P->n_consumed;
      }
      // Current output full, finalize current persist, create next and continue
      else {
        // Finalize current changelog to disk
        __stage( self, ptrP );
      }
    } while( n_remain > 0 );

    return 0;

  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __flush( __change_persist_t *P ) {
  Cm256iQueue_t *Q = P->queue;
  int64_t sz_persist = CALLABLE( Q )->Length( Q );
  if( sz_persist > 0 ) {
    return CALLABLE( Q )->FlushNolock( Q );
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_THREAD_FUNCTION( __changelog_entrypoint, "framehash_changelog/", framehash_t, parent ) {
  SET_CURRENT_THREAD_LABEL( "fhash_changelog" );
  framehash_t *self = parent;

  if( self->_CSTR__basename ) {
    APPEND_THREAD_NAME( CStringValue( self->_CSTR__basename ) );
  }

  // 5 seconds max between comsumption of input changes to the internal processor
  static const int64_t consume_delay_ms = 5000;

  // 15 seconds max between flush of persist queue to output file
  static const int64_t flush_delay_ms = 15000;

  // 60 seconds max idle - exit thread when no recent activity
  static const int64_t idle_max_ms = 60000;
  
  // Queues are not allowed to grow larger than this. Consume/persist are triggered when
  // this size is exceeded, regardless of time passed since last consume/persist.
  // (Notice this is a soft limit, used to initialize queues and to trigger internal transfer.)
  // 128 operations per 4k mem page
  // 512 pages
  const int64_t max_queue_size = changelog_size;

  Cm256iQueue_vtable_t *iQ = (Cm256iQueue_vtable_t*)COMLIB_CLASS_VTABLE( Cm256iQueue_t );

  // Persisted output queue
  __change_persist_t *P = NULL;

  // Initialize triggers 
  int64_t t0 = __MILLISECONDS_SINCE_1970();
  int64_t t1 = t0;
  int64_t t_trigger_consume = t1 + consume_delay_ms;
  int64_t t_trigger_flush = t1 + flush_delay_ms;
  int64_t t_trigger_exit = t1 + idle_max_ms;

  // Update changelog state variables
  DYNAMIC_LOCK( &self->_dynamic ) {
    self->changelog.state.__running = 1;
    self->changelog.state.__req_start = 0;
  } DYNAMIC_RELEASE;

  // Run until shutdown.
  int sleep_ms = 333;
  int running = 1;
  while( running ) {

    int64_t sz_input = 0;
    t1 = __MILLISECONDS_SINCE_1970();
    DYNAMIC_LOCK( &self->_dynamic ) {
      // Check idle
      if( t1 > t_trigger_exit ) {
        self->changelog.state.__req_stop = 1;
      }
      // Check run state
      if( self->changelog.state.__req_stop ) {
        sleep_ms = 1;
        running = 0;
      }
      // Get input size
      sz_input = iQ->Length( self->changelog.Qapi );
    } DYNAMIC_RELEASE;

    // Consume
    if( t1 > t_trigger_consume || sz_input > max_queue_size ) {
      t_trigger_consume = t1 + consume_delay_ms;
      int64_t n = __consume( self, &P );
      if( n > 0 ) {
        t_trigger_exit = t1 + idle_max_ms;
      }
      else if( n < 0 ) {
        CRITICAL( 0xC52, "Framehash changelog consume error" );
      }
    }

    // Flush
    if( P ) {
      Cm256iQueue_t *Q = P->queue;
      if( t1 > t_trigger_flush || CALLABLE( Q )->Length( Q ) > max_queue_size ) {
        t_trigger_flush = t1 + flush_delay_ms;
        if( __flush( P ) < 0 ) {
          CRITICAL( 0xC53, "Framehash changelog flush error" );
        }
      }
    }

    // Sleep
    sleep_milliseconds( sleep_ms );
  }

  // Consume any remaining input
  if( __consume( self, &P ) < 0 ) {
    CRITICAL( 0xC54, "Framehash changelog consume error" );
  }
  
  // Flush
  if( P ) {
    if( __flush( P ) < 0 ) {
      CRITICAL( 0xC55, "Framehash changelog persist error" );
    }
    // Finalize current changelog to disk and destroy output queue
    __stage( self, &P );
  }

  DYNAMIC_LOCK( &self->_dynamic ) {
    self->changelog.state.__running = 0;
    self->changelog.state.__req_stop = 0;
  } DYNAMIC_RELEASE;

  // Thread dead
  // 

} END_THREAD_FUNCTION;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __start_changelog_monitor( framehash_t *self, bool async ) {

  int ret = 0;

  SYNCHRONIZE_ON( self->_dynamic.lock ) {
    if( self->changelog.enable ) {
      // Already running?
      if( self->changelog.state.__running ) {
        ret = 0;
      }
      // Run already requested?
      else if( self->changelog.state.__req_start ) {
        ret = 0;
      }
      // Start changelong thread
      else {
        // Set the start trigger
        self->changelog.state.__req_start = 1;
        uint32_t thread_id;
        if( (ret = THREAD_START( &self->changelog.monitor, &thread_id, __changelog_entrypoint, self )) == 0 ) {
          if( async == false ) {
            // Wait until thread is running
            const int timeout_ms = 10000;
            SYNCHRONIZED_WAIT_UNTIL( self->changelog.state.__running, timeout_ms );
            if( !self->changelog.state.__running ) {
              self->changelog.state.__req_start = 0;
              THREAD_JOIN( self->changelog.monitor, 1000 );
              ret = -1;
            }
          }
        }
      }
    }
    else {
      ret = -1;
    }
  } RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __stop_changelog_monitor( framehash_t *self, bool async ) {
  
  int ret = 0;

  SYNCHRONIZE_ON( self->_dynamic.lock ) {
    // Thread is running
    if( self->changelog.state.__running ) {
      // Streaming thread is running - shut down thread and flush
      // Request stop
      self->changelog.state.__req_stop = 1;

      // Block until shutdown
      if( async == false ) {
        const int timeout_ms = 30000;
        SYNCHRONIZED_WAIT_UNTIL( !self->changelog.state.__running, timeout_ms );
        // Timeout?
        if( self->changelog.state.__running ) {
          self->changelog.state.__req_stop = 0;
          CRITICAL( 0xC61, "Framehash changelog thread unresponsive after %lld seconds, forcing shutdown", timeout_ms/1000 );
          ret = -1;
        }
        // No longer running, join
        else {
          THREAD_JOIN( self->changelog.monitor, 10000 );
          memset( &self->changelog.monitor, 0, sizeof( cxlib_thread_t ) );
        }
      }
    }
  } RELEASE;

  // If changes still pending because no changelog processor thread was running, move them to output file now and close
  __change_persist_t *P = NULL;
  while( __consume( self, &P ) > 0 );
  if( P ) {
    __flush( P );
    __stage( self, &P );
  }

  return ret;
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_changelog.h"

DLL_HIDDEN test_descriptor_t _framehash_changelog_tests[] = {
  { "changelog",   __utest_framehash_changelog },
  {NULL}
};
#endif

#endif
