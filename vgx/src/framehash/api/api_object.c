/*######################################################################
 *#
 *# api_class.c
 *#
 *#
 *######################################################################
 */

#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*******************************************************************//**
 * _framehash_api_object__set
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__set( framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, const comlib_object_t * const obj ) {
  framehash_retcode_t insertion;
  framehash_context_t insert_obj_context = {
    .control  = {0}
  };
  insert_obj_context.control.cache.enable_write = 1;
  
  if( __framehash__set_context_key( self, &insert_obj_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_WRITE( self, &insert_obj_context ) {
      // OBJECT128
      if( ktype == CELL_KEY_TYPE_HASH128 ) {
        insert_obj_context.vtype = CELL_VALUE_TYPE_OBJECT128;
        insert_obj_context.value.pobject = (comlib_object_t*)obj;
        __SYNCHRONIZE_RADIX_ON_OBID( self, insert_obj_context.obid ) {
          // insert object keyed by 128-bit obid
          CHANGELOG_EMIT_OPERATION( self, &insert_obj_context );
          insertion = _framehash_radix__set( &insert_obj_context );
          __MOD_INCREMENT( insertion, self );
        } __RELEASE_RADIX;
      }
      // OBJECT64
      else {
        insert_obj_context.vtype = CELL_VALUE_TYPE_OBJECT64;
        insert_obj_context.value.obj56 = (comlib_object_t*)obj;
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, insert_obj_context.key.shortid ) {
          // insert object keyed by 64-bit shortid or plain integer key
          insertion = _framehash_radix__set( &insert_obj_context );
          __MOD_INCREMENT( insertion, self );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_WRITE( &insertion );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }

  return __ACTIONCODE_BY_OPERATION( insertion, insert_obj_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_object__del
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__del(  framehash_t * const self, const framehash_keytype_t ktype, const void *pkey ) {
  framehash_retcode_t deletion;
  framehash_context_t delete_obj_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = {0},
    .control  = {0}
  };
  delete_obj_context.control.cache.enable_write = 1;

  if( __framehash__set_context_key( self, &delete_obj_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_WRITE( self, &delete_obj_context ) {
      if( ktype == CELL_KEY_TYPE_HASH128 ) { 
        __SYNCHRONIZE_RADIX_ON_OBID( self, delete_obj_context.obid ) {
          // delete object by 128-bit hash
          CHANGELOG_EMIT_OPERATION( self, &delete_obj_context );
          deletion = _framehash_radix__del( &delete_obj_context );
          __MOD_DECREMENT( deletion, self );
        } __RELEASE_RADIX;
      }
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, delete_obj_context.key.shortid ) {
          // delete object by 64-bit shortid or plain integer key
          deletion = _framehash_radix__del( &delete_obj_context );
          __MOD_DECREMENT( deletion, self );
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_WRITE( &deletion );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }

  return __ACTIONCODE_BY_OPERATION( deletion, delete_obj_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_object__get
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__get(  framehash_t * const self, const framehash_keytype_t ktype, const void *pkey, comlib_object_t ** const retobj ) {
  framehash_retcode_t retrieval;
  framehash_context_t query_obj_context = {
    .vtype    = CELL_VALUE_TYPE_NULL,
    .value    = {0},
    .control  = {0}
  };
  query_obj_context.control.cache.enable_read = self->_flag.cache.r_ena;

  if( __framehash__set_context_key( self, &query_obj_context, ktype, pkey ) == ktype ) {
    BEGIN_FRAMEHASH_READ( self, &query_obj_context ) {
      // OBJECT128
      if( ktype == CELL_KEY_TYPE_HASH128 ) {
        __SYNCHRONIZE_RADIX_ON_OBID( self, query_obj_context.obid ) {
          retrieval = _framehash_radix__get( &query_obj_context );
          if( retrieval.completed ) {
            *retobj = query_obj_context.value.pobject;
          }
        } __RELEASE_RADIX;
      }
      // OBJECT64
      else {
        __SYNCHRONIZE_RADIX_ON_SHORTID( self, query_obj_context.key.shortid ) {
          // get pointer by 64-bit shortid or plain integer key
          retrieval = _framehash_radix__get( &query_obj_context );
          if( retrieval.completed ) {
            *retobj = query_obj_context.value.obj56;
          }
        } __RELEASE_RADIX;
      }
    } END_FRAMEHASH_READ( &retrieval );
  }
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
  return __ACTIONCODE_BY_OPERATION( retrieval, query_obj_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_object__set_object128_nolock
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__set_object128_nolock( framehash_t * const self, const objectid_t *pobid, const comlib_object_t * const obj ) {
  framehash_retcode_t insertion;
  framehash_context_t insert_obj_context = {
    .frame          = &self->_topframe,
    .key = {
      .plain        = FRAMEHASH_KEY_NONE,
      .shortid      = pobid->L
    },
    .obid           = pobid,
    .dynamic        = &self->_dynamic,
    .value.pobject  = COMLIB_OBJECT( obj ),
    .ktype          = CELL_KEY_TYPE_HASH128,
    .vtype          = CELL_VALUE_TYPE_OBJECT128,
    .control        = {0}
  };
  insert_obj_context.control.cache.enable_write = 1;
  BEGIN_FRAMEHASH_WRITE( self, &insert_obj_context ) {
    CHANGELOG_EMIT_OPERATION( self, &insert_obj_context );
    insertion = _framehash_cache__set( &insert_obj_context );
    __MOD_INCREMENT( insertion, self );
  } END_FRAMEHASH_WRITE( &insertion );
  return __ACTIONCODE_BY_OPERATION( insertion, CELL_VALUE_TYPE_OBJECT128 );
}



/*******************************************************************//**
 * _framehash_api_object__del_object128_nolock
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__del_object128_nolock( framehash_t * const self, const objectid_t *pobid ) {
  framehash_retcode_t deletion;
  framehash_context_t delete_obj_context = {
    .frame          = &self->_topframe,
    .key = {
      .plain        = FRAMEHASH_KEY_NONE,
      .shortid      = pobid->L
    },
    .obid           = pobid,
    .dynamic        = &self->_dynamic,
    .value          = {0},
    .ktype          = CELL_KEY_TYPE_HASH128,
    .vtype          = CELL_VALUE_TYPE_NULL,
    .control        = {0}
  };
  delete_obj_context.control.cache.enable_write = 1;
  BEGIN_FRAMEHASH_WRITE( self, &delete_obj_context ) {
    CHANGELOG_EMIT_OPERATION( self, &delete_obj_context );
    deletion = _framehash_cache__del( &delete_obj_context );
    __MOD_DECREMENT( deletion, self );
  } END_FRAMEHASH_WRITE( &deletion );
  return __ACTIONCODE_BY_OPERATION( deletion, delete_obj_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_object__get_object128_nolock
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__get_object128_nolock(  framehash_t * const self, const objectid_t *pobid, comlib_object_t ** const retobj ) {
  framehash_retcode_t retrieval;
  framehash_context_t query_obj_context = {
    .frame          = &self->_topframe,
    .key = {
      .plain        = FRAMEHASH_KEY_NONE,
      .shortid      = pobid->L
    },
    .obid           = pobid,
    .dynamic        = &self->_dynamic,
    .value          = {0},
    .ktype          = CELL_KEY_TYPE_HASH128,
    .vtype          = CELL_VALUE_TYPE_NULL,
    .control        = {0}
  };
  query_obj_context.control.cache.enable_read = self->_flag.cache.r_ena;
  BEGIN_FRAMEHASH_READ( self, &query_obj_context ) {
    retrieval = _framehash_cache__get( &query_obj_context );
    if( retrieval.completed ) {
      *retobj = query_obj_context.value.pobject;
    }
  } END_FRAMEHASH_READ( &retrieval );
  return __ACTIONCODE_BY_OPERATION( retrieval, query_obj_context.vtype );
}



/*******************************************************************//**
 * _framehash_api_object__has_object128_nolock
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_object__has_object128_nolock( framehash_t * const self, const objectid_t *pobid ) {
  framehash_retcode_t existence;
  framehash_context_t has_obj_context = {
    .frame          = &self->_topframe,
    .key = {
      .plain        = FRAMEHASH_KEY_NONE,
      .shortid      = pobid->L
    },
    .obid           = pobid,
    .dynamic        = &self->_dynamic,
    .value          = {0},
    .ktype          = CELL_KEY_TYPE_HASH128,
    .vtype          = CELL_VALUE_TYPE_NULL,
    .control        = {0}
  };
  has_obj_context.control.cache.enable_read = self->_flag.cache.r_ena;
  BEGIN_FRAMEHASH_READ( self, &has_obj_context ) {
    existence = _framehash_cache__get( &has_obj_context );
  } END_FRAMEHASH_READ( &existence );
  return __ACTIONCODE_BY_OPERATION( existence, has_obj_context.vtype );
}





#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_object.h"

DLL_HIDDEN test_descriptor_t _framehash_api_object_tests[] = {
  { "api_object",   __utest_framehash_api_object },
  {NULL}
};
#endif



