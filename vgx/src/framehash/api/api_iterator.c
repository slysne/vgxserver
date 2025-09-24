/*######################################################################
 *#
 *# api_iterator.c
 *#
 *#
 *######################################################################
 */

#include "_framehash.h"



SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );



/*******************************************************************//**
 * _framehash_api_iterator__get_items
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__get_items( framehash_t * const self ) {
  return _framehash_processor__collect_cells_into_list( self );
}



/*******************************************************************//**
 * _framehash_api_iterator__get_keys
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__get_keys( framehash_t * const self ) {
  return _framehash_processor__collect_annotations_into_list( self );
}



/*******************************************************************//**
 * _framehash_api_iterator__get_values
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__get_values( framehash_t * const self ) {
  return _framehash_processor__collect_refs_into_list( self );
}



/*******************************************************************//**
 * _framehash_api_iterator__count_nactive
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__count_nactive( framehash_t * const self ) {
  int64_t n_active = 0;
  framehash_context_t fh_context = {
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };

  framehash_retcode_t ret;

  if( self->_flag.readonly ) {
    BEGIN_FRAMEHASH_READ( self, &fh_context ) {
      __SYNCHRONIZE_ALL_SUBTREES( self ) {
        n_active = _framehash_processor__count_nactive( self );
      } __RELEASE_ALL_SUBTREES;
    } END_FRAMEHASH_READ( &ret );
  }
  else {
    BEGIN_FRAMEHASH_WRITE( self, &fh_context ) {
      __SYNCHRONIZE_ALL_SUBTREES( self ) {
        n_active = _framehash_processor__count_nactive( self );
      } __RELEASE_ALL_SUBTREES;
    } END_FRAMEHASH_WRITE( &ret );

  }

  return n_active;
}



/*******************************************************************//**
 * _framehash_api_iterator__process
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__process( framehash_t * const self, framehash_processing_context_t * const processor ) {
  int64_t n_proc = 0;
  framehash_context_t fh_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };

  processor->instance.frame = fh_context.frame;
  processor->instance.dynamic = fh_context.dynamic;
  // Force processor readonly if instance is readonly
  if( self->_flag.readonly ) {
    processor->flags.readonly = 1;
  }

  framehash_retcode_t ret;
  
  // Readonly processor
  if( processor->flags.readonly ) {
    BEGIN_FRAMEHASH_READ( self, &fh_context ) {
      __SYNCHRONIZE_ALL_SUBTREES( self ) {
        n_proc = _framehash_processor__process_nolock_nocache( processor );
      } __RELEASE_ALL_SUBTREES;
    } END_FRAMEHASH_READ( &ret );
  }
  // Modifying processor
  else {
    BEGIN_FRAMEHASH_WRITE( self, &fh_context ) {
      __SYNCHRONIZE_ALL_SUBTREES( self ) {
        int64_t count = self->_nobj;
        n_proc = _framehash_processor__process_nolock_nocache( processor );
        self->_nobj = count + processor->__internal.__delta_items;
      } __RELEASE_ALL_SUBTREES;
    } END_FRAMEHASH_WRITE( &ret );
  }
  
  return n_proc;
}



/*******************************************************************//**
 * _framehash_api_iterator__process_partial
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_iterator__process_partial( framehash_t * const self, framehash_processing_context_t * const processor, uint64_t selector ) {
  int64_t n_proc = 0;
  framehash_context_t fh_context = {
    .frame = &self->_topframe,
    .dynamic = &self->_dynamic,
    .control = FRAMEHASH_CONTROL_FLAGS_INIT
  };

  processor->instance.frame = fh_context.frame;
  processor->instance.dynamic = fh_context.dynamic;
  // Force processor readonly if instance is readonly
  if( self->_flag.readonly ) {
    processor->flags.readonly = 1;
  }

  framehash_retcode_t ret;
  
  // Readonly processor
  if( processor->flags.readonly ) {
    BEGIN_FRAMEHASH_READ( self, &fh_context ) {
      __SYNCHRONIZE_SUBTREE( self, selector ) {
        n_proc = _framehash_processor__process_cache_partial_nolock_nocache( processor, selector );
      } __RELEASE_SUBTREE;
    } END_FRAMEHASH_READ( &ret );
  }
  // Modifying processor
  else {
    BEGIN_FRAMEHASH_WRITE( self, &fh_context ) {
      __SYNCHRONIZE_SUBTREE( self, selector ) {
        int64_t count = self->_nobj;
        n_proc = _framehash_processor__process_cache_partial_nolock_nocache( processor, selector );
        self->_nobj = count + processor->__internal.__delta_items;
      } __RELEASE_SUBTREE;
    } END_FRAMEHASH_WRITE( &ret );
  }
  
  return n_proc;
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_iterator.h"

DLL_HIDDEN test_descriptor_t _framehash_api_iterator_tests[] = {
  { "api_iterator",   __utest_framehash_api_iterator },
  {NULL}
};
#endif



