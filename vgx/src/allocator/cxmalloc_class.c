/*######################################################################
 *#
 *# cxmalloc_class.c
 *#
 *######################################################################
 */

#include "_cxmalloc.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_CXMALLOC );




static int g_cxmalloc_initialized = 0;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void cxmalloc_family_RegisterClass( void ) {
  COMLIB_REGISTER_CLASS( cxmalloc_family_t, CXLIB_OBTYPE_ALLOCATOR, &cxmalloc_family_Methods, OBJECT_IDENTIFIED_BY_LONGSTRING, OBJECTID_LONGSTRING_MAX );

#define __sz_cacheline        sizeof(cacheline_t)
#define __sz_half_cacheline   (__sz_cacheline / 2 )
#define __sz_pointer          sizeof( void* )
#define __sz_qword            sizeof( QWORD )

  ASSERT_TYPE_SIZE( cxmalloc_linebyte_t,         1                    );
  ASSERT_TYPE_SIZE( cxmalloc_linechunk_t,         __sz_half_cacheline );
  ASSERT_TYPE_SIZE( cxmalloc_linequant_t,     1 * __sz_cacheline      );
  ASSERT_TYPE_SIZE( cxmalloc_linehead_t,          __sz_half_cacheline );
  ASSERT_TYPE_SIZE( cxmalloc_datashape_t,     1 * __sz_cacheline      );
  ASSERT_TYPE_SIZE( cxmalloc_lineregister_t,  4 * __sz_pointer        );
  ASSERT_TYPE_SIZE( cxmalloc_block_t,         3 * __sz_cacheline      );
  ASSERT_TYPE_SIZE( cxmalloc_allocator_t,     4 * __sz_cacheline      );

  ASSERT_TYPE_SIZE( cxmalloc_metaflex_element_t,  __sz_qword          );
  ASSERT_TYPE_SIZE( cxmalloc_metaflex_t,      2 * __sz_qword          );
  ASSERT_TYPE_SIZE( cxmalloc_header_t,            __sz_half_cacheline );
  ASSERT_TYPE_SIZE( cxmalloc_descriptor_t,    3 * __sz_cacheline      );
  ASSERT_TYPE_SIZE( cxmalloc_family_t,        3 * __sz_cacheline      );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void cxmalloc_family_UnregisterClass( void ) {
  COMLIB_UNREGISTER_CLASS( cxmalloc_family_t );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int cxmalloc_INIT(void) {
  if( !g_cxmalloc_initialized ) {
    SET_EXCEPTION_CONTEXT
    cxmalloc_family_RegisterClass();
    g_cxmalloc_initialized = 1;
    return 1;
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
DLL_EXPORT void cxmalloc_DESTROY(void) {
  if( g_cxmalloc_initialized ) {
    cxmalloc_family_UnregisterClass();
    g_cxmalloc_initialized = 0;
  }
}

