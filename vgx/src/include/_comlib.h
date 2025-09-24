/*
###################################################
#
# File:   _comlib.h
#
###################################################
*/
#ifndef _COMLIB_INCLUDED_H
#define _COMLIB_INCLUDED_H

// This will cause DLL_PUBLIC symbols to be exported. The default is to import.
// Code that includes _comlib.h will export
// Code that includes comlib.h will import
#define _COMLIB_EXPORT

#include "comlib.h"


/* TEST DESCRIPTORS */
#ifdef INCLUDE_UNIT_TESTS
extern test_descriptor_t _comlib_tests[];
#endif

extern test_descriptor_t _comlib_cxcstring_tests[];

extern test_descriptor_t _comlib_cstringqueue_tests[];

extern test_descriptor_t _comlib_cbytequeue_tests[];
extern test_descriptor_t _comlib_cwordqueue_tests[];
extern test_descriptor_t _comlib_cdwordqueue_tests[];
extern test_descriptor_t _comlib_cqwordqueue_tests[];
extern test_descriptor_t _comlib_cm128iqueue_tests[];
extern test_descriptor_t _comlib_cm256iqueue_tests[];
extern test_descriptor_t _comlib_cm512iqueue_tests[];
extern test_descriptor_t _comlib_ctptrqueue_tests[];
extern test_descriptor_t _comlib_cx2tptrqueue_tests[];
extern test_descriptor_t _comlib_captrqueue_tests[];

extern test_descriptor_t _comlib_cbyteheap_tests[];
extern test_descriptor_t _comlib_cwordheap_tests[];
extern test_descriptor_t _comlib_cdwordheap_tests[];
extern test_descriptor_t _comlib_cqwordheap_tests[];
extern test_descriptor_t _comlib_cm128iheap_tests[];
extern test_descriptor_t _comlib_cm256iheap_tests[];
extern test_descriptor_t _comlib_cm512iheap_tests[];
extern test_descriptor_t _comlib_ctptrheap_tests[];
extern test_descriptor_t _comlib_cx2tptrheap_tests[];
extern test_descriptor_t _comlib_captrheap_tests[];

extern test_descriptor_t _comlib_cbytelist_tests[];
extern test_descriptor_t _comlib_cwordlist_tests[];
extern test_descriptor_t _comlib_cdwordlist_tests[];
extern test_descriptor_t _comlib_cqwordlist_tests[];
extern test_descriptor_t _comlib_cm128ilist_tests[];
extern test_descriptor_t _comlib_cm256ilist_tests[];
extern test_descriptor_t _comlib_cm512ilist_tests[];
extern test_descriptor_t _comlib_ctptrlist_tests[];
extern test_descriptor_t _comlib_cx2tptrlist_tests[];
extern test_descriptor_t _comlib_captrlist_tests[];
extern test_descriptor_t _comlib_cqwordlist_tests[];


#endif

