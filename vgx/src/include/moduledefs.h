/*
###################################################
#
# File:   moduledefs.h
#
###################################################
*/
#ifndef COMLIB_MODULEDEFS_H
#define COMLIB_MODULEDEFS_H


/* ERROR MODULE               ___XX___  */
typedef enum e_comlib_message_module_t {
  COMLIB_MSG_MOD_COMLIB          = 0x00011000,
  COMLIB_MSG_MOD_COMLIBSEQUENCE  = 0x00012000,
  COMLIB_MSG_MOD_CXTOKENIZER     = 0x00013000,
  COMLIB_MSG_MOD_CXSTRING        = 0x00014000,
  COMLIB_MSG_MOD_UTEST           = 0x00016000,
  COMLIB_MSG_MOD_CXMALLOC        = 0x00017000,
  COMLIB_MSG_MOD_FRAMEHASH       = 0x0001F000,
  COMLIB_MSG_MOD_VGX             = 0x00030000,
  COMLIB_MSG_MOD_VGX_INDEX       = 0x00031000,
  COMLIB_MSG_MOD_VGX_VECTOR      = 0x00032000,
  COMLIB_MSG_MOD_VGX_LSH         = 0x00033000,

  COMLIB_MSG_MOD_VGX_ALLOC       = 0x00037000,
  COMLIB_MSG_MOD_VGX_GRAPH       = 0x00037000,

  COMLIB_MSG_MOD_API             = 0x00061000,
  COMLIB_MSG_MOD_SELFTEST        = 0x00071000,
  COMLIB_MSG_MOD_GENERAL         = 0x00080000,

  COMLIB_MSG_MOD_VGX_IFACE_COMMON  = 0x000f1000,
  COMLIB_MSG_MOD_INDEXER           = 0x000f2000,
  COMLIB_MSG_MOD_SEARCHER          = 0x000f3000,
  COMLIB_MSG_MOD_DISPATCHER        = 0x000f4000,
  COMLIB_MSG_MOD_VGX_IFACE         = 0x000f0000,
} comlib_message_module_t;


#endif
