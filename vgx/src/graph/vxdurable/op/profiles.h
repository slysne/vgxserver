/*
###################################################
#
# File:   profiles.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXDURABLE_PROFILES_H
#define _VGX_VXDURABLE_PROFILES_H


#include "execf.h"


/*******************************************************************//**
 * 
 ***********************************************************************
 */
static OperationProcessorOpProfile OpProfiles[] = {
  // Consumer DENY
  {
    .mode = OP_PROFILE_MODE__DENY,
    .id = OP_PROFILE_ID__CONSUMER_DENY,
    .name = "Operation consumer DENY profile",
    .opcodes = {
      OPCODE_SYSTEM_CLEAR_REGISTRY,
      OPCODE_SYSTEM_CLONE_GRAPH,
      OPCODE_SYSTEM_DELETE_GRAPH,
      OPCODE_GRAPH_TRUNCATE,
      OPCODE_GRAPH_PERSIST,
      OPCODE_GRAPH_STATE,
      OPCODE_GRAPH_READONLY,
      OPCODE_GRAPH_EVENTS,
      OPCODE_GRAPH_NOEVENTS,
      OPCODE_GRAPH_EVENT_EXEC,
      0
    }
  },

  // Consumer ALLOW
  {
    .mode = OP_PROFILE_MODE__ALLOW,
    .id = OP_PROFILE_ID__CONSUMER_ALLOW,
    .name = "Operation consumer ALLOW profile",
    .opcodes = {
      OPCODE_VERTEX_NEW,
      OPCODE_VERTEX_DELETE,
      OPCODE_VERTEX_SET_RANK,
      OPCODE_VERTEX_SET_TYPE,
      OPCODE_VERTEX_SET_TMX,
      OPCODE_VERTEX_CONVERT,
      OPCODE_VERTEX_SET_PROPERTY,
      OPCODE_VERTEX_DELETE_PROPERTY,
      OPCODE_VERTEX_CLEAR_PROPERTIES,
      OPCODE_VERTEX_SET_VECTOR,
      OPCODE_VERTEX_DELETE_VECTOR,
      OPCODE_VERTEX_DELETE_OUTARCS,
      OPCODE_VERTEX_DELETE_INARCS,
      OPCODE_VERTEX_ACQUIRE,
      OPCODE_VERTEX_RELEASE,
      OPCODE_ARC_CONNECT,
      OPCODE_ARC_DISCONNECT,
      OPCODE_SYSTEM_SIMILARITY,
      OPCODE_SYSTEM_SEND_COMMENT,
      OPCODE_SYSTEM_SEND_RAW_DATA,
      OPCODE_SYSTEM_CREATE_GRAPH,
      OPCODE_GRAPH_TICK,
      OPCODE_VERTICES_ACQUIRE_WL,
      OPCODE_VERTICES_RELEASE,
      OPCODE_VERTICES_RELEASE_ALL,
      OPCODE_ENUM_ADD_VXTYPE,
      OPCODE_ENUM_DELETE_VXTYPE,
      OPCODE_ENUM_ADD_REL,
      OPCODE_ENUM_DELETE_REL,
      OPCODE_ENUM_ADD_DIM,
      OPCODE_ENUM_DELETE_DIM,
      OPCODE_ENUM_ADD_KEY,
      OPCODE_ENUM_DELETE_KEY,
      OPCODE_ENUM_ADD_STRING,
      OPCODE_ENUM_DELETE_STRING,
      0
    }
  },


  // END
  {0}

};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __is_execf_patch_normal_SYS_CS( __execf_entry *entry ) {
  return entry->execf.patched == entry->execf.normal;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static f_execute_operator __patch_execf_entry_SYS_CS( __execf_entry *entry, f_execute_operator patch ) {
  if( patch ) {
    entry->execf.patched = patch;
  }
  else {
    entry->execf.patched = __execute_op_DENY;
  }
  return entry->execf.patched;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __deny_execf_entries_SYS_CS( OperationProcessorOpCode probe, OperationProcessorOpCode mask ) {
  int count = 0;
  bool found = false;
  __execf_entry *cursor = (__execf_entry*)&g_execf_map;

  OperationProcessorOpCode match = probe & mask;
  // Search for all matches and patch the function to deny
  while( cursor->op.code != 0 ) {
    if( (cursor->op.code & mask) == match ) {
      found = true;
      if( __is_execf_patch_normal_SYS_CS( cursor ) ) {
        __patch_execf_entry_SYS_CS( cursor, NULL );
        cxlib_symbol_name name = cxlib_get_symbol_name( (uintptr_t)cursor->execf.patched );
        if( name.value[0] != '<' ) {
          INFO( 0x001, "DENY OPCODE: %08X %s (patch = %s())", cursor->op.code, cursor->op.name, name.value );
        }
        else {
          INFO( 0x002, "DENY OPCODE: %08X %s", cursor->op.code, cursor->op.name );
        }
        ++count;
      }
    }
    ++cursor;
  }

  if( found ) {
    return count;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __allow_execf_entries_SYS_CS( OperationProcessorOpCode probe, OperationProcessorOpCode mask ) {
  int count = 0;
  bool found = false;
  __execf_entry *cursor = (__execf_entry*)&g_execf_map;

  OperationProcessorOpCode match = probe & mask;
  // Search for all matches and patch the function back to its normal
  while( cursor->op.code != 0 ) {
    if( (cursor->op.code & mask) == match ) {
      found = true;
      if( !__is_execf_patch_normal_SYS_CS( cursor ) ) {
        __patch_execf_entry_SYS_CS( cursor, cursor->execf.normal );
        cxlib_symbol_name name = cxlib_get_symbol_name( (uintptr_t)cursor->execf.patched );
        if( name.value[0] != '<' ) {
          INFO( 0x001, "ALLOW OPCODE: %08X %s (patch = %s())", cursor->op.code, cursor->op.name, name.value );
        }
        else {
          INFO( 0x002, "ALLOW OPCODE: %08X %s", cursor->op.code, cursor->op.name );
        }
        ++count;
      }
    }
    ++cursor;
  }

  if( found ) {
    return count;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __deny_execf_by_opcode_SYS_CS( int64_t opcode_filter ) {

  OperationProcessorOpCode probe = (int)(opcode_filter & 0xFFFFFFFF);

  switch( probe & __OPCODE_PROBE_MASK ) {
  // Deny all opcodes
  case __OPCODE_PROBE__ANY:
    return __deny_execf_entries_SYS_CS( probe, __OPCODE_MASK__ZERO );
  // Deny exact opcode
  case __OPCODE_PROBE__EXACT:
    return __deny_execf_entries_SYS_CS( probe, __OPCODE_MASK__ALL );
  // Deny opcodes by action probe
  case __OPCODE_PROBE__ACTION:
    return __deny_execf_entries_SYS_CS( probe, __OPCODE_MASK__ACTION );
  // Deny opcodes by scope probe
  case __OPCODE_PROBE__SCOPE:
    return __deny_execf_entries_SYS_CS( probe, __OPCODE_MASK__SCOPE );
  // Deny opcodes by target probe
  case __OPCODE_PROBE__TARGET:
    return __deny_execf_entries_SYS_CS( probe, __OPCODE_MASK__TARGET );
  // Bad probe
  default:
    return -1;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __allow_execf_by_opcode_SYS_CS( int64_t opcode_filter ) {

  OperationProcessorOpCode probe = (int)(opcode_filter & 0xFFFFFFFF);

  switch( probe & __OPCODE_PROBE_MASK ) {
  // Allow all opcodes
  case __OPCODE_PROBE__ANY:
    return __allow_execf_entries_SYS_CS( probe, __OPCODE_MASK__ZERO );
  // Allow exact opcode
  case __OPCODE_PROBE__EXACT:
    return __allow_execf_entries_SYS_CS( probe, __OPCODE_MASK__ALL );
  // Allow opcodes by action probe
  case __OPCODE_PROBE__ACTION:
    return __allow_execf_entries_SYS_CS( probe, __OPCODE_MASK__ACTION );
  // Allow opcodes by scope probe
  case __OPCODE_PROBE__SCOPE:
    return __allow_execf_entries_SYS_CS( probe, __OPCODE_MASK__SCOPE );
  // Allow opcodes by target probe
  case __OPCODE_PROBE__TARGET:
    return __allow_execf_entries_SYS_CS( probe, __OPCODE_MASK__TARGET );
  }

  return -1;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __apply_execf_profile_SYS_CS( OperationProcessorOpProfileID profile_id ) {
  OperationProcessorOpProfile *profile = OpProfiles;

  // Find the profile
  while( profile->id != OP_PROFILE_ID__NONE && profile->id != profile_id ) {
    ++profile;
  }

  // Not found
  if( profile->id == OP_PROFILE_ID__NONE ) {
    return -1;
  }

  // Apply the profile
  int count = 0;
  OperationProcessorOpCode *cursor = profile->opcodes;
  OperationProcessorOpCode opcode;
  switch( profile->mode ) {
  case OP_PROFILE_MODE__DENY:
    INFO( 0x001, "DENY OPCODES PROFILE: '%s'", profile->name );
    // First allow everything
    __allow_execf_entries_SYS_CS( __OPCODE_PROBE__ANY, __OPCODE_MASK__ZERO );
    // Then deny according to profile
    while( (opcode = *cursor++) != 0 ) {
      if( __deny_execf_entries_SYS_CS( opcode, __OPCODE_MASK__ALL ) == 1 ) {
        ++count;
      }
    }
    return count;
  case OP_PROFILE_MODE__ALLOW:
    INFO( 0x001, "ALLOW OPCODES PROFILE: '%s'", profile->name );
    // First deny everything
    __deny_execf_entries_SYS_CS( __OPCODE_PROBE__ANY, __OPCODE_MASK__ZERO );
    // Then allow according to profile
    while( (opcode = *cursor++) != 0 ) {
      if( __allow_execf_entries_SYS_CS( opcode, __OPCODE_MASK__ALL ) == 1 ) {
        ++count;
      }
    }
    return count;
  default:
    return -1;
  }

}





#endif
