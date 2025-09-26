/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxenum.c
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

#include "_vxenum.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static vgx_StringList_t * _vxenum__new_string_list( object_allocator_context_t *allocator_context, int64_t sz );
static CString_t * _vxenum__string_list_set_item( vgx_StringList_t *list, int64_t i, const char *str );
static CString_t * _vxenum__string_list_set_item_len( vgx_StringList_t *list, int64_t i, const char *str, int len );
static CString_t * _vxenum__string_list_set_item_steal( vgx_StringList_t *list, int64_t i, CString_t **CSTR__str );
static CString_t * _vxenum__string_list_get_item( vgx_StringList_t *list, int64_t i );
static CString_t * _vxenum__string_list_get_own_item( vgx_StringList_t *list, int64_t i );
static const char * _vxenum__string_list_get_chars( const vgx_StringList_t *list, int64_t i );
static const objectid_t * _vxenum__string_list_get_obid( vgx_StringList_t *list, int64_t i );
static int _vxenum__string_list_contains( const vgx_StringList_t *list, const char *str, bool ignore_case );
static int _vxenum__string_list_remove_item( vgx_StringList_t *list, const char *str, bool ignore_case );
static CString_t * _vxenum__string_list_append( vgx_StringList_t *list, const char *str );
static CString_t * _vxenum__string_list_append_steal( vgx_StringList_t *list, CString_t **CSTR__str );
static CString_t ** _vxenum__string_list_data( vgx_StringList_t *list );
static int64_t _vxenum__string_list_size( const vgx_StringList_t *list );
static void _vxenum__string_list_sort( vgx_StringList_t *list, bool ascending );
static vgx_StringList_t * _vxenum__string_list_clone( const vgx_StringList_t *list );
static void _vxenum__discard_string_list( vgx_StringList_t **list );
static int _vxenum__list_dir( const char *dirpath, const char *query, vgx_StringList_t **rlist );



static char * _vxenum__cstring_dumps( const CString_t *CSTR__string );
static CString_t * _vxenum__cstring_loads( const char *data );


/*******************************************************************//**
 *
 *  20160802 MP/SL
 *  TODO:  System relationships and vertex types
 *  ============================================
 *  1. Salted hash for internal vertex names -> we get separate namespace for internal vertices
 *  2. System relationships use internal enumeration values
 *     A '*' query will not return internal enumeration values
 *  3. Add bit for vertices and relationships indicating if they are internal or user defined
 *     Allows us to cleanly determine if we should return a given vertex or relationship to the user
 *     For "debug mode" return all values, otherwise only return user defined values.
 *
 ***********************************************************************
 */


static const cacheline_t __vxenum__bitmap_char = {
  .bytes = {
                  //  VALID KEY CHARACTER
                  //  -------------------------------------------------------
                  //  byte     0     1     2     3     4     5     6     7
                  //  -------------------------------------------------------
    0b00000000,   //  0        NUL   SOH   STX   ETX   EOT   ENQ   ACK   BEL
    0b00000000,   //  1        BS    TAB   LF    VT    FF    CR    SO    SI
    0b00000000,   //  2        DLE   DC1   DC2   DC3   DC4   NAK   SYN   ETB
    0b00000000,   //  3        CAN   EM    SUB   ESC   FS    GS    RS    US
    0b00011100,   //  4              !     "     #     $     %     &     '
    0b00000010,   //  5        (     )     *     +     ,     -     .     /
    0b11111111,   //  6        0     1     2     3     4     5     6     7
    0b11000000,   //  7        8     9     :     ;     <     =     >     ?
    0b11111111,   //  8        @     A     B     C     D     E     F     G
    0b11111111,   //  9        H     I     J     K     L     M     N     O
    0b11111111,   // 10        P     Q     R     S     T     U     V     W
    0b11100001,   // 11        X     Y     Z     [     \     ]     ^     _
    0b01111111,   // 12        `     a     b     c     d     e     f     g
    0b11111111,   // 13        h     i     j     k     l     m     n     o
    0b11111111,   // 14        p     q     r     s     t     u     v     w
    0b11100000,   // 15        x     y     z     {     |     }     ~     DEL
                  //  -------------------------------------------------------


                  //  ALPHABETIC CHARACTER
                  //  -------------------------------------------------------
                  //  byte     0     1     2     3     4     5     6     7
                  //  -------------------------------------------------------
    0b00000000,   //  0        NUL   SOH   STX   ETX   EOT   ENQ   ACK   BEL
    0b00000000,   //  1        BS    TAB   LF    VT    FF    CR    SO    SI
    0b00000000,   //  2        DLE   DC1   DC2   DC3   DC4   NAK   SYN   ETB
    0b00000000,   //  3        CAN   EM    SUB   ESC   FS    GS    RS    US
    0b00000000,   //  4              !     "     #     $     %     &     '
    0b00000000,   //  5        (     )     *     +     ,     -     .     /
    0b00000000,   //  6        0     1     2     3     4     5     6     7
    0b00000000,   //  7        8     9     :     ;     <     =     >     ?
    0b01111111,   //  8        @     A     B     C     D     E     F     G
    0b11111111,   //  9        H     I     J     K     L     M     N     O
    0b11111111,   // 10        P     Q     R     S     T     U     V     W
    0b11100000,   // 11        X     Y     Z     [     \     ]     ^     _
    0b01111111,   // 12        `     a     b     c     d     e     f     g
    0b11111111,   // 13        h     i     j     k     l     m     n     o
    0b11111111,   // 14        p     q     r     s     t     u     v     w
    0b11100000,   // 15        x     y     z     {     |     }     ~     DEL
                  //  -------------------------------------------------------


                  //  RFC3986 pchar CHARACTER  (url path segment)
                  //  -------------------------------------------------------
                  //  byte     0     1     2     3     4     5     6     7
                  //  -------------------------------------------------------
    0b00000000,   //  0        NUL   SOH   STX   ETX   EOT   ENQ   ACK   BEL
    0b00000000,   //  1        BS    TAB   LF    VT    FF    CR    SO    SI
    0b00000000,   //  2        DLE   DC1   DC2   DC3   DC4   NAK   SYN   ETB
    0b00000000,   //  3        CAN   EM    SUB   ESC   FS    GS    RS    US
    0b01001011,   //  4              !     "     #     $     %     &     '
    0b11111111,   //  5        (     )     *     +     ,     -     .     /
    0b11111111,   //  6        0     1     2     3     4     5     6     7
    0b11110100,   //  7        8     9     :     ;     <     =     >     ?
    0b11111111,   //  8        @     A     B     C     D     E     F     G
    0b11111111,   //  9        H     I     J     K     L     M     N     O
    0b11111111,   // 10        P     Q     R     S     T     U     V     W
    0b11100001,   // 11        X     Y     Z     [     \     ]     ^     _
    0b01111111,   // 12        `     a     b     c     d     e     f     g
    0b11111111,   // 13        h     i     j     k     l     m     n     o
    0b11111111,   // 14        p     q     r     s     t     u     v     w
    0b11100010,   // 15        x     y     z     {     |     }     ~     DEL
                  //  -------------------------------------------------------


    0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0
  }
};


static const uint8_t * __key_map = __vxenum__bitmap_char.bytes;
static const uint8_t * __alpha_map = __vxenum__bitmap_char.bytes + 16;
static const uint8_t * __uri_path_map = __vxenum__bitmap_char.bytes + 32;



#define __BITMAP_BYTE( C ) ( C >> 3 )
#define __BITMAP_MASK( C ) (0b10000000 >> (C & 0b111))


__inline static int __IS_KEYCHAR( const char C ) {
  return (  *(  __key_map + __BITMAP_BYTE( C )) & __BITMAP_MASK( C ) );
}



__inline static int __IS_ALPHA( const char C ) {
  return (  *(  __alpha_map + __BITMAP_BYTE( C )) & __BITMAP_MASK( C ) );
}



__inline static int __IS_URI_PATH( const char C ) {
  return (  *(  __uri_path_map + __BITMAP_BYTE( C )) & __BITMAP_MASK( C ) );
}




/*******************************************************************//**
 *
 * Stored keys must start with an alphabetic character [a-zA-Z]
 * Remaining characters must be [.#$%0-9@A-Z_a-z]
 * Whitespace is not allowed.
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__is_valid_storable_key( const char *key ) {
  // Proceed if first char is valid: 
  if( __IS_ALPHA( *key++ ) ) {
    // Scan to end as long as valid key character
    while( __IS_KEYCHAR( *key++ ) );
  }
  // Cursor should now be just beyond \0 if key was valid
  return *(--key) == '\0';
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__is_valid_select_key( const char *key ) {
  // Select keys are allowed to start with _
  if( *key == '_' ) {
    ++key; // ok
  }
  // Scan to end as long as valid key character
  while( __IS_KEYCHAR( *key++ ) );
  // Cursor should now be just beyond \0 if key was valid
  return *(--key) == '\0';
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__is_valid_uri_path_segment( const char *key ) {
  // Scan to end as long as valid uri path character
  while( __IS_URI_PATH( *key++ ) );
  // Cursor should now be just beyond \0 if key was valid
  return *(--key) == '\0';
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__parse_allow_prefix_wildcard_and_escape( vgx_Graph_t *self, const char *probe, vgx_value_comparison *vcomp, CString_t **CSTR__error ) {
  /*
      Use the Unicode Private Use Area in the Basic Multilingual Plane for special escapes.
      The PUA starts at 0xE000. Add the ascii value of the special character we want to protect, in this case the '*'.
      >>> unichr( int("0xE000",16) + ord("*") ).encode("utf-8")
      '\xee\x80\xaa'
  */

  const char asterisk[] = {0xee, 0x80, 0xaa, 0}; // utf-8 encoding of the protected asterisk transposed to the PUA

  // Clear any previous error
  if( CSTR__error && *CSTR__error ) {
    CStringDelete( *CSTR__error );
    *CSTR__error = NULL;
  }

  CString_t *CSTR__probe = NULL;
  CString_t *CSTR__probe_esc = NULL;
  CString_t *CSTR__prefix_esc = NULL;

  XTRY {
    // When asterisks exist, we need more processing
    if( CharsContainsConst( probe, "*" ) ) {
      // Create the probe string, protecting escaped asterisks
      if( (CSTR__probe_esc = CStringNewReplace( probe, "\\*", asterisk )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x761 );
      } 

      // After protecting escaped '*', check if wildcard * exists
      int32_t wild_pos = CStringFind( CSTR__probe_esc, "*", 0 );

      // Wildcard found somewhere
      if( wild_pos >= 0 ) {
        // prefix: "xyz*" (first '*' occurrence is the very last character)
        if( wild_pos == CStringLength( CSTR__probe_esc ) - 1 ) {
          // Special code to indicate prefix match
          *vcomp ^= VGX_VALUE_GTE; // trick: convert EQU(1100)->LTE(0100) and NEQ(1110)->GT(0110)

          // Strip the last character which is the trailing '*'
          int end = -1;
          if( (CSTR__prefix_esc = CStringSlice( CSTR__probe_esc, NULL, &end )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x762 );
          }
        }
        // unsupported wildcard: "xy*z", "x*yz*", "*xyz*", etc...
        else {
          if( CSTR__error ) {
            *CSTR__error = CStringNewFormat( "Unsupported wildcard: %s", probe );
          }
          THROW_SILENT( CXLIB_ERR_API, 0x763 );
        }
      }

      // Replace our protected asterisks with a plain '*' which will be used literally in the core
      CSTR__probe = CStringReplace( (CSTR__prefix_esc ? CSTR__prefix_esc : CSTR__probe_esc), asterisk, "*" );

    }
    // No asterisks in probe, just create the id string
    else {
      if( self ) {
        CSTR__probe = NewEphemeralCString( self, probe );
      }
      else {
        CSTR__probe = CStringNew( probe );
      }

      if( CSTR__probe ) {
        // Generate the obid for use later
        if( CStringObid( CSTR__probe ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x764 );
        }
      }
    }

    // Check that probe id was created
    if( CSTR__probe == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x765 );
    }

  }
  XCATCH( errcode ) {
    if( CSTR__probe ) {
      CStringDelete( CSTR__probe );
      CSTR__probe = NULL;
    }
    *vcomp = VGX_VALUE_NONE;
  }
  XFINALLY {
    if( CSTR__probe_esc ) {
      CStringDelete( CSTR__probe_esc );
    }
    if( CSTR__prefix_esc ) {
      CStringDelete( CSTR__prefix_esc );
    }
  }

  return CSTR__probe;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IEnumerator_t iEnumerator_OPEN = {
  // Relationship
  .Relationship = {
    .Set                    = _vxenum_rel__set_OPEN,
    .Encode                 = _vxenum_rel__encode_OPEN,
    .GetEnum                = _vxenum_rel__get_enum_OPEN,
    .Decode                 = _vxenum_rel__decode_OPEN,
    .ClearCache             = _vxenum_rel__clear_thread_cache,
    .Exists                 = _vxenum_rel__has_mapping_OPEN,
    .ExistsEnum             = _vxenum_rel__has_mapping_rel_OPEN,
    .GetAll                 = _vxenum_rel__entries_OPEN,
    .Count                  = _vxenum_rel__count_OPEN,
    .OpSync                 = _vxenum_rel__operation_sync_OPEN,
    .Remove                 = _vxenum_rel__remove_mapping_OPEN
  },
  // Vertex Type
  .VertexType = {
    .Set                    = _vxenum_vtx__set_OPEN,
    .Encode                 = _vxenum_vtx__encode_OPEN,
    .GetEnum                = _vxenum_vtx__get_enum_OPEN,
    .Decode                 = _vxenum_vtx__decode_OPEN,
    .ClearCache             = _vxenum_vtx__clear_cache,
    .Exists                 = _vxenum_vtx__has_mapping_OPEN,
    .ExistsEnum             = _vxenum_vtx__has_mapping_vtx_OPEN,
    .GetAll                 = _vxenum_vtx__entries_OPEN,
    .Count                  = _vxenum_vtx__count_OPEN,
    .OpSync                 = _vxenum_vtx__operation_sync_OPEN,
    .Remove                 = _vxenum_vtx__remove_mapping_OPEN,
    .Reserved = {
      .None                   = _vxenum_vtx__reserved_None,
      .Vertex                 = _vxenum_vtx__reserved_Vertex,
      .System                 = _vxenum_vtx__reserved_System,
      .Unknown                = _vxenum_vtx__reserved_Unknown,
      .LockObject             = _vxenum_vtx__reserved_LockObject
    }
  },
  // Dimension
  .Dimension = {
    .Set                    = _vxenum_dim__set_OPEN,
    .EncodeChars            = _vxenum_dim__encode_chars_OPEN,
    .Decode                 = _vxenum_dim__decode_OPEN,
    .Own                    = _vxenum_dim__own_OPEN,
    .Discard                = _vxenum_dim__discard_OPEN,
    .Exists                 = _vxenum_dim__has_mapping_chars_OPEN,
    .ExistsEnum             = _vxenum_dim__has_mapping_dim_OPEN,
    .GetAll                 = _vxenum_dim__entries_OPEN,
    .Count                  = _vxenum_dim__count_OPEN,
    .OpSync                 = _vxenum_dim__operation_sync_OPEN,
    .Remain                 = _vxenum_dim__remain_OPEN
  },
  // Property
  .Property = {
    // Key
    .Key = {
      .Set                  = _vxenum_propkey__set_key_OPEN,
      .SetChars             = _vxenum_propkey__set_key_chars_OPEN,
      .Get                  = _vxenum_propkey__get_key_OPEN,
      .New                  = _vxenum_propkey__new_key,
      .NewSelect            = _vxenum_propkey__new_select_key,
      .Encode               = _vxenum_propkey__encode_key_OPEN,
      .GetEnum              = _vxenum_propkey__get_enum_OPEN,
      .Decode               = _vxenum_propkey__decode_key_OPEN,
      .Own                  = _vxenum_propkey__own_key_OPEN,
      .OwnByHash            = _vxenum_propkey__own_key_by_hash_OPEN,
      .Discard              = _vxenum_propkey__discard_key_OPEN,
      .DiscardByHash        = _vxenum_propkey__discard_key_by_hash_OPEN,
      .GetAll               = _vxenum_propkey__keys_OPEN,
      .Count                = _vxenum_propkey__count_OPEN,
      .OpSync               = _vxenum_propkey__operation_sync_OPEN
    },
    // Value
    .Value = {
      .StoreChars           = _vxenum_propval__store_value_chars_OPEN,
      .Store                = _vxenum_propval__store_value_OPEN,
      .Get                  = _vxenum_propval__get_value_OPEN,
      .Own                  = _vxenum_propval__own_value_OPEN,
      .Discard              = _vxenum_propval__discard_value_OPEN,
      .GetAll               = _vxenum_propval__values_OPEN,
      .Count                = _vxenum_propval__count_OPEN,
      .OpSync               = _vxenum_propval__operation_sync_OPEN
    }
  }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IEnumerator_t iEnumerator_CS = {
  // Relationship
  .Relationship = {
    .Set                    = _vxenum_rel__set_CS,
    .Encode                 = _vxenum_rel__encode_CS,
    .GetEnum                = _vxenum_rel__get_enum_CS,
    .Decode                 = _vxenum_rel__decode_CS,
    .ClearCache             = _vxenum_rel__clear_thread_cache,
    .Exists                 = _vxenum_rel__has_mapping_CS,
    .ExistsEnum             = _vxenum_rel__has_mapping_rel_CS,
    .GetAll                 = _vxenum_rel__entries_CS,
    .GetEnums               = _vxenum_rel__codes_CS,
    .Count                  = _vxenum_rel__count_CS,
    .OpSync                 = _vxenum_rel__operation_sync_CS,
    .Remove                 = _vxenum_rel__remove_mapping_CS
  },
  // Vertex Type
  .VertexType = {
    .Set                    = _vxenum_vtx__set_CS,
    .Encode                 = _vxenum_vtx__encode_CS,
    .GetEnum                = _vxenum_vtx__get_enum_CS,
    .Decode                 = _vxenum_vtx__decode_CS,
    .ClearCache             = _vxenum_vtx__clear_cache,
    .Exists                 = _vxenum_vtx__has_mapping_CS,
    .ExistsEnum             = _vxenum_vtx__has_mapping_vtx_CS,
    .GetAll                 = _vxenum_vtx__entries_CS,
    .GetEnums               = _vxenum_vtx__codes_CS,
    .Count                  = _vxenum_vtx__count_CS,
    .OpSync                 = _vxenum_vtx__operation_sync_CS,
    .Remove                 = _vxenum_vtx__remove_mapping_CS,
    .Reserved = {
      .None                   = _vxenum_vtx__reserved_None,
      .Vertex                 = _vxenum_vtx__reserved_Vertex,
      .System                 = _vxenum_vtx__reserved_System,
      .Unknown                = _vxenum_vtx__reserved_Unknown,
      .LockObject             = _vxenum_vtx__reserved_LockObject
    }
  },
  // Dimension
  .Dimension = {
    .Set                    = _vxenum_dim__set_CS,
    .EncodeChars            = _vxenum_dim__encode_chars_CS,
    .Decode                 = _vxenum_dim__decode_CS,
    .Own                    = _vxenum_dim__own_CS,
    .Discard                = _vxenum_dim__discard_CS,
    .Exists                 = _vxenum_dim__has_mapping_chars_CS,
    .ExistsEnum             = _vxenum_dim__has_mapping_dim_CS,
    .GetAll                 = _vxenum_dim__entries_CS,
    .Count                  = _vxenum_dim__count_CS,
    .OpSync                 = _vxenum_dim__operation_sync_CS,
    .Remain                 = _vxenum_dim__remain_CS
  },
  // Property
  .Property = {
    // Key
    .Key = {
      .Set                  = _vxenum_propkey__set_key_CS,
      .SetChars             = _vxenum_propkey__set_key_chars_CS,
      .Get                  = _vxenum_propkey__get_key_CS,
      .New                  = _vxenum_propkey__new_key,
      .NewSelect            = _vxenum_propkey__new_select_key,
      .Encode               = _vxenum_propkey__encode_key_CS,
      .GetEnum              = _vxenum_propkey__get_enum_CS,
      .Decode               = _vxenum_propkey__decode_key_CS,
      .Own                  = _vxenum_propkey__own_key_CS,
      .OwnByHash            = _vxenum_propkey__own_key_by_hash_CS,
      .Discard              = _vxenum_propkey__discard_key_CS,
      .DiscardByHash        = _vxenum_propkey__discard_key_by_hash_CS,
      .GetAll               = _vxenum_propkey__keys_CS,
      .Count                = _vxenum_propkey__count_CS,
      .OpSync               = _vxenum_propkey__operation_sync_CS
    },
    // Value
    .Value = {
      .StoreChars           = _vxenum_propval__store_value_chars_CS,
      .Store                = _vxenum_propval__store_value_CS,
      .Get                  = _vxenum_propval__get_value_CS,
      .Own                  = _vxenum_propval__own_value_CS,
      .Discard              = _vxenum_propval__discard_value_CS,
      .GetAll               = _vxenum_propval__values_CS,
      .Count                = _vxenum_propval__count_CS,
      .OpSync               = _vxenum_propval__operation_sync_CS
    }
  }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT vgx_IString_t iString = {
  .New                    = _vxenum__new_string,
  .NewLen                 = _vxenum__new_string_len,
  .SetNew                 = _vxenum__set_new_string,
  .NewFormat              = _vxenum__new_string_format,
  .SetNewFormat           = _vxenum__set_new_string_format,
  .NewVFormat             = _vxenum__new_string_vformat,
  .SetNewVFormat          = _vxenum__set_new_string_vformat,
  .Clone                  = _vxenum__clone_string,
  .Discard                = _vxenum__discard_string,
  // List
  .List = {
    .New                  = _vxenum__new_string_list,
    .SetItem              = _vxenum__string_list_set_item,
    .SetItemLen           = _vxenum__string_list_set_item_len,
    .SetItemSteal         = _vxenum__string_list_set_item_steal,
    .GetItem              = _vxenum__string_list_get_item,
    .GetOwnItem           = _vxenum__string_list_get_own_item,
    .GetChars             = _vxenum__string_list_get_chars,
    .GetObid              = _vxenum__string_list_get_obid,
    .Contains             = _vxenum__string_list_contains,
    .RemoveItem           = _vxenum__string_list_remove_item,
    .Append               = _vxenum__string_list_append,
    .AppendSteal          = _vxenum__string_list_append_steal,
    .Data                 = _vxenum__string_list_data,
    .Size                 = _vxenum__string_list_size,
    .Sort                 = _vxenum__string_list_sort,
    .Clone                = _vxenum__string_list_clone,
    .Discard              = _vxenum__discard_string_list

  },
  // Serialization
  .Serialization = {
    .SetAllocatorContext    = _vxenum__set_cstring_deserialization_allocator_context,
    .GetAllocatorContext    = _vxenum__get_cstring_deserialization_allocator_context,
    .Serialize              = _vxenum__serialize_cstring,
    .DeserializeNolock      = _vxenum__deserialize_cstring_nolock,
    .Dumps                  = _vxenum__cstring_dumps,
    .Loads                  = _vxenum__cstring_loads
  },
  // Validate
  .Validate = {
    .StorableKey            = _vxenum__is_valid_storable_key,
    .SelectKey              = _vxenum__is_valid_select_key,
    .UriPathSegment         = _vxenum__is_valid_uri_path_segment
  },
  // Utility
  .Utility = {
    .NewGraphMapName        = _vxenum__new_graph_map_name,
    .ListDir                = _vxenum__list_dir
  },
  // Parse
  .Parse = {
    .AllowPrefixWildcard    = _vxenum__parse_allow_prefix_wildcard_and_escape
  }
};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __THREAD object_allocator_context_t *gt_CSTRING_DESERIALIZATION_ALLOCATOR_CONTEXT = NULL;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxenum__set_cstring_deserialization_allocator_context( object_allocator_context_t *context ) {
  gt_CSTRING_DESERIALIZATION_ALLOCATOR_CONTEXT = context;
  CStringSetAllocatorContext( context );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN object_allocator_context_t * _vxenum__get_cstring_deserialization_allocator_context( void ) {
  return gt_CSTRING_DESERIALIZATION_ALLOCATOR_CONTEXT;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxenum__serialize_cstring( const CString_t *CSTR__string, CQwordQueue_t *output ) {
  QWORD handle_qword = icstringobject.AsHandle( CSTR__string ).qword;
  return CALLABLE( output )->WriteNolock( output, &handle_qword, 1 );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN CString_t * _vxenum__deserialize_cstring_nolock( comlib_object_t *container, CQwordQueue_t *input ) {
  cxmalloc_handle_t handle;
  if( CALLABLE( input )->NextNolock( input, &handle.qword ) == 1 ) {
    return icstringobject.FromHandleNolock( handle, gt_CSTRING_DESERIALIZATION_ALLOCATOR_CONTEXT );
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char * _vxenum__cstring_dumps( const CString_t *CSTR__string ) {
  char *data = NULL;
  icstringobject.Serialize( &data, CSTR__string );
  return data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * _vxenum__cstring_loads( const char *data ) {
  return icstringobject.Deserialize( data, NULL );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__new_string( object_allocator_context_t *allocator_context, const char *str ) {
  CString_constructor_args_t args = {
    .string       = str,
    .len          = -1,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = allocator_context
  };
  return COMLIB_OBJECT_NEW( CString_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__new_string_len( object_allocator_context_t *allocator_context, const char *str, int len ) {
  CString_constructor_args_t args = {
    .string       = str,
    .len          = len,
    .ucsz         = 0,
    .format       = NULL,
    .format_args  = NULL,
    .alloc        = allocator_context
  };
  return COMLIB_OBJECT_NEW( CString_t, NULL, &args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__set_new_string( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *str ) {
  int ret = 0;
  if( CSTR__obj ) {
    CString_constructor_args_t args = {
      .string       = str,
      .len          = -1,
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = allocator_context
    };
    // Delete previous string if any, and reuse its allocator context
    if( *CSTR__obj != NULL ) {
      args.alloc = (*CSTR__obj)->allocator_context;
      COMLIB_OBJECT_DESTROY( *CSTR__obj );
    }
    // Create the new string
    if( (*CSTR__obj = COMLIB_OBJECT_NEW( CString_t, NULL, &args )) == NULL ) {
      ret = -1;
    }
  }
  else {
    ret = -1;
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__new_string_format( object_allocator_context_t *allocator_context, const char *format, ... ) {
  va_list args;
  va_start( args, format );
  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = -1,
    .ucsz         = 0,
    .format       = format,
    .format_args  = &args,
    .alloc        = allocator_context
  };
  CString_t *CSTR__string = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args );
  va_end( args );
  return CSTR__string;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__set_new_string_format( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, ... ) {
  int ret = 0;
  va_list args;

  if( CSTR__obj ) {
    va_start( args, format );
    CString_constructor_args_t string_args = {
      .string       = NULL,
      .len          = -1,
      .ucsz         = 0,
      .format       = format,
      .format_args  = &args,
      .alloc        = allocator_context
    };
    // Delete previous string if any, and reuse its allocator context
    if( *CSTR__obj != NULL ) {
      string_args.alloc = (*CSTR__obj)->allocator_context;
      COMLIB_OBJECT_DESTROY( *CSTR__obj );
    }
    // Create the new string
    if( (*CSTR__obj = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
      ret = -1;
    }
  }
  else {
    ret = -1;
  }

  va_end( args );
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__new_string_vformat( object_allocator_context_t *allocator_context, const char *format, va_list *args ) {
  CString_constructor_args_t string_args = {
    .string       = NULL,
    .len          = -1,
    .ucsz         = 0,
    .format       = format,
    .format_args  = args,
    .alloc        = allocator_context
  };
  return COMLIB_OBJECT_NEW( CString_t, NULL, &string_args );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxenum__set_new_string_vformat( object_allocator_context_t *allocator_context, CString_t **CSTR__obj, const char *format, va_list *args ) {
  int ret = 0;

  if( CSTR__obj ) {
    CString_constructor_args_t string_args = {
      .string       = NULL,
      .len          = -1,
      .ucsz         = 0,
      .format       = format,
      .format_args  = args,
      .alloc        = allocator_context
    };
    // Delete previous string if any, and reuse its allocator context
    if( *CSTR__obj != NULL ) {
      string_args.alloc = (*CSTR__obj)->allocator_context;
      COMLIB_OBJECT_DESTROY( *CSTR__obj );
    }
    // Create the new string
    if( (*CSTR__obj = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
      ret = -1;
    }
  }
  else {
    ret = -1;
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__clone_string( const CString_t *CSTR__string ) {
  return CALLABLE( CSTR__string )->Clone( CSTR__string );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxenum__discard_string( CString_t **CSTR__obj ) {
  if( CSTR__obj && *CSTR__obj ) {
    COMLIB_OBJECT_DESTROY( *CSTR__obj );
    *CSTR__obj = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_StringList_t * _vxenum__new_string_list( object_allocator_context_t *allocator_context, int64_t sz ) {
  vgx_StringList_t *list = (vgx_StringList_t*)calloc( 1, sizeof( vgx_StringList_t ) );
  if( list ) {
    list->sz = sz;
    list->item_allocator = allocator_context;
    if( (list->data = (CString_t**)calloc( sz+1, sizeof( CString_t* ) )) == NULL ) {
      iString.List.Discard( &list );
    }
  }
  return list;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * _vxenum__string_list_set_item( vgx_StringList_t *list, int64_t i, const char *str ) {
  if( i >= 0 && i < list->sz ) {
    iString.Discard( &list->data[ i ] );
    return list->data[ i ] = iString.New( list->item_allocator, str );
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
static CString_t * _vxenum__string_list_set_item_len( vgx_StringList_t *list, int64_t i, const char *str, int len ) {
  if( i >= 0 && i < list->sz ) {
    iString.Discard( &list->data[ i ] );
    return list->data[ i ] = iString.NewLen( list->item_allocator, str, len );
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
static CString_t * _vxenum__string_list_set_item_steal( vgx_StringList_t *list, int64_t i, CString_t **CSTR__str ) {
  CString_t *CSTR__ret = NULL;
  if( i >= 0 && i < list->sz ) {
    CSTR__ret = *CSTR__str;
    *CSTR__str = NULL;
    iString.Discard( &list->data[ i ] );
    list->data[ i ] = CSTR__ret;
  }
  return CSTR__ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * _vxenum__string_list_get_item( vgx_StringList_t *list, int64_t i ) {
  if( i >= 0 && i < list->sz ) {
    return list->data[ i ];
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
static CString_t * _vxenum__string_list_get_own_item( vgx_StringList_t *list, int64_t i ) {
  CString_t *CSTR__item = _vxenum__string_list_get_item( list, i );
  if( CSTR__item ) {
    return OwnOrCloneCString( CSTR__item, list->item_allocator );
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
static const char * _vxenum__string_list_get_chars( const vgx_StringList_t *list, int64_t i ) {
  CString_t *CSTR__str;
  if( i >= 0 && i < list->sz && (CSTR__str = list->data[ i ]) != NULL ) {
    return CStringValue( CSTR__str );
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
static const objectid_t * _vxenum__string_list_get_obid( vgx_StringList_t *list, int64_t i ) {
  CString_t *CSTR__str;
  if( i >= 0 && i < list->sz && (CSTR__str = list->data[ i ]) != NULL ) {
    return CALLABLE( CSTR__str )->Obid( CSTR__str );
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
__inline static bool __cstring_match( const CString_t *CSTR__str, const char *str, bool ignore_case ) {
  if( CSTR__str == NULL ) {
    return false;
  }
  if( ignore_case ) {
    const char *val = CStringValue( CSTR__str );
    return CharsEqualsConstNocase( val, str );
  }
  else {
    return CStringEqualsChars( CSTR__str, str );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxenum__string_list_contains( const vgx_StringList_t *list, const char *str, bool ignore_case ) {
  CString_t **cursor = list->data;
  CString_t **end = cursor + list->sz;
  while( cursor < end ) {
    const CString_t *CSTR__str = *cursor++;
    if( __cstring_match( CSTR__str, str, ignore_case ) ) {
      return 1;
    }
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxenum__string_list_remove_item( vgx_StringList_t *list, const char *str, bool ignore_case ) {
  CString_t **cursor = list->data;
  CString_t **end = cursor + list->sz;
  // Scan all items
  while( cursor < end ) {
    // Found item to remove
    if( __cstring_match( *cursor, str, ignore_case ) ) {
      // Reduce size
      list->sz--;
      end--;
      // Move subsequent items one slot up
      while( cursor < end ) {
        *cursor = *(cursor+1);
        cursor++;
      }
      // Clear last (unused slot)
      *cursor = NULL;
      return 1;
    }
    ++cursor;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * _vxenum__string_list_append( vgx_StringList_t *list, const char *str ) {
  CString_t *CSTR__str = iString.New( list->item_allocator, str );
  CString_t *CSTR__ret = _vxenum__string_list_append_steal( list, &CSTR__str );
  if( CSTR__ret == NULL ) {
    iString.Discard( &CSTR__str );
  }
  return CSTR__ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * _vxenum__string_list_append_steal( vgx_StringList_t *list, CString_t **CSTR__str ) {
  CString_t *CSTR__ret = NULL;
  CString_t **new_data = (CString_t**)realloc( list->data, (list->sz + 1) * sizeof( CString_t* ) );
  if( new_data ) {
    CSTR__ret = *CSTR__str;
    *CSTR__str = NULL;
    list->data = new_data;
    list->data[ list->sz++ ] = CSTR__ret;
  }
  return CSTR__ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t ** _vxenum__string_list_data( vgx_StringList_t *list ) {
  return list->data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxenum__string_list_size( const vgx_StringList_t *list ) {
  return list->sz;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmpsort_asc( const CString_t **CSTR__a, const CString_t **CSTR__b ) {
  const char *a = CStringValue( *CSTR__a );
  const char *b = CStringValue( *CSTR__b );
  return strcmp(a, b);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmpsort_desc( const CString_t **CSTR__a, const CString_t **CSTR__b ) {
  const char *a = CStringValue( *CSTR__a );
  const char *b = CStringValue( *CSTR__b );
  return strcmp(b, a);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxenum__string_list_sort( vgx_StringList_t *list, bool ascending ) {
  int (*compare)( const void *a, const void *b ) = (int(*)(const void*, const void*)) (ascending ? __cmpsort_asc : __cmpsort_desc);
  qsort( list->data, list->sz, sizeof( CString_t* ), compare );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_StringList_t * _vxenum__string_list_clone( const vgx_StringList_t *list ) {
  vgx_StringList_t *clone = iString.List.New( list->item_allocator, list->sz );
  if( clone ) {
    CString_t **cursor = list->data;
    CString_t **end = cursor + list->sz;
    CString_t **dest = clone->data;
    while( cursor < end ) {
      if( (*dest++ = iString.Clone( *cursor++ )) == NULL ) {
        iString.List.Discard( &clone );
        break;
      }
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void _vxenum__discard_string_list( vgx_StringList_t **list ) {
  if( list && *list ) {
    CString_t **cursor = (*list)->data;
    CString_t **end = cursor + (*list)->sz;
    while( cursor < end ) {
      iString.Discard( cursor );
      ++cursor;
    }
    free( (*list)->data );
    free( *list );
    *list = NULL;
  }
}



/*******************************************************************//**
 *
 * 
 ***********************************************************************
 */
static int _vxenum__list_dir( const char *dirpath, const char *query, vgx_StringList_t **rlist ) {
  int ret = 0;

  // Allocate return list
  vgx_StringList_t *list = iString.List.New( NULL, 0 );
  if( list == NULL ) {
    return -1;
  }

#ifdef CXPLAT_WINDOWS_X64

  char path[MAX_PATH+1] = {0};
  WIN32_FIND_DATA find_data = {0};
  HANDLE find_handle = NULL;

  XTRY {
    
    // Make sure the provided directory exists
    snprintf( path, MAX_PATH, "%s\\*", dirpath );
    SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED
    if( (find_handle = FindFirstFile( path, &find_data )) == INVALID_HANDLE_VALUE ) {
      // Directory does not exist
      THROW_SILENT( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    FindClose( find_handle );
    find_handle = NULL;
    memset( &find_data, 0, sizeof( WIN32_FIND_DATA ) );

    // Populate return list with all matching files found in directory
    snprintf( path, MAX_PATH, "%s\\%s", dirpath, query );
    SUPPRESS_WARNING_STRING_NOT_ZERO_TERMINATED
    if( (find_handle = FindFirstFile( path, &find_data )) == INVALID_HANDLE_VALUE ) {
      // No matching files
      XBREAK;
    }

    // First file
    if( iString.List.Append( list, find_data.cFileName ) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
    }

    // Rest of files
    while( FindNextFile( find_handle, &find_data ) ) {
      if( iString.List.Append( list, find_data.cFileName ) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
    }
    FindClose( find_handle );
    find_handle = NULL;
  }
  XCATCH( errcode ) {
    ret = -1;
    iString.List.Discard( &list );
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

  XTRY {
    // Make sure the provided directory exists and can be accessed
    if( (d = opendir( dirpath )) == NULL ) {
      char error_buf[ SZ_ERRBUF ] = {0};
      const char* error_str = get_error_reason( errno, error_buf, SZ_ERRBUF );
      REASON( 0x000, "Unable to open directory '%s': %s", dirpath, error_str );
      THROW_SILENT( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    
    while( (current = readdir( d )) != NULL ) {
      if( !fnmatch( query, current->d_name, 0 ) ) {
        if( iString.List.Append( list, current->d_name ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
        }
      }
    }

  }
  XCATCH( errcode ) {
    ret = -1;
    iString.List.Discard( &list );
  }
  XFINALLY {
    if( d != NULL ) {
      closedir( d );
    }
  }

#endif
  *rlist = list;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxenum__new_graph_map_name( vgx_Graph_t *self, const CString_t *CSTR__prefix ) {
  if( CSTR__prefix ) {
    const char *prefix = CStringValue( CSTR__prefix );
    const char *graph_name = self ? CStringValue( self->CSTR__name ) : "ephemeral";
    return _vxenum__new_string_format( NULL, "%s_[%s]", prefix, graph_name );
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
DLL_HIDDEN int _vxenum__initialize( void ) {
  if(    _vxenum_dim__initialize()
      && _vxenum_prop__initialize()
      && _vxenum_rel__initialize()
      && _vxenum_vtx__initialize()
    )
  {
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
DLL_HIDDEN int _vxenum__destroy( void ) {
  if(    _vxenum_vtx__destroy()
      && _vxenum_rel__destroy()
      && _vxenum_prop__destroy()
      && _vxenum_dim__destroy()
    )
  {
    return 1;
  }
  else {
    return 0;
  }
}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxenum.h"
  
test_descriptor_t _vgx_vxenum_tests[] = {
  { "VGX Graph Enumeration Tests", __utest_vxenum },
  {NULL}
};
#endif
