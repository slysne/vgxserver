/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxvertex_property.c
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

#include "_vgx.h"
#include "_vxenum.h"
#include "_vgx_serialization.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct __s_serialized_property {
  union {
    QWORD qwords[3];
    struct {
      shortid_t namehash;
      vgx_value_t value;
    };
  };
} __serialized_property;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
static const __serialized_property __END_OF_PROPERTIES = {
  .namehash = 0,                  //
  .value = {                      //
    .type = VGX_VALUE_TYPE_NULL,  //
    .data = {                     //
      .bits = 0                   //
    }                             //
  }                               //
};


static int __insert_or_update_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop );
static vgx_VertexProperty_t * __insert_or_increment_numeric_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop );
static vgx_VertexProperty_t * __get_property_RO( const vgx_Vertex_t *self_RO, const framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop );
static vgx_VertexProperty_t * __get_internal_attribute_RO( const vgx_CollectorItem_t *item_RO, vgx_VertexProperty_t *dest );
static int64_t __collect_cell_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const cell );
static vgx_SelectProperties_t * __get_all_properties_RO_CS( vgx_Vertex_t *self_RO, framehash_dynamic_t *dynamic );
static int __match_int64( const int64_t value, const vgx_value_condition_t *condition );
static int __match_double( const double value, const vgx_value_condition_t *condition );
static int __match_string( const char *value, const vgx_value_condition_t *condition );
static int __match_enumerated_string( const CString_t *CSTR__value, const vgx_value_condition_t *condition );
static int __value_match( const vgx_value_t *value, const vgx_value_condition_t *condition );
static bool __has_property_by_keyhash_RO( const vgx_Vertex_t *self_RO, framehash_dynamic_t *dynamic, const vgx_VertexProperty_t *prop, shortid_t keyhash );
static int64_t __num_properties_RO( vgx_Vertex_t *self_RO );
static int __del_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop );
static int64_t __OBJECT64_destroy_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __serialize_property_RO_CS( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell );
static int64_t __deserialize_property_WL_CS( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, vgx_Graph_t *graph_CS, __serialized_property *psprop );
static void __discard_internal_property_data_CS( vgx_Graph_t *self, vgx_VertexProperty_t *prop );
static void _vxvertex_property__free_select_properties( vgx_Graph_t *self, vgx_SelectProperties_t **selected );
static void _vxvertex_property__clear_select_property( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop );
static int __value_condition_is_valid( const vgx_value_condition_t *cond );

static int _vxvertex_property__add_cstring_value( vgx_value_t *dest, CString_t **CSTR__string );
static vgx_value_t * _vxvertex_property__clone_value( const vgx_value_t *other );
static int _vxvertex_property__clone_value_into( vgx_value_t *dest, const vgx_value_t *src );
static void _vxvertex_property__delete_value( vgx_value_t **value );
static void _vxvertex_property__clear_value( vgx_value_t *value );

static vgx_value_condition_t * _vxvertex_property__new_value_condition( void );
static int _vxvertex_property__clone_value_condition_into( vgx_value_condition_t *dest, const vgx_value_condition_t *src );
static vgx_value_condition_t * _vxvertex_property__clone_value_condition( const vgx_value_condition_t *other );
static void _vxvertex_property__clear_value_condition( vgx_value_condition_t *value_condition );
static void _vxvertex_property__delete_value_condition( vgx_value_condition_t **value_condition );

static vgx_VertexProperty_t * _vxvertex_property__new_from_value_condition( vgx_Graph_t *graph, const char *key, vgx_value_condition_t **value_condition );
static vgx_VertexProperty_t * _vxvertex_property__new_default( vgx_Graph_t *graph, const char *key );
static vgx_VertexProperty_t * _vxvertex_property__new_integer( vgx_Graph_t *graph, const char *key, int64_t value );
static vgx_VertexProperty_t * _vxvertex_property__new_real( vgx_Graph_t *graph, const char *key, double value );
static vgx_VertexProperty_t * _vxvertex_property__new_string( vgx_Graph_t *graph, const char *key, const char *value );
static vgx_VertexProperty_t * _vxvertex_property__new_int_array( vgx_Graph_t *graph, const char *key, int64_t sz, const QWORD data[] );

static int _vxvertex_property__clone_into( vgx_VertexProperty_t *dest, const vgx_VertexProperty_t *src );
static vgx_VertexProperty_t * _vxvertex_property__clone( const vgx_VertexProperty_t *other );
static void _vxvertex_property__clear( vgx_VertexProperty_t *property_condition );
static void _vxvertex_property__delete( vgx_VertexProperty_t **property_condition );

static vgx_PropertyConditionSet_t * _vxvertex_property__new_set( bool positive );
static int _vxvertex_property__clone_set_into( vgx_PropertyConditionSet_t *dest, const vgx_PropertyConditionSet_t *src );
static vgx_PropertyConditionSet_t * _vxvertex_property__clone_set( const vgx_PropertyConditionSet_t *other );
static void _vxvertex_property__clear_set( vgx_PropertyConditionSet_t *property_condition_set );
static void _vxvertex_property__delete_set( vgx_PropertyConditionSet_t **property_condition_set );


static int64_t _vxvertex_property__write_virtual_property_CS( vgx_Graph_t *graph, const objectid_t *vertex_obid, int64_t offset, const vgx_VertexProperty_t *prop );
static int _vxvertex_property__erase_virtual_property_CS( vgx_Graph_t *graph, int64_t offset );


/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __cstring_enum_equals( const CString_t *CSTR__A, const CString_t *CSTR__B ) {
  return (CStringAttributes( CSTR__A ) == CStringAttributes( CSTR__B )) && CStringEquals( CSTR__A, CSTR__B );
}



/*******************************************************************//**
 *
 * Return: 1  = new name=value inserted
 *         0  = previous name=value overwritten (or same name=value)
 *        -1  = error
 ***********************************************************************
 */
static int __insert_or_update_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop ) {
  int n_inserted = 0;

  vgx_Graph_t *graph_CS = self_WL->graph;

  comlib_object_t *new_shared_value = NULL;
  CString_t *CSTR__mapped_key = NULL;
  

  // Add the key string to the set of all property keys for this graph, unless the key already exists. If it does not
  // already exist the key map will add it and own a reference to the key it enters into itself. This string could be
  // the one we pass in (it will be increfed) or a new string clone. After this call the key map is guaranteed to own
  // exactly one reference to the key. The vertex must take ownership of the shared instance if this is a new property.
  if( (prop->keyhash = iEnumerator_CS.Property.Key.Encode( graph_CS, prop->key, &CSTR__mapped_key, true )) == 0 ) {
    return -1;
  }
  
  XTRY {
    framehash_keytype_t ktype = CELL_KEY_TYPE_HASH64;
    framehash_key_t fkey = &prop->keyhash;
    framehash_valuetype_t vtype;
    framehash_value_t fvalue;
    comlib_object_t *previous_object = NULL;
    CString_t *CSTR__shared = NULL;
    bool vertex_becomes_key_owner = false;
    int64_t vprop_offset = 0;
    objectid_t *vertex_obid = __vertex_internalid( self_WL );

    // Get previous value
    framehash_valuetype_t prev_vtype = iFramehash.simple.Get( self_WL->properties, dynamic, ktype, fkey, &fvalue );

    // Property does not exist for vertex
    if( prev_vtype == CELL_VALUE_TYPE_NULL ) {
      // Vertex must take ownership of mapped key after successful insertion later
      vertex_becomes_key_owner = true;
    }
    else if( prev_vtype == CELL_VALUE_TYPE_ERROR ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x481 );
    }
    // A previous property with same name exist, no ownership of key by vertex needed
    // The previous value is an object
    else if( prev_vtype == CELL_VALUE_TYPE_OBJECT64 ) {
      // Previous value is CString
      comlib_object_t *obj64 = COMLIB_OBJECT(fvalue);
      if( COMLIB_OBJECT_ISINSTANCE( obj64, CString_t ) ) {
        previous_object = obj64;
        // New value is CString
        if( prop->val.type == VGX_VALUE_TYPE_ENUMERATED_CSTRING ) {
          CString_t *CSTR__previous = (CString_t*)previous_object;
          // New and previous CStrings are the same
          if( CStringEquals( prop->val.data.simple.CSTR__string, CSTR__previous ) ) {
            // No further action needed
            XBREAK;
          }
        }
      }
      // No other object types supported at this time! (future?)
      else {
        // We should never find an OBJECT64 in the map that isn't a CString (at this time)
        THROW_ERROR( CXLIB_ERR_ASSERTION, 0x482 );
      }
    }
    // Previous value is a virtual (disk) property reference
    else if( prev_vtype == CELL_VALUE_TYPE_UNSIGNED ) {
      vprop_offset = (int64_t)(*((uint64_t*)&fvalue));
      if( _vxvertex_property__erase_virtual_property_CS( graph_CS, vprop_offset ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x483 );
      }
    }
    // Previous value is a plain value
    else {
      // We don't have to check things since it can we overwritten without dealing with refcounts
    }

    // Set up insertion context for name=value
    switch( prop->val.type ) {
    case VGX_VALUE_TYPE_BOOLEAN:
      vtype = CELL_VALUE_TYPE_MEMBER;
      fvalue = 0; // doesn't matter when using HASH64 keys
      break;
    case VGX_VALUE_TYPE_INTEGER:
      vtype = CELL_VALUE_TYPE_INTEGER;  // 56-bit signed int
      *((int64_t*)&fvalue) = prop->val.data.simple.integer;
      break;
    case VGX_VALUE_TYPE_REAL:
      vtype = CELL_VALUE_TYPE_REAL;     // 56-bit floating point
      *((double*)&fvalue) = prop->val.data.simple.real;
      break;
    case VGX_VALUE_TYPE_CSTRING:
      // Virtual (disk) property
      if( (vprop_offset = _vxvertex_property__write_virtual_property_CS( graph_CS, vertex_obid, vprop_offset, prop )) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x484 );
      }
      // We store the offset as memory property
      vtype = CELL_VALUE_TYPE_UNSIGNED; // unsigned type signifies disk reference in the context of properties
      *((uint64_t*)&fvalue) = (uint64_t)vprop_offset;
      break;
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      // Normal resident property
      // Add this string to the set of all property values for this graph. The value map will own a reference to the string
      // it enters into itself (if it does not already exist.) When a new string is added to the map the ownership will either
      // be in the form of an incref of the string we pass in, or a new clone. In either case the value map's own string object
      // is returned. The vertex needs to incref this string instance to take ownership as well. This happens below.
      if( (CSTR__shared = iEnumerator_CS.Property.Value.Store( graph_CS, prop->val.data.simple.CSTR__string )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x485 );
      }
      else {
        // The vertex will now own a reference to the shared value
        icstringobject.IncrefNolock( CSTR__shared ); // <--------- Vertex is also owner
        new_shared_value = COMLIB_OBJECT( CSTR__shared );
        vtype = CELL_VALUE_TYPE_OBJECT64;
        fvalue = new_shared_value;
      }
      break;
    case VGX_VALUE_TYPE_STRING:
      // Plain strings not supported
      THROW_ERROR( CXLIB_ERR_API, 0x486 );
      break;
    default:
      // Unsupported property value type
      THROW_ERROR( CXLIB_ERR_API, 0x487 );
    }
    
    // Perform insertion of name=value
    DYNAMIC_LOCK( dynamic ) {
      n_inserted = iFramehash.simple.Set( &self_WL->properties, dynamic, ktype, fkey, vtype, fvalue );
    } DYNAMIC_RELEASE;
    if( n_inserted > 0 ) {
      // Increment global counter
      IncGraphPropCount( graph_CS );
    }
    else if( n_inserted < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x488 );
    }

    // Vertex takes ownership of key if this is a new property for the vertex
    if( vertex_becomes_key_owner ) {
      iEnumerator_CS.Property.Key.Own( graph_CS, CSTR__mapped_key );
    }

    // We replaced a previous object pointer (and framehash doesn't decref OBJECT64 instances when replaced)
    if( previous_object ) {
      // Previous value was a CString
      if( COMLIB_OBJECT_ISINSTANCE( previous_object, CString_t ) ) {
        // Manually give up ownership of the previous value we just replaced
        _vxenum_propval__discard_value_CS( graph_CS, (CString_t*)previous_object );
      }
    }

    // Capture
    iOperation.Vertex_WL.SetProperty( self_WL, prop );


  }
  XCATCH( errcode ) {
    if( new_shared_value ) {
      if( COMLIB_OBJECT_ISINSTANCE( new_shared_value, CString_t ) ) {
        _vxenum_propval__discard_value_CS( graph_CS, (CString_t*)new_shared_value );
      }
    }
    n_inserted = -1;
  }
  XFINALLY {
  }

  return n_inserted;
}



/*******************************************************************//**
 *
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * __insert_or_increment_numeric_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop ) {

  vgx_VertexProperty_t *ret_prop = NULL;

  framehash_valuetype_t incvtype;
  framehash_value_t finc;

  switch( prop->val.type ) {
  case VGX_VALUE_TYPE_INTEGER:
    incvtype = CELL_VALUE_TYPE_INTEGER;
    *((int64_t*)&finc) = prop->val.data.simple.integer;
    break;
  case VGX_VALUE_TYPE_REAL:
    incvtype = CELL_VALUE_TYPE_REAL;
    *((double*)&finc) = prop->val.data.simple.real;
    break; // ok
  default:
    // Incompatible inc value
    return NULL;
  }

  vgx_Graph_t *graph_CS = self_WL->graph;

  CString_t *CSTR__mapped_key = NULL;

  // Add the key string to the set of all property keys for this graph, unless the key already exists. If it does not
  // already exist the key map will add it and own a reference to the key it enters into itself. This string could be
  // the one we pass in (it will be increfed) or a new string clone. After this call the key map is guaranteed to own
  // exactly one reference to the key. The vertex must take ownership of the shared instance if this is a new property.
  if( (prop->keyhash = iEnumerator_CS.Property.Key.Encode( graph_CS, prop->key, &CSTR__mapped_key, true )) == 0 ) {
    return NULL;
  }
  
  XTRY {
    framehash_keytype_t ktype = CELL_KEY_TYPE_HASH64;
    framehash_key_t fkey = &prop->keyhash;
    framehash_value_t fvalue;
    framehash_valuetype_t vtype;
    bool property_is_new = false;

    DYNAMIC_LOCK( dynamic ) {
      vtype = iFramehash.simple.Inc( &self_WL->properties, dynamic, ktype, fkey, incvtype, finc, &fvalue, &property_is_new );
    } DYNAMIC_RELEASE;

    // Set return value
    switch( vtype ) {
    case CELL_VALUE_TYPE_INTEGER:
      prop->val.type = VGX_VALUE_TYPE_INTEGER;
      prop->val.data.simple.integer = (int64_t)fvalue;
      break;
    case CELL_VALUE_TYPE_REAL:
      prop->val.type = VGX_VALUE_TYPE_REAL;
      prop->val.data.simple.real = *((double*)&fvalue);
      break;
    default:
      // Can't increment, either internal error or more likely incompatible previous value
      THROW_SILENT( CXLIB_ERR_LOOKUP, 0x491 );
    }

    // New property for vertex, it must take ownership of key string
    if( property_is_new ) {
      // Increment global counter
      IncGraphPropCount( graph_CS );
      iEnumerator_CS.Property.Key.Own( graph_CS, CSTR__mapped_key );
    }

    // Capture
    iOperation.Vertex_WL.SetProperty( self_WL, prop );
    
    // Success
    ret_prop = prop;

  }
  XCATCH( errcode ) {
    ret_prop = NULL;
  }
  XFINALLY {
  }

  return ret_prop;
}



/*******************************************************************//**
 *
 *
 *
 *
 ***********************************************************************
 */
__inline static int __fh_insert_internal_integer_DCS( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, shortid_t key, int64_t value ) {
  framehash_value_t fval;
  *((int64_t*)&fval) = value;
  return iFramehash.simple.Set( entrypoint, dynamic, CELL_KEY_TYPE_HASH64, &key, CELL_VALUE_TYPE_INTEGER, fval );
}



/*******************************************************************//**
 *
 *
 *
 *
 ***********************************************************************
 */
static int __insert_internal_integer_WL( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, shortid_t key, int64_t value ) {
  int ins;
  DYNAMIC_LOCK( dynamic ) {
    ins = __fh_insert_internal_integer_DCS( &self_WL->properties, dynamic, key, value );
  } DYNAMIC_RELEASE;

  if( ins < 0 ) {
    return -1;
  }

  vgx_VertexProperty_t prop = {0};
  prop.keyhash = key;
  prop.val.type = VGX_VALUE_TYPE_INTEGER;
  prop.val.data.simple.integer = value;

  // Capture
  iOperation.Vertex_WL.SetProperty( self_WL, &prop );

  return 0;
}



/*******************************************************************//**
 *
 *
 * WARNING: CLIENT IS RESPONSIBLE for the passed (and returned) prop 
 * pointer, and needs to free prop when done with it. Also, if the prop->type
 * is set to enumerated string (CString) upon return, then the caller OWNS
 * ANOTHER REFERENCE to prop->val.pointer and must decref prop->val.pointer exactly
 * once before freeing the prop. The caller must use the ienumerator interface
 * to decref the value, NOT the icstringobject interface. This is to ensure
 * the property mapping can clean up properly if the last reference to
 * the property is being deleted in another thread right after this thread
 * did the get_property call. (Ok, extreme edge case.)
 * NOTE: The prop->name is already owned by caller and can be discarded
 * any way the caller sees fit. We do not grant another ownership of key
 * in this function.
 ***********************************************************************
 */
static vgx_VertexProperty_t * __get_property_RO( const vgx_Vertex_t *self_RO, const framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop ) {

  if( IsPropertyKeyHashVertexEnum( prop->keyhash ) ) {
    prop->val.type = VGX_VALUE_TYPE_NULL;
    prop->val.data.simple.integer = 0;
    return prop;
  }

  vgx_VertexProperty_t *ret_prop = NULL;
  vgx_Graph_t *graph = self_RO->graph;

  // Look up the namehash for the property key
  shortid_t namehash = prop->keyhash;
  if( namehash == 0 ) {
    namehash = iEnumerator_OPEN.Property.Key.GetEnum( graph, prop->key );
  }
  
  XTRY {
    framehash_keytype_t ktype = CELL_KEY_TYPE_HASH64;
    framehash_key_t fkey = &namehash;
    framehash_valuetype_t vtype;
    framehash_value_t fvalue;

    // Look up the property
    if( (vtype = iFramehash.simple.Get( self_RO->properties, dynamic, ktype, fkey, &fvalue )) != CELL_VALUE_TYPE_NULL ) {
      // Set the return value based on what we found
      switch( vtype ) {
      case CELL_VALUE_TYPE_MEMBER:
        prop->val.type = VGX_VALUE_TYPE_BOOLEAN;
        prop->val.data.simple.integer = 0;
        break;
      case CELL_VALUE_TYPE_UNSIGNED:
        // Special case, if query indicates type integer we fall thru and return the offset instead of the string
        if( prop->val.type != VGX_VALUE_TYPE_INTEGER ) {
          if( (prop->val.data.simple.CSTR__string = _vxvertex_property__read_virtual_property( graph, (uint64_t)fvalue, NULL, NULL )) != NULL ) {
            prop->val.type = VGX_VALUE_TYPE_CSTRING;
            break;
          }
        }
        /* FALLTHRU */
      case CELL_VALUE_TYPE_INTEGER:
        prop->val.type = VGX_VALUE_TYPE_INTEGER;
        prop->val.data.simple.integer = (int64_t)fvalue;
        break;
      case CELL_VALUE_TYPE_REAL:
        prop->val.type = VGX_VALUE_TYPE_REAL;
        prop->val.data.simple.real = *((double*)&fvalue);
        break;
      case CELL_VALUE_TYPE_OBJECT64:
        // Enumeration requested (don't decode)
        if( CStringValue( prop->key )[0] == '#' ) {
          prop->val.type = VGX_VALUE_TYPE_INTEGER;
          prop->val.data.simple.integer = (int64_t)fvalue;
        }
        // Decode
        else {
          prop->val.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING;
          prop->val.data.simple.CSTR__string = (CString_t*)fvalue;
          _vxenum_propval__own_value_OPEN( graph, prop->val.data.simple.CSTR__string );
        }
        break;
      default:
        // For now, no other types (future?)
        THROW_ERROR( CXLIB_ERR_ASSERTION, 0x4A1 );
      }
    }
    // No property with the requested name found
    else {
      prop->val.type = VGX_VALUE_TYPE_NULL;
      prop->val.data.simple.integer = 0;
    }

    // Return value is the passed prop when no error occurred
    ret_prop = prop;
  }
  XCATCH( errcode ) {
    prop->val.type = VGX_VALUE_TYPE_NULL;
    prop->val.data.simple.integer = 0;
    ret_prop = NULL;
  }
  XFINALLY {
  }

  return ret_prop;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
//static vgx_VertexProperty_t * __get_internal_attribute_RO_CS( const vgx_CollectorItem_t *item_RO_CS, vgx_VertexProperty_t *dest ) {
static vgx_VertexProperty_t * __get_internal_attribute_RO( const vgx_CollectorItem_t *item_RO, vgx_VertexProperty_t *dest ) {
  CString_t *CSTR__str;

  // WARNING: This is currently dangerous to use unless the vertex is guaranteed to remain allocated
  // while the returned attribute is accessed. Vertex identifier data is referenced directly so if
  // the returned attribute is held on to and accessed beyond the lifetime of the vertex the data
  // will be invalid and result in undefined behavior.
  vgx_ArcHead_t archead = {
    .vertex = item_RO->headref->vertex,
    .predicator = item_RO->predicator
  };

  const vgx_Vertex_t *vertex = item_RO->headref->vertex;

  if( vertex ) {
    // Initialize the type to integer since this is a common value type, override if needed
    dest->val.type = VGX_VALUE_TYPE_INTEGER;

    switch( (int)dest->keyhash ) {
    case VGX_RESPONSE_ATTR_ID:
      if( (CSTR__str = vertex->identifier.CSTR__idstr) != NULL ) {
        dest->val.data.simple.CSTR__string = CSTR__str;
        dest->val.type = VGX_VALUE_TYPE_CSTRING;
        GRAPH_LOCK( vertex->graph ) {
          icstringobject.IncrefNolock( CSTR__str );
        } GRAPH_RELEASE;
      }
      else {
        dest->val.data.simple.string = vertex->identifier.idprefix.data;
        dest->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
      }
      return dest;
    case VGX_RESPONSE_ATTR_OBID:
      if( (dest->val.data.simple.string = malloc( 33 )) != NULL ) {
        idtostr( (char*)dest->val.data.simple.string, __vertex_internalid( vertex ) );
        dest->val.type = VGX_VALUE_TYPE_STRING;
        return dest;
      }
      break; // error, set output to null value
    case VGX_RESPONSE_ATTR_TYPENAME:
      GRAPH_LOCK( vertex->graph ) {
        CSTR__str = (CString_t*)_vxenum_vtx__decode_CS( vertex->graph, vertex->descriptor.type.enumeration );
      } GRAPH_RELEASE;
      if( CSTR__str != NULL ) {
        dest->val.data.simple.string = CStringValue( CSTR__str );
        dest->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
        return dest;
      }
      break; // error, set output to null value
    case VGX_RESPONSE_ATTR_TYPENAME | VGX_RESPONSE_ATTR_AS_ENUM:
      dest->val.data.simple.integer = vertex->descriptor.type.enumeration;
      return dest;
    case VGX_RESPONSE_ATTR_DEGREE:
      dest->val.data.simple.integer = CALLABLE( vertex )->Degree( vertex );
      return dest;
    case VGX_RESPONSE_ATTR_INDEGREE:
      dest->val.data.simple.integer = CALLABLE( vertex )->InDegree( vertex );
      return dest;
    case VGX_RESPONSE_ATTR_OUTDEGREE:
      dest->val.data.simple.integer = CALLABLE( vertex )->OutDegree( vertex );
      return dest;
    case VGX_RESPONSE_ATTR_ARCDIR:
      dest->val.data.simple.string = __reverse_arcdir_map[ archead.predicator.rel.dir ];
      dest->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
      return dest;
    case VGX_RESPONSE_ATTR_ARCDIR | VGX_RESPONSE_ATTR_AS_ENUM:
      dest->val.data.simple.integer = archead.predicator.rel.dir;
      return dest;
    case VGX_RESPONSE_ATTR_RELTYPE:
      GRAPH_LOCK( vertex->graph ) {
        CSTR__str = (CString_t*)_vxenum_rel__decode_CS( vertex->graph, archead.predicator.rel.enc );
      } GRAPH_RELEASE;
      if( CSTR__str != NULL ) {
        dest->val.data.simple.string = CStringValue( CSTR__str );
        dest->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
        return dest;
      }
      break; // error, set output to null value
    case VGX_RESPONSE_ATTR_RELTYPE | VGX_RESPONSE_ATTR_AS_ENUM:
      dest->val.data.simple.integer = archead.predicator.rel.enc;
      return dest;
    case VGX_RESPONSE_ATTR_MODIFIER:
      dest->val.data.simple.string = _vgx_modifier_as_string( archead.predicator.mod );
      dest->val.type = VGX_VALUE_TYPE_BORROWED_STRING;
      return dest;
    case VGX_RESPONSE_ATTR_MODIFIER | VGX_RESPONSE_ATTR_AS_ENUM:
      dest->val.data.simple.integer = archead.predicator.mod.bits;
      return dest;
    case VGX_RESPONSE_ATTR_VALUE:
      if( _vgx_predicator_value_is_float( archead.predicator ) ) {
        dest->val.data.simple.real = archead.predicator.val.real;
        dest->val.type = VGX_VALUE_TYPE_REAL;
      }
      else {
        dest->val.data.simple.integer = archead.predicator.val.integer;
      }
      return dest;
    case VGX_RESPONSE_ATTR_VALUE | VGX_RESPONSE_ATTR_AS_ENUM:
      dest->val.data.simple.integer = archead.predicator.val.integer;
      return dest;
    case VGX_RESPONSE_ATTR_VECTOR:
      if( vertex->vector ) {
        vgx_Graph_t *graph = vertex->graph;
        vgx_Vector_t *V_ext = CALLABLE( graph->similarity )->TranslateVector( graph->similarity, vertex->vector, true, NULL );
        if( V_ext ) {
          // NOTE: We know the external vector formatter renders 42 bytes max per element, always true since last element
          //       doesn't include ", " and therefore accounts for the first "[" and last "]".
          int sz = (V_ext->metas.vlen * 42 + 64) & ~63; // allocate in multiples of 64 bytes (note the \0 term included in 64=1+63)
          sz += 64; // safety
          if( (dest->val.data.simple.string = calloc( sz, 1 )) != NULL ) {
            char *cursor = (char*)dest->val.data.simple.string;
            CALLABLE( V_ext )->ToBuffer( V_ext, sz-1, &cursor );
            dest->val.type = VGX_VALUE_TYPE_STRING;
            return dest;
          }
        }
      }
      break; // no vector or error, set output to null value
    case VGX_RESPONSE_ATTR_VECTOR | VGX_RESPONSE_ATTR_AS_ENUM:
      if( vertex->vector ) {
        vgx_Vector_t * V_int = vertex->vector;
        // NOTE: We know the external vector formatter renders 17 bytes max per element, always true since last element
        //       doesn't include ", " and therefore accounts for the first "[" and last "]".
        int sz = (V_int->metas.vlen * 17 + 64) & ~63; // allocate in multiples of 64 bytes (note the \0 term included in 64=1+63)
        sz += 64; // safety
        if( (dest->val.data.simple.string = calloc( sz, 1 )) != NULL ) {
          char *cursor = (char*)dest->val.data.simple.string;
          CALLABLE( V_int )->ToBuffer( V_int, sz-1, &cursor );
          dest->val.type = VGX_VALUE_TYPE_STRING;
          return dest;
        }
      }
      break; // no vector or error, set output to null value
    case VGX_RESPONSE_ATTR_RANKSCORE:
      dest->val.data.simple.real = item_RO->sort.flt64.value;
      dest->val.type = VGX_VALUE_TYPE_REAL;
      return dest;
    case VGX_RESPONSE_ATTR_TMC:
      dest->val.data.simple.integer = vertex->TMC;
      return dest;
    case VGX_RESPONSE_ATTR_TMM:
      dest->val.data.simple.integer = vertex->TMM;
      return dest;
    case VGX_RESPONSE_ATTR_TMX:
      dest->val.data.simple.integer = vertex->TMX.vertex_ts;
      return dest;
    case VGX_RESPONSE_ATTR_DESCRIPTOR:
      dest->val.data.simple.integer = vertex->descriptor.bits;
      return dest;
    case VGX_RESPONSE_ATTR_ADDRESS:
      dest->val.data.simple.integer = (int64_t)vertex;
      return dest;
    case VGX_RESPONSE_ATTR_HANDLE:
      dest->val.data.simple.integer = _vxoballoc_vertex_as_handle( vertex ).qword;
      return dest;
    case VGX_RESPONSE_ATTR_RAW_VERTEX:
      {
        int nqw = (int)qwsizeof( vgx_AllocatedVertex_t );
        int sz = (nqw*20 + 64) & ~63;;
        if( (dest->val.data.simple.string = calloc( sz, 1 )) != NULL ) {
          vgx_AllocatedVertex_t *av = ivertexobject.AsAllocatedVertex( vertex );
          QWORD *pq = (QWORD*)av;
          QWORD *endq = pq + nqw;
          char *wp = (char*)dest->val.data.simple.string;
          while( pq < endq ) {
            wp += sprintf( wp, "0x%016llx ", *pq++ );
          }
          *--wp = '\0';
          dest->val.type = VGX_VALUE_TYPE_STRING;
          return dest;
        }
      }
      break; // error
    }
  }

  // Missing field or error, set output to null value
  dest->val.data.simple.integer = 0;
  dest->val.type = VGX_VALUE_TYPE_NULL;
  return dest;

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __collect_cell_into_list( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  const CaptrList_vtable_t *iList = processor->processor.input; // abuse
  CaptrList_t *cells = processor->processor.output;
  return iList->Append( cells, cell );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_SelectProperties_t * _vxvertex_property__get_properties_RO_CS( vgx_Vertex_t *self_RO_CS ) {
  vgx_SelectProperties_t *selected_all = NULL;
  vgx_Graph_t *graph_CS = self_RO_CS->graph;
  framehash_dynamic_t *dynamic = &graph_CS->property_fhdyn;
  CaptrList_t *cells = dynamic->cell_list;
  CaptrList_vtable_t *iList = CALLABLE( cells );
  CString_t *CSTR__value = NULL;

  XTRY {     
    framehash_cell_t cell;

    // Allocate return list
    if( (selected_all = calloc( 1, sizeof( vgx_SelectProperties_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B8 );
    }

    vgx_VertexProperty_t *propcursor = NULL;

    if( self_RO_CS->properties ) {
      // We have to synchronize the framehash dynamic too, because another WL vertex that doesn't need the graph CS
      // may be in the process of doing stuff that also uses this same instance of the dynamic.
      DYNAMIC_LOCK( dynamic ) {
        // Check that cell list is empty before we start collection
        if( iList->Length( cells ) != 0 ) {
          // The cell list isn't empty! Can't collect properties.
          FORCE_DYNAMIC_RELEASE;
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B1 );
        }

        // Run collection
        if( iFramehash.simple.Process( self_RO_CS->properties, __collect_cell_into_list, iList, cells ) < 0 ) {
          FORCE_DYNAMIC_RELEASE;
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B2 );
        }

        // Number of properties
        int64_t n_collected = iList->Length( cells );

        // Allocate properties, with null terminator.
        if( (selected_all->properties = calloc( n_collected + 1, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
          FORCE_DYNAMIC_RELEASE;
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B3 );
        }

        // Build result list from collected cells
        propcursor = selected_all->properties;
        for( int64_t px=0; px<n_collected; px++ ) {
          if( iList->Get( cells, px, &cell ) < 0 ) {
            FORCE_DYNAMIC_RELEASE;
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B4 );
          }
          shortid_t namehash = cell.annotation;

          // Ignore hidden property
          if( IsPropertyKeyHashVertexEnum( namehash ) ) {
            continue;
          }

          CString_t *CSTR__key = (CString_t*)_vxenum_propkey__decode_key_CS( graph_CS, namehash );
          // Caller owns one additional reference to the key and has to decref when done
          // KEY INCREF
          if( CSTR__key && _vxenum_propkey__own_key_CS( graph_CS, CSTR__key ) > 0 ) {
            propcursor->key = CSTR__key;
          }
          // Not incref-able. Ignore property.
          else {
            continue;
          }

          switch( APTR_AS_DTYPE( &cell ) ) {
          case TAGGED_DTYPE_ID56:
            propcursor->val.type = VGX_VALUE_TYPE_BOOLEAN;
            propcursor->val.data.simple.integer = 0;
            break;
          case TAGGED_DTYPE_UINT56:
            if( (propcursor->val.data.simple.CSTR__string = _vxvertex_property__read_virtual_property( graph_CS, APTR_AS_UNSIGNED( &cell ), NULL, NULL )) != NULL ) {
              propcursor->val.type = VGX_VALUE_TYPE_CSTRING;
              break;
            }
            /* FALLTHRU */
          case TAGGED_DTYPE_INT56:
            propcursor->val.type = VGX_VALUE_TYPE_INTEGER;
            propcursor->val.data.simple.integer = APTR_AS_INTEGER( &cell );
            break;
          case TAGGED_DTYPE_REAL56:
            propcursor->val.type = VGX_VALUE_TYPE_REAL;
            propcursor->val.data.simple.real = APTR_GET_REAL( &cell );
            break;
          case TAGGED_DTYPE_OBJ56:
            propcursor->val.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING; // CString_t
            CSTR__value = APTR_GET_OBJ56( &cell );
            // Caller owns one additional reference to the CString value and has to decref when done
            _vxenum_propval__own_value_CS( graph_CS, CSTR__value ); // VALUE INCREF
            propcursor->val.data.simple.CSTR__string = CSTR__value;
            break;
          default:
            FORCE_DYNAMIC_RELEASE;
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B6 );
            break;
          }

          // Count
          selected_all->len++;

          // Next
          ++propcursor;
        }
      } DYNAMIC_RELEASE;
    }
    // No properties
    else {
      // Allocate empty properties, with null terminator.
      if( (selected_all->properties = calloc( 1, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4B7 );
      }
      propcursor = selected_all->properties;
    }
    // Terminate
    if( propcursor ) {
      propcursor->val.type = VGX_VALUE_TYPE_NULL;
      propcursor->val.data.simple.integer = 0;
    }
  }
  XCATCH( errcode ) {
    iVertexProperty.FreeSelectProperties( graph_CS, &selected_all );
  }
  XFINALLY {
    if( cells ) {
      iList->Clear( cells );
    }
  }
  return selected_all;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_SelectProperties_t * _vxvertex_property__get_properties_RO( vgx_Vertex_t *self_RO ) {
  vgx_SelectProperties_t *selected_all = NULL;

  GRAPH_LOCK( self_RO->graph ) {
    selected_all = _vxvertex_property__get_properties_RO_CS( self_RO );
  } GRAPH_RELEASE;

  return selected_all;
}



/*******************************************************************//**
 * Construct a list of property values by executing the evaluator on
 * the vertex.
 *
 * The return value is a copy of the evaluator's stack after completion
 * and will contain interleaved keys and values in the order specified
 * by the evaluator expression.
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_ExpressEvalStack_t * _vxvertex_property__eval_properties_RO_CS( const vgx_CollectorItem_t *item_RO_CS, vgx_Evaluator_t *selecteval, vgx_VertexIdentifier_t *ptail_id, vgx_VertexIdentifier_t *phead_id ) {
  vgx_Vertex_t *tail_RO_CS = item_RO_CS->tailref->vertex;
  vgx_ArcHead_t head_RO_CS = {
    .vertex = item_RO_CS->headref->vertex,
    .predicator = item_RO_CS->predicator
  };
  double rank = item_RO_CS->sort.flt64.value;
  vgx_ExpressEvalStack_t *eval_properties = NULL;

  // Return all properties
  if( selecteval == NULL ) {
    if( head_RO_CS.vertex ) {
      vgx_Vertex_t *vertex_RO_CS = head_RO_CS.vertex;
      vgx_Graph_t *graph = vertex_RO_CS->graph;
      vgx_SelectProperties_t *properties = _vxvertex_property__get_properties_RO_CS( vertex_RO_CS );
      if( properties ) {
        eval_properties = iEvaluator.NewKeyValStack_CS( graph, properties );
      }
    }
    else {
      return NULL;
    }
  }
  // Evaluate select expression to compute properties
  else {
    // Set evaluator context
    CALLABLE( selecteval )->SetContext( selecteval, tail_RO_CS, &head_RO_CS, NULL, rank );
    // Execute evaluator
    vgx_LockableArc_t larc = {
      .tail = tail_RO_CS,
      .head = head_RO_CS,
      .acquired = {
        .tail_lock = 1,
        .head_lock = 1
      }
    };
    if( CALLABLE( selecteval )->EvalArc( selecteval, &larc )->type != STACK_ITEM_TYPE_NONE ) {
      // Copy the evaluator stack after completed evaluation
      eval_properties = iEvaluator.CloneKeyValStack_CS( selecteval, tail_RO_CS, ptail_id, phead_id );
    }
  }

  return eval_properties;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
// CELL PROCESSOR
static int64_t __sync_property( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  vgx_Vertex_t *vertex_WL = processor->processor.input; // abuse

  vgx_VertexProperty_t prop = {0};
  CString_t *CSTR__value = NULL;

  prop.keyhash = cell->annotation;

  switch( APTR_AS_DTYPE( cell ) ) {
  case TAGGED_DTYPE_ID56:
    prop.val.type = VGX_VALUE_TYPE_BOOLEAN;
    prop.val.data.simple.integer = 0;
    break;
  case TAGGED_DTYPE_UINT56:
    if( (CSTR__value = _vxvertex_property__read_virtual_property( vertex_WL->graph, APTR_AS_UNSIGNED( cell ), NULL, NULL )) != NULL ) {
      prop.val.data.simple.CSTR__string = CSTR__value;
      prop.val.type = VGX_VALUE_TYPE_CSTRING;
      break;
    }
    /* FALLTHRU */
  case TAGGED_DTYPE_INT56:
    prop.val.type = VGX_VALUE_TYPE_INTEGER;
    prop.val.data.simple.integer = APTR_AS_INTEGER( cell );
    break;
  case TAGGED_DTYPE_REAL56:
    prop.val.type = VGX_VALUE_TYPE_REAL;
    prop.val.data.simple.real = APTR_GET_REAL( cell );
    break;
  case TAGGED_DTYPE_OBJ56:
    prop.val.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING; // CString_t
    prop.val.data.simple.CSTR__string = APTR_GET_OBJ56( cell );
    break;
  default:
    return 0;
  }

  iOperation.Vertex_WL.SetProperty( vertex_WL, &prop );
  iString.Discard( &CSTR__value );
  return 1;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__sync_properties_WL_CS_NT( vgx_Vertex_t *self_WL_CS ) {
  if( self_WL_CS->properties ) {
    return iFramehash.simple.Process( self_WL_CS->properties, __sync_property, self_WL_CS, NULL );
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
__inline static int __match_int64( const int64_t value, const vgx_value_condition_t *condition ) {
  switch( condition->vcomp ) {
  case VGX_VALUE_LTE:       return !(value > condition->value1.data.simple.integer);
  case VGX_VALUE_GT:        return   value >  condition->value1.data.simple.integer;
  case VGX_VALUE_GTE:       return !(value < condition->value1.data.simple.integer);
  case VGX_VALUE_LT:        return   value <  condition->value1.data.simple.integer;
  case VGX_VALUE_EQU:       return   value == condition->value1.data.simple.integer;
  case VGX_VALUE_NEQ:       return !(value == condition->value1.data.simple.integer);
  case VGX_VALUE_RANGE:     return !(value < condition->value1.data.simple.integer || value > condition->value2.data.simple.integer);
  case VGX_VALUE_NRANGE:    return   value < condition->value1.data.simple.integer || value > condition->value2.data.simple.integer;
  default:                  return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __match_double( const double value, const vgx_value_condition_t *condition ) {
  // We need to be careful because double isn't really double... we are dealing with
  // 56-bit doubles due to internal restrictions, so epsilon must be used for all comparisons.
  static const double epsi56 = 1.0 / (1LL<<42); // with some margin

#define __DELTA_V1  ( value - condition->value1.data.simple.real )
#define __DELTA_V2  ( value - condition->value2.data.simple.real )
#define __GT_V1     ( __DELTA_V1 > epsi56 )
#define __LT_V1     ( __DELTA_V1 < -epsi56 )
#define __EQ_V1     ( fabs( __DELTA_V1 ) < epsi56 )
#define __GT_V2     ( __DELTA_V2 > epsi56 )

  switch( condition->vcomp ) {
  case VGX_VALUE_LTE:     return __LT_V1 || __EQ_V1;
  case VGX_VALUE_GT:      return __GT_V1;
  case VGX_VALUE_GTE:     return __GT_V1 || __EQ_V1;
  case VGX_VALUE_LT:      return __LT_V1;
  case VGX_VALUE_EQU:     return __EQ_V1;
  case VGX_VALUE_NEQ:     return !__EQ_V1;
  case VGX_VALUE_RANGE:   return !(__LT_V1 || __GT_V2);
  case VGX_VALUE_NRANGE:  return __LT_V1 || __GT_V2;
  default:                return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __match_string( const char *value, const vgx_value_condition_t *condition ) {
  switch( condition->vcomp ) {
  case VGX_VALUE_ANY:
    return 1;
  case VGX_VALUE_LTE:
    return CharsStartsWithConst( value, condition->value1.data.simple.string );     /* stored matches prefix* */
  case VGX_VALUE_GT:
    return !CharsStartsWithConst( value, condition->value1.data.simple.string );    /* stored does NOT match prefix* */
  case VGX_VALUE_EQU:
    return condition->value1.data.simple.string == NULL                             /* any stored */
           ||                                                                       /* OR */
           CharsEqualsConst( value, condition->value1.data.simple.string );  /* stored == probe */
  case VGX_VALUE_NEQ:
    return condition->value1.data.simple.string != NULL                             /* stored exists */
           &&                                                                       /* AND */
           !CharsEqualsConst( value, condition->value1.data.simple.string ); /* stored != probe */
  default:
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __match_enumerated_string( const CString_t *CSTR__value, const vgx_value_condition_t *condition ) {
  switch( condition->vcomp ) {
  case VGX_VALUE_ANY:
    return 1;
  case VGX_VALUE_LTE: 
    return CStringStartsWith( CSTR__value, CStringValue( condition->value1.data.simple.CSTR__string ) );    /* stored has probe as a prefix */
  case VGX_VALUE_GT:
    return !CStringStartsWith( CSTR__value, CStringValue( condition->value1.data.simple.CSTR__string ) );   /* stored doe NOT have probe as a prefix */
  case VGX_VALUE_EQU:
    return CStringEquals( CSTR__value, condition->value1.data.simple.CSTR__string );                        /* stored == probe */
  case VGX_VALUE_NEQ:
    return !CStringEquals( CSTR__value, condition->value1.data.simple.CSTR__string );                       /* stored != probe */
  default:
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int __value_match( const vgx_value_t *value, const vgx_value_condition_t *condition ) {
  vgx_value_type_t ctp = condition->value1.type;
  switch( value->type ) {
  case VGX_VALUE_TYPE_BOOLEAN:
    if( ctp == VGX_VALUE_TYPE_BOOLEAN ) {
      return __match_int64( value->data.simple.integer, condition );
    }
    return 0;
  // Stored Integer
  case VGX_VALUE_TYPE_INTEGER:
    switch( ctp ) {
    case VGX_VALUE_TYPE_INTEGER:
      return __match_int64( value->data.simple.integer, condition );
    case VGX_VALUE_TYPE_REAL:
      return __match_double( (double)value->data.simple.integer, condition );
    default:
      return 0;
    }
  // Stored Real
  case VGX_VALUE_TYPE_REAL:
    switch( ctp ) {
    case VGX_VALUE_TYPE_REAL:
      return __match_double( value->data.simple.real, condition );
    case VGX_VALUE_TYPE_INTEGER:
      {
        vgx_value_condition_t dcond;
        dcond.value1.type = VGX_VALUE_TYPE_REAL;
        dcond.value1.data.simple.real = (double)condition->value1.data.simple.integer;
        dcond.value2.type = VGX_VALUE_TYPE_REAL;
        dcond.value2.data.simple.real = (double)condition->value2.data.simple.integer;
        dcond.vcomp = condition->vcomp;
        return __match_double( value->data.simple.real, &dcond );
      }
    default:
      return 0;
    }
  // Stored CString_t*
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
    /* FALLTHRU */
  case VGX_VALUE_TYPE_CSTRING:
    switch( ctp ) {
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_CSTRING:
      return __match_enumerated_string( value->data.simple.CSTR__string, condition );
    case VGX_VALUE_TYPE_STRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_BORROWED_STRING:
      return __match_string( CStringValue( value->data.simple.CSTR__string ), condition );
    default:
      return 0;
    }
  // Stored const char *
  case VGX_VALUE_TYPE_STRING:
    /* FALLTHRU */
  case VGX_VALUE_TYPE_BORROWED_STRING:
    switch( ctp ) {
    case VGX_VALUE_TYPE_STRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_BORROWED_STRING:
      return __match_string( value->data.simple.string, condition );
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_CSTRING:
      {
        vgx_value_condition_t scond;
        scond.value1.type = VGX_VALUE_TYPE_STRING;
        scond.value1.data.simple.string = CStringValue( condition->value1.data.simple.CSTR__string );
        scond.value2.type = VGX_VALUE_TYPE_NULL;
        scond.value2.data.simple.string = NULL;
        scond.vcomp = condition->vcomp;
        return __match_string( value->data.simple.string, &scond );
      }
    default:
      return 0;
    }
  default:
    return 0;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static bool __has_property_by_keyhash_RO( const vgx_Vertex_t *self_RO, framehash_dynamic_t *dynamic, const vgx_VertexProperty_t *prop, shortid_t keyhash ) {

  // Key hash already computer
  framehash_key_t fkey = &keyhash;

  // Check if named property exists
  if( prop->condition.value1.type == VGX_VALUE_TYPE_NULL ) {
    // Check if property exists (ignore -1 error)
    // and return true/false
    return iFramehash.simple.Has( self_RO->properties, dynamic, CELL_KEY_TYPE_HASH64, fkey ) == 1 ? true : false;
  }
  // Check if named property exists and meets the value condition
  else {
    framehash_value_t fvalue;
    framehash_valuetype_t vtype = iFramehash.simple.Get( self_RO->properties, dynamic, CELL_KEY_TYPE_HASH64, fkey, &fvalue );
    // Property does not exist
    if( vtype == CELL_VALUE_TYPE_NULL ) {
      return false;
    }
    // Property exists, now check value condition
    else {
      vgx_value_t stored_value;
      switch( vtype ) {
      // bool
      case CELL_VALUE_TYPE_MEMBER: // TODO: there's something fishy about boolean properties. Seems to be that existence of KEY really means KEY=True, i.e. always True when KEY exists
        stored_value.type = VGX_VALUE_TYPE_BOOLEAN;
        stored_value.data.simple.integer = 1;
        break;
      // int64_t
      case CELL_VALUE_TYPE_INTEGER:
        stored_value.type = VGX_VALUE_TYPE_INTEGER;
        stored_value.data.simple.integer = (int64_t)fvalue;
        break;
      // double
      case CELL_VALUE_TYPE_REAL:
        stored_value.type = VGX_VALUE_TYPE_REAL;
        stored_value.data.simple.real = *((double*)&fvalue);
        break;
      // CString_t *
      case CELL_VALUE_TYPE_OBJECT64:
        stored_value.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING;
        stored_value.data.simple.CSTR__string = (CString_t*)fvalue;
        break;
      // Invalid stored value
      default:
        // TODO: report error?
        return false;
      }
      // Run the comparison
      return __value_match( &stored_value, &prop->condition );
    }
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int64_t __num_properties_RO( vgx_Vertex_t *self_RO ) {
  return iFramehash.simple.Length( self_RO->properties ) - Vertex_HasEnum( self_RO );
}



/*******************************************************************//**
 *
 * Return:  1 = Item was deleted
 *          0 = Item does not exist and was not deleted
 *         -1 = Error
 *
 ***********************************************************************
 */
static int __del_property_WL_CS( vgx_Vertex_t *self_WL, framehash_dynamic_t *dynamic, vgx_VertexProperty_t *prop ) {
  int n_deleted = 0;
  vgx_Graph_t *graph_CS = self_WL->graph;

  // Look up the namehash for the property key if not provided
  if( prop->keyhash == 0 ) {
    prop->keyhash = _vxenum_propkey__encode_key_CS( graph_CS, prop->key, NULL, false );
  }
  framehash_keytype_t ktype = CELL_KEY_TYPE_HASH64;
  framehash_key_t fkey = &prop->keyhash;
  framehash_valuetype_t vtype;
  framehash_value_t fvalue;

  comlib_object_t *deleted_object = NULL;

  // We need to retrieve the value before we delete so we can decref if necessary
  if( (vtype = iFramehash.simple.Get( self_WL->properties, dynamic, ktype, fkey, &fvalue )) == CELL_VALUE_TYPE_OBJECT64 ) {
    // The value is an object and may need to be decref'ed
    deleted_object = (comlib_object_t*)fvalue;
  }
  
  // The property exists, now delete
  if( vtype != CELL_VALUE_TYPE_NULL ) {
    // Remove the property from the map
    DYNAMIC_LOCK( dynamic ) {
      n_deleted = iFramehash.simple.Del( &self_WL->properties, dynamic, ktype, fkey );
    } DYNAMIC_RELEASE;
    
    // We deleted the property
    if( n_deleted == 1 ) {
      // Value was previously determined to be OBJECT64 (comlib_object_t*)
      if( deleted_object ) {
        // It is a CString so decref it
        if( COMLIB_OBJECT_ISINSTANCE( deleted_object, CString_t ) ) {
          _vxenum_propval__discard_value_CS( graph_CS, (CString_t*)deleted_object );
        }
      }
      // Virtual property
      else if( vtype == CELL_VALUE_TYPE_UNSIGNED ) {
        int64_t vprop_offset = (int64_t)(*((uint64_t*)&fvalue));
        if( _vxvertex_property__erase_virtual_property_CS( graph_CS, vprop_offset ) < 0 ) {
          n_deleted = -1;
        }
      }

      // Decrement global counter
      DecGraphPropCount( graph_CS );
      
      // Discard the key
      _vxenum_propkey__discard_key_by_hash_CS( graph_CS, prop->keyhash );

      // Capture
      iOperation.Vertex_WL.DelProperty( self_WL, prop->keyhash );

      // NOTE!!!
      // We will only capture a property delete operation if the property exists.
      //
    }
  }

  return n_deleted;
}



/*******************************************************************//**
 *
 * Return: 1  = new inserted
 *         0  = overwritten
 *        -1  = error
 ***********************************************************************
 */
DLL_HIDDEN int _vxvertex_property__set_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  int n_inserted = -1;

  // We'll be using framehash
  framehash_dynamic_t *dynamic = &self_WL->graph->property_fhdyn;
  
  XTRY {
    // No properties exist - create framehash derivative mapping structure
    if( self_WL->properties == NULL ) {
      if( (self_WL->properties = iFramehash.simple.New( dynamic )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4C1 );
      }
    }

    // Insert or update property
    GRAPH_LOCK( self_WL->graph ) {
      n_inserted = __insert_or_update_property_WL_CS( self_WL, dynamic, prop );
    } GRAPH_RELEASE;
  }
  XCATCH( errcode ) {
    if( self_WL->properties ) {
      if( iFramehash.simple.Empty( self_WL->properties ) ) {
        DYNAMIC_LOCK( dynamic ) {
          iFramehash.simple.Destroy( &self_WL->properties, dynamic );
        } DYNAMIC_RELEASE;
      }
    }
    n_inserted = -1;
  }
  XFINALLY {
  }

  return n_inserted;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VertexProperty_t * _vxvertex_property__inc_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  vgx_VertexProperty_t *ret_prop = NULL;

  // We'll be using framehash
  framehash_dynamic_t *dynamic = &self_WL->graph->property_fhdyn;
  
  XTRY {
    // No properties exist - create framehash derivative mapping structure
    if( self_WL->properties == NULL ) {
      if( (self_WL->properties = iFramehash.simple.New( dynamic )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x4D1 );
      }
    }

    // Increment numeric property
    GRAPH_LOCK( self_WL->graph ) {
      ret_prop = __insert_or_increment_numeric_property_WL_CS( self_WL, dynamic, prop );
    } GRAPH_RELEASE;
  }
  XCATCH( errcode ) {
    if( self_WL->properties ) {
      if( iFramehash.simple.Empty( self_WL->properties ) ) {
        DYNAMIC_LOCK( dynamic ) {
          iFramehash.simple.Destroy( &self_WL->properties, dynamic );
        } DYNAMIC_RELEASE;
      }
    }
    ret_prop = NULL;
  }
  XFINALLY {
  }

  return ret_prop;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int32_t _vxvertex_property__set_vertex_enum_WL( vgx_Vertex_t *self_WL, int32_t e32 ) {

  // We'll be using framehash
  framehash_dynamic_t *dynamic = &self_WL->graph->property_fhdyn;

  XTRY {
    // No properties exist - create framehash derivative mapping structure
    if( self_WL->properties == NULL ) {
      if( (self_WL->properties = iFramehash.simple.New( dynamic )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }
    }
    
    // Insert internal integer property
    if( __insert_internal_integer_WL( self_WL, dynamic, _vgx__vertex_enum_key, e32 ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }
  }
  XCATCH( errcode ) {
    if( self_WL->properties ) {
      if( iFramehash.simple.Empty( self_WL->properties ) ) {
        DYNAMIC_LOCK( dynamic ) {
          iFramehash.simple.Destroy( &self_WL->properties, dynamic );
        } DYNAMIC_RELEASE;
      }
    }
    e32 = -1;
  }
  XFINALLY {
  }

  return e32;
}



/*******************************************************************//**
 * Return vertex enum
 * If enum not yet defined, generate enum then return it (requires WL)
 *
 ***********************************************************************
 */
DLL_HIDDEN int32_t _vxvertex_property__vertex_enum_LCK( vgx_Vertex_t *self_LCK ) {

  // Quick lookup
  int32_t e32 = Vertex_GetEnum( self_LCK );
  if( e32 > 0 ) {
    return e32;
  }

  // Enum not defined, now generate
  // Must be writable to proceed
  if( !__vertex_is_locked_writable_by_current_thread( self_LCK ) ) {
    return -1;
  }
  vgx_Vertex_t *self_WL = self_LCK;

  return Vertex_SetEnum( self_WL, _vxoballoc_vertex_enum32( self_WL ) );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VertexProperty_t * _vxvertex_property__get_internal_attribute_RO( const vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop ) {

  vgx_VertexRef_t headref = {0};
  headref.vertex = (vgx_Vertex_t*)self_RO;

  vgx_CollectorItem_t item_RO = {
    .tailref    = NULL,
    .predicator = VGX_PREDICATOR_NONE,
    .headref    = &headref,
    .sort       = {0}
  };

  return __get_internal_attribute_RO( &item_RO, prop );
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VertexProperty_t * _vxvertex_property__get_property_RO( const vgx_Vertex_t *self_RO, vgx_VertexProperty_t *prop ) {
  vgx_VertexProperty_t *ret_prop = prop;
  static const framehash_dynamic_t get_dynamic = {
    .hashf = ihash64
  };

  // Vertex has properties
  if( self_RO->properties ) {
    // Perform lookup for property name
    ret_prop = __get_property_RO( self_RO, &get_dynamic, prop );
  }
  // Vertex has no properties
  else {
    // Indicate that no property with requested name exists in vertex
    ret_prop->val.type = VGX_VALUE_TYPE_NULL;
    ret_prop->val.data.simple.integer = 0;
  }

  return ret_prop;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _vxvertex_property__has_property_RO( const vgx_Vertex_t *self_RO, const vgx_VertexProperty_t *prop ) {
  bool exists = false;

  // Vertex has properties
  if( self_RO->properties ) {
    shortid_t keyhash = prop->keyhash;
    // Compute the keyhash if not provided
    if( keyhash == 0 ) {
      keyhash = iEnumerator_OPEN.Property.Key.GetEnum( self_RO->graph, prop->key );
    }
    // Perform lookup for property name
    exists = __has_property_by_keyhash_RO( self_RO, &self_RO->graph->property_fhdyn, prop, keyhash );
  }

  return exists;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__num_properties_RO( vgx_Vertex_t *self_RO ) {
  int64_t n_prop = 0;
  // Vertex has properties
  if( self_RO->properties ) {
    // Perform lookup for property name
    n_prop = __num_properties_RO( self_RO );
  }

  return n_prop;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxvertex_property__del_property_WL( vgx_Vertex_t *self_WL, vgx_VertexProperty_t *prop ) {
  int n_deleted = 0;
  framehash_dynamic_t *dynamic = &self_WL->graph->property_fhdyn;

  // Vertex has properties
  if( self_WL->properties ) {
    // Try to delete the property
    GRAPH_LOCK( self_WL->graph ) {
      n_deleted = __del_property_WL_CS( self_WL, dynamic, prop );
    } GRAPH_RELEASE;

    // If we deleted something check if all properties are deleted
    if( n_deleted == 1 ) {
      if( iFramehash.simple.Empty( self_WL->properties ) ) {
        DYNAMIC_LOCK( dynamic ) {
          iFramehash.simple.Destroy( &self_WL->properties, dynamic );
        } DYNAMIC_RELEASE;
      }
    }
  }

  return n_deleted;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __OBJECT64_destroy_WL_CS( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  shortid_t namehash = fh_cell->annotation;
  if( IsPropertyKeyHashVertexEnum( namehash ) ) {
    return 0;
  }

  vgx_Graph_t *graph_CS = (vgx_Graph_t*)processor->processor.output;
  // KEY DECREF
  _vxenum_propkey__discard_key_by_hash_CS( graph_CS, namehash );

  tagged_dtype_t tp = APTR_AS_DTYPE(fh_cell);

  if( tp == TAGGED_DTYPE_OBJ56 ) {
    comlib_object_t *obj56 = COMLIB_OBJECT( APTR_GET_OBJ56(fh_cell) );
    if( obj56 ) {
      if( COMLIB_OBJECT_ISINSTANCE( obj56, CString_t ) ) {
        // VALUE DECREF
        _vxenum_propval__discard_value_CS( graph_CS, (CString_t*)obj56 );
      }
    }
  }
  else if( tp == TAGGED_DTYPE_UINT56 ) {
    int64_t offset = APTR_AS_UNSIGNED(fh_cell);
    _vxvertex_property__erase_virtual_property_CS( graph_CS, offset );
  }
  return 1;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__del_properties_WL( vgx_Vertex_t *self_WL ) {

  int64_t n_deleted = 0;
  if( self_WL->properties && __num_properties_RO( self_WL ) > 0 ) {

    // Save vertex enum in case we need to re-insert
    int32_t e32 = Vertex_GetEnum( self_WL ); // may be -1 if we don't have enum

    // Give up ownership of all refcounted values
    GRAPH_LOCK( self_WL->graph ) {
      vgx_Graph_t *graph_CS = self_WL->graph;
      n_deleted = iFramehash.simple.Process( self_WL->properties, __OBJECT64_destroy_WL_CS, NULL, graph_CS );
      // Decrement global counter
      SubGraphPropCount( self_WL->graph, n_deleted );
    } GRAPH_RELEASE;
    
    // Destroy the property map
    framehash_dynamic_t *dynamic = &self_WL->graph->property_fhdyn;
    DYNAMIC_LOCK( dynamic ) {
      iFramehash.simple.Destroy( &self_WL->properties, dynamic );
    } DYNAMIC_RELEASE;

    // Capture (delete all properties)
    iOperation.Vertex_WL.DelProperties( self_WL );

    // NOTE!!!
    // We only capture the delete-all operation if the vertex has properties
    //
    //

    // Add back vertex enum if one existed
    if( e32 > 0 ) {
      Vertex_SetEnum( self_WL, e32 );
    }

  }
  return n_deleted;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
static int64_t __serialize_property_RO_CS( framehash_processing_context_t * const processor, framehash_cell_t * const fh_cell ) {
  int64_t __NQWORDS = 0;
  CQwordQueue_t *__OUTPUT = (CQwordQueue_t*)processor->processor.output;

  __serialized_property sprop = {
    .namehash = fh_cell->annotation,
  };

  switch( APTR_AS_DTYPE(fh_cell) ) {
  case TAGGED_DTYPE_ID56:
    sprop.value.type = VGX_VALUE_TYPE_BOOLEAN;
    sprop.value.data.simple.integer = 0;
    break;
  case TAGGED_DTYPE_UINT56:
    sprop.value.type = VGX_VALUE_TYPE_QWORD;
    sprop.value.data.simple.qword = APTR_AS_UNSIGNED(fh_cell);
    break;
  case TAGGED_DTYPE_INT56:
    sprop.value.type = VGX_VALUE_TYPE_INTEGER;
    sprop.value.data.simple.integer = APTR_AS_INTEGER(fh_cell);
    break;
  case TAGGED_DTYPE_REAL56:
    sprop.value.type = VGX_VALUE_TYPE_REAL;
    sprop.value.data.simple.real = APTR_GET_REAL(fh_cell);
    break;
  case TAGGED_DTYPE_OBJ56:
    sprop.value.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING; // CString_t
    sprop.value.data.bits = _vxoballoc_cstring_as_handle( APTR_GET_OBJ56(fh_cell) ).qword;
    break;
  default:
    sprop.namehash = 0;
    sprop.value.type = VGX_VALUE_TYPE_NULL;
    sprop.value.data.bits = 0;
    break;
  }

  WRITE_OR_RETURN( sprop.qwords, qwsizeof( __serialized_property ) );

  return __NQWORDS;

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
__inline static int64_t __deserialize_property_WL_CS( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, vgx_Graph_t *graph_CS, __serialized_property *psprop ) {

  shortid_t namehash = psprop->namehash;

  if( IsPropertyKeyHashVertexEnum( namehash ) ) {
    return __fh_insert_internal_integer_DCS( entrypoint, dynamic, namehash, psprop->value.data.simple.integer );
  }

  framehash_value_t fvalue;
  framehash_valuetype_t vtype;
  switch( psprop->value.type ) {
  case VGX_VALUE_TYPE_BOOLEAN:
    vtype = CELL_VALUE_TYPE_MEMBER;
    fvalue = 0;
    break;
  case VGX_VALUE_TYPE_INTEGER:
    vtype = CELL_VALUE_TYPE_INTEGER;
    *((int64_t*)&fvalue) = psprop->value.data.simple.integer;
    break;
  case VGX_VALUE_TYPE_REAL:
    vtype = CELL_VALUE_TYPE_REAL;
    *((double*)&fvalue) = psprop->value.data.simple.real;
    break;
  case VGX_VALUE_TYPE_QWORD:
    vtype = CELL_VALUE_TYPE_UNSIGNED;
    *((uint64_t*)&fvalue) = psprop->value.data.simple.qword;
    break;
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
    {
      cxmalloc_handle_t handle;
      handle.qword = psprop->value.data.bits;
      vtype = CELL_VALUE_TYPE_OBJECT64;
      // Own another reference to the CString value
      fvalue = icstringobject.FromHandleNolock( handle, graph_CS->property_allocator_context ); // VALUE INCREF
    }
    break;
  default:
    return -1;
  }

  // Set property
  if( iFramehash.simple.Set( entrypoint, dynamic, CELL_KEY_TYPE_HASH64, &namehash, vtype, fvalue ) == 1 ) {
    // Increment global counter
    IncGraphPropCount( graph_CS );
    // Own another reference to the CString key
    _vxenum_propkey__own_key_by_hash_CS( graph_CS, namehash ); // KEY INCREF
    return 0;
  }
  else {
    return -1;
  }

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__serialize_RO_CS( vgx_Vertex_t *self_RO, CQwordQueue_t *__OUTPUT ) {
  int64_t __NQWORDS = 0;
  int64_t n;

  // Start section
  WRITE_OR_RETURN( g_BEGIN_SECTION_DELIM, qwsizeof( g_BEGIN_SECTION_DELIM ) );

  // Write all properties
  if( self_RO->properties ) {
    if( (n = iFramehash.simple.Process( self_RO->properties, __serialize_property_RO_CS, NULL, __OUTPUT )) < 0 ) {
      return -1;
    }
    __NQWORDS += n;
  }

  // Terminate
  WRITE_OR_RETURN( __END_OF_PROPERTIES.qwords, qwsizeof( __serialized_property ) );

  // End section
  WRITE_OR_RETURN( g_END_SECTION_DELIM, qwsizeof( g_END_SECTION_DELIM ) );

  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__deserialize_WL_CS( vgx_Vertex_t *self_WL, CQwordQueue_t *__INPUT ) {
  int64_t __NQWORDS = 0;

  __serialized_property sprop;
  __serialized_property *psprop = &sprop;

  // Start section
  EXPECT_OR_RETURN( g_BEGIN_SECTION_DELIM, qwsizeof( g_BEGIN_SECTION_DELIM ) );

  // Read first item (may be terminator if no properties)
  READ_OR_RETURN( psprop, qwsizeof( __serialized_property ) );

  // Vertex has properties
  if( sprop.value.type != __END_OF_PROPERTIES.value.type ) {
    int64_t n;
    vgx_Graph_t *graph_CS = self_WL->graph;
    framehash_dynamic_t *dynamic = &graph_CS->property_fhdyn;
    CQwordQueue_vtable_t *iqueue = CALLABLE( __INPUT );

    // Create minimal framehash 
    if( (self_WL->properties = iFramehash.simple.New( dynamic )) == NULL ) {
      return -1;
    }

    // Read all properties and populate framehash
    do {
      // Enter item into framehash
      if( __deserialize_property_WL_CS( &self_WL->properties, dynamic, graph_CS, psprop ) < 0 ) {
        return -1;
      }

      // Read next item from input
      if( (n = iqueue->ReadNolock( __INPUT, (void**)&psprop, qwsizeof( __serialized_property ) )) < 0 ) {
        return -1;
      }
      __NQWORDS += n;
    } while( sprop.value.type != __END_OF_PROPERTIES.value.type );

  }
  // Vertex has no properties
  else {
    // No properties
    self_WL->properties = NULL;
  }

  // End section
  EXPECT_OR_RETURN( g_END_SECTION_DELIM, qwsizeof( g_END_SECTION_DELIM ) );

  return __NQWORDS;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
__inline static void __discard_internal_property_data_CS( vgx_Graph_t *self, vgx_VertexProperty_t *prop ) {
  // Discard value
  switch( prop->val.type ) {
  // Discard using property enumerator
  case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
    _vxenum_propval__discard_value_CS( self, prop->val.data.simple.CSTR__string );
    return;
  // Normal CString, use standard destructor
  case VGX_VALUE_TYPE_CSTRING:
    CStringDelete( prop->val.data.simple.CSTR__string );
    return;
  // Malloc'ed char array
  case VGX_VALUE_TYPE_STRING:
    CharsDelete( prop->val.data.simple.string );
    return;
  // Read-only char array, no action
  case VGX_VALUE_TYPE_BORROWED_STRING:
    return;
  default:
    return;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxvertex_property__free_select_properties_CS( vgx_Graph_t *self, vgx_SelectProperties_t **selected ) {
  if( selected && *selected ) {
    if( (*selected)->properties ) {
      vgx_VertexProperty_t *cursor = (*selected)->properties;
      int64_t n_prop = (*selected)->len;
      for( int64_t px=0; px<n_prop; px++ ) {
        // For SELECT properties we own the key on the outside, do NOT modify the internal property key register!
        _vxenum__discard_string( &cursor->key );
        __discard_internal_property_data_CS( self, cursor );
        ++cursor;
      }
      free( (*selected)->properties );
    }
    free( *selected );
    *selected = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxvertex_property__clear_select_property_CS( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop ) {
  if( selectprop ) {
    // For SELECT properties we own the key on the outside, do NOT modify the internal property key register!
    iString.Discard( &selectprop->key );
    __discard_internal_property_data_CS( self, selectprop );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__free_select_properties( vgx_Graph_t *self, vgx_SelectProperties_t **selected ) {
  GRAPH_LOCK( self ) {
    _vxvertex_property__free_select_properties_CS( self, selected );
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__clear_select_property( vgx_Graph_t *self, vgx_VertexProperty_t *selectprop ) {
  if( selectprop ) {
    GRAPH_LOCK( self ) {
      _vxvertex_property__clear_select_property_CS( self, selectprop );
    } GRAPH_RELEASE;
  }
}



/*******************************************************************//**
 *
 * Basic validation of a value condition.
 *
 ***********************************************************************
 */
static int __value_condition_is_valid( const vgx_value_condition_t *cond ) {
  // ---
  // ---
  const vgx_value_t *values[] = {
    &cond->value1,
    &cond->value2
  };
  // Make sure strings are correctly typed
  for( int vx=0; vx<2; vx++ ) {
    const vgx_value_t *val = values[ vx ];
    switch( val->type ) {
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_CSTRING:
      if( val->data.simple.CSTR__string == NULL || !COMLIB_OBJECT_ISINSTANCE( val->data.simple.CSTR__string, CString_t ) ) {
        return 0;
      }
      break;
    case VGX_VALUE_TYPE_STRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_BORROWED_STRING:
      if( val->data.simple.string == NULL ) {
        return 0;
      }
      break;
    default:
      break;
    }
  }
  // Make sure comparison code is compatible with value(s)
  if( cond->vcomp == VGX_VALUE_ANY ) {
    // no values should be provided
    if( cond->value1.type != VGX_VALUE_TYPE_NULL || cond->value2.type != VGX_VALUE_TYPE_NULL ) {
      return 0;
    }
  }
  else if( _vgx_is_basic_value_comparison( cond->vcomp ) ) {
    // only value 1 should be provided
    if( cond->value1.type == VGX_VALUE_TYPE_NULL || cond->value2.type != VGX_VALUE_TYPE_NULL ) {
      return 0;
    }
  }
  else if( cond->vcomp == VGX_VALUE_RANGE || cond->vcomp == VGX_VALUE_NRANGE ) {
    // both value 1 and 2 should be provided and have the same type
    if( cond->value1.type == VGX_VALUE_TYPE_NULL || cond->value2.type == VGX_VALUE_TYPE_NULL ) {
      return 0;
    }
    if( cond->value1.type != cond->value2.type ) {
      return 0;
    }
  }

  return 1; // valid
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__add_cstring_value( vgx_value_t *dest, CString_t **CSTR__string ) {
  int err = 0;
  dest->type = VGX_VALUE_TYPE_CSTRING;
  if( (dest->data.simple.CSTR__string = *CSTR__string) != NULL ) { // <== STEAL the string
    *CSTR__string = NULL;
  }
  else {
    err = -1;
  }
  return err;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_value_t * _vxvertex_property__clone_value( const vgx_value_t *other ) {
  vgx_value_t *value = calloc( 1, sizeof( vgx_value_t ) );
  if( value ) {
    if( _vxvertex_property__clone_value_into( value, other ) < 0 ) {
      free( value );
      value = NULL;
    }
  }
  return value;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__clone_value_into( vgx_value_t *dest, const vgx_value_t *src ) {
  int err = 0;
  if( src && dest ) {
    switch( (dest->type = src->type) ) {
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_CSTRING:
      if( (dest->data.simple.CSTR__string = CStringClone( src->data.simple.CSTR__string )) == NULL ) {
        err = -1;
      }
      break;
    case VGX_VALUE_TYPE_STRING:
      /* FALLTHRU */
    case VGX_VALUE_TYPE_BORROWED_STRING:
      if( (dest->data.simple.string = CharsNew( src->data.simple.string )) == NULL ) {
        err = -1;
      }
      break;
    default:
      dest->data.bits = src->data.bits;
    }
  }
  else {
    err = -1;
  }
  return err;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__delete_value( vgx_value_t **value ) {
  if( value && *value ) {
    _vxvertex_property__clear_value( *value );
    free( *value );
    *value = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__clear_value( vgx_value_t *value ) {
  if( value ) {
    switch( value->type ) {
    case VGX_VALUE_TYPE_ENUMERATED_CSTRING:
      // NOTE: If probes are using enumerated strings we got them from the property system
      // in the first place. It should be ok to use normal destructor because it will use
      // the property allocator to decref the string. The refcount should not go below 2
      // here since the string is still owned both by the vertex and the property map.
      /* FALLTHRU */
    case VGX_VALUE_TYPE_CSTRING:
      CStringDelete( value->data.simple.CSTR__string );
      break;
    case VGX_VALUE_TYPE_STRING:
      CharsDelete( value->data.simple.string );
      break;
    default:
      break;
    }
    value->data.bits = 0;
    value->type = VGX_VALUE_TYPE_NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_value_condition_t * _vxvertex_property__new_value_condition( void ) {
  vgx_value_condition_t *value_condition = NULL;
  if( (value_condition = malloc( sizeof( vgx_value_condition_t ) )) != NULL ) {
    *value_condition = DEFAULT_VGX_VALUE_CONDITION;
  }
  return value_condition;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__clone_value_condition_into( vgx_value_condition_t *dest, const vgx_value_condition_t *src ) {
  int err = 0;
  if( dest && src ) {
    if( iVertexProperty.CloneValueInto( &dest->value1, &src->value1 ) < 0
        ||
        iVertexProperty.CloneValueInto( &dest->value2, &src->value2 ) < 0
      ) 
    {
      iVertexProperty.ClearValueCondition( dest );
      err = -1;
    }
    else {
      dest->vcomp = src->vcomp;
    }
  }
  return err;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_value_condition_t * _vxvertex_property__clone_value_condition( const vgx_value_condition_t *other ) {
  vgx_value_condition_t *clone = NULL;
  if( other ) {
    if( (clone = iVertexProperty.NewValueCondition()) != NULL ) {
      if( iVertexProperty.CloneValueConditionInto( clone, other ) < 0 ) {
        iVertexProperty.DeleteValueCondition( &clone );
      }
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__clear_value_condition( vgx_value_condition_t *value_condition ) {
  if( value_condition ) {
    iVertexProperty.ClearValue( &value_condition->value1 );
    iVertexProperty.ClearValue( &value_condition->value2 );
    value_condition->vcomp = VGX_VALUE_ANY;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__delete_value_condition( vgx_value_condition_t **value_condition ) {
  if( value_condition && *value_condition ) {
    iVertexProperty.ClearValueCondition( *value_condition );
    free( *value_condition );
    *value_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_from_value_condition( vgx_Graph_t *graph, const char *key, vgx_value_condition_t **value_condition ) {

  vgx_VertexProperty_t *property_condition = NULL;

  XTRY {
    // Allocate the property
    if( (property_condition = (vgx_VertexProperty_t*)calloc( 1, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x4F1 );
    }

    // Allocate and set the key
    if( key ) {
      if( (property_condition->key = iEnumerator_OPEN.Property.Key.NewSelect( graph, key, NULL ) ) == NULL ) {
        THROW_SILENT( CXLIB_ERR_API, 0x4F2 );
      }
    }
    else {
      property_condition->key = NULL;
    }

    // No keyhash
    property_condition->keyhash = 0;

    // No value
    property_condition->val = DEFAULT_VGX_VALUE;

    // STEAL the value condition, ensure it is correctly formed
    if( value_condition && *value_condition ) {
      // We don't want to steal the condition unless we verify it first
      if( !__value_condition_is_valid( *value_condition ) ) {
        THROW_ERROR( CXLIB_ERR_API, 0x4F3 );
      }
      // STEAL by copying and then destroying the original
      property_condition->condition = **value_condition; // Shallow copy, inner allocations remain and are owned by the new property condition
      free( *value_condition );
      *value_condition = NULL;
    }

  }
  XCATCH( errcode ) {
    iVertexProperty.Delete( &property_condition );
  }
  XFINALLY {
  }

  return property_condition;

}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * __new_property( vgx_Graph_t *graph, const char *key ) {
  vgx_VertexProperty_t *property = NULL;

  XTRY {
    // Allocate the property
    if( (property = (vgx_VertexProperty_t*)calloc( 1, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x501 );
    }

    // Allocate and set the key
    if( key ) {
      if( (property->key = NewEphemeralCString( graph, key )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x502 );
      }
    }
  }
  XCATCH( errcode ) {
    iVertexProperty.Delete( &property );
  }
  XFINALLY {
  }

  return property;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_default( vgx_Graph_t *graph, const char *key ) {
  vgx_VertexProperty_t *p = __new_property( graph, key );
  if( p ) {
    p->val = DEFAULT_VGX_VALUE;
    p->condition = DEFAULT_VGX_VALUE_CONDITION;
  }
  return p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_integer( vgx_Graph_t *graph, const char *key, int64_t value ) {
  vgx_VertexProperty_t *p = NULL;
  if( key && (p = __new_property( graph, key )) != NULL ) {
    p->val.type = VGX_VALUE_TYPE_INTEGER;
    p->val.data.simple.integer = value;
  }
  return p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_real( vgx_Graph_t *graph, const char *key, double value ) {
  vgx_VertexProperty_t *p = NULL;
  if( key && (p = __new_property( graph, key )) != NULL ) {
    p->val.type = VGX_VALUE_TYPE_REAL;
    p->val.data.simple.real = value;
  }
  return p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_string( vgx_Graph_t *graph, const char *key, const char *value ) {
  vgx_VertexProperty_t *p = NULL;
  if( key && value && (p = __new_property( graph, key )) != NULL ) {
    p->val.type = VGX_VALUE_TYPE_CSTRING;
    if( (p->val.data.simple.CSTR__string = NewEphemeralCString( graph, value )) == NULL ) {
      iVertexProperty.Delete( &p );
    }
  }
  return p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__new_int_array( vgx_Graph_t *graph, const char *key, int64_t sz, const QWORD data[] ) {
  vgx_VertexProperty_t *p = NULL;
  if( key && sz > 0 && data && (p = __new_property( graph, key )) != NULL ) {
    p->val.type = VGX_VALUE_TYPE_CSTRING;
    // Construct destination string
    CString_constructor_args_t string_args = {
      .string       = NULL,
      .len          = (int32_t)sz * 8,
      .ucsz         = 0,
      .format       = NULL,
      .format_args  = NULL,
      .alloc        = NULL
    };
    // Copy data into string
    if( (p->val.data.simple.CSTR__string = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
      iVertexProperty.Delete( &p );
    }
    else {
      QWORD *qwords = CALLABLE( p->val.data.simple.CSTR__string )->ModifiableQwords( p->val.data.simple.CSTR__string );
      QWORD *dest = qwords;
      const QWORD *src = data;
      for( int64_t i=0; i<sz; i++ ) {
        *dest++ = *src++; 
      }
      CString_attr attr = CSTRING_ATTR_ARRAY_INT;
      CStringAttributes( p->val.data.simple.CSTR__string ) = attr;
    }
  }
  return p;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__clone_into( vgx_VertexProperty_t *dest, const vgx_VertexProperty_t *src ) {
  int err = 0;

  if( src && dest ) {
    XTRY {
      // Clone the key
      if( (dest->key = CStringClone( src->key )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x511 );
      }

      // Copy the keyhash
      dest->keyhash = src->keyhash;

      // Copy/Clone all values
      if( _vxvertex_property__clone_value_into( &dest->val, &src->val ) < 0
          ||
          _vxvertex_property__clone_value_into( &dest->condition.value1, &src->condition.value1 ) < 0
          ||
          _vxvertex_property__clone_value_into( &dest->condition.value2, &src->condition.value2 ) < 0
        )
      {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x512 );
      }

      // Copy vcomp
      dest->condition.vcomp = src->condition.vcomp;
    }
    XCATCH( errcode ) {
      iVertexProperty.Delete( &dest );
      err = -1;
    }
    XFINALLY {
    }
  }
  else {
    err = -1;
  }

  return err;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_VertexProperty_t * _vxvertex_property__clone( const vgx_VertexProperty_t *other ) {
  vgx_VertexProperty_t *clone = NULL;

  if( other ) {
    XTRY {
      // Allocate the property
      if( (clone = (vgx_VertexProperty_t*)calloc( 1, sizeof( vgx_VertexProperty_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x521 );
      }
      
      // Clone into
      if( iVertexProperty.CloneInto( clone, other ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x522 );
      }
    }
    XCATCH( errcode ) {
      iVertexProperty.Delete( &clone );
    }
    XFINALLY {
    }
  }

  return clone;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__clear( vgx_VertexProperty_t *property_condition ) {
  if( property_condition ) {
    // Delete the key if any
    if( property_condition->key ) {
      CStringDelete( property_condition->key );
    }

    // Delete any values 
    _vxvertex_property__clear_value( &property_condition->val );
    _vxvertex_property__clear_value( &property_condition->condition.value1 );
    _vxvertex_property__clear_value( &property_condition->condition.value2 );
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__delete( vgx_VertexProperty_t **property_condition ) {
  if( property_condition && *property_condition ) {
    // Clean up data
    _vxvertex_property__clear( *property_condition );

    // Delete the property 
    free( *property_condition );
    *property_condition = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_PropertyConditionSet_t * _vxvertex_property__new_set( bool positive ) {
  vgx_PropertyConditionSet_t *property_condition_set = NULL;

  // Create the wrapper condition set
  if( (property_condition_set = calloc( 1, sizeof( vgx_PropertyConditionSet_t ) )) != NULL ) {
    // Create the list of property addresses
    if( (property_condition_set->__data = COMLIB_OBJECT_NEW_DEFAULT( CQwordList_t )) != NULL ) {
      CQwordList_vtable_t *iList = CALLABLE( property_condition_set->__data );
      property_condition_set->Length = (f_vgx_PropertyConditionSet_length)iList->Length;
      property_condition_set->Get = (f_vgx_PropertyConditionSet_get)iList->Get;
      property_condition_set->Append = (f_vgx_PropertyConditionSet_append)iList->Append;
      property_condition_set->positive = positive;
    }
    else {
      // Clean up after error
      iVertexProperty.DeleteSet( &property_condition_set );
    }
  }

  return property_condition_set;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__clone_set_into( vgx_PropertyConditionSet_t *dest, const vgx_PropertyConditionSet_t *src ) {
  int err = 0;
  // Make sure both dest and src are valid condition sets (src's data will be cloned into dest)
  if( dest && dest->__data && src && src->__data ) {
    vgx_VertexProperty_t *destprop = NULL;
    XTRY {
      dest->Length = src->Length;
      dest->Get = src->Get;
      dest->Append = src->Append;
      dest->positive = src->positive;
      int64_t len = CALLABLE( src->__data )->Length( src->__data );
      for( int64_t px=0; px<len; px++ ) {
        vgx_VertexProperty_t *srcprop;
        if( src->Get( src->__data, px, &srcprop ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x531 );
        }
        if( (destprop = _vxvertex_property__clone( srcprop )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x532 );
        }
        if( dest->Append( dest->__data, (const vgx_VertexProperty_t**)&destprop ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x533 );
        }
        destprop = NULL; // stolen by list
      }
    }
    XCATCH( errcode ) {
      iVertexProperty.Delete( &destprop );
      err = -1;
    }
    XFINALLY {
    }
  }
  else {
    err = -1;
  }
  return err;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static vgx_PropertyConditionSet_t * _vxvertex_property__clone_set( const vgx_PropertyConditionSet_t *other ) {
  vgx_PropertyConditionSet_t *condition_set = NULL;
  if( (condition_set = _vxvertex_property__new_set( other->positive )) != NULL ) {
    if( _vxvertex_property__clone_set_into( condition_set, other ) < 0 ) {
      _vxvertex_property__delete_set( &condition_set );
    }
  }
  return condition_set;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__clear_set( vgx_PropertyConditionSet_t *property_condition_set ) {
  if( property_condition_set ) {
    // Individual properties are referenced by address in a comlib sequence
    CQwordList_t *list = property_condition_set->__data;
    if( list ) {
      int64_t len = property_condition_set->Length( list );
      // Get each property address from our comlib sequence and then delete the property
      for( int64_t px=0; px<len; px++ ) {
        vgx_VertexProperty_t *prop;
        property_condition_set->Get( list, px, &prop );  // fetch address from sequence
        iVertexProperty.Delete( &prop );   // run deletion on the fetched property
      }
      // Empty the list
      CALLABLE( list )->Clear( list );
    }
    property_condition_set->positive = true;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
static void _vxvertex_property__delete_set( vgx_PropertyConditionSet_t **property_condition_set ) {
  if( property_condition_set && *property_condition_set ) {
    // Delete all data
    _vxvertex_property__clear_set( *property_condition_set );

    // Delete the comlib sequence
    if( (*property_condition_set)->__data ) {
      COMLIB_OBJECT_DESTROY( (*property_condition_set)->__data );
    }

    // Delete the condition set wrapper
    free( *property_condition_set );
    *property_condition_set = NULL;
  }
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef union u_vgx_VirtualPropertiesHeader_t {
  QWORD qwords[512];
  struct {
    // Zero head
    BYTE _head[8 * sizeof(QWORD)];

    // Parent graph identifier
    objectid_t graph_id;

    // Commit point
    int64_t commit;

    // Total string data bytes
    int64_t bytes;

    // Total virtual properties
    int64_t count;

    // rsv
    int64_t _rsv1[3];

    // Zero tail
    BYTE _tail[(512-8-5-3) * sizeof(QWORD)];
  };
} vgx_VirtualPropertiesHeader_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VirtualPropertiesHeader_t * __virtual_properties_init_header_VPCS( vgx_Graph_t *graph, vgx_VirtualPropertiesHeader_t *header ) {
  idcpy( &header->graph_id, &graph->obid );
  header->commit = graph->vprop.commit = sizeof( vgx_VirtualPropertiesHeader_t );
  header->bytes = 0;
  header->count = 0;
  return header;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __virtual_properties_load_header_VPCS( vgx_Graph_t *graph ) {

  // Beginning of file
  CX_SEEK( graph->vprop.fd, 0, SEEK_SET );

  // Read header
  vgx_VirtualPropertiesHeader_t header = {0};
  if( CX_READ( header.qwords, sizeof( vgx_VirtualPropertiesHeader_t ), 1, graph->vprop.fd ) != 1 ) {
    return -1;
  }

  // Verify graph match
  if( !idmatch( &graph->obid, &header.graph_id ) ) {
    REASON( 0x000, "Virtual properties graph mismatch!" );
    return -1;
  }

  // Restore commit point and metas
  graph->vprop.commit = header.commit;
  graph->vprop.bytes = header.bytes;
  graph->vprop.count = header.count;

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __virtual_properties_store_header_VPCS( vgx_Graph_t *graph, int64_t commit_point ) {
  int ret = 0;
  XTRY {
    int fd = graph->vprop.fd;
    // Read existing header from file
    if( CX_SEEK( fd, 0, SEEK_SET ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    vgx_VirtualPropertiesHeader_t header = {0};
    if( CX_READ( header.qwords, sizeof( vgx_VirtualPropertiesHeader_t ), 1, fd ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
    }
    // Update header with new metas
    if( CX_SEEK( fd, 0, SEEK_SET ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x003 );
    }
    idcpy( &header.graph_id, &graph->obid );
    header.commit = commit_point;
    header.bytes = graph->vprop.bytes;
    header.count = graph->vprop.count;
    // Write header back to file
    if( CX_WRITE( header.qwords, sizeof( vgx_VirtualPropertiesHeader_t ), 1, fd ) != 1 ) {
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x004 );
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
 *
 ***********************************************************************
 */
typedef union u_vgx_VirtualProperty_t {
  BYTE data[64];
  struct {
    objectid_t DELIM;
    objectid_t vertex_id;
    int64_t nqw;
    int64_t szkey;
    int64_t szval;
    struct {
      vgx_value_type_t type;
      CString_attr attr;
    } value;
  };
} vgx_VirtualProperty_t;


static const objectid_t VPROP_DELIM = {
  .L = 0x4f52502158475621,
  .H = 0x214d494c45442150
};


#define VPROP_MINPAD 2
#define VPROP_MAXPAD 64

static const QWORD VPROP_PADDING[ VPROP_MAXPAD ] = {0};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __inc_vprop_VPCS( vgx_Graph_t *graph, const vgx_VertexProperty_t *prop ) {
  graph->vprop.bytes += CStringLength( prop->val.data.simple.CSTR__string );
  graph->vprop.count++;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __dec_vprop_VPCS( vgx_Graph_t *graph, vgx_VirtualProperty_t *vprop ) {
  // Adjust total byte count for vprop value data
  graph->vprop.bytes -= vprop->szval;
  graph->vprop.count--;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __read_virtual_property_head_VPCS( vgx_VirtualProperty_t *dest, int64_t offset, int fd ) {
  // Seek to existing property offset
  if( CX_SEEK( fd, offset, SEEK_SET ) < 0 ) {
    return -1;
  }
  // Load existing property header
  if( CX_READ( dest->data, sizeof( vgx_VirtualProperty_t ), 1, fd ) != 1 ) {
    return -1;
  }
  // Verify valid property delimiter
  if( !idmatch( &dest->DELIM, &VPROP_DELIM ) ) {
    return -1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __new_virtual_property_VPCS( int fd ) {
  int64_t offset;

  // TODO: A smarter allocator that can reuse discarded chunks of the files
  //
  //

  // Fallback: Append new property data to end of file (minus the trailing delimiter)
  int64_t szdelim = sizeof(objectid_t);
  if( (offset = CX_SEEK( fd, -szdelim, SEEK_END )) < sizeof( vgx_VirtualPropertiesHeader_t ) ) {
    return -1;
  }
  return offset;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __delete_virtual_property_VPCS( int fd, int64_t offset ) {
  // Seek to offset for property to delete
  if( CX_SEEK( fd, offset, SEEK_SET ) < 0 ) {
    return -1;
  }
  // Read property header at offset location
  vgx_VirtualProperty_t vprop;
  if( CX_READ( vprop.data, sizeof( vgx_VirtualProperty_t ), 1, fd ) != 1 ) {
    return -1;
  }
  // Ensure the offset points to a property
  if( !idmatch( &VPROP_DELIM, &vprop.DELIM ) ) {
    return -1;
  }
  // Erase (set vertex id to all zeros)
  idunset( &vprop.vertex_id );
  // Adjust payload metas
  vprop.szkey = 0;
  vprop.szval = 0;
  vprop.value.type = 0;
  vprop.value.attr = 0;
  // Seek to offset for property to delete
  if( CX_SEEK( fd, offset, SEEK_SET ) < 0 ) {
    return -1;
  }
  // Write property header back
  if( CX_WRITE( vprop.data, sizeof( vgx_VirtualProperty_t ), 1, fd ) != 1 ) {
    return -1;
  }

  // TODO: Maintain an index of deleted segments for reuse by __new_virtual_property_VPCS
  //
  //

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __write_new_virtual_property_VPCS( int fd, const objectid_t *vertex_obid, const vgx_VertexProperty_t *prop ) {
  int64_t offset;
  int64_t szkey = CStringLength( prop->key );
  int64_t szval = CStringLength( prop->val.data.simple.CSTR__string );
  size_t qwkey = qwcount( szkey );
  size_t qwval = qwcount( szval );
  // 2 - 64 qwords of padding, typically ~12% extra in the value size range 128 - 4096 bytes
  size_t qwpad = (qwval < 16) ? VPROP_MINPAD : (qwval < (VPROP_MAXPAD << 3)) ? (qwval >> 3) : VPROP_MAXPAD;
  size_t nqw = qwkey + qwval + qwpad;
  
  vgx_VirtualProperty_t vprop = {
    .DELIM = VPROP_DELIM,
    .vertex_id = *vertex_obid,
    .nqw = nqw,
    .szkey = szkey,
    .szval = szval,
    .value = {
      .type = prop->val.type,
      .attr = CStringAttributes( prop->val.data.simple.CSTR__string )
    }
  };

  // Get a file segment
  if( (offset = __new_virtual_property_VPCS( fd )) < 0 ) {
    return -1;
  }

  // Write virtual property header
  if( CX_WRITE( vprop.data, sizeof( vgx_VirtualProperty_t ), 1, fd ) != 1 ) {
    return -1;
  }

  const QWORD *pq;

  // Write key
  pq = CStringValueAsQwords(prop->key);
  if( CX_WRITE( pq, sizeof( QWORD ), qwkey, fd ) != qwkey ) {
    return -1;
  }

  // Write value
  pq = CStringValueAsQwords(prop->val.data.simple.CSTR__string);
  if( CX_WRITE( pq, sizeof( QWORD ), qwval, fd ) != qwval ) {
    return -1;
  }

  // Write pad
  if( CX_WRITE( VPROP_PADDING, sizeof( QWORD ), qwpad, fd ) != qwpad ) {
    return -1;
  }

  // Write delim
  if( CX_WRITE( VPROP_DELIM.shortstring, 1, sizeof(objectid_t), fd ) != sizeof(objectid_t) ) {
    return -1;
  }
  
  return offset;

}



/*******************************************************************//**
 * Try to write property at given offset
 * Return: 1 if property written
 *         0 if property not written (not enough space)
 *        -1 on error
 *
 ***********************************************************************
 */
static int __try_overwrite_virtual_property_VPCS( vgx_Graph_t *graph, const objectid_t *vertex_obid, int64_t offset, const vgx_VertexProperty_t *prop ) {
  int fd = graph->vprop.fd;

  // Load existing property header at this offset
  vgx_VirtualProperty_t vprop;
  if( __read_virtual_property_head_VPCS( &vprop, offset, fd ) < 0 ) {
    return -1;
  }
  // Check for sufficient space
  int64_t szkey = CStringLength( prop->key );
  int64_t szval = CStringLength( prop->val.data.simple.CSTR__string );
  int64_t qw_required = qwcount( szkey ) + qwcount( szval );
  // Not enough space
  if( qw_required > vprop.nqw ) {
    return 0;
  }
  // Overwrite
  vprop.vertex_id = *vertex_obid;
  vprop.szkey = szkey;
  vprop.szval = szval;
  vprop.value.type = prop->val.type;
  vprop.value.attr = CStringAttributes( prop->val.data.simple.CSTR__string );

  // Seek to offset
  if( CX_SEEK( fd, offset, SEEK_SET ) < 0 ) {
    return -1;
  }

  // Write virtual property header
  if( CX_WRITE( vprop.data, sizeof( vgx_VirtualProperty_t ), 1, fd ) != 1 ) {
    return -1;
  }

  const QWORD *pq;
  size_t qwkey = qwcount( vprop.szkey );
  size_t qwval = qwcount( vprop.szval );

  // Write key
  pq = CStringValueAsQwords(prop->key);
  if( CX_WRITE( pq, sizeof( QWORD ), qwkey, fd ) != qwkey ) {
    return -1;
  }

  // Write value
  pq = CStringValueAsQwords(prop->val.data.simple.CSTR__string);
  if( CX_WRITE( pq, sizeof( QWORD ), qwval, fd ) != qwval ) {
    return -1;
  }

  // Overwrite ok
  return 1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t _vxvertex_property__write_virtual_property_CS( vgx_Graph_t *graph, const objectid_t *vertex_obid, int64_t offset, const vgx_VertexProperty_t *prop ) {
  // Virtual property value must be CString
  if( prop->val.type != VGX_VALUE_TYPE_CSTRING ) {
    return -1;
  }

  GRAPH_SUSPEND_LOCK( graph ) {


    SYNCHRONIZE_ON( graph->vprop.lock ) {
      int fd = graph->vprop.fd;
      XTRY {
        // File must be open
        if( fd <= 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
        }

        // Try overwrite at existing offset if beyond commit point
        if( offset >= graph->vprop.commit ) {
          int ow = __try_overwrite_virtual_property_VPCS( graph, vertex_obid, offset, prop );
          if( ow > 0 ) {
            // Manage counters
            __inc_vprop_VPCS( graph, prop );
            XBREAK;
          }
          else if( ow < 0 ) {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
          }
        }

        // Append new property data to end of file
        if( (offset = __write_new_virtual_property_VPCS( fd, vertex_obid, prop )) < 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x003 );
        }

        // Manage counters
        __inc_vprop_VPCS( graph, prop );

      }
      XCATCH( errcode ) {
        offset = -1;
      }
      XFINALLY {
      }
    } RELEASE;
  } GRAPH_RESUME_LOCK;
  return offset;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN CString_t * _vxvertex_property__read_virtual_property( vgx_Graph_t *graph, int64_t offset, CString_t *CSTR__buffer, int64_t *rsz ) {
  CString_t *CSTR__data = NULL;

  SYNCHRONIZE_ON( graph->vprop.lock ) {
    int fd = graph->vprop.fd;
    XTRY {
      // File must be open
      if( fd <= 0 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
      }

      // Read header
      vgx_VirtualProperty_t vprop;
      if( __read_virtual_property_head_VPCS( &vprop, offset, fd ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
      if( rsz ) {
        *rsz = vprop.szval;
      }

      // Verify string type
      if( vprop.value.type != VGX_VALUE_TYPE_CSTRING ) {
        THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x003 );
      }

      // Skip to value
      size_t qwkey = qwcount( vprop.szkey );
      int64_t val_offset = offset + sizeof(vgx_VirtualProperty_t) + 8*qwkey;
      if( CX_SEEK( fd, val_offset, SEEK_SET ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x004 );
      }

      int32_t sz = (int32_t)vprop.szval;

      if( CSTR__buffer ) {
        // ASSUMPTION: buffer is un-initialized and has a max allocated size set
        CSTR__data = CSTR__buffer;
        int32_t lim = CStringLength( CSTR__buffer ); // max allocated size
        if( sz > lim ) {
          sz = lim; // limited to max size
        }
      }
      else {
        // Construct empty destination string
        CString_constructor_args_t string_args = {
          .string       = NULL,
          .len          = sz,
          .ucsz         = 0,
          .format       = NULL,
          .format_args  = NULL,
          .alloc        = graph->ephemeral_string_allocator_context
        };
        // Allow short strings by not using allocator when size is below cutoff, and guard against oversized
        if( sz < __CSTRING_MAX_SHORT_STRING_BYTES || sz > _VXOBALLOC_CSTRING_MAX_LENGTH ) {
          string_args.alloc = NULL;
        }
        if( (CSTR__data = COMLIB_OBJECT_NEW( CString_t, NULL, &string_args )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
        }
      }

      // Make sure string internal flags have correct values
      __CStringInitComplete( CSTR__data, sz );

      // Read value into empty destination string
      size_t qwval = qwcount( sz );
      char *p = (char*)CStringValue( CSTR__data );
      if( CX_READ( (QWORD*)p, sizeof(QWORD), qwval, fd ) != qwval ) {
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x006 );
      }
      p[CStringLength(CSTR__data)] = '\0';

      // Finalize destionation string
      CStringAttributes( CSTR__data ) = vprop.value.attr;

    }
    XCATCH( errcode ) {
      if( CSTR__data != CSTR__buffer ) {
        iString.Discard( &CSTR__data );
      }
      CSTR__data = NULL;
    }
    XFINALLY {
    }
  } RELEASE;

  return CSTR__data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int _vxvertex_property__erase_virtual_property_CS( vgx_Graph_t *graph, int64_t offset ) {
  int ret = 0;
  GRAPH_SUSPEND_LOCK( graph ) {

    SYNCHRONIZE_ON( graph->vprop.lock ) {
      int fd = graph->vprop.fd;
      XTRY {
        // File must be open
        if( fd <= 0 ) {
          THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
        }

        // Read header
        vgx_VirtualProperty_t vprop;
        if( __read_virtual_property_head_VPCS( &vprop, offset, fd ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
        
        // Delete property if offset is beyond commit point
        if( offset >= graph->vprop.commit && __delete_virtual_property_VPCS( fd, offset ) < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }

        // Manage counters
        __dec_vprop_VPCS( graph, &vprop );

      }
      XCATCH( errcode ) {
        ret = -1;
      }
      XFINALLY {
      }
    } RELEASE;

  } GRAPH_RESUME_LOCK;

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxvertex_property__virtual_properties_close( vgx_Graph_t *graph ) {
  GRAPH_LOCK( graph ) {
    if( graph->vprop.ready ) {
      SYNCHRONIZE_ON( graph->vprop.lock ) {
        // Close file
        if( graph->vprop.fd > 0 ) {
          CX_CLOSE( graph->vprop.fd );
          graph->vprop.fd = 0;
        }
      } RELEASE;
    }
  } GRAPH_RELEASE;
}



/*******************************************************************//**
 * Destroy the virtual property system
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void _vxvertex_property__virtual_properties_destroy( vgx_Graph_t *graph ) {

  GRAPH_LOCK( graph ) {

    if( graph->vprop.ready ) {

      _vxvertex_property__virtual_properties_close( graph );

      // Delete lock
      DEL_CRITICAL_SECTION( &graph->vprop.lock.lock );

      // Clear
      graph->vprop.ready = false;

    }

  } GRAPH_RELEASE;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __write_new_virtual_properties_header_VPCS( vgx_Graph_t *graph ) {
  if( graph->vprop.fd <= 0 ) {
    return -1;
  }

  // Initialize empty header
  vgx_VirtualPropertiesHeader_t header = {0};
  __virtual_properties_init_header_VPCS( graph, &header );

  // Write header at beginning of file
  CX_SEEK( graph->vprop.fd, 0, SEEK_SET );
  if( CX_WRITE( header.qwords, sizeof( vgx_VirtualPropertiesHeader_t ), 1, graph->vprop.fd ) != 1 ) {
    return -1;
  }

  // Initial delimiter
  if( CX_WRITE( VPROP_DELIM.shortstring, 1, sizeof(objectid_t), graph->vprop.fd ) != sizeof(objectid_t) ) {
    return -1;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __create_new_virtual_properties_VPCS( vgx_Graph_t *graph, const char *fpath ) {
  // File already exists
  if( file_exists( fpath ) ) {
    return 0;
  }
  // Create properties file if it doesn't exist
  errno_t err = OPEN_W_SEQ( &graph->vprop.fd, fpath );
  if( err != 0 ) {
    REASON( 0x001, "Cannot create virtual properties file: %s", strerror( err ) );
    return -1;
  }
  // Initialize with header
  int ret = __write_new_virtual_properties_header_VPCS( graph );
  // Close
  CX_CLOSE( graph->vprop.fd );
  graph->vprop.fd = 0;
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __virtual_properties_path( vgx_Graph_t *graph, char *path, const char *tmpbase ) {
  // Graph path
  const char *graphpath = tmpbase ? tmpbase : CALLABLE( graph )->FullPath( graph );
  
  snprintf( path, MAX_PATH, "%s/%s/%s", graphpath, VGX_PATHDEF_INSTANCE_PROPERTY, VGX_PATHDEF_INSTANCE_VIRTUAL );

  // Ensure the virtual property directory exists
  if( !dir_exists( path ) && create_dirs( path ) < 0 ) {
    return NULL;
  }

  return path;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __virtual_properties_filename( vgx_Graph_t *graph, char *buffer ) {
  const char *name = CStringValue( CALLABLE(graph)->Name(graph) );
  snprintf( buffer, MAX_PATH, "" VGX_PATHDEF_VIRTUAL_PROPERTIES_FMT VGX_PATHDEF_EXT_DATA, name, 0 );
  return buffer;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static const char * __virtual_properties_fullpath( vgx_Graph_t *graph, char *buffer, const char *tmpbase ) {

  // Path to directory containing file
  char path[MAX_PATH+1] = {0};
  if( __virtual_properties_path( graph, path, tmpbase ) == NULL ) {
    return NULL;
  }

  // Filename
  char fname[MAX_PATH+1] = {0};
  if( __virtual_properties_filename( graph, fname ) == NULL ) {
    return NULL;
  }

  // Full file path
  snprintf( buffer, MAX_PATH, "%s/%s", path, fname );

  return buffer;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxvertex_property__virtual_properties_open( vgx_Graph_t *graph, const char *tmpbase ) {

  int ret = 0;

  GRAPH_LOCK( graph ) {

    if( graph->vprop.ready ) {

      SYNCHRONIZE_ON( graph->vprop.lock ) {

        if( graph->vprop.fd <= 0 ) {

          XTRY {
            char fullpath[MAX_PATH+1] = {0};

            if( __virtual_properties_fullpath( graph, fullpath, tmpbase ) == NULL ) {
              THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
            }

            // Create properties file if it doesn't exist
            if( !file_exists( fullpath ) && __create_new_virtual_properties_VPCS( graph, fullpath ) < 0 ) {
              THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x003 );
            }
            
            // Open properties file read/write, keep open
            if( OPEN_RW_RAND( &graph->vprop.fd, fullpath ) != 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x004, "Cannot open virtual properties file: %s", strerror( errno ) );
            }

            // Restore metas from header
            __virtual_properties_load_header_VPCS( graph );

            // Position at end
            CX_SEEK( graph->vprop.fd, 0, SEEK_END );

          }
          XCATCH( errcode) {
            ret = -1;
          }
          XFINALLY {
          }
        }
      } RELEASE;
    }
    else {
      ret = -1;
    }
  } GRAPH_RELEASE;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxvertex_property__virtual_properties_move( vgx_Graph_t *graph, const char *tmpbase ) {

  int ret = 0;

  GRAPH_LOCK( graph ) {

    if( graph->vprop.ready ) {

      SYNCHRONIZE_ON( graph->vprop.lock ) {

        XTRY {
          // Full path to source file
          char src[MAX_PATH+1] = {0};
          if( __virtual_properties_fullpath( graph, src, tmpbase ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
          }

          // Full path to destination file
          char dest[MAX_PATH+1] = {0};
          if( __virtual_properties_fullpath( graph, dest, NULL ) == NULL ) {
            THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
          }

          // Source and destination are different
          if( !CharsEqualsConst( src, dest ) ) {
            bool was_open = graph->vprop.fd > 0;

            // Close file if open
            if( was_open ) {
              CX_CLOSE( graph->vprop.fd );
              graph->vprop.fd = 0;
            }

            // MOVE
            if( rename( src, dest ) < 0 ) {
              THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x003, "Failed to move virtual properties %s -> %s: %s", src, dest, strerror( errno ) );
            }

            // Re-open
            if( was_open ) {
              if( OPEN_RW_RAND( &graph->vprop.fd, dest ) != 0 ) {
                THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x004, "Cannot open virtual properties file: %s", strerror( errno ) );
              }
            }
          }

        }
        XCATCH( errcode) {
          ret = -1;
        }
        XFINALLY {
        }

      } RELEASE;
    }
    else {
      ret = -1;
    }
  } GRAPH_RELEASE;

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxvertex_property__virtual_properties_commit( vgx_Graph_t *graph ) {

  int64_t commit_point = -1;

  GRAPH_LOCK( graph ) {
    if( graph->vprop.ready ) {
      SYNCHRONIZE_ON( graph->vprop.lock ) {
        int fd = graph->vprop.fd;
        // File is open
        if( fd > 0 ) {
          XTRY {
            // Determine commit point
            int64_t szdelim = sizeof(objectid_t);
            // Commit point is end of last property
            if( (commit_point = CX_SEEK( fd, -szdelim, SEEK_END )) < 0 ) {
              THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
            }
            // Bad commit point
            if( commit_point < graph->vprop.commit ) {
              THROW_ERROR( CXLIB_ERR_CORRUPTION, 0x002 );
            }
            // Persist updated header to file
            if( __virtual_properties_store_header_VPCS( graph, commit_point ) < 0 ) {
              THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x003 );
            }

            // Success: Update commit point
            graph->vprop.commit = commit_point;
          }
          XCATCH( errcode ) {
            commit_point = -1;
          }
          XFINALLY {
          }
        }
        else {
          commit_point = graph->vprop.commit;
        }
      } RELEASE;
    }
  } GRAPH_RELEASE;

  return commit_point;

}



/*******************************************************************//**
 * Initialize the virtual property system
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxvertex_property__virtual_properties_init( vgx_Graph_t *graph ) {

  int ret = 0;

  GRAPH_LOCK( graph ) {

    // Do nothing if already initialized
    if( !graph->vprop.ready ) {
      XTRY {

        // Virtual properties lock
        INIT_SPINNING_CRITICAL_SECTION( &graph->vprop.lock.lock, 4000 );
        graph->vprop.ready = true;

        // Open the file
        if( (ret = _vxvertex_property__virtual_properties_open( graph, NULL )) < 0 ) {
          _vxvertex_property__virtual_properties_destroy( graph );
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
        }

        // Truncate at commit point (plus the final property delimiter)
        int64_t end = graph->vprop.commit + sizeof( objectid_t );
        if( CX_TRUNCATE( graph->vprop.fd, end ) !=0 ) {
          THROW_ERROR_MESSAGE( CXLIB_ERR_FILESYSTEM, 0x002, "Failed to truncate virtual properties at commit point %lld: %s", graph->vprop.commit, strerror( errno ) );
        }
      }
      XCATCH( errcode ) {
        ret = -1;
      }
      XFINALLY {
      }
    }
  } GRAPH_RELEASE;

  return ret;
}



/*******************************************************************//**
 *
 *
 * Convenience interface
 ***********************************************************************
 */
DLL_EXPORT vgx_IVertexProperty_t iVertexProperty = {
  .SetProperty_WL                 = _vxvertex_property__set_property_WL,
  .IncProperty_WL                 = _vxvertex_property__inc_property_WL,
  .SetVertexEnum_WL               = _vxvertex_property__set_vertex_enum_WL,
  .VertexEnum_LCK                 = _vxvertex_property__vertex_enum_LCK,
  .GetProperty_RO                 = _vxvertex_property__get_property_RO,
  .GetProperties_RO_CS            = _vxvertex_property__get_properties_RO_CS,
  .GetProperties_RO               = _vxvertex_property__get_properties_RO,
  .EvalProperties_RO_CS           = _vxvertex_property__eval_properties_RO_CS,
  .HasProperty_RO                 = _vxvertex_property__has_property_RO,
  .NumProperties_RO               = _vxvertex_property__num_properties_RO,
  .DelProperty_WL                 = _vxvertex_property__del_property_WL,
  .DelProperties_WL               = _vxvertex_property__del_properties_WL,
  .Serialize_RO_CS                = _vxvertex_property__serialize_RO_CS,
  .Deserialize_WL_CS              = _vxvertex_property__deserialize_WL_CS,
  
  .FreeSelectProperties_CS        = _vxvertex_property__free_select_properties_CS,
  .ClearSelectProperty_CS         = _vxvertex_property__clear_select_property_CS,

  .FreeSelectProperties           = _vxvertex_property__free_select_properties,
  .ClearSelectProperty            = _vxvertex_property__clear_select_property,

  .AddCStringValue                = _vxvertex_property__add_cstring_value,
  .CloneValue                     = _vxvertex_property__clone_value,
  .CloneValueInto                 = _vxvertex_property__clone_value_into,
  .DeleteValue                    = _vxvertex_property__delete_value,
  .ClearValue                     = _vxvertex_property__clear_value,

  .NewValueCondition              = _vxvertex_property__new_value_condition,
  .CloneValueConditionInto        = _vxvertex_property__clone_value_condition_into,
  .CloneValueCondition            = _vxvertex_property__clone_value_condition,
  .ClearValueCondition            = _vxvertex_property__clear_value_condition,
  .DeleteValueCondition           = _vxvertex_property__delete_value_condition,

  .NewFromValueCondition          = _vxvertex_property__new_from_value_condition,
  .NewDefault                     = _vxvertex_property__new_default,
  .NewInteger                     = _vxvertex_property__new_integer,
  .NewReal                        = _vxvertex_property__new_real,
  .NewString                      = _vxvertex_property__new_string,
  .NewIntArray                    = _vxvertex_property__new_int_array,


  .CloneInto                      = _vxvertex_property__clone_into,
  .Clone                          = _vxvertex_property__clone,
  .Clear                          = _vxvertex_property__clear,
  .Delete                         = _vxvertex_property__delete,

  .NewSet                         = _vxvertex_property__new_set,
  .CloneSetInto                   = _vxvertex_property__clone_set_into,
  .CloneSet                       = _vxvertex_property__clone_set,
  .ClearSet                       = _vxvertex_property__clear_set,
  .DeleteSet                      = _vxvertex_property__delete_set

};




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxvertex_property.h"

test_descriptor_t _vgx_vxvertex_property_tests[] = {
  { "VGX Graph Vertex Property Tests", __utest_vxvertex_property },

  {NULL}
};
#endif
