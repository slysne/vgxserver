/*######################################################################
 *#
 *# api_simple.c
 *#
 *#
 *######################################################################
 */

#include "_framehash.h"
#include "_cxmalloc.h"
#include "_simple.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


/*******************************************************************//**
 * Create a new framehash structure starting at domain=1. (I.e. no top cache.)
 * 
 * Return: pointer to the topcell of the new frame.
 ***********************************************************************
 */
DLL_HIDDEN framehash_cell_t * _framehash_api_simple__new( framehash_dynamic_t *dynamic ) {

  framehash_cell_t *entrypoint = NULL;

  framehash_cell_t eph_top;
  framehash_slot_t *frame_slots = NULL;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &eph_top, dynamic );
  context.control.bits = 0;
  context.control.growth.minimal = 1;
  context.control.loadfactor.high = 100;
  context.control.loadfactor.low = 1;


  // Create top frame as LEAF: order=0, domain=1
  if( (frame_slots = iFramehash.memory.NewFrame( &context, 0, 1, FRAME_TYPE_LEAF )) != NULL ) {
    // Get pointer to the start address of the new leaf, which we will return
    entrypoint = __frame_top_from_slots( frame_slots );
    // Custom: Use M2 in the frame header to keep track of item count.
    APTR_AS_INT64( entrypoint ) = 0;
  }

  return entrypoint;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_simple__destroy( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic ) {
  framehash_cell_t eph_top;

  // Prepare the framehash context for insertion
  framehash_context_t discard_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .key      = { 0 },
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { 0 },
    .ktype    = CELL_KEY_TYPE_NONE,
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };

  int64_t ret =  iFramehash.memory.DiscardFrame( &discard_context );
  *entrypoint = NULL;
  return ret;
}



/*******************************************************************//**
 *
 * Return:  1 = The item was inserted
 *          0 = A previous value with the same key was overwritten
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__set( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t fvalue ) {

  // Ephemeral top cell: A temporary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );
  
  // Prepare the framehash context for insertion
  framehash_context_t set_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .dynamic  = dynamic,
    .ktype    = ktype,
    .vtype    = vtype,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  set_context.control.growth.minimal = 1;
  set_context.control.loadfactor.high = 100;

  // Initialize the context key and value
  __init_key_and_value( &set_context, fkey, fvalue );

  // Perform insertion
  framehash_retcode_t insertion = _framehash_radix__set( &set_context );

  // Success (insert new or overwrite)
  if( insertion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( insertion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    if( insertion.delta ) {
      APTR_AS_INT64( current_top ) = count + 1;
      return 1; // new item inserted
    }
    else {
      return 0; // previous item overwritten
    }
  }
  // Error
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * Return:  1 = The value was inserted
 *          0 = A previous value with the same key was overwritten
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__set_int( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key, int64_t value ) {

  // Ephemeral top cell: A temporary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );
  
  // Prepare the framehash context for insertion
  framehash_context_t set_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .key      = { 
      .plain    = key,
      .shortid  = dynamic->hashf( key )
    },
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { .raw56 = value },
    .ktype    = CELL_KEY_TYPE_PLAIN64,
    .vtype    = CELL_VALUE_TYPE_INTEGER,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  set_context.control.growth.minimal = 1;
  set_context.control.loadfactor.high = 100;

  // Perform insertion
  framehash_retcode_t insertion = _framehash_radix__set( &set_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( set_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after insert!" );
    }
  }
#endif

  // Success (insert new or overwrite)
  if( insertion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( insertion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    if( insertion.delta ) {
      APTR_AS_INT64( current_top ) = count + 1;
      return 1; // new item inserted
    }
    else {
      return 0; // previous item overwritten
    }
  }
  // Error
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * Return:  Type of value after delta is applied. The sign parameter is used as multiplier for delta to get inc or dec behavior.
 *          If existsing item found, its value is modified and updated values placed into QWORD pointed to by fvalue
 *          - The returned type indicates how to interpret the QWORD.
 *          - If an error occurred then CELL_VALUE_TYPE_ERROR is returned.
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t __mod_delta( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t fdelta, framehash_value_t *pfvalue, int sign, bool *created ) {

  switch( vtype ) {
  case CELL_VALUE_TYPE_INTEGER:
  case CELL_VALUE_TYPE_REAL:
    break;
  default:
    return CELL_VALUE_TYPE_ERROR; // Incompatible value type for increment
  }

  // Ephemeral top cell: A temporary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );

  // Prepare the framehash context for increment
  framehash_context_t upsert_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .dynamic  = dynamic,
    .ktype    = ktype,
    .vtype    = vtype,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  // TODO: Find a way to control caching for the simple API
  upsert_context.control.growth.minimal = 1;
  upsert_context.control.loadfactor.high = 100;

  // Initialize 
  __init_key_and_value( &upsert_context, fkey, fdelta );

  // Prepare the framehash context for lookup
  framehash_context_t get_context;
  memcpy( &get_context, &upsert_context, sizeof( framehash_context_t ) );
  get_context.vtype = CELL_VALUE_TYPE_NULL;

  // Perform lookup
  _framehash_radix__get( &get_context );

  int64_t previous_integer;
  double previous_real;

  //  
  switch( get_context.vtype ) {
  // No previous value
  case CELL_VALUE_TYPE_NULL:
    // Keep upsert context as initialized to inc value
    break;
  // Previous value was int56
  case CELL_VALUE_TYPE_INTEGER:
    previous_integer = get_context.value.int56;
    // Inc value is also int56
    if( vtype == CELL_VALUE_TYPE_INTEGER ) {
      struct {
        int64_t i56 : 56;
        int64_t _H  : 8;
      } val;
      val.i56 = previous_integer * sign;
      val.i56 += upsert_context.value.int56;
      upsert_context.value.int56 = val.i56;
    }
    // Inc value is real56
    else {
      upsert_context.value.real56 += (previous_integer * sign);
    }
    break;
  case CELL_VALUE_TYPE_REAL:
    // Previous value was real56
    previous_real = get_context.value.real56;
    // Inc value is also real56
    if( vtype == CELL_VALUE_TYPE_REAL ) {
      upsert_context.value.real56 += (previous_real * sign);
    }
    // Inc value is int56 - promote to double
    else {
      upsert_context.value.real56 = previous_real + (upsert_context.value.int56 * sign);
      upsert_context.vtype = CELL_VALUE_TYPE_REAL;
    }
    break;
  case CELL_VALUE_TYPE_ERROR:
    // Internal error
    return CELL_VALUE_TYPE_ERROR;
  default:
    // Incompatible previous value type
    return CELL_VALUE_TYPE_ERROR;
  }

  // Perform increment
  framehash_retcode_t upsertion = _framehash_radix__set( &upsert_context );

  // Success (insert new or increment)
  if( upsertion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( upsertion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    if( upsertion.delta ) {
      APTR_AS_INT64( current_top ) = count + 1;
    }
    // Flag creation of new item
    *created = get_context.vtype == CELL_VALUE_TYPE_NULL ? true : false;
    // Put the incremented value in the supplied pointer and return its value type (may have been promoted)
    return __return_value( &upsert_context, pfvalue );
  }
  // Error
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 *
 * Return:  Type of value after increment.
 *          If existsing item found, its value is modified and updated values placed into QWORD pointed to by fvalue
 *          - The returned type indicates how to interpret the QWORD.
 *          - If an error occurred then CELL_VALUE_TYPE_ERROR is returned.
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_simple__inc( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t finc, framehash_value_t *pfvalue, bool *created ) {
  return __mod_delta( entrypoint, dynamic, ktype, fkey, vtype, finc, pfvalue, 1, created );
}



/*******************************************************************//**
 *
 * Return:  1 = New value established, equal to the inc amount
 *          0 = Existing value was incremented by inc amount
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__inc_int( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key, int64_t inc, int64_t *value ) {

  // Ephemeral top cell: A temporary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );

  // Prepare the framehash context for increment
  framehash_context_t upsert_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .key      = { 
      .plain    = key,
      .shortid  = dynamic->hashf( key )
    },
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { .raw56 = inc },
    .ktype    = CELL_KEY_TYPE_PLAIN64,
    .vtype    = CELL_VALUE_TYPE_INTEGER,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  upsert_context.control.growth.minimal = 1;
  upsert_context.control.loadfactor.high = 100;

  // Prepare the framehash context for lookup
  framehash_context_t get_context;
  memcpy( &get_context, &upsert_context, sizeof( framehash_context_t ) );
  get_context.vtype = CELL_VALUE_TYPE_NULL;

  // Perform lookup
  _framehash_radix__get( &get_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( get_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after get!" );
    }
  }
#endif

  int64_t previous_value;

  // Previous value found, add increment to the upsert context
  if( get_context.vtype == CELL_VALUE_TYPE_INTEGER ) {
    previous_value = get_context.value.int56;
    upsert_context.value.int56 += previous_value;
  }
  // No previous value
  else if( get_context.vtype != CELL_VALUE_TYPE_NULL ) {
    return CELL_VALUE_TYPE_ERROR;
  }

  // Perform increment
  framehash_retcode_t upsertion = _framehash_radix__set( &upsert_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( upsert_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after set!" );
    }
  }
#endif

  // Success (insert new or increment)
  if( upsertion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( upsertion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    if( upsertion.delta ) {
      APTR_AS_INT64( current_top ) = count + 1;
    }
    // Set the output value to new value stored
    if( value ) {
      *value = upsert_context.value.int56;
    }
    // New value was created
    if( get_context.vtype == CELL_VALUE_TYPE_NULL ) {
      return 1;
    }
    // Existing value was incremented
    else {
      return 0;
    }
  }
  // Error
  else {
    return CELL_VALUE_TYPE_ERROR;
  }
}



/*******************************************************************//**
 *
 * Return:  Type of value after increment
 *          If existsing item found, its value is incremented and updated values placed into QWORD pointed to by fvalue
 *          - The returned type indicates how to interpret the QWORD.
 *          - If an error occurred then CELL_VALUE_TYPE_ERROR is returned.
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_simple__dec( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_valuetype_t vtype, const framehash_value_t fdec, framehash_value_t *pfvalue, bool *created ) {
  return __mod_delta( entrypoint, dynamic, ktype, fkey, vtype, fdec, pfvalue, -1, created );
}



/*******************************************************************//**
 * Decrement value. If resulting value becomes exactly zero and autodelete
 * is true then remove the item from the map and set the deleted output
 * flag to true.
 *
 * Return:  1 = New value established, equal to the negative dec amount
 *          0 = Existing value was decremented by dec amount
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__dec_int( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key, int64_t dec, int64_t *value, bool autodelete, bool *deleted ) {
  int64_t decvalue = 0;
  int ret = _framehash_api_simple__inc_int( entrypoint, dynamic, key, -dec, &decvalue );
  if( autodelete && decvalue == 0 ) {
    if( _framehash_api_simple__del_int( entrypoint, dynamic, key ) == 1 ) {
      if( deleted ) {
        *deleted = true;
      }
    }
  }
  if( value ) {
    *value = decvalue;
  }
  return ret;
}



/*******************************************************************//**
 *
 * Return:  Type of value found
 *          If item found, its value (concrete or pointer) is placed into QWORD pointed to by fvalue
 *          - The returned type indicates how to interpret the QWORD.
 *          - If no value found then CELL_VALUE_TYPE_NULL is returned.
 *          - If an error occurred then CELL_VALUE_TYPE_ERROR is returned.
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_simple__get( framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey, framehash_value_t *pfvalue ) {
  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  
  // Prepare the framehash context for lookup
  framehash_context_t get_context = {
    .frame    = __set_ephemeral_top( &eph_top, entrypoint ),
    .dynamic  = (framehash_dynamic_t*)dynamic, // const ok, all we use for Get() is the hash function
    .ktype    = ktype,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };

  // Initialize the context key
  __init_key( &get_context, fkey );

  // Perform lookup
  _framehash_radix__get( &get_context );

  // Put the value, if found, into the pvalue pointer and return the value type
  return __return_value( &get_context, pfvalue );

}



/*******************************************************************//**
 *
 * Return :  1 : lookup ok, result placed in output value parameter
 *           0 : key not found
 *          -1 : error
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__get_int( framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, QWORD key, int64_t *value ) {
  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  
  // Prepare the framehash context for lookup
  framehash_context_t get_context = {
    .frame    = __set_ephemeral_top( &eph_top, entrypoint ),
    .key      = { 
      .plain    = key,
      .shortid  = dynamic->hashf( key )
    },
    .obid     = NULL,
    .dynamic  = (framehash_dynamic_t*)dynamic, // const ok, all we use for Get() is the hash function
    .ktype    = CELL_KEY_TYPE_PLAIN64,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };

  // Perform lookup
  _framehash_radix__get( &get_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( get_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after get!" );
    }
  }
#endif

  switch( get_context.vtype ) {
  case CELL_VALUE_TYPE_NULL:
    return 0;
  case CELL_VALUE_TYPE_INTEGER:
    *value = get_context.value.int56;
    return 1;
  default:
    return -1;
  }
}



/*******************************************************************//**
 *
 * Return:  
 *          1: If integer found, its value is placed into *rvalue and 1 is returned
 *          0: If nothing is found, 0 is returned
 *         -1: If object other than integer is found, -1 is returned
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__get_int_hash64( framehash_cell_t *entrypoint, shortid_t key, int64_t *rvalue ) {

  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;

  // Prepare the framehash context for lookup
  framehash_context_t get_context = {0};
  get_context.frame = __set_ephemeral_top( &eph_top, entrypoint );
  get_context.key.shortid = key;
  get_context.ktype = CELL_KEY_TYPE_HASH64;
  _FRAMEHASH_CONTEXT_SET_NULL_INSTRUMENT( get_context );

  // Perform lookup
  _framehash_radix__get( &get_context );

  if( get_context.vtype == CELL_VALUE_TYPE_INTEGER ) {
    *rvalue = (int64_t)get_context.value.raw56;
    return 1;
  }
  else if( get_context.vtype == CELL_VALUE_TYPE_NULL ) {
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 * Return:  Type of value found
 *          If item found, its value (concrete or pointer) is placed into QWORD pointed to by fvalue
 *          - The returned type indicates how to interpret the QWORD.
 *          - If no value found then CELL_VALUE_TYPE_NULL is returned.
 *          - If an error occurred then CELL_VALUE_TYPE_ERROR is returned.
 *
 ***********************************************************************
 */
DLL_HIDDEN framehash_valuetype_t _framehash_api_simple__get_hash64( framehash_cell_t *entrypoint, shortid_t key, framehash_value_t *pfvalue ) {

  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;

  // Prepare the framehash context for lookup
  framehash_context_t get_context = {0};
  get_context.frame = __set_ephemeral_top( &eph_top, entrypoint );
  get_context.key.shortid = key;
  get_context.ktype = CELL_KEY_TYPE_HASH64;
  _FRAMEHASH_CONTEXT_SET_NULL_INSTRUMENT( get_context );

  // Perform lookup
  _framehash_radix__get( &get_context );

  // Put the value, if found, into the pvalue pointer and return the value type
  return __return_value( &get_context, pfvalue );

}




/*******************************************************************//**
 *
 * Return:  1 = Item exists
 *          0 = Item does not exist
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__has( framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey ) {
  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  
  // Prepare the framehash context for lookup
  framehash_context_t has_context = {
    .frame    = __set_ephemeral_top( &eph_top, entrypoint ),
    .dynamic  = (framehash_dynamic_t*)dynamic, // const ok, all we use for Has() is the hash function
    .ktype    = ktype,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };

  // Initialize the context key
  __init_key( &has_context, fkey );

  // Perform lookup
  framehash_retcode_t lookup = _framehash_radix__get( &has_context );

  // Item exists
  if( lookup.completed ) {
    return 1;
  }
  // Item does not exist
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 * Return:  1 = Item exists
 *          0 = Item does not exist
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__has_int( framehash_cell_t *entrypoint, const framehash_dynamic_t *dynamic, QWORD key ) {
  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  
  // Prepare the framehash context for lookup
  framehash_context_t has_context = {
    .frame    = __set_ephemeral_top( &eph_top, entrypoint ),
    .key      = { 
      .plain    = key,
      .shortid  = dynamic->hashf( key )
    },
    .obid     = NULL,
    .dynamic  = (framehash_dynamic_t*)dynamic, // const ok, all we use for Has() is the hash function
    .ktype    = CELL_KEY_TYPE_PLAIN64,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };

  // Perform lookup
  framehash_retcode_t lookup = _framehash_radix__get( &has_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( has_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after has!" );
    }
  }
#endif

  // Item exists
  if( lookup.completed ) {
    return 1;
  }
  // Item does not exist
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 * Return:  1 = Item exists
 *          0 = Item does not exist
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__has_hash64( framehash_cell_t *entrypoint, shortid_t key ) {
  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;

  // Prepare the framehash context for lookup
  framehash_context_t has_context = {0};
  has_context.frame = __set_ephemeral_top( &eph_top, entrypoint );
  has_context.key.shortid = key;
  has_context.ktype = CELL_KEY_TYPE_HASH64;
  _FRAMEHASH_CONTEXT_SET_NULL_INSTRUMENT( has_context );

  // Perform lookup
  framehash_retcode_t lookup = _framehash_radix__get( &has_context );

  // Item exists
  if( lookup.completed ) {
    return 1;
  }
  // Item does not exist
  else {
    return 0;
  }
}



/*******************************************************************//**
 *
 * Return:  1 = Item was deleted
 *          0 = Item does not exist and was not deleted
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__del( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, framehash_keytype_t ktype, framehash_key_t fkey ) {

  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );
  
  // Prepare the framehash context for deletion
  framehash_context_t del_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .dynamic  = dynamic,
    .ktype    = ktype,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  del_context.control.growth.minimal = 1;
  del_context.control.loadfactor.high = 100;
  del_context.control.loadfactor.low = 25;

  // Initialize the context key
  __init_key( &del_context, fkey );

  // Perform deletion
  framehash_retcode_t deletion = _framehash_radix__del( &del_context );

  // Item was deleted
  if( deletion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( deletion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    APTR_AS_INT64( current_top ) = count - 1;
    return 1; // item deleted
  }
  // Item did not exist - nothing deleted
  else if( !deletion.error ) {
    // Zero items deleted
    return 0;
  }
  // Deletion error
  else {
    // Error, and nothing deleted
    return -1;
  }

}



/*******************************************************************//**
 *
 * Return:  1 = Item was deleted
 *          0 = Item does not exist and was not deleted
 *         -1 = Error
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__del_int( framehash_cell_t **entrypoint, framehash_dynamic_t *dynamic, QWORD key ) {

  // Ephemeral top cell: A temorary stable reference to the top frame
  framehash_cell_t eph_top;
  // Current item COUNT
  int64_t count = APTR_AS_INT64( *entrypoint );
  
  // Prepare the framehash context for deletion
  framehash_context_t del_context = {
    .frame    = __set_ephemeral_top( &eph_top, *entrypoint ),
    .key      = { 
      .plain    = key,
      .shortid  = dynamic->hashf( key )
    },
    .obid     = NULL,
    .dynamic  = dynamic,
    .ktype    = CELL_KEY_TYPE_PLAIN64,
    .control  = {0}
    _FRAMEHASH_CONTEXT_NULL_INSTRUMENT
  };
  del_context.control.growth.minimal = 1;
  del_context.control.loadfactor.high = 100;
  del_context.control.loadfactor.low = 25;

  // Perform deletion
  framehash_retcode_t deletion = _framehash_radix__del( &del_context );

#ifndef NDEBUG
  {
    framehash_metas_t *__metas__ = CELL_GET_FRAME_METAS( del_context.frame );
    if( !( __metas__->domain == FRAME_TYPE_LEAF || __metas__->domain == FRAME_TYPE_INTERNAL ) ) {
      CRITICAL( 0xEEE, "Frame cell corruption after delete!" );
    }
  }
#endif

  // Item was deleted
  if( deletion.completed ) {
    // The top frame in the radix must have its own metas updated to reflect correct state of frame since there is nothing above that frame to record the information
    framehash_cell_t *current_top = __frame_top_from_slots( CELL_GET_FRAME_SLOTS( &eph_top ) );
    // Update the top frame's metas    
    current_top->annotation = eph_top.annotation;
    // The underlying structure was modified and the address of the structure's top frame has changed due to re-size
    if( deletion.modified && *entrypoint != current_top ) {
      // Update the entrypoint
      *entrypoint = current_top;
    }
    // Update the top frame's item count
    APTR_AS_INT64( current_top ) = count - 1;
    return 1; // item deleted
  }
  // Item did not exist - nothing deleted
  else if( !deletion.error ) {
    // Zero items deleted
    return 0;
  }
  // Deletion error
  else {
    // Error, and nothing deleted
    return -1;
  }

}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
// CELL PROCESSOR
DLL_HIDDEN int64_t __collect_cell_as_key_and_value( framehash_processing_context_t * const processor, framehash_cell_t * const cell ) {
  Cm128iList_t *items = processor->processor.output;
  Key64Value56_t item = {
    .key = cell->annotation,
    .value56 = APTR_AS_INTEGER( cell )
  };
  return CALLABLE( items )->Append( items, &item.m128 );
}



/*******************************************************************//**
 *
 * Return:  number of items
 *
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_simple__length( framehash_cell_t *entrypoint ) {
  return SIMPLE_FRAMEHASH_LENGTH( entrypoint );
}



/*******************************************************************//**
 *
 * Return:  true if empty, false otherwise
 *
 ***********************************************************************
 */
DLL_HIDDEN bool _framehash_api_simple__empty( framehash_cell_t *entrypoint ) {
  return SIMPLE_FRAMEHASH_IS_EMPTY( entrypoint );
}



/*******************************************************************//**
 *
 * Return:  0
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__set_readonly( framehash_cell_t *entrypoint ) {
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( entrypoint );
  metas->flags.readonly = 1;
  return 0;
}



/*******************************************************************//**
 *
 * Return:  0 or 1
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__is_readonly( framehash_cell_t *entrypoint ) {
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( entrypoint );
  return metas->flags.readonly;
}



/*******************************************************************//**
 *
 * Return:  0
 *
 ***********************************************************************
 */
DLL_HIDDEN int _framehash_api_simple__clear_readonly( framehash_cell_t *entrypoint ) {
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( entrypoint );
  metas->flags.readonly = 0;
  return 0;
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _framehash_api_simple__process( framehash_cell_t *entrypoint, f_framehash_cell_processor_t cellfunc, void *input, void *output ) {
  int64_t n_proc = 0;
  framehash_metas_t *metas = CELL_GET_FRAME_METAS( entrypoint );
  framehash_cell_t eph_top;
  framehash_processing_context_t process = FRAMEHASH_PROCESSOR_NEW_CONTEXT( __set_ephemeral_top( &eph_top, entrypoint ), NULL, cellfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &process, input, output );
  if( !metas->flags.readonly ) {
    FRAMEHASH_PROCESSOR_MAY_MODIFY( &process );
    int64_t count = APTR_AS_INT64( entrypoint );
    n_proc = iFramehash.processing.ProcessNolock( &process );
    APTR_AS_INT64( entrypoint ) = count + process.__internal.__delta_items;
  }
  else {
    n_proc = iFramehash.processing.ProcessNolock( &process );
  }
  return n_proc;
}



/*******************************************************************//**
 *
 * Return items as entries in a list of 128-bit items, 64-bit key and 64-bit value
 *
 * LEAK WARNING: Caller owns the returned list of items if created here!
 ***********************************************************************
 */
DLL_HIDDEN Key64Value56List_t * _framehash_api_simple__int_items( framehash_cell_t *entrypoint, framehash_dynamic_t *dynamic, Key64Value56List_t *output ) {

  // If no output list provided, create new
  Key64Value56List_t *items = NULL;
  bool local = false;
  if( output == NULL ) {
    if( (items = COMLIB_OBJECT_NEW_DEFAULT( Cm128iList_t )) == NULL ) {
      return NULL;
    }
    local = true;
  }
  else {
    items = output;
  }

  int64_t n_proc;
  CS_LOCK *plock = dynamic ? dynamic->pflock : NULL;
  SYNCHRONIZE_ON_PTR( plock ) {
    n_proc = iFramehash.simple.Process( entrypoint, __collect_cell_as_key_and_value, NULL, items );
  } RELEASE;

  if( n_proc < 0 ) {
    if( local ) {
      COMLIB_OBJECT_DESTROY( items );
    }
    items = NULL;
  }

  return items;
}






#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_api_simple.h"

DLL_HIDDEN test_descriptor_t _framehash_api_simple_tests[] = {
  { "api_simple",   __utest_framehash_api_simple },
  {NULL}
};
#endif



