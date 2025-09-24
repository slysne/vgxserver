/*######################################################################
 *#
 *# vxarcvector_fhash.c
 *#
 *#
 *######################################################################
 */



#include "_vxarcvector.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




/* Framehash layer */
static int  __framehash_load_context_from_arc_cell( framehash_context_t *context, const vgx_ArcVector_cell_t *V );
static void __framehash_update_arc_from_context(    framehash_context_t *context, vgx_ArcVector_cell_t *V, bool is_modified );
static int  __framehash_set(                        framehash_context_t *context, vgx_ArcVector_cell_t *V );
static int  __framehash_upsert(                     framehash_context_t *context, vgx_ArcVector_cell_t *V );
static int  __framehash_del(                        framehash_context_t *context, vgx_ArcVector_cell_t *V );
static int  __framehash_get(                        framehash_context_t *context, const vgx_ArcVector_cell_t *V );




/*******************************************************************//**
 ********** 
 ********    FRAMEHASH STRUCTURE CONVERSION LAYER
 ********** 
 ***********************************************************************
 */


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__convert_simple_arc_to_array_of_arcs( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V ) {
  int retcode = 0;

  if( __arcvector_has_framepointer( V ) ) {
    return retcode; // primary already framehash
  }

  // The simple arc we're replacing with the new array of arcs
  vgx_Arc_t simple_arc = {
    .tail = NULL,
    .head = {
      .vertex     = __arcvector_get_vertex( V ),
      .predicator = { .data = __arcvector_as_predicator_bits( V ) },
    }
  };
  
  // The array of arcs is framehash based
  framehash_slot_t *frame_slots = NULL;
  framehash_cell_t top;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &top, dynamic );

  XTRY {
    // Create top frame as LEAF
    if( (frame_slots = iFramehash.memory.NewFrame( &context, 0, 1, FRAME_TYPE_LEAF )) == NULL ) {
      __ARCVECTOR_ERROR( NULL, NULL, NULL, NULL );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x291 );
    }

    // Convert the arcvector cell to array of arcs
    framehash_cell_t *frametop = iFramehash.access.TopCell( frame_slots );
    __arcvector_cell_set_array_of_arcs( V, 0, frametop );

    // Re-insert the simple arc (if we had one) into array of arcs
    if( simple_arc.head.vertex ) {
      vgx_ArcVector_cell_t arc_cell;
      __arcvector_cell_set_no_arc( &arc_cell ); // by definition, since we just created an empty framehash
      if( _vxarcvector_dispatch__set_arc( dynamic, V, &arc_cell, &simple_arc ) < 0 ) {
        __ARCVECTOR_ERROR( &simple_arc, simple_arc.head.vertex, V, NULL );
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x292 );
      }
    }
  }
  XCATCH( errcode ) {
    if( frame_slots ) {
      iFramehash.memory.DiscardFrame( &context );
    }
    // Roll back
    __arcvector_cell_set_simple_arc( V, &simple_arc );
    retcode = -1;
  }
  XFINALLY {}

  return retcode;

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__convert_array_of_arcs_to_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V ) {
  int retcode = 0;

  // NOTE: Don't call this function unless V is indeed an array of arcs cell and the array of arcs contains exactly one arc! (which may be a multiple arc!)
  if( iFramehash.access.SubtreeLength( __arcvector_as_frametop( V ), dynamic, 2 ) != 1 ) {
    CRITICAL( 0x2A1, "Illegal conversion attempt for array of arcs: size != 1" );
    return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }

  // Collect the first (and presumably only) arc cell from the array of arcs framehash
  framehash_cell_t singleton;
  if( _vxarcvector_cellproc__collect_first_cell( V, &singleton ) != 1 ) {
    CRITICAL( 0x2A2, "Failed to collect cell from array of arcs" );
    return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }

  // This is not good practice but we need to peek into framehash internal cell encoding
  // to find out whether the singleton cell is a simple arc or a multiple arc (pointer to another framehash)
  // BLUE: Last arc is Simple Arc
  if( APTR_AS_DTYPE( &singleton ) == TAGGED_DTYPE_UINT56 ) {
    // Discard the array of arcs framehash
    if( _vxarcvector_fhash__discard( dynamic, V ) != 1 ) { 
      CRITICAL( 0x2A3, "Unexpected failure to discard array of arcs. Frame leaked." );
    }

    // Interpret the singleton framehash cell we collected as a simple arc
    vgx_Arc_t simple_arc = {
      .tail = NULL,
      .head = {
        .vertex     = (vgx_Vertex_t*)APTR_AS_ANNOTATION( &singleton ),
        .predicator = { .data = APTR_AS_UNSIGNED( &singleton) },
      }
    };
    __arcvector_cell_set_simple_arc( V, &simple_arc );

    retcode = 1;
  }

  return retcode;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__convert_simple_arc_to_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *simple_arc_cell ) {
  int retcode = 0;

  // The simple arc we're replacing with the new multiple arc predicator array
  vgx_Arc_t simple_arc = {
    .tail = NULL,
    .head = {
      .vertex     = __arcvector_get_vertex( simple_arc_cell ),
      .predicator = { .data = __arcvector_as_predicator_bits( simple_arc_cell ) },
    }
  };

  // The multiple arc predicator array is framehash based
  framehash_slot_t *frame_slots = NULL;
  framehash_cell_t top;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &top, dynamic );

  XTRY {
    // Create top frame as LEAF
    if( (frame_slots = iFramehash.memory.NewFrame( &context, 0, 1, FRAME_TYPE_LEAF )) == NULL ) {
      __ARCVECTOR_ERROR( NULL, NULL, NULL, NULL );
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x2B1 );
    }

    // Convert the simple arc cell to multiple arc predicator array
    framehash_cell_t *frametop = iFramehash.access.TopCell( frame_slots );
    vgx_ArcVector_cell_t *MAV = __arcvector_cell_set_multiple_arc( simple_arc_cell, simple_arc.head.vertex, frametop );

    // Re-insert the simple arc predicator into multiple arc predicator array
    if( _vxarcvector_fhash__append_to_multiple_arc( dynamic, MAV, simple_arc.head.predicator ) < 0 ) {
      __ARCVECTOR_ERROR( &simple_arc, NULL, MAV, NULL );
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x2B2 );
    }
  }
  XCATCH( errcode ) {
    if( frame_slots ) {
      iFramehash.memory.DiscardFrame( &context );
    }
    // Roll back
    __arcvector_cell_set_simple_arc( simple_arc_cell, &simple_arc );
    retcode = -1;
  }
  XFINALLY {}

  return retcode;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__convert_multiple_arc_to_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV ) {
  int retcode = 0;

  // NOTE: Don't call this function unless MAV is indeed a multiple arc cell and the multiple arc contains exactly one prediator!
  // TODO: Check and enforce this condition.

  // Collect the first (and presumably only) predicator arc cell from the prediator framehash
  framehash_cell_t singleton;
  if( _vxarcvector_cellproc__collect_first_cell( MAV, &singleton ) != 1 ) {
    // PROBLEM
    return __ARCVECTOR_ERROR( NULL, NULL, MAV, NULL );
  }

  // We collected a SINGLE (and only) framehash cell from the multiple arc, which we mow interpret as a predicator
  vgx_Arc_t simple_arc = {
    .tail = NULL,
    .head = {
      .vertex          = __arcvector_get_vertex( MAV ),    // The vertex pointer
      .predicator.data = APTR_AS_UNSIGNED( &singleton )   // The predicator from (the only) cell left in the multiple arc
    }
  };

  // Re-purpose (overwrite) the cell in array of arcs to be a simple arc instead of the multiple arc
  retcode = _vxarcvector_fhash__set_simple_arc( dynamic, V, &simple_arc );

  // Discard the old multiple arc framehash we just replaced
  _vxarcvector_fhash__discard( dynamic, MAV );

  // Invalidate the MAV
  __arcvector_cell_set_no_arc( MAV );  

  return retcode;
}



/*******************************************************************//**
 ********** 
 ********    FRAMEHASH LAYER
 ********** 
 ***********************************************************************
 */



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN void _vxarcvector_fhash__trap_bad_frame_reference( const vgx_ArcVector_cell_t *V ) {
  framehash_cell_t *frametop = __arcvector_as_frametop( V );
  if( !iFramehash.memory.IsFrameValid( frametop ) ) {
    switch( __arcvector_cell_type( V ) ) {
    case VGX_ARCVECTOR_ARRAY_OF_ARCS:
      {
        int64_t degree = __arcvector_get_degree( V );
        CRITICAL( 0xFC1, "Array of arcs is invalid. (degree=%lld frame_memory=%llp)", degree, frametop );
      }
    case VGX_ARCVECTOR_MULTIPLE_ARC:
      {
        vgx_Vertex_t *vertex = __arcvector_get_vertex( V );
        const char *id = CALLABLE( vertex )->IDString( vertex );
        vgx_AllocatedVertex_t *vertex_memory = ivertexobject.AsAllocatedVertex( vertex );
        CRITICAL( 0xFC2, "Multiple arc is invalid. (vertex_id=%s vertex_memory=%llp frame_memory=%llp)", id, vertex_memory, frametop );
      }
    default:
      {
        CRITICAL( 0xFC3, "Invalid frame reference in arc cell: %llp", frametop );
      }
    }
    FATAL( 0xFFF, "Force termination." );
  }
}



/*******************************************************************//**
 * ARC CELL has pointer to the TOP CELL of framehash frame containing frame
 * metas (in M1) and frame slot array pointer (in M2) COPY frame's top cell
 * to the TEMPORARY top cell pointed to by CONTEXT's frame pointer. The CONTEXT
 * frame is now a proper TOP frameref containing the metas for the entrypoint
 * frame and the pointer to the start of frame's slot array, as required by
 * the framehash _subframe_xxx() internal API.                                              
 ***********************************************************************
 */
__inline static int __framehash_load_context_from_arc_cell( framehash_context_t *context, const vgx_ArcVector_cell_t *V ) {
#ifdef VGX_CONSISTENCY_CHECK
  _vxarcvector_fhash__trap_bad_frame_reference( V );
#endif
  __arcvector_set_ephemeral_top( V, context->frame );
  return !APTR_IS_NULL( context->frame );
}



/*******************************************************************//**
 * The context frameref pointer is a temporary top cell pointing to the
 * slot array of the frame. Framehash will update the meta information and
 * slot array pointer in the referring top cell automatically to ensure the
 * top cell is up-to-date after structural modifications of the framehash radix.
 * However, the ArcVector implementation uses ephemeral top cells (temp copy
 * from frame's own top cell before API call) and needs to manually update
 * the frame pointer in the Arc when a structural change has occurred. 
 * Otherwise, the Arc would point to a frame that no longer exists.
 ***********************************************************************
 */
__inline static void __framehash_update_arc_from_context( framehash_context_t *context, vgx_ArcVector_cell_t *V, bool is_modified ) {
  iFramehash.access.UpdateTopCell( context->frame );
  if( is_modified ) {
    framehash_slot_t *slots = CELL_GET_FRAME_SLOTS( context->frame );
    framehash_cell_t *topcell = iFramehash.access.TopCell( slots );
    TPTR_SET_POINTER_AND_TAG( &V->FxP, topcell, VGX_ARCVECTOR_FxP_FRAME );
  }
#ifdef VGX_CONSISTENCY_CHECK
  _vxarcvector_fhash__trap_bad_frame_reference( V );
#endif
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __framehash_set( framehash_context_t *context, vgx_ArcVector_cell_t *V ) {
  int n_inserted; 

  // Compute the key hash
  context->key.shortid = context->dynamic->hashf( context->key.plain );

  // Finalize preparation of context by loading the entry point (top frame) from the TOP CELL of frame referenced by arc
  if( __framehash_load_context_from_arc_cell( context, V ) ) {
    // Perform insertion
    context->control.loadfactor.high = 100;
    context->control.growth.minimal = 1;
    framehash_retcode_t insertion = iFramehash.access.SetContext( context );

    // Success
    if( insertion.completed ) {
      // Ensure latest framehash state is kept in arc
      __framehash_update_arc_from_context( context, V, insertion.modified );

      // Insertion may have added or overwritten existing
      n_inserted = insertion.delta ? 1 : 0;
    }
    else {
      n_inserted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
    }
  }
  else {
    n_inserted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }

  return n_inserted;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __framehash_upsert( framehash_context_t *context, vgx_ArcVector_cell_t *V ) {
  int n_upserted;

  // Compute the key hash
  context->key.shortid = context->dynamic->hashf( context->key.plain );

  // Finalize preparation of context by loading the entry point (top frame) from the TOP CELL of frame referenced by arc
  if( __framehash_load_context_from_arc_cell( context, V ) ) {
    // Perform retrieval (if accumulator value) and update the value
    vgx_predicator_t set_pred = { .data = context->value.raw56 };
    if( _vgx_predicator_value_is_accumulator( set_pred ) ) {
      framehash_valuetype_t vtype = context->vtype;
      framehash_retcode_t retrieval = iFramehash.access.GetContext( context );
      if( retrieval.completed ) {
        vgx_predicator_t prev_pred = { .data = context->value.raw56 };
        set_pred = _vgx_update_predicator_accumulator( prev_pred, set_pred );
      }
      else if( !retrieval.error ) {
        context->vtype = vtype;
      }
      else {
        return __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
      }
      context->value.raw56 = set_pred.data;
    }
    
    // Perform upsertion
    context->control.loadfactor.high = 100;
    context->control.growth.minimal = 1;
    framehash_retcode_t upsertion = iFramehash.access.SetContext( context );

    // Success
    if( upsertion.completed ) {
      // Ensure latest framehash state is kept in arc
      __framehash_update_arc_from_context( context, V, upsertion.modified );

      // Insertion may have added or overwritten existing
      n_upserted = upsertion.delta ? 1 : 0;
    }
    else {
      n_upserted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
    }
  }
  else {
    n_upserted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }

  return n_upserted;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __framehash_del( framehash_context_t *context, vgx_ArcVector_cell_t *V ) {
  int n_deleted;

  // Compute the key hash
  context->key.shortid = context->dynamic->hashf( context->key.plain );

  // Finalize preparation of context by loading the entry point (top frame) from the TOP CELL of frame referenced by arc
  if( __framehash_load_context_from_arc_cell( context, V ) ) {
    // Perform deletion
    framehash_retcode_t deletion = iFramehash.access.DelContext( context );

    // Success
    if( deletion.error == false ) {
      // Ensure latest framehash state is kept in arc
      __framehash_update_arc_from_context( context, V, deletion.modified );

      // Deletion may or may not have removed
      n_deleted = deletion.delta ? 1 : 0;
    }
    else {
      n_deleted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
    }
  }
  else {
    n_deleted = __ARCVECTOR_ERROR( NULL, NULL, V, NULL );
  }

  return n_deleted;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
static int __framehash_get( framehash_context_t *context, const vgx_ArcVector_cell_t *V ) {
  int n_retrieved = -1;  // default error if we don't succeed below

#ifdef FRAMEHASH_INSTRUMENTATION
  context->instrument = &null_instrument;
#endif
  
  // Compute the key hash
  context->key.shortid = context->dynamic->hashf( context->key.plain );

  // Finalize preparation of context by loading the entry point (top frame) from the TOP CELL of frame referenced by arc
  if( __framehash_load_context_from_arc_cell( context, V ) ) {
    // Perform deletion
    framehash_retcode_t retrieval = iFramehash.access.GetContext( context );

    // Hit
    if( retrieval.completed == true ) {
      n_retrieved = 1;
    }
    // Miss
    else if( retrieval.error == false ) {
      n_retrieved = 0;
    }
    // Error
    else {
      n_retrieved = -1;
    }
  }

  return n_retrieved;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_ArcVector_cell_t * _vxarcvector_fhash__get_arc_cell( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V, const vgx_Vertex_t *KEY_vertex, vgx_ArcVector_cell_t *ret_arc_cell ) {

  framehash_cell_t eph_top; //
  
  // Prepare the framehash context for retrieval
  framehash_context_t search_context = {
    .frame    = &eph_top,
    .key      = { .plain = (QWORD)KEY_vertex },     //  key=VERTEX ADDRESS
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = {0},                            //  init to nothing
    .ktype    = CELL_KEY_TYPE_PLAIN64,          //  the vertex pointer QWORD
    .vtype    = CELL_VALUE_TYPE_NULL,           //  nothing yet
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };

  // Execute lookup
  int n_retrieved = __framehash_get( &search_context, V );

  // We found ...
  if( n_retrieved == 1 ) {
    // BLUE: Simple Arc
    if( search_context.vtype == CELL_VALUE_TYPE_UNSIGNED ) {
      vgx_Arc_t arc = {
        .tail = NULL,
        .head = {
          .vertex     = (vgx_Vertex_t*)KEY_vertex,
          .predicator = { .data = search_context.value.raw56 }
        }
      };
      __arcvector_cell_set_simple_arc( ret_arc_cell, &arc );
    }
    // GREEN: Multiple Arc (reference to another framehash)
    else if( search_context.vtype == CELL_VALUE_TYPE_POINTER ) {
      framehash_cell_t *frametop = (framehash_cell_t*)search_context.value.ptr56;
      __arcvector_cell_set_multiple_arc( ret_arc_cell, KEY_vertex, frametop );
    }
    // ... something unexpected
    else {  
      FATAL( 0x2C1, "Let's be honest, we have data corruption." );
    }
  }
  else if( n_retrieved == 0 ) {
    // ... nothing (WHITE)
    __arcvector_cell_set_no_arc( ret_arc_cell );
  }
  else {
    CRITICAL( 0x2C2, "Framehash lookup error." );
  }

  return ret_arc_cell;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN vgx_predicator_t _vxarcvector_fhash__get_predicator( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *MAV, vgx_predicator_t KEY_predicator ) {
  vgx_predicator_t pred = VGX_PREDICATOR_NONE;
  // We need a key predicator with both relationship and modifier to be able to look up directly
  if( _vgx_predicator_specific( KEY_predicator ) ) {
    framehash_cell_t eph_top; //
    // Prepare the framehash context for retrieval
    framehash_context_t get_context = {
      .frame    = &eph_top,
      .key      = { .plain = KEY_predicator.rkey }, //  key=PREDICATOR KEY BITS (see definition of predicator), ignoring the direction bits!
      .obid     = NULL,
      .dynamic  = dynamic,
      .value    = {0},                            //  init to nothing
      .ktype    = CELL_KEY_TYPE_PLAIN64,          //  the predicator key
      .vtype    = CELL_VALUE_TYPE_NULL,           //  nothing yet
      .control  = FRAMEHASH_CONTROL_FLAGS_INIT
  #ifdef FRAMEHASH_INSTRUMENTATION
     ,.instrument = &null_instrument
  #endif
    };

    // Execute lookup
    if( __framehash_get( &get_context, MAV ) == 1 ) {
      pred.data = get_context.value.raw56;
    }
  }
  // We can't do a direct lookup
  else {
    _vxarcvector_cellproc__first_predicator( MAV, KEY_predicator, &pred );
  }

  return pred;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__set_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Arc_t *arc ) {
  framehash_cell_t eph_top;
  // Prepare the framehash context for insertion
  framehash_context_t simple_arc_context = {
    .frame    = &eph_top,
    .key      = { .plain = (QWORD)arc->head.vertex },   //  key=VERTEX ADDRESS
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { .raw56 = arc->head.predicator.data }, //  value=PREDICATOR BITS
    .ktype    = CELL_KEY_TYPE_PLAIN64,                  //  the vertex pointer QWORD
    .vtype    = CELL_VALUE_TYPE_UNSIGNED,               //  the 56-bit predicator 
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };
  return __framehash_set( &simple_arc_context, V );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__set_predicator_map( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *KEY_vertex, framehash_cell_t *VAL_framehash ) {
  framehash_cell_t eph_top;
  // Prepare the framehash context for retrieval
  framehash_context_t context = {
    .frame    = &eph_top,
    .key      = { .plain = (QWORD)KEY_vertex },     //  key=VERTEX ADDRESS
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { .ptr56 = (void*)VAL_framehash},   //  value=FRAME OF PREDICATORS
    .ktype    = CELL_KEY_TYPE_PLAIN64,              //  vertex pointer QWORD
    .vtype    = CELL_VALUE_TYPE_POINTER,            //  framehash_cell_t pointer 56-bit packed
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };
  return __framehash_set( &context, V );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__append_to_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t predicator ) {
  // It's illegal to set a NONE relationship!
  if( predicator.rel.enc != VGX_PREDICATOR_REL_NONE ) {
    framehash_cell_t eph_top;

    // Prepare the framehash context for insertion
    framehash_context_t predicator_insertion_context = {
      .frame    = &eph_top,
      .key      = { .plain = predicator.rkey },     //  key=PREDICATOR KEY BITS (see definition of predicator), ignoring the direction bits!
      .obid     = NULL,
      .dynamic  = dynamic,
      .value    = { .raw56 = predicator.data },     //  value=PREDICATOR BITS
      .ktype    = CELL_KEY_TYPE_PLAIN64,            //  the predicator
      .vtype    = CELL_VALUE_TYPE_UNSIGNED,         //  the predicator 
      .control  = FRAMEHASH_CONTROL_FLAGS_INIT
  #ifdef FRAMEHASH_INSTRUMENTATION
     ,.instrument = &null_instrument
  #endif
    };
    return __framehash_upsert( &predicator_insertion_context, MAV );
  }
  else {
    return __ARCVECTOR_ERROR( NULL, NULL, MAV, NULL );
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__del_simple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_Vertex_t *KEY_vertex ) {
  framehash_cell_t eph_top;
  // Prepare the framehash context for deletion
  framehash_context_t simple_arc_context = {
    .frame    = &eph_top,
    .key      = { .plain = (QWORD)KEY_vertex },         //  key=VERTEX ADDRESS
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { 0 },
    .ktype    = CELL_KEY_TYPE_PLAIN64,                  //  the vertex pointer QWORD
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };
  return __framehash_del( &simple_arc_context, V );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_fhash__del_predicator_map( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V, vgx_ArcVector_cell_t *MAV ) {
  vgx_Vertex_t *key_vertex = __arcvector_get_vertex( MAV );

  // Prepare the framehash context for deletion
  framehash_cell_t eph_top;
  framehash_context_t multiple_arc_context = {
    .frame    = &eph_top,
    .key      = { .plain = (QWORD)key_vertex },         //  key=VERTEX ADDRESS
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = { 0 },
    .ktype    = CELL_KEY_TYPE_PLAIN64,                  //  the vertex pointer QWORD
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };

  if( __framehash_del( &multiple_arc_context, V ) < 0 ) {
    return __ARCVECTOR_ERROR( NULL, key_vertex, V, NULL );  // deletion failed
  }
  else {
    return _vxarcvector_fhash__discard( dynamic, MAV );
  }

}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__multiple_arc_has_key( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t key_predicator ) {
  // Predicator must have a specific key (REL and MOD)
  if( !_vgx_predicator_specific( key_predicator ) ) {
    return -1;
  }

  framehash_cell_t eph_top;
  // Prepare the framehash context for lookup
  framehash_context_t predicator_lookup_context = {
    .frame    = &eph_top,
    .key      = { .plain = key_predicator.rkey },     //  key=PREDICATOR KEY
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = {0},
    .ktype    = CELL_KEY_TYPE_PLAIN64,                //  the predicator QWORD
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };

  // Look up entry
  int key_exists = __framehash_get( &predicator_lookup_context, MAV );

  // REL and MOD found
  if( key_exists > 0 ) {
    // VAL = *
    if( !_vgx_predicator_has_val( key_predicator ) ) {
      return 1;
    }
    // VAL
    else {
      vgx_predicator_t stored_predicator = { .data=predicator_lookup_context.value.raw56 };
      return predmatchfunc.Generic( key_predicator, stored_predicator );
    }
  }
  // Miss
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__remove_key_from_multiple_arc( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *MAV, vgx_predicator_t key_predicator ) {
  // Predicator must have a specific key (REL and MOD)
  if( !_vgx_predicator_specific( key_predicator ) ) {
    return -1;
  }

  framehash_cell_t eph_top;
  // Prepare the framehash context for deletion
  framehash_context_t predicator_deletion_context = {
    .frame    = &eph_top,
    .key      = { .plain = key_predicator.rkey },     //  key=PREDICATOR KEY
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = {0},
    .ktype    = CELL_KEY_TYPE_PLAIN64,                //  the predicator QWORD
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };

  int ret = -1;
  // We can try to delete the entry directly since the predicator value is a wildcard
  // VAL = *
  if( !_vgx_predicator_has_val( key_predicator ) ) {
    ret = __framehash_del( &predicator_deletion_context, MAV );
  }
  // We have a specific value filter, must first try to retrieve and then delete if exists with specified value
  // VAL
  else {
    // Entry found, now check predicator value and delete if match
    if( (ret = __framehash_get( &predicator_deletion_context, MAV )) > 0 ) {
      // Value match?
      vgx_predicator_t stored_predicator = { .data=predicator_deletion_context.value.raw56 };
      // Yes match, proceed with deletion
      if( predmatchfunc.Generic( key_predicator, stored_predicator ) ) {
        ret = __framehash_del( &predicator_deletion_context, MAV );
      }
      // No, don't delete
      else {
        ret = 0;
      }
    }
  }
  return ret;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int _vxarcvector_fhash__compactify( framehash_dynamic_t *dynamic, vgx_ArcVector_cell_t *V ) {
  int compacted = 0;
  framehash_cell_t eph_top;
  framehash_context_t context = CONTEXT_INIT_TOP_FRAME( &eph_top, dynamic );
  
  if( __framehash_load_context_from_arc_cell( &context, V ) ) {
    // Try to compactify, may or may not actually do anything (depends on loadfactor)
    if( (compacted = iFramehash.access.Compactify( &context )) > 0 ) {
      // New structure, we must update the arc cell
      // Update the actual framehash data with fresh metas and update the arc cell with the correct address of this framehash structure
      __framehash_update_arc_from_context( &context, V, true );
    }
  }

  return compacted;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
DLL_HIDDEN int64_t _vxarcvector_fhash__discard( framehash_dynamic_t *dynamic, const vgx_ArcVector_cell_t *V ) {
  framehash_cell_t eph_top;
  __arcvector_set_ephemeral_top( V, &eph_top );

  // Prepare the framehash context for frame discard
  framehash_context_t discard_context = {
    .frame    = &eph_top,
    .key      = {0},
    .obid     = NULL,
    .dynamic  = dynamic,
    .value    = {0},
    .ktype    = CELL_KEY_TYPE_NONE,
    .vtype    = CELL_VALUE_TYPE_NULL,
    .control  = FRAMEHASH_CONTROL_FLAGS_INIT
#ifdef FRAMEHASH_INSTRUMENTATION
   ,.instrument = &null_instrument
#endif
  };

  // Discard
  return iFramehash.memory.DiscardFrame( &discard_context );
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxarcvector_fhash.h"


test_descriptor_t _vgx_vxarcvector_fhash_tests[] = {
  { "VGX Arcvector Framehash Layer Tests",     __utest_vxarcvector_fhash },
  {NULL}
};
#endif

