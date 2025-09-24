/*
###################################################
#
# File:   vxbase.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef VGX_VXBASE_H
#define VGX_VXBASE_H

#include "cxlib.h"
#include "comlib.h"
#include "framehash.h"
#include "vxdlldef.h"




struct s_vgx_Similarity_t;
union u_vgx_predicator_t;
struct s_vgx_KeyVal_t;



typedef union u_vgx_ArcVector_cell_t {
  x2tptr_t data;
  struct {
    tptr_t VxD;   /* pointer: Vertex      data: Degree      */
    tptr_t FxP;   /* pointer: Frameslots  data: Predicator  */
  };
} vgx_ArcVector_cell_t;






/*******************************************************************//**
 * weighted_offset_t
 * 
 ***********************************************************************
 */
typedef struct s_weighted_offset_t {
  uint32_t offset : 23;
  uint32_t _rsv_  : 1;
  uint32_t weight : 8;
} weighted_offset_t;





DISABLE_WARNING_NUMBER(4201 4214) /* warning C4201: nonstandard extension used : nameless struct/union 
                                     warning C4214: nonstandard extension user : bit field types other than int
                                   */


/*******************************************************************//**
 * vgx_vector_dimension_encode_t
 *
 ***********************************************************************
 */
typedef vector_feature_t (*vgx_vector_dimension_encoder_t)( struct s_vgx_Similarity_t *self, const ext_vector_feature_t *feature );


__inline static bool __vector_dimension_in_range( int code ) {
  return (bool)( code >= (int)FEATURE_VECTOR_DIMENSION_MIN && code <= (int)FEATURE_VECTOR_DIMENSION_MAX );
}




#define VGX_CSTRING_FLAG__KEY_INDEX_BIT   CStringUserFlag0
#define VGX_CSTRING_FLAG__VAL_INDEX_BIT   CStringUserFlag1
#define VGX_CSTRING_FLAG__DYNAMIC         CStringUserFlag2
#define VGX_CSTRING_FLAG__LITERAL         CStringUserFlag3
#define VGX_CSTRING_FLAG__WILDCARD        CStringUserFlag4
#define VGX_CSTRING_FLAG__PREFIX          CStringUserFlag5
#define VGX_CSTRING_FLAG__INFIX           CStringUserFlag6
#define VGX_CSTRING_FLAG__SUFFIX          CStringUserFlag7


#define VGX_CSTRING_ARRAY_LENGTH( CString ) qwcount( CStringLength( CString ) )

static const int VGX_CSTRING_ARRAY_MAP_KEY_NONE = 0x80000000l;


/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union e_vgx_cstring_array_map_header_t {
  QWORD bits;
  struct {
    uint16_t mask;  // table size (power of 2) minus one
    uint16_t items; // number of occupied cells
    uint32_t _collisions;
  };
} vgx_cstring_array_map_header_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef union e_vgx_cstring_arrray_map_cell_t {
  QWORD bits;
  struct {
    int key;
    float value;
  };
} vgx_cstring_array_map_cell_t;



#define VGX_CSTRING_ARRAY_MAP_OFFSET( Key, Header )  (ihash64( Key ) & (Header)->mask)



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int vgx_cstring_array_map_hashkey( const QWORD *item_bits ) {
  vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)item_bits;
  return (int)(ihash64( cell->key ) & 0xFFFFFFFF);
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_cstring_array_map_cell_t * vgx_cstring_array_map_set( QWORD *cstring_data, int key, float value ) {
  vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)cstring_data;
  vgx_cstring_array_map_cell_t *htable = (vgx_cstring_array_map_cell_t*)cstring_data + 1;
  // Initial offset
  uint16_t offset = VGX_CSTRING_ARRAY_MAP_OFFSET( key, header );
  vgx_cstring_array_map_cell_t *cell = htable + offset;

  // Optimistic start
  if( cell->key == VGX_CSTRING_ARRAY_MAP_KEY_NONE || cell->key == key ) {
    cell->key = key;
    cell->value = value;
    return cell;
  }

  // Probe until no collision
  vgx_cstring_array_map_cell_t *next;
  do {
    header->_collisions++;
    if( (next = htable + (++offset & header->mask)) == cell ) {
      return NULL; // all slots taken
    }
  } while( next->key != VGX_CSTRING_ARRAY_MAP_KEY_NONE && next->key != key );

  // Assign
  next->key = key;
  next->value = value;
  return next;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static vgx_cstring_array_map_cell_t * vgx_cstring_array_map_get( QWORD *cstring_data, int key ) {
  vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)cstring_data;
  vgx_cstring_array_map_cell_t *htable = (vgx_cstring_array_map_cell_t*)cstring_data + 1;
  // Initial offset
  uint16_t offset = VGX_CSTRING_ARRAY_MAP_OFFSET( key, header );
  vgx_cstring_array_map_cell_t *cell = htable + offset;
  // Optimistic start
  if( cell->key == key ) {
    return cell;
  }
  // Probe
  vgx_cstring_array_map_cell_t *next;
  do {
    next = htable + (++offset & header->mask);
    if( next->key == VGX_CSTRING_ARRAY_MAP_KEY_NONE || next == cell ) { // (safeguard against infinite loop)
      return NULL; // end of probe
    }
  } while( next->key != key ); 
  // Success after probe
  return next;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int vgx_cstring_array_map_len( const QWORD *cstring_data ) {
  vgx_cstring_array_map_header_t *header = (vgx_cstring_array_map_header_t*)cstring_data;
  return header->items;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static int vgx_cstring_array_map_key( const QWORD *item_bits ) {
  vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)item_bits;
  return cell->key;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static float vgx_cstring_array_map_val( const QWORD *item_bits ) {
  vgx_cstring_array_map_cell_t *cell = (vgx_cstring_array_map_cell_t*)item_bits;
  return cell->value;
}



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
__inline static QWORD vgx_cstring_array_map_item_from_key_and_val( int key, float val ) {
  vgx_cstring_array_map_cell_t cell;
  cell.key = key;
  cell.value = val;
  return cell.bits;
}






/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_vgx_KeyVal_char_int64_t {
  char *key;
  int64_t value;
} vgx_KeyVal_char_int64_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_StringList_t {
  int64_t sz;
  CString_t **data;
  object_allocator_context_t *item_allocator;
} vgx_StringList_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_StringTuple_t {
  CString_t *CSTR__key;
  CString_t *CSTR__value;
} vgx_StringTuple_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_StringTupleList_t {
  int64_t sz;
  vgx_StringTuple_t **data;
  object_allocator_context_t *item_allocator;
} vgx_StringTupleList_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ByteArray_t {
  int64_t len;
  BYTE *data;
} vgx_ByteArray_t;



/*******************************************************************//**
 * 
 * 
 ***********************************************************************
 */
typedef struct s_vgx_ByteArrayList_t {
  int64_t sz;
  vgx_ByteArray_t *entries;
} vgx_ByteArrayList_t;



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_vgx_arc_direction {
  VGX_ARCDIR_ANY     = 0x0,  // 00
  VGX_ARCDIR_IN      = 0x1,  // 01
  VGX_ARCDIR_OUT     = 0x2,  // 10
  VGX_ARCDIR_BOTH    = 0x3,  // 11
  __VGX_ARCDIR_MASK  = 0x3   // 11
} vgx_arc_direction;

static const char *__reverse_arcdir_map[] = {
  "D_ANY",    // 00
  "D_IN",     // 01
  "D_OUT",    // 10
  "D_BOTH"    // 11
};

__inline static const char * _vgx_arcdir_as_string( const vgx_arc_direction dir ) {
  return __reverse_arcdir_map[ dir & __VGX_ARCDIR_MASK ];
}

__inline static int _vgx_arcdir_valid( const vgx_arc_direction dir ) {
  return (dir & ~__VGX_ARCDIR_MASK) == 0;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_vgx_boolean_logic {
  VGX_LOGICAL_NO_LOGIC  = 0x0,
  VGX_LOGICAL_AND       = 0x1,
  VGX_LOGICAL_OR        = 0x2,
  VGX_LOGICAL_XOR       = 0x3
} vgx_boolean_logic;

static const char *__reverse_logic_map[] = {
  "NONE",  // 00
  "AND",   // 01
  "OR",    // 10
  "XOR"    // 11
};

__inline static const char * _vgx_logic_as_string( const vgx_boolean_logic logic ) {
  return __reverse_logic_map[ logic & 3 ];
}



/*******************************************************************//**
 *
 * This is a general purpose QWORD that may or may not be interpreted as
 *  a pointer, depending on the actual value type
 *
 ***********************************************************************
 */
typedef union u_vgx_simple_value_t {
  uint64_t qword;
  int64_t integer;
  double real;
  CString_t *CSTR__string;
  const char *string;
  void *pointer;
} vgx_simple_value_t;
DLL_VISIBLE extern const vgx_simple_value_t DEFAULT_VGX_SIMPLE_VALUE;



/*******************************************************************//**
 * vgx_value_type_t
 *
 ***********************************************************************
 */
typedef enum e_vgx_value_type_t {
  VGX_VALUE_TYPE_NULL                 = 0x00,   // 0000 0000    no/any value
  VGX_VALUE_TYPE_BOOLEAN              = 0x01,   // 0000 0001    int64_t
  VGX_VALUE_TYPE_INTEGER              = 0x02,   // 0000 0010    int64_t
  VGX_VALUE_TYPE_REAL                 = 0x04,   // 0000 0100    double
  VGX_VALUE_TYPE_QWORD                = 0x08,   // 0000 1000    uint64_t
  VGX_VALUE_TYPE_ENUMERATED_CSTRING   = 0x11,   // 0001 0001    const CString_t *
  VGX_VALUE_TYPE_CSTRING              = 0x12,   // 0001 0010    const CString_t *
  VGX_VALUE_TYPE_BORROWED_STRING      = 0x14,   // 0001 0100    const char *
  VGX_VALUE_TYPE_STRING               = 0x18,   // 0001 1000    const char *
  VGX_VALUE_TYPE_POINTER              = 0x20    // 0010 0000    void *
} vgx_value_type_t;




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_vgx_value_comparison {

  /* ================================================ */
  /* The 4-bit values can be represented within the   */
  /* predicator modifier                              */
  /*                                                  */
  /*                                                  */
  /* MASK   1111    PROBE MASK                        */
  /* --- fundamentals                                 */
  /* ANY    0000    ANY VALUE                         */
  /* NEG    0010    NEGATIVE MATCH                    */
  /* LTE    0100    LESS THAN OR EQUAL                */
  /* GTE    1000    GREATER THAN OR EQUAL             */
  /*                                                  */
  /* --- derived codes                                */
  /* EQU    1100    EQUAL (LTE AND GTE = EQUAL)       */
  /* NEQ    1110    NOT EQUAL                         */
  /* LT     1010    LESS THAN (NOT GTE)               */
  /* GT     0110    GREATER THAN (NOT LTE)            */
  /*                                                  */
  /* --- interpretation of NEG                        */
  /* NEG    0010    Since neither GTE nor LTE is set  */
  /*                we have "ANY", but since the NEG  */
  /*                bit is set we have NOT ANY, which */
  /*                means no match regardless of      */
  /*                value.                            */
  /*                                                  */
  /*                                                  */
  VGX_VALUE_MASK        = 0x0F,   // 0 1111
  VGX_VALUE_NONE        = 0x01,   // 0 0001 (nothing: error)
  VGX_VALUE_ANY         = 0x00,   // 0 0000
  VGX_VALUE_NEG         = 0x02,   // 0 0010
  VGX_VALUE_LTE         = 0x04,   // 0 0100
  VGX_VALUE_GT          = 0x06,   // 0 0110 (not LTE)
  VGX_VALUE_GTE         = 0x08,   // 0 1000
  VGX_VALUE_LT          = 0x0A,   // 0 1010 (not GTE)
  VGX_VALUE_EQU         = 0x0C,   // 0 1100
  VGX_VALUE_NEQ         = 0x0E,   // 0 1110 (not EQU)

  /* ================================================ */
  /* The 5-bit values must be represented in the      */
  /* ephemeral part of the predicator                 */
  /*                                                  */
  /* 10xxx : Various extended comparisons             */
  /* 11xxx : Dynamic comparisons                      */
  /*                                                  */
  VGX_VALUE_EPH_MASK    = 0x1F,   // 1 1111
  VGX_VALUE_DYN_MASK    = 0x18,   // 1 1000     to match 1 1xxx
  VGX_VALUE_RANGE_MASK  = 0x1D,   // 1 1101
  //VGX_VALUE_HAMDIST_MASK= 0x1C,   // 1 1100     to match 1 01xx
  //VGX_VALUE_HAMDIST_X   = 0x14,   // 1 0100     
  VGX_VALUE_DRANGE_MASK = 0x1E,   // 1 1110

  // Various extended
  VGX_VALUE_RANGE       = 0x10,   // 1 0000     RANGE - need two predicators to specify lower and higher bounds
  VGX_VALUE_RSV_11      = 0x11,   // 1 0001
  VGX_VALUE_NRANGE      = 0x12,   // 1 0010     NOT RANGE
  VGX_VALUE_RSV_13      = 0x13,   // 1 0011
  //VGX_VALUE_HAMDIST_2   = 0x14,   // 1 0100     = 2
  //VGX_VALUE_HAMDIST_3   = 0x15,   // 1 0101     = 3
  //VGX_VALUE_HAMDIST_4   = 0x16,   // 1 0110     = 4
  //VGX_VALUE_HAMDIST_5   = 0x17,   // 1 0111     = 5

                                  // Dynamic (bits 2,1,0 map to bits 3,2,1 of basic 4-bit codes for the single value comparisons)
  VGX_VALUE_DYN_RANGE   = 0x18,   // 1 1000     dynamic RANGE ( value in interval: [ previous + x, previous + y ] ) - need two predicators to specify L and H bounds
  VGX_VALUE_DYN_RANGE_R = 0x19,   // 1 1001     dynamic RANGE ( value in interval: [ previous * x, previous * y ] ) - need two predicators to specify L and H bounds
  VGX_VALUE_DYN_LTE     = 0x1A,   // 1 1010     dynamic LTE (value <= arc_value_from_previous_neighborhood_to_current_neighborhood)
  VGX_VALUE_DYN_GT      = 0x1B,   // 1 1011     dynamic GT  (value > arc_value_from_previous_...)
  VGX_VALUE_DYN_GTE     = 0x1C,   // 1 1100     dynamic GTE (value >= arc_value_from_previous_...)
  VGX_VALUE_DYN_LT      = 0x1D,   // 1 1101     dynamic LT  (value < arc_value_from_previous_...)
  VGX_VALUE_DYN_EQU     = 0x1E,   // 1 1110     dynamic EQU (value == arc_value_from_previous_...)
  VGX_VALUE_DYN_NEQ     = 0x1F    // 1 1111     dynamic NEQ (value != arc_value_from_previous_...)


} vgx_value_comparison;


static const char *__reverse_value_comparison_map[] = {
  "V_ANY",        // 00
  "V_NONE",       // 01
  "V_NOT",        // 02
  "V_NOT",        // 03 (ena)
  "V_LTE",        // 04
  "V_LTE",        // 05 (ena)
  "V_GT",         // 06
  "V_GT",         // 07 (ena)
  "V_GTE",        // 08
  "V_GTE",        // 09 (ena)
  "V_LT",         // 0A
  "V_LT",         // 0B (ena)
  "V_EQ",         // 0C
  "V_EQ",         // 0D (ena)
  "V_NEQ",        // 0E
  "V_NEQ",        // 0F
  "V_RANGE",      // 10
  "?V_11",        // 11
  "V_NRANGE",     // 12
  "?V_13",        // 13
  "?V_14",        // 14
  "?V_15",        // 15
  "?V_16",        // 16
  "?V_17",        // 17
  "V_DYN_DELTA",  // 18
  "V_DYN_RATIO",  // 19
  "V_DYN_LTE",    // 1A
  "V_DYN_GT",     // 1B
  "V_DYN_GTE",    // 1C
  "V_DYN_LT",     // 1D
  "V_DYN_EQ",     // 1E
  "V_DYN_NEQ",    // 1F
};

__inline static const char * _vgx_vcomp_as_string( const vgx_value_comparison vcomp ) {
  return __reverse_value_comparison_map[ vcomp & 0x1F ];
}



__inline static int _vgx_is_exact_value_comparison( const vgx_value_comparison vcomp ) {
  return ((vcomp & VGX_VALUE_EQU) == VGX_VALUE_EQU) || ((vcomp & VGX_VALUE_DYN_EQU) == VGX_VALUE_DYN_EQU);
}


__inline static int _vgx_is_basic_value_comparison( const vgx_value_comparison vcomp ) {
  // allow only these bits for basic: -xxx-
  static const unsigned mask = ~0x0000000EU;
  return !((unsigned)vcomp & mask);
}


__inline static int _vgx_is_extended_value_comparison( const vgx_value_comparison vcomp ) {
  // allow 5 bits for extended
  return (vcomp & 0x10) && vcomp < 0x20;
}


__inline static int _vgx_is_valid_value_comparison( const vgx_value_comparison vcomp ) {
  return _vgx_is_basic_value_comparison( vcomp ) || _vgx_is_extended_value_comparison( vcomp );
}


__inline static int _vgx_is_dynamic_value_comparison( const vgx_value_comparison vcomp ) {
  return (vcomp & VGX_VALUE_DYN_MASK) == VGX_VALUE_DYN_MASK;
}


__inline static int _vgx_is_value_range_comparison( const vgx_value_comparison vcomp ) {
  return ((vcomp & VGX_VALUE_RANGE_MASK) == VGX_VALUE_RANGE) || ((vcomp & VGX_VALUE_DRANGE_MASK) == VGX_VALUE_DYN_RANGE);
}



__inline static vgx_value_comparison _vgx_basic_value_comparison_from_dynamic( const vgx_value_comparison dynamic_vcomp ) {
  // dynamic vcomp  11xxx
  //                (<<1)
  //               11xxx0
  //               (&mask)
  // basic vcomp     xxx0
  vgx_value_comparison basic_vcomp = (vgx_value_comparison)((dynamic_vcomp << 1) & VGX_VALUE_MASK);
  return basic_vcomp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_value_t {
  vgx_value_type_t type;
  union {
    QWORD bits;
    vgx_simple_value_t simple;
  } data;
} vgx_value_t;

DLL_VISIBLE extern const vgx_value_t DEFAULT_VGX_VALUE;



/******************************************************************************
 *
 *
 ******************************************************************************
 */
typedef struct s_vgx_KeyVal_t {
  char *key;
  vgx_value_t val;
} vgx_KeyVal_t;



/*******************************************************************//**
 * vgx_value_constraint_t
 *
 ***********************************************************************
 */
typedef struct s_vgx_value_constraint_t {
  vgx_value_type_t type;
  vgx_simple_value_t minval;
  vgx_simple_value_t maxval;
} vgx_value_constraint_t;
DLL_VISIBLE extern const vgx_value_constraint_t DEFAULT_VGX_VALUE_CONSTRAINT;



/*******************************************************************//**
 * vgx_no_value_constraint()
 ***********************************************************************
 */
static vgx_value_constraint_t _vgx_no_value_constraint( void ) {
  return DEFAULT_VGX_VALUE_CONSTRAINT;
}



/*******************************************************************//**
 * vgx_boolean_value_constraint()
 ***********************************************************************
 */
static vgx_value_constraint_t _vgx_boolean_value_constraint( void ) {
  vgx_value_constraint_t vc;
  vc.type = VGX_VALUE_TYPE_BOOLEAN;
  vc.minval.integer = 0;
  vc.maxval.integer = 1;
  return vc;
}



/*******************************************************************//**
 * vgx_integer_value_constraint()
 ***********************************************************************
 */
static vgx_value_constraint_t _vgx_integer_value_constraint( int64_t minval, int64_t maxval ) {
  vgx_value_constraint_t vc;
  vc.type = VGX_VALUE_TYPE_INTEGER;
  vc.minval.integer = minval;
  vc.maxval.integer = maxval;
  return vc;
}



/*******************************************************************//**
 * vgx_real_value_constraint()
 ***********************************************************************
 */
static vgx_value_constraint_t _vgx_real_value_constraint( double minval, double maxval ) {
  vgx_value_constraint_t vc;
  vc.type = VGX_VALUE_TYPE_REAL;
  vc.minval.real = minval;
  vc.maxval.real = maxval;
  return vc;
}



/*******************************************************************//**
 * vgx_enumerated_string_value_constraint()
 ***********************************************************************
 */
static vgx_value_constraint_t _vgx_enumerated_string_value_constraint( int minlen, int maxlen ) {
  vgx_value_constraint_t vc;
  vc.type = VGX_VALUE_TYPE_ENUMERATED_CSTRING;
  vc.minval.integer = minlen;
  vc.maxval.integer = maxlen;
  return vc;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_value_condition_t {
  vgx_value_t value1;
  vgx_value_t value2;
  vgx_value_comparison vcomp;
} vgx_value_condition_t;

DLL_VISIBLE extern const vgx_value_condition_t DEFAULT_VGX_VALUE_CONDITION;





// Max expiration time: Thu Dec 31 23:59:59 2099
// Never expires:       Fri Jan 01 00:00:00 2100
#define TIME_EXPIRES_NEVER 4102444800
#define TIMESTAMP_ERROR 0
#define TIMESTAMP_MIN 1                         // Thu Jan 01 00:00:01 1970
#define TIMESTAMP_MAX (TIME_EXPIRES_NEVER-1)    // Thu Dec 31 23:59:59 2099




/*******************************************************************//**
 * vgx_predicator_t
 * 
 * 
 ***********************************************************************
 */
typedef union u_vgx_predicator_val_t {
  int32_t   integer;
  uint32_t  uinteger;
  float     real;
  DWORD     bits;
} vgx_predicator_val_t;


// NOTE: The order is really important here because the enc part of rel needs to be adjacent to the mod part
typedef struct s_vgx_predicator_rel_t {
  uint16_t dir  : 2;  // vgx_arc_direction
  uint16_t enc  : 14; // encoding
} vgx_predicator_rel_t;


typedef union u_vgx_predicator_mod_t {
  uint8_t   bits;
  struct {
    uint8_t type : 4; // predicator modifier type
    uint8_t f    : 1; // predicator value type
    uint8_t xfwd : 1; // Exclusively forward-only arc
    uint8_t _ign : 2; // ignored in stored predicator
  } stored;
  struct {
    uint8_t type : 4; // probe modifier type
    uint8_t f    : 1; // predicator value type f=1 => float, f=0 => int
    uint8_t NEG  : 1; // probe value match negation
    uint8_t LTE  : 1; // Less than or equal to <=
    uint8_t GTE  : 1; // Greater than or equal to >=
  } probe;
} vgx_predicator_mod_t;


// NOTE: The ephemeral part of a predicator cannot be stored. It is only for 
// transferring temporary information between parts of the system. When stored
// in arcvectors the ephemeral part will be lost due to the 56-bit restriction
// of integer values in arcvector arcs. (tagged pointers)
typedef struct s_vgx_predicator_eph_t {
   uint8_t value  : 4; // 0 - 15
   uint8_t type   : 3; // 0 - 7
   uint8_t neg    : 1; 
} vgx_predicator_eph_t;

static const uint8_t VGX_PREDICATOR_EPH_DISTANCE_MAX = 15;


typedef enum e_vgx_predicator_eph_enum {
  //                                          N  TYPE   VALUE      
  //                                          =  =====  =======
  //                                             . . .  . . . .
  VGX_PREDICATOR_EPH_TYPE_NONE       = 0,  // 0  0 0 0  0 0 0 0
  VGX_PREDICATOR_EPH_TYPE_LSH        = 1,  // 0  0 0 1  [ 0 -15 ] = general purpose value depending on application
  VGX_PREDICATOR_EPH_TYPE_DYNDELTA   = 2,  // 0  0 1 0  1 x x x  = comparison code, predicator value is a delta relative to another predicator
  VGX_PREDICATOR_EPH_TYPE_DYNRATIO   = 3,  // 0  0 1 1  1 x x x  = comparison code, predicator value is a ratio relative to another predicator
  VGX_PREDICATOR_EPH_TYPE_DISTANCE   = 4,  // 0  1 0 0  [ 0 - 15]
  VGX_PREDICATOR_EPH_TYPE_RESERVED_5 = 5,  // 0  1 0 1
  VGX_PREDICATOR_EPH_TYPE_FWDARCONLY = 6,  // 0  1 1 0  x x x x  = only forward arcs are created (reverse arc not created)
  VGX_PREDICATOR_EPH_TYPE_AUTO_TM    = 7   // 0  1 1 1  x x x x  = automatically create timestamp arcs when creating new relationships
} vgx_predicator_eph_enum;



typedef enum e_vgx_predicator_val_enum {
  /* */
  VGX_PREDICATOR_VAL_ZERO = 0,
} vgx_predicator_val_enum;


typedef enum e_vgx_predicator_val_type {
  VGX_PREDICATOR_VAL_TYPE_NONE,
  VGX_PREDICATOR_VAL_TYPE_UNITY,
  VGX_PREDICATOR_VAL_TYPE_INTEGER,
  VGX_PREDICATOR_VAL_TYPE_UNSIGNED,
  VGX_PREDICATOR_VAL_TYPE_REAL,
  VGX_PREDICATOR_VAL_TYPE_ERROR
} vgx_predicator_val_type;


typedef enum e_vgx_predicator_rel_enum {
  /* NONE / WILDCARD */
    VGX_PREDICATOR_REL_NONE                = 0x0000,
    VGX_PREDICATOR_REL_WILDCARD            = 0x0000,
  
  /* Reserved Range */
  __VGX_PREDICATOR_REL_START_RSV_RANGE     = 0x0001,
   _VGX_PREDICATOR_REL_TEST1               = 0x0011,
   _VGX_PREDICATOR_REL_TEST2               = 0x0012,
   _VGX_PREDICATOR_REL_TEST3               = 0x0013,
  __VGX_PREDICATOR_REL_END_RSV_RANGE       = 0x00ff,

  /* System Range */
  __VGX_PREDICATOR_REL_START_SYS_RANGE     = 0x0100,
    VGX_PREDICATOR_REL_RELATED             = 0x0101,
    VGX_PREDICATOR_REL_SIMILAR             = 0x0102,
    VGX_PREDICATOR_REL_NONEXIST            = 0x0103,
    VGX_PREDICATOR_REL_SELF                = 0x0104,
    VGX_PREDICATOR_REL_SYNTHETIC           = 0x0105,
  __VGX_PREDICATOR_REL_END_SYS_RANGE       = 0x01ff,
  
  /* User Range - available for dynamic code assignment */
  __VGX_PREDICATOR_REL_START_USER_RANGE    = 0x0200,
  __VGX_PREDICATOR_REL_END_USER_RANGE      = 0x3eff,
  __VGX_PREDICATOR_REL_USER_RANGE_SIZE     = __VGX_PREDICATOR_REL_END_USER_RANGE - __VGX_PREDICATOR_REL_START_USER_RANGE + 1,

  /* Full Valid Range */
  __VGX_PREDICATOR_REL_VALID_RANGE_SIZE    = __VGX_PREDICATOR_REL_END_USER_RANGE - __VGX_PREDICATOR_REL_START_SYS_RANGE + 1,
  
  /* Exception Range */
  __VGX_PREDICATOR_REL_START_EXC_RANGE     = 0x3f00,
    VGX_PREDICATOR_REL_NO_MAPPING          = 0x3f01,
    VGX_PREDICATOR_REL_LOCKED              = 0x3f09,
    VGX_PREDICATOR_REL_AMBIGUOUS           = 0x3f0a,
    VGX_PREDICATOR_REL_INVALID             = 0x3f0b,
    VGX_PREDICATOR_REL_COLLISION           = 0x3f0c,
    VGX_PREDICATOR_REL_ERROR               = 0x3f0e,
  __VGX_PREDICATOR_REL_END_EXC_RANGE       = 0x3fff,

  /* 14-bit max value */
  VGX_PREDICATOR_REL_MAX                   = 0x3fff,
} vgx_predicator_rel_enum;


DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_NONE_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_WILDCARD_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_RELATED_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_SIMILAR_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_SYNTHETIC_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_SYSTEM_STRING;
DLL_HIDDEN extern const CString_t *CSTR__VGX_PREDICATOR_REL_UNKNOWN_STRING;


typedef enum e_vgx_predicator_modifier_enum {

  /*                                                                                */





  /* ------------------------------------------------------------------------------ */
  /*                                   Stored:  - - X f  t t t t                    */
  /*                                   Probe:   G L N f  t t t t                    */
  /*                                            ^ ^ ^ ^  ^ ^ ^ ^                    */
  /*           greater than or equal (probe) ---| | | |  \     /                    */
  /*           less than or equal (probe) --------| | |   \   /                     */
  /*     fwd-only (stored), negation (probe) -------| |    \ /                      */
  /*                            float value ----------|     /                       */
  /*                                                        |                       */
  /*                            modifier type --------------|                       */
  /*                                                                                */
  /*  REMARKS:                                                                      */
  /*  1. G and L bits are used by modifier probes to specify how the target         */
  /*     predicator value should be evaluated by the filter logic. G means ">=" and */
  /*     L means "<=". When both G and L are 0 the probe does not require any       */
  /*     specific value for the target predicator value. GL=00 is a value wildcard. */
  /*     When only G is set in the probe the target predicator value must be        */
  /*     greater than or equal to the probe value. When only L is set in the probe  */
  /*     the target predicator value must be less than or equal to the probe value. */
  /*     When both G and L are set in the probe the target predicator value must be */
  /*     equal to the probe value.                                                  */
  /*     G L = 0 0    any target value (*)                                          */
  /*     G L = 0 1    target value <= probe value                                   */
  /*     G L = 1 0    target value >= probe value                                   */
  /*     G L = 1 1    target value == probe value                                   */
  /*                                                                                */
  /*  2. G and L bits are not used (ignored) in the stored arc predicators, they    */
  /*     are only used in probes.                                                   */
  /*                                                                                */
  /*  3. Probe arc predicators:                                                     */
  /*       N bit specifies probe match negation, i.e. when N=1 the probe will       */
  /*       match the target when the probe condition is not met.                    */
  /*                                                                                */
  /*     Stored arc predicators:                                                    */
  /*       X bit indicates arc is exclusive forward-only arc (no implicit reverse   */
  /*       arc mirrored back from the terminal.)                                    */
  /*                                                                                */
  /*  4. The "f" bit specifies the value type of the predicator value to be either  */
  /*     integer or floating point. When f=0 the value is integer. When f=1 the     */
  /*     value is floating point.                                                   */
  /*     f = 0        predicator value is integer type                              */
  /*     f = 1        predicator value is single precision floating point type      */
  /*                                                                                */
  /*  5. The type bits "t" specify the modifier type.                               */
  /*                                                                                */
  /* ------------------------------------------------------------------------------ */

  /* MASKS                                                                          */
  /*                                                          G L N f  t t t t      */
  _VGX_PREDICATOR_MOD_TYP_MASK      = 0x0F,               /*           1 1 1 1      */         
  _VGX_PREDICATOR_MOD_FLT_MASK      = 0x10,               /*        1               */
  _VGX_PREDICATOR_MOD_STO_MASK      = 0x1F,               /*        1  1 1 1 1      */         
  _VGX_PREDICATOR_MOD_NEG_MASK      = VGX_VALUE_NEG << 4, /*      1                 */
  _VGX_PREDICATOR_MOD_LTE_MASK      = VGX_VALUE_LTE << 4, /*    1                   */
  _VGX_PREDICATOR_MOD_GT_MASK       = VGX_VALUE_GT  << 4, /*    1 1                 */
  _VGX_PREDICATOR_MOD_GTE_MASK      = VGX_VALUE_GTE << 4, /*  1                     */
  _VGX_PREDICATOR_MOD_LT_MASK       = VGX_VALUE_LT  << 4, /*  1   1                 */
  _VGX_PREDICATOR_MOD_EQU_MASK      = VGX_VALUE_EQU << 4, /*  1 1                   */
  _VGX_PREDICATOR_MOD_NEQ_MASK      = VGX_VALUE_NEQ << 4, /*  1 1 1                 */
  _VGX_PREDICATOR_MOD_ANY_MASK      = VGX_VALUE_ANY << 4, /*                        */
  _VGX_PREDICATOR_MOD_CMP_MASK      = 0xE0,               /*  1 1 1                 */

  _VGX_PREDICATOR_MOD_NOT           = 0x20,               /*  0 0 1 x  x x x x      */
  _VGX_PREDICATOR_MOD_CLS_MASK      = 0x0C,               /*           1 1          */
  _VGX_PREDICATOR_MOD_CLS_SYS       = 0x00,               /*           0 0          */
  _VGX_PREDICATOR_MOD_CLS_BAS       = 0x04,               /*           0 1          */
  _VGX_PREDICATOR_MOD_CLS_ACC       = 0x08,               /*           1 0          */
  _VGX_PREDICATOR_MOD_CLS_TIM       = 0x0C,               /*           1 1          */
  _VGX_PREDICATOR_MOD_ALL_MASK      = 0xFF,               /*  1 1 1 1  1 1 1 1      */

  _VGX_PREDICATOR_MOD_FWD           = 0x20,               /*      1                 */         

  /* WILDCARD                                                                       */
  /*                                        G L N f  t t t t                        */
  /*                                        0 0 0 0  0 0 0 0                        */
  VGX_PREDICATOR_MOD_WILDCARD       = 0x00,


  /* MOD CLASS: SYSTEM 00xx */

  /* NONE                                                                           */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  0 0 0 0                        */
  VGX_PREDICATOR_MOD_NONE           = 0x00,

  /* M_STAT                                                                         */
  /* STATIC                                                                         */
  /*    Simple relationship A-(rel)->B without any further information about the    */
  /*    relationship. The predicator value is ignored.                              */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  0 0 0 1                        */
  /*                                        - - n 0  0 0 0 1                        */
  VGX_PREDICATOR_MOD_STATIC         = 0x01, 

  /* M_SIM                                                                          */
  /* SIMILARITY                                                                     */
  /*    Feature vector similarity measure                                           */
  /*    (float) -1.0 - 1.0                                                          */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 1  0 0 1 0  (stored)              */
  /*                                        g l n 1  0 0 1 0  (probe)               */
  VGX_PREDICATOR_MOD_SIMILARITY     = 0x12,

  /* M_DIST                                                                         */
  /* DISTANCE                                                                       */
  /*    Distance measure                                                            */
  /*    (float) 0.0 - 3.4e+38                                                       */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 1  0 0 1 1  (stored)              */
  /*                                        g l n 1  0 0 1 1  (probe)               */
  VGX_PREDICATOR_MOD_DISTANCE       = 0x13,


  /* MOD CLASS: BASIC VALUES 01xx */

  /* M_LSH                                                                          */
  /* LOCALITY SENSITIVE HASH                                                        */
  /*    Bit pattern subject to approximate match by hamming distance                */
  /*    (unsigned int) 0x00000000 - 0xffffffff                                      */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  0 1 0 0  (stored)              */
  /*                                        g l n 0  0 1 0 0  (probe)               */
  VGX_PREDICATOR_MOD_LSH            = 0x04, 
  
  /* M_INT                                                                          */
  /* INTEGER                                                                        */
  /*    Generic signed integer value                                                */
  /*    (int) -2147483648 - 2147483647                                              */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  0 1 0 1  (stored)              */
  /*                                        g l n 0  0 1 0 1  (probe)               */
  VGX_PREDICATOR_MOD_INTEGER        = 0x05, 

  /* M_UINT                                                                         */
  /* UNSIGNED                                                                       */
  /*    Generic signed integer value                                                */
  /*    (unsigned int) 0 - 4294967295                                               */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  0 1 1 0  (stored)              */
  /*                                        g l n 0  0 1 1 0  (probe)               */
  VGX_PREDICATOR_MOD_UNSIGNED       = 0x06, 

  /* M_FLT                                                                          */
  /* FLOAT                                                                          */
  /*    Generic single precision floating point value                               */
  /*    (float) -3.4e+38 - 3.4e+38                                                  */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 1  0 1 1 1  (stored)              */
  /*                                        g l n 1  0 1 1 1  (probe)               */
  VGX_PREDICATOR_MOD_FLOAT          = 0x17, 


  /* MOD CLASS: ACCUMULATORS 10xx */

  /* M_CNT                                                                          */
  /* COUNTER                                                                        */
  /*    Integer accumulator                                                         */
  /*    (unsigned int) 0 - 4294967295                                               */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 0 0 0  (stored)              */
  /*                                        g l n 0  1 0 0 0  (probe)               */
  VGX_PREDICATOR_MOD_COUNTER        = 0x08, 

  /* M_ACC                                                                          */
  /* ACCUMULATOR                                                                    */
  /*    Floating point accumulator                                                  */
  /*    (float) -3.4e+38 - 3.4e+38                                                  */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 1  1 0 0 1  (stored)              */
  /*                                        g l n 1  1 0 0 1  (probe)               */
  VGX_PREDICATOR_MOD_ACCUMULATOR    = 0x19, 

  /* M_INTAGGR                                                                      */
  /* INTEGER AGGREGATOR                                                             */
  /*    Internal aggregation of several                                             */
  /*    predicators with integer values                                             */
  /*                                                                                */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 0 1 0  (stored)              */
  /*                                        g l n 0  1 0 1 0  (probe)               */
  VGX_PREDICATOR_MOD_INT_AGGREGATOR = 0x0A, 

  /* M_FLTAGGR                                                                      */
  /* FLOAT AGGREGATOR                                                               */
  /*    Internal aggregation of several                                             */
  /*    predicators with floating point values                                      */
  /*                                                                                */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 1  1 0 1 1  (stored)              */
  /*                                        g l n 1  1 0 1 1  (probe)               */
  VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR =   0x1B, 


  /* MOD CLASS: TIME STAMPS 11xx */

  /* M_TMC                                                                          */
  /* TIME CREATED                                                                   */
  /* 0 - 4294967295 seconds since epoch                                             */
  /* 'Thu Jan 01 00:00:00 1970' to 'Sun Feb 07 06:28:15 2106' UTC                   */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 1 0 0  (stored)              */
  /*                                        g l n 0  1 1 0 0  (probe)               */
  VGX_PREDICATOR_MOD_TIME_CREATED   = 0x0C,

  /* M_TMM                                                                          */
  /* TIME MODIFIED                                                                  */
  /* 0 - 4294967295 seconds since epoch                                             */
  /* 'Thu Jan 01 00:00:00 1970' to 'Sun Feb 07 06:28:15 2106' UTC                   */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 1 0 1  (stored)              */
  /*                                        g l n 0  1 1 0 1  (probe)               */
  VGX_PREDICATOR_MOD_TIME_MODIFIED  = 0x0D,

  /* M_TMX                                                                          */
  /* TIME EXPIRES                                                                   */
  /* 0 - 4294967295 seconds since epoch                                             */
  /* 'Thu Jan 01 00:00:00 1970' to 'Sun Feb 07 06:28:15 2106' UTC                   */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 1 1 0  (stored)              */
  /*                                        g l n 0  1 1 1 0  (probe)               */
  VGX_PREDICATOR_MOD_TIME_EXPIRES   = 0x0E,

  /* M_15                                                                           */
  /* RESERVED 1111                                                                  */
  /*                                                                                */
  /*                                        G L N f  t t t t                        */
  /*                                        - - - 0  1 1 1 1  (stored)              */
  /*                                        g l n 0  1 1 1 1  (probe)               */
  VGX_PREDICATOR_MOD_RESERVED_15    = 0x0F,


  /*                                                                                */
  /* Special internal use. When bitmask set, process the creation of new            */
  /* relationship in special ways.                                                  */
  /*                                                                                */

  /* Automatically add timestamp arcs                                               */
  _VGX_PREDICATOR_MOD_AUTO_TM       = 0x100,

  /* Only create the forward arc                                                    */
  _VGX_PREDICATOR_MOD_FORWARD_ONLY  = 0x800

} vgx_predicator_modifier_enum;


__inline static vgx_predicator_mod_t _vgx_predicator_mod_from_enum( vgx_predicator_modifier_enum mod_enum ) {
  vgx_predicator_mod_t mod;
  mod.bits = (uint8_t)((int)mod_enum & _VGX_PREDICATOR_MOD_ALL_MASK);
  return mod;
}



__inline static vgx_predicator_mod_t _vgx_set_predicator_mod_condition_from_value_comparison( vgx_predicator_mod_t *modifier, vgx_value_comparison vcomp ) {
  modifier->bits |= (uint8_t)(vcomp << 4);
  return *modifier;
}



static const char *__reverse_modifier_map[] = {
  "M_NONE",     // 0000
  "M_STAT",     // 0001
  "M_SIM",      // 0010
  "M_DIST",     // 0011
  "M_LSH",      // 0100
  "M_INT",      // 0101
  "M_UINT",     // 0110
  "M_FLT",      // 0111
  "M_CNT",      // 1000
  "M_ACC",      // 1001
  "M_INTAGGR",  // 1010
  "M_FLTAGGR",  // 1011
  "M_TMC",      // 1100
  "M_TMM",      // 1101
  "M_TMX",      // 1110
  "M_15"        // 1111
};



static const char *__reverse_modifier_map_fwdonly[] = {
  "M_NONE|M_FWDONLY",     // xx1x 0000
  "M_STAT|M_FWDONLY",     // xx1x 0001
  "M_SIM|M_FWDONLY",      // xx1x 0010
  "M_DIST|M_FWDONLY",     // xx1x 0011
  "M_LSH|M_FWDONLY",      // xx1x 0100
  "M_INT|M_FWDONLY",      // xx1x 0101
  "M_UINT|M_FWDONLY",     // xx1x 0110
  "M_FLT|M_FWDONLY",      // xx1x 0111
  "M_CNT|M_FWDONLY",      // xx1x 1000
  "M_ACC|M_FWDONLY",      // xx1x 1001
  "M_INTAGGR|M_FWDONLY",  // xx1x 1010
  "M_FLTAGGR|M_FWDONLY",  // xx1x 1011
  "M_TMC|M_FWDONLY",      // xx1x 1100
  "M_TMM|M_FWDONLY",      // xx1x 1101
  "M_TMX|M_FWDONLY",      // xx1x 1110
  "M_15|M_FWDONLY"        // xx1x 1111
};



__inline static const char * _vgx_modifier_as_string( const vgx_predicator_mod_t mod ) {
  int i = mod.bits & _VGX_PREDICATOR_MOD_TYP_MASK;
  return (mod.bits & _VGX_PREDICATOR_MOD_FWD) ? __reverse_modifier_map_fwdonly[i] : __reverse_modifier_map[i];
}



typedef union u_vgx_predicator_t {
  struct {
    // PREDICATOR LAYOUT:
    //
    // 63      55 52   47           34 31                             0
    // |       |  |    |            |  |                              |
    // ................................................................
    // xxxxxxxx[  MOD ][    REL     ]DD[             VAL              ]
    // --------GLNFTTTTrrrrrrrrrrrrrrOIvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    // \     / TTE|3210             |UN
    //  \   /  EEG|                 |T
    //   \ /      ...................
    //  (eph)     [       KEY       ]
    //            FTTTTrrrrrrrrrrrrrr
    //
    union {
      vgx_predicator_val_t val;     // For code clarity only, use the appropriate one 
      vgx_predicator_val_t delta;   // when accessing the value so it's easier to read what's going on
      vgx_predicator_val_t ratio;   // when accessing the value so it's easier to read what's going on
    };
    vgx_predicator_rel_t rel;   //  14-bit relationship enumerations per graph - some are reserved, 2 bits for arc direction
    vgx_predicator_mod_t mod;   //  8-bit Predicator modifier
    vgx_predicator_eph_t eph;   //  8-bit Ephemeral flags (non-persistable)
  };
  struct {
    uint64_t ___v : 32; // (value excluded)
    uint64_t ___d : 2;  // (arc direction excluded)
    uint64_t rkey : 19; // rel.enc (14) and mod (5) = 19
    uint64_t ___c : 3;  // (condition excluded)
    uint64_t ___0 : 8;  // (ephemeral excluded)
  };
  struct {
    uint64_t __data56 : 56; // storable date
    uint64_t __0__7b : 8;   // ephemeral data
  };
  uint64_t data;
} vgx_predicator_t;


/* Predicator match requires relationship and modifier to be the same   */
/* Let's do masks on the qword level locally since we know what we're   */
/* doing and it's a little faster than relying on bitfields.            */
/*                                     EECTRRRrvvvvvvvv                 */
/*                                       f    d                         */
#define __VGX_PREDICATOR_VAL_MASK    0x00000000ffffffffULL  // value
#define __VGX_PREDICATOR_DIR_MASK    0x0000000300000000ULL  // direction
#define __VGX_PREDICATOR_REL_MASK    0x0000fffc00000000ULL  // relationship type
#define __VGX_PREDICATOR_MOD_MASK    0x00ff000000000000ULL  // modifier, including comparison spec
#define __VGX_PREDICATOR_SMT_MASK    0x001f000000000000ULL  // stored modifier type
#define __VGX_PREDICATOR_FWD_MASK    0x0020000000000000ULL  // exclusive forward-only arc
#define __VGX_PREDICATOR_MCL_MASK    0x000c000000000000ULL  // modifier class
#define __VGX_PREDICATOR_CMP_MASK    0x00e0000000000000ULL  // comparison spec
#define __VGX_PREDICATOR_FLT_MASK    0x0010000000000000ULL  // float value flag
#define __VGX_PREDICATOR_LTE_MASK    0x0040000000000000ULL  // comparison less-than-or-equal bit
#define __VGX_PREDICATOR_GTE_MASK    0x0080000000000000ULL  // comparison greater-than-or-equal bit
#define __VGX_PREDICATOR_EQU_MASK    0x00c0000000000000ULL  // comparison equal bit
#define __VGX_PREDICATOR_KEY_MASK    0x001ffffc00000000ULL  // predicator unique key for arcvector indexing
#define __VGX_PREDICATOR_DAT_MASK    0x001ffffcffffffffULL  // significant data mask (relationship type, stored modifier, value)  (NO DIRECTION)
#define __VGX_PREDICATOR_EPH_MASK    0xff00000000000000ULL  // ephemeral portion (non-storable)
#define __VGX_PREDICATOR_NEG_MASK    0x8000000000000000ULL  // invert predicator match bit
#define __VGX_PREDICATOR_WILDCARD    0x0000000000000000ULL  // no predicator

                                                            //  GE LE N
#define __VGX_PREDICATOR_CMP_NEQ     0x00E0000000000000ULL  //  1  1  1  0    -  -  -  -
#define __VGX_PREDICATOR_CMP_LT      0x00A0000000000000ULL  //  1  0  1  0    -  -  -  -
#define __VGX_PREDICATOR_CMP_LTE     0x0040000000000000ULL  //  0  1  0  0    -  -  -  -
#define __VGX_PREDICATOR_CMP_EQU     0x00C0000000000000ULL  //  1  1  0  0    -  -  -  -
#define __VGX_PREDICATOR_CMP_GTE     0x0080000000000000ULL  //  1  0  0  0    -  -  -  -
#define __VGX_PREDICATOR_CMP_GT      0x0060000000000000ULL  //  0  1  1  0    -  -  -  -

#define __VGX_PREDICATOR_SGN_INT     0x0005000000000000ULL  //  -  -  -  -    0  1  0  1

#define __VGX_PREDICATOR_MCL_SYS    ((uint64_t)_VGX_PREDICATOR_MOD_CLS_SYS << 48)
#define __VGX_PREDICATOR_MCL_BAS    ((uint64_t)_VGX_PREDICATOR_MOD_CLS_BAS << 48)
#define __VGX_PREDICATOR_MCL_ACC    ((uint64_t)_VGX_PREDICATOR_MOD_CLS_ACC << 48)
#define __VGX_PREDICATOR_MCL_TIM    ((uint64_t)_VGX_PREDICATOR_MOD_CLS_TIM << 48)


DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_ANY;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_ANY_OUT;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_ANY_IN;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_NONE;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_AMBIGUOUS;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_ERROR;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_RELATED;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_SIMILAR;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_INTEGER;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_UNSIGNED;
DLL_VISIBLE extern const vgx_predicator_t VGX_PREDICATOR_FLOAT;
DLL_VISIBLE extern const uint64_t VGX_PREDICATOR_NONE_BITS;

DLL_VISIBLE extern const vgx_predicator_val_t VGX_PREDICATOR_VAL_INIT;

DLL_VISIBLE extern const vgx_predicator_rel_t VGX_PREDICATOR_REL_INIT;

DLL_VISIBLE extern const vgx_predicator_mod_t VGX_PREDICATOR_MOD_INIT;

DLL_VISIBLE extern const vgx_predicator_eph_t VGX_PREDICATOR_EPH_NONE;
DLL_VISIBLE extern const vgx_predicator_eph_t VGX_PREDICATOR_EPH_DYNDELTA;
DLL_VISIBLE extern const vgx_predicator_eph_t VGX_PREDICATOR_EPH_DISTANCE;


__inline static int _vgx_predicator_is_none( const vgx_predicator_t pred ) {
  return pred.data == VGX_PREDICATOR_NONE.data;
}


__inline static int _vgx_predicator_is_synthetic( const vgx_predicator_t pred ) {
  return pred.rel.enc == VGX_PREDICATOR_REL_SYNTHETIC;
}


__inline static int _vgx_predicator_cmp_is_NEQ( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_NEQ;
}


__inline static int _vgx_predicator_cmp_is_LT( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_LT;
}


__inline static int _vgx_predicator_cmp_is_LTE( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_LTE;
}


__inline static int _vgx_predicator_cmp_is_EQU( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_EQU;
}


__inline static int _vgx_predicator_cmp_is_GTE( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_GTE;
}


__inline static int _vgx_predicator_cmp_is_GT( const vgx_predicator_t pred ) {
  return (pred.data & __VGX_PREDICATOR_CMP_MASK) == __VGX_PREDICATOR_CMP_GT;
}


__inline static void _vgx_predicator_eph_invert( vgx_predicator_t *pred ) {
  pred->eph.neg = !pred->eph.neg;
}


__inline static void _vgx_predicator_eph_set_negative( vgx_predicator_t *pred ) {
  pred->eph.neg = 1;
}


__inline static void _vgx_predicator_eph_set_positive( vgx_predicator_t *pred ) {
  pred->eph.neg = 0;
}


__inline static int _vgx_predicator_eph_is_negative( const vgx_predicator_t predicator ) {
  return (int)predicator.eph.neg;
}


__inline static int _vgx_predicator_eph_is_positive( const vgx_predicator_t predicator ) {
  return !(int)predicator.eph.neg;
}


__inline static void _vgx_predicator_eph_set_lsh_and_distance( vgx_predicator_t *pred, int distance_threshold ) {
  pred->eph.value = (uint8_t)distance_threshold;
  pred->eph.type = VGX_PREDICATOR_EPH_TYPE_LSH;
}


__inline static int _vgx_predicator_eph_is_lsh( const vgx_predicator_t predicator ) {
  return predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_LSH;
}


__inline static int _vgx_predicator_eph_get_lsh_distance( const vgx_predicator_t predicator ) {
  return _vgx_predicator_eph_is_lsh( predicator ) ? (int)predicator.eph.value : -1;
}


__inline static void _vgx_predicator_eph_set_distance( vgx_predicator_t *pred, unsigned distance ) {
  pred->eph.value = (uint8_t)distance;
  pred->eph.type = VGX_PREDICATOR_EPH_TYPE_DISTANCE;
}


__inline static int _vgx_predicator_eph_is_distance( const vgx_predicator_t predicator ) {
  return predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_DISTANCE;
}


__inline static unsigned _vgx_predicator_eph_get_value( const vgx_predicator_t predicator ) {
  return (unsigned)predicator.eph.value;
}


__inline static void _vgx_predicator_eph_set_dyndelta( vgx_predicator_t *pred, vgx_value_comparison vcomp, vgx_predicator_val_t delta ) {
  vgx_value_comparison basic_vcomp = _vgx_basic_value_comparison_from_dynamic( vcomp );
  _vgx_set_predicator_mod_condition_from_value_comparison( &pred->mod, basic_vcomp );
  pred->delta = delta;
  pred->eph.type = VGX_PREDICATOR_EPH_TYPE_DYNDELTA;
  pred->eph.value = VGX_VALUE_MASK & vcomp; // only room for 4 bits in the eph.value (th 5th bit=1 is implied by the eph context)
}

__inline static int _vgx_predicator_eph_is_dyndelta( const vgx_predicator_t predicator ) {
  return predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_DYNDELTA;
}


__inline static void _vgx_predicator_eph_set_dynratio( vgx_predicator_t *pred, vgx_value_comparison vcomp, vgx_predicator_val_t ratio ) {
  vgx_value_comparison basic_vcomp = _vgx_basic_value_comparison_from_dynamic( vcomp );
  _vgx_set_predicator_mod_condition_from_value_comparison( &pred->mod, basic_vcomp );
  pred->ratio.real = ratio.real; // reminder that we're always using float values for the ratio
  pred->eph.type = VGX_PREDICATOR_EPH_TYPE_DYNRATIO;
  pred->eph.value = VGX_VALUE_MASK & vcomp; // only room for 4 bits in the eph.value (th 5th bit=1 is implied by the eph context)
}

__inline static int _vgx_predicator_eph_is_dynratio( const vgx_predicator_t predicator ) {
  return predicator.eph.type == VGX_PREDICATOR_EPH_TYPE_DYNRATIO;
}


__inline static int _vgx_predicator_eph_is_dynamic( const vgx_predicator_t predicator ) {
  return _vgx_predicator_eph_is_dyndelta( predicator ) || _vgx_predicator_eph_is_dynratio( predicator );
}


__inline static bool __relationship_enumeration_in_user_range( vgx_predicator_rel_enum enc ) {
  return enc >= __VGX_PREDICATOR_REL_START_USER_RANGE
         &&
         enc <= __VGX_PREDICATOR_REL_END_USER_RANGE
         ? true : false;
}


__inline static bool __relationship_in_user_range( int rel ) {
  return (bool)(rel >= (int)__VGX_PREDICATOR_REL_START_USER_RANGE && rel <= (int)__VGX_PREDICATOR_REL_END_USER_RANGE);
}


__inline static bool __relationship_enumeration_valid( int rel ) {
  return rel >= VGX_PREDICATOR_REL_NONE && rel < __VGX_PREDICATOR_REL_START_EXC_RANGE;
}


__inline static bool __predicator_has_exception( vgx_predicator_t pred ) {
  return __relationship_enumeration_valid(
      (vgx_predicator_rel_enum)pred.rel.enc ) == false ? true : false;
}


__inline static bool __predicator_has_relationship( vgx_predicator_t pred ) {
  return pred.rel.enc >= __VGX_PREDICATOR_REL_START_SYS_RANGE
         &&
         pred.rel.enc <= __VGX_PREDICATOR_REL_END_USER_RANGE
         ? true : false;
}


__inline static vgx_arc_direction __predicator_direction( vgx_predicator_t pred ) {
  return (vgx_arc_direction)pred.rel.dir;
}


__inline static int _vgx_predicator_mod_is_system( const vgx_predicator_t predicator ) {
  // return true if predicator modifier class is time
  return !((predicator.data ^ __VGX_PREDICATOR_MCL_SYS) & __VGX_PREDICATOR_MCL_MASK);
}


__inline static int _vgx_predicator_mod_is_basic( const vgx_predicator_t predicator ) {
  // return true if predicator modifier class is basic
  return !((predicator.data ^ __VGX_PREDICATOR_MCL_BAS) & __VGX_PREDICATOR_MCL_MASK);
}


__inline static int _vgx_predicator_mod_is_accumulator( const vgx_predicator_t predicator ) {
  // return true if predicator modifier class is accumulator
  return !((predicator.data ^ __VGX_PREDICATOR_MCL_ACC) & __VGX_PREDICATOR_MCL_MASK);
}


__inline static int _vgx_predicator_mod_is_time( const vgx_predicator_t predicator ) {
  // return true if predicator modifier class is time
  return !((predicator.data ^ __VGX_PREDICATOR_MCL_TIM) & __VGX_PREDICATOR_MCL_MASK);
}


__inline static int _vgx_predicator_value_is_float( const vgx_predicator_t predicator ) {
  return (__VGX_PREDICATOR_FLT_MASK & predicator.data) != 0;
}


__inline static float _vgx_predicator_get_value_as_float( const vgx_predicator_t predicator ) {
  if( __VGX_PREDICATOR_FLT_MASK & predicator.data ) {
    return predicator.val.real;
  }
  else {
    return (float)predicator.val.integer;
  }
}


__inline static int _vgx_predicator_data_match( const vgx_predicator_t p1, const vgx_predicator_t p2 ) {
  return ((p1.data ^ p2.data) & __VGX_PREDICATOR_DAT_MASK) == 0;
}


__inline static vgx_predicator_t _vgx_predicator_merge_inherit_key( const vgx_predicator_t probe_predicator, const vgx_predicator_t source ) {
  vgx_predicator_t merged = probe_predicator;
  merged.rkey = source.rkey;
  return merged;
}



__inline static vgx_predicator_val_type _vgx_predicator_value_range( void *min, void *max, const vgx_predicator_modifier_enum mod_enum ) {

  switch( ((int)mod_enum & _VGX_PREDICATOR_MOD_STO_MASK) ) {
  // NONE
  case VGX_PREDICATOR_MOD_NONE:
    return VGX_PREDICATOR_VAL_TYPE_NONE;

  // STATIC
  case VGX_PREDICATOR_MOD_STATIC:
    if(min) *((unsigned int*)min) = 1;
    if(max) *((unsigned int*)max) = 1;
    return VGX_PREDICATOR_VAL_TYPE_UNITY;

  // SIMILARITY (float) -1.0 - 1.0
  case VGX_PREDICATOR_MOD_SIMILARITY:
    if(min) *((float*)min) = -1.0F;
    if(max) *((float*)max) = 1.0F;
    return VGX_PREDICATOR_VAL_TYPE_REAL;

  // DISTANCE (float) 0.0 - 3.4e+38
  case VGX_PREDICATOR_MOD_DISTANCE:
    if(min) *((float*)min) = 0.0F;
    if(max) *((float*)max) = FLT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_REAL;

  // LSH (unsigned int) 0x00000000 - 0xffffffff
  case VGX_PREDICATOR_MOD_LSH:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  // INTEGER (int) -2147483648 - 2147483647
  case VGX_PREDICATOR_MOD_INTEGER:
    if(min) *((int*)min) = INT_MIN;
    if(max) *((int*)max) = INT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_INTEGER;

  // UNSIGNED (unsigned int) 0 - 4294967295
  case VGX_PREDICATOR_MOD_UNSIGNED:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  // FLOAT (float) -3.4e+38 - 3.4e+38
  case VGX_PREDICATOR_MOD_FLOAT:
    if(min) *((float*)min) = -FLT_MAX;
    if(max) *((float*)max) = FLT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_REAL;

  // COUNTER (unsigned int) 0 - 4294967295
  case VGX_PREDICATOR_MOD_COUNTER:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  // ACCUMULATOR (float) -3.4e+38 - 3.4e+38
  case VGX_PREDICATOR_MOD_ACCUMULATOR:
    if(min) *((float*)min) = -FLT_MAX;
    if(max) *((float*)max) = FLT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_REAL;

  // INTEGER AGGREGATOR (int) -2147483648 - 2147483647
  case VGX_PREDICATOR_MOD_INT_AGGREGATOR:
    if(min) *((int*)min) = INT_MIN;
    if(max) *((int*)max) = INT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_INTEGER;

  // FLOAT AGGREGATOR (float) -3.4e+38 - 3.4e+38
  case VGX_PREDICATOR_MOD_FLOAT_AGGREGATOR:
    if(min) *((float*)min) = -FLT_MAX;
    if(max) *((float*)max) = FLT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_REAL;

  // TIME CREATED (unsigned int) 0 - 4294967295
  case VGX_PREDICATOR_MOD_TIME_CREATED:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  // TIME MODIFIED (unsigned int) 0 - 4294967295
  case VGX_PREDICATOR_MOD_TIME_MODIFIED:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  // TIME EXPIRES (unsigned int) 0 - 4294967295
  case VGX_PREDICATOR_MOD_TIME_EXPIRES:
    if(min) *((unsigned int*)min) = 0;
    if(max) *((unsigned int*)max) = UINT_MAX;
    return VGX_PREDICATOR_VAL_TYPE_UNSIGNED;

  //
  default:
    return VGX_PREDICATOR_VAL_TYPE_ERROR;
  }
}


__inline static bool _vgx_modifier_is_valid( const vgx_predicator_modifier_enum modifier ) {
  if( _vgx_predicator_value_range( NULL, NULL, modifier ) == VGX_PREDICATOR_VAL_TYPE_ERROR ) {
    return false;
  }
  else {
    return true;
  }
}


__inline static int _vgx_predicator_value_is_accumulator( const vgx_predicator_t pred ) {
  return (pred.mod.bits & _VGX_PREDICATOR_MOD_CLS_MASK) == _VGX_PREDICATOR_MOD_CLS_ACC;
}


__inline static vgx_predicator_t _vgx_update_predicator_accumulator( vgx_predicator_t previous, vgx_predicator_t set ) {
  vgx_predicator_t updated = previous;
  if( (set.data & __VGX_PREDICATOR_FLT_MASK) ) {
    updated.val.real = previous.val.real + set.val.real;
  }
  else {
    int64_t newval = (int64_t)previous.val.uinteger + set.val.integer;
    updated.val.uinteger = !(newval & 0xFFFFFFFF00000000ULL) ? (uint32_t)newval : newval < 0 ? 0 : UINT32_MAX;
  }
  return updated;
}


__inline static vgx_predicator_t _vgx_update_predicator_value_if_accumulator( vgx_predicator_t previous, vgx_predicator_t set ) {
  if( _vgx_predicator_value_is_accumulator(set) ) {
    vgx_predicator_t updated = previous;
    if( (set.data & __VGX_PREDICATOR_FLT_MASK) ) {
      updated.val.real = previous.val.real + set.val.real;
    }
    else {
      int64_t newval = (int64_t)previous.val.uinteger + set.val.integer;
      updated.val.uinteger = !(newval & 0xFFFFFFFF00000000ULL) ? (uint32_t)newval : newval < 0 ? 0 : UINT32_MAX;
    }
    return updated;
  }
  else {
    return set;
  }
}





__inline static vgx_predicator_t _vgx_init_none_predicator( void ) {
  return VGX_PREDICATOR_NONE;
}


__inline static vgx_predicator_t _vgx_init_related_predicator( vgx_arc_direction dir ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_RELATED;
  predicator.rel.dir = dir;
  return predicator;
}


__inline static vgx_predicator_t _vgx_init_static_predicator( vgx_predicator_rel_enum enc, vgx_arc_direction dir ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_RELATED;
  predicator.rel.dir = dir;
  predicator.rel.enc = enc;
  return predicator;
}


__inline static vgx_predicator_t _vgx_init_similarity_predicator( float similarity, vgx_arc_direction dir ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_SIMILAR;
  predicator.val.real = similarity;
  predicator.rel.dir = dir;
  return predicator;
}


__inline static vgx_predicator_t _vgx_init_general_integer_predicator( vgx_predicator_rel_enum enc, vgx_arc_direction dir, int32_t value ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_INTEGER;
  predicator.val.integer = value;
  predicator.rel.dir = dir;
  predicator.rel.enc = enc;
  return predicator;
}


__inline static vgx_predicator_t _vgx_init_general_unsigned_predicator( vgx_predicator_rel_enum enc, vgx_arc_direction dir, uint32_t value ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_UNSIGNED;
  predicator.val.uinteger = value;
  predicator.rel.dir = dir;
  predicator.rel.enc = enc;
  return predicator;
}


__inline static vgx_predicator_t _vgx_init_general_real_predicator( vgx_predicator_rel_enum enc, vgx_arc_direction dir, float value ) {
  vgx_predicator_t predicator = VGX_PREDICATOR_FLOAT;
  predicator.val.real = value;
  predicator.rel.dir = dir;
  predicator.rel.enc = enc;
  return predicator;
}


__inline static int _vgx_predicator_full_wildcard( const vgx_predicator_t probe ) {
  // return true if predicator probe is a complete wildcard (i.e. no restrictions on REL or MOD)
  return (probe.data & __VGX_PREDICATOR_KEY_MASK) == __VGX_PREDICATOR_WILDCARD;
}


__inline static int _vgx_predicator_has_rel( const vgx_predicator_t probe ) {
  // return true if predicator specifies a relationship
  return (probe.data & __VGX_PREDICATOR_REL_MASK) != 0;
}


__inline static int _vgx_predicator_is_arcdir_both( const vgx_predicator_t probe ) {
  // return true if predicator specifies arcdirection to be bidirectional
  return probe.rel.dir == VGX_ARCDIR_BOTH;
}


__inline static int _vgx_predicator_has_mod( const vgx_predicator_t probe ) {
  // return true if predicator specifies a modifier
  return (probe.data & __VGX_PREDICATOR_SMT_MASK) != 0;
}


__inline static int _vgx_predicator_has_val( const vgx_predicator_t probe ) {
  // return true if predicator specifies a value
  return (probe.data & __VGX_PREDICATOR_EQU_MASK) != 0;
}




__inline static int _vgx_predicator_relationship_anymod_anyval( const vgx_predicator_t probe ) {
  // return true if predicator probe specifies a relationship with a wildcard modifier (i.e. restricted by REL but not by MOD)
  return _vgx_predicator_has_rel( probe )     // REL
         &&
         !_vgx_predicator_has_mod( probe )    // MOD=*
         &&
         !_vgx_predicator_has_val( probe );   // VAL=*
}



__inline static int _vgx_predicator_relationship_value_anymod( const vgx_predicator_t probe ) {
  // return true if predicator probe specifies a relationship and a value with a wildcard modifier (i.e. restricted by REL and VAL but not by MOD)
  return _vgx_predicator_has_rel( probe )     // REL
         &&
         !_vgx_predicator_has_mod( probe )    // MOD=*
         &&
         _vgx_predicator_has_val( probe );    // VAL
}



__inline static int _vgx_predicator_modifier_anyval_anyrel( const vgx_predicator_t probe ) {
  // return true if predicator probe specifies a modifier with a wildcard relationship (i.e. restricted by MOD but not by REL)
  return _vgx_predicator_has_mod( probe )     // MOD
         &&
         !_vgx_predicator_has_val( probe )    // VAL=*
         &&
         !_vgx_predicator_has_rel( probe );   // REL=*
}


__inline static int _vgx_predicator_modifier_value_anyrel( const vgx_predicator_t probe ) {
  // return true if predicator probe specifies a modifier and a value with a wildcard relationship (i.e. restricted by MOD and VAL but not by REL)
  return _vgx_predicator_has_mod( probe )     // MOD
         &&
         _vgx_predicator_has_val( probe )     // VAL
         &&
         !_vgx_predicator_has_rel( probe );   // REL=*
}



__inline static int _vgx_predicator_specific( const vgx_predicator_t probe ) {
  // return true if predicator probe specifies both relationship and modifier
  return _vgx_predicator_has_rel( probe )     // REL
         &&
         _vgx_predicator_has_mod( probe );    // MOD
}



__inline static int _vgx_predicator_specific_anyval( const vgx_predicator_t probe ) {
  // return true if predicator probe is fully specified with any value (i.e. no wildcard components except for the value)
  return _vgx_predicator_has_rel( probe )     // REL
         &&
         _vgx_predicator_has_mod( probe )     // MOD
         &&
         !_vgx_predicator_has_val( probe );   // VAL=*
}


__inline static int _vgx_predicator_specific_value( const vgx_predicator_t probe ) {
  // return true if predicator probe is fully specified (i.e. no wildcard components)
  return _vgx_predicator_has_rel( probe )     // REL
         &&
         _vgx_predicator_has_mod( probe )     // MOD
         &&
         _vgx_predicator_has_val( probe );    // VAL
}



__inline static int _vgx_predicator_is_sortable( const vgx_predicator_t probe ) {
  // return true if predicator probe defines a value-type, i.e. values of matching predicator will all be compatible and therefore sortable
  return (probe.data & __VGX_PREDICATOR_SMT_MASK) != __VGX_PREDICATOR_WILDCARD
         &&
         probe.mod.probe.type != VGX_PREDICATOR_MOD_STATIC;
}


__inline static vgx_predicator_modifier_enum _vgx_predicator_as_modifier_enum( const vgx_predicator_t pred ) {
  return (vgx_predicator_modifier_enum)(pred.mod.bits & _VGX_PREDICATOR_MOD_STO_MASK);
}


__inline static int _vgx_predicator_is_expiration( const vgx_predicator_t probe ) {
  return (probe.mod.bits & _VGX_PREDICATOR_MOD_STO_MASK) == VGX_PREDICATOR_MOD_TIME_EXPIRES;
}


__inline static int _vgx_predicator_is_expired( const vgx_predicator_t probe, uint32_t now_ts ) {
  return _vgx_predicator_is_expiration( probe ) && probe.val.uinteger <= now_ts;
}




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef uint8_t vgx_vertex_type_t;




/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_vgx_vertex_probe_spec {
  //
  // Vertex Type ------------------------------|
  // Vertex Degree ---------------------------||
  // Vertex Indegree ------------------------|||
  // Vertex Outdegree ----------------------||||
  // Vertex Fingerprint Hamming Distance --||||| 
  // Vertex Similarity -------------------||||||
  // Vertex Id Prefix -------------------||||||| 
  // Advanced--- -----------------------||||||||
  //                                    ||||||||
  //                                    VVVVVVVV
  VERTEX_PROBE_WILDCARD_MASK        = 0x11111111,
  VERTEX_PROBE_WILDCARD             = 0x00000000,

  __VTXPV_TEST_ENA                  = 0x00000001, // 0001

  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* VERTEX TYPE                      -------M            */
  /* */
  _VERTEX_PROBE_TYPE_OFFSET         = 0,
  _VERTEX_PROBE_TYPE_MASK           = VGX_VALUE_MASK << _VERTEX_PROBE_TYPE_OFFSET,          // 0000 0000 0000 0000 0000 0000 0000 1111
  _VERTEX_PROBE_TYPE_MASK_INV       = ~_VERTEX_PROBE_TYPE_MASK,                             // 1111 1111 1111 1111 1111 1111 1111 0000
  _VERTEX_PROBE_TYPE_ENA            = __VTXPV_TEST_ENA  << _VERTEX_PROBE_TYPE_OFFSET,       // ---- ---- ---- ---- ---- ---- ---- 0001
  _VERTEX_PROBE_TYPE_EQU            = VGX_VALUE_EQU  << _VERTEX_PROBE_TYPE_OFFSET,          // ---- ---- ---- ---- ---- ---- ---- 1100
  _VERTEX_PROBE_TYPE_NEQ            = VGX_VALUE_NEQ  << _VERTEX_PROBE_TYPE_OFFSET,          // ---- ---- ---- ---- ---- ---- ---- 1110


  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* DEGREE                           ------M-            */
  _VERTEX_PROBE_DEGREE_OFFSET       = 4,
  _VERTEX_PROBE_DEGREE_MASK         = VGX_VALUE_MASK << _VERTEX_PROBE_DEGREE_OFFSET,        // 0000 0000 0000 0000 0000 0000 1111 0000
  _VERTEX_PROBE_DEGREE_MASK_INV     = ~_VERTEX_PROBE_DEGREE_MASK,                           // 1111 1111 1111 1111 1111 1111 0000 1111
  _VERTEX_PROBE_DEGREE_ENA          = __VTXPV_TEST_ENA  << _VERTEX_PROBE_DEGREE_OFFSET,     // ---- ---- ---- ---- ---- ---- 0001 ----
  _VERTEX_PROBE_DEGREE_LTE          = VGX_VALUE_LTE  << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 0100 ----
  _VERTEX_PROBE_DEGREE_GTE          = VGX_VALUE_GTE  << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 1000 ----
  _VERTEX_PROBE_DEGREE_EQU          = VGX_VALUE_EQU  << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 1100 ----
  _VERTEX_PROBE_DEGREE_NEQ          = VGX_VALUE_NEQ  << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 1110 ----
  _VERTEX_PROBE_DEGREE_LT           = VGX_VALUE_LT   << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 1010 ----
  _VERTEX_PROBE_DEGREE_GT           = VGX_VALUE_GT   << _VERTEX_PROBE_DEGREE_OFFSET,        // ---- ---- ---- ---- ---- ---- 0110 ----

  /* INDEGREE                         -----M--            */
  _VERTEX_PROBE_INDEGREE_OFFSET     = 8,
  _VERTEX_PROBE_INDEGREE_MASK       = VGX_VALUE_MASK << _VERTEX_PROBE_INDEGREE_OFFSET,      // 0000 0000 0000 0000 0000 1111 0000 0000
  _VERTEX_PROBE_INDEGREE_MASK_INV   = ~_VERTEX_PROBE_INDEGREE_MASK,                         // 1111 1111 1111 1111 1111 0000 1111 1111
  _VERTEX_PROBE_INDEGREE_ENA        = __VTXPV_TEST_ENA  << _VERTEX_PROBE_INDEGREE_OFFSET,   // ---- ---- ---- ---- ---- 0001 ---- ----
  _VERTEX_PROBE_INDEGREE_LTE        = VGX_VALUE_LTE  << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 0100 ---- ----
  _VERTEX_PROBE_INDEGREE_GTE        = VGX_VALUE_GTE  << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 1000 ---- ----
  _VERTEX_PROBE_INDEGREE_EQU        = VGX_VALUE_EQU  << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 1100 ---- ----
  _VERTEX_PROBE_INDEGREE_NEQ        = VGX_VALUE_NEQ  << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 1110 ---- ----
  _VERTEX_PROBE_INDEGREE_LT         = VGX_VALUE_LT   << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 1010 ---- ----
  _VERTEX_PROBE_INDEGREE_GT         = VGX_VALUE_GT   << _VERTEX_PROBE_INDEGREE_OFFSET,      // ---- ---- ---- ---- ---- 0110 ---- ----

  /* OUTDEGREE                        ----M---            */
  _VERTEX_PROBE_OUTDEGREE_OFFSET    = 12,
  _VERTEX_PROBE_OUTDEGREE_MASK      = VGX_VALUE_MASK << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // 0000 0000 0000 0000 1111 0000 0000 0000
  _VERTEX_PROBE_OUTDEGREE_MASK_INV  = ~_VERTEX_PROBE_OUTDEGREE_MASK,                        // 1111 1111 1111 1111 0000 1111 1111 1111
  _VERTEX_PROBE_OUTDEGREE_ENA       = __VTXPV_TEST_ENA  << _VERTEX_PROBE_OUTDEGREE_OFFSET,  // ---- ---- ---- ---- 0001 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_LTE       = VGX_VALUE_LTE  << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 0100 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_GTE       = VGX_VALUE_GTE  << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 1000 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_EQU       = VGX_VALUE_EQU  << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 1100 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_NEQ       = VGX_VALUE_NEQ  << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 1110 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_LT        = VGX_VALUE_LT   << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 1010 ---- ---- ----
  _VERTEX_PROBE_OUTDEGREE_GT        = VGX_VALUE_GT   << _VERTEX_PROBE_OUTDEGREE_OFFSET,     // ---- ---- ---- ---- 0110 ---- ---- ----

  /* any degree enabled mask */
  _VERTEX_PROBE_ANY_DEGREE_ENA      = _VERTEX_PROBE_DEGREE_ENA | _VERTEX_PROBE_INDEGREE_ENA | _VERTEX_PROBE_OUTDEGREE_ENA,
                                                                                            // 0000 0000 0000 0000 1111 1111 1111 0000
  _VERTEX_PROBE_ANY_DEGREE_MASK     = _VERTEX_PROBE_DEGREE_MASK | _VERTEX_PROBE_INDEGREE_MASK | _VERTEX_PROBE_OUTDEGREE_MASK,
  _VERTEX_PROBE_ANY_DEGREE_MASK_INV = ~_VERTEX_PROBE_ANY_DEGREE_MASK,                       // 1111 1111 1111 1111 0000 0000 0000 1111


 
  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* ** RESERVED1 **                  --M-----           */
  _VERTEX_PROBE__rsv1__OFFSET       = 16,
  _VERTEX_PROBE__rsv1__MASK         = VGX_VALUE_MASK << _VERTEX_PROBE__rsv1__OFFSET,        // 0000 0000 0000 1111 0000 0000 0000 0000
  _VERTEX_PROBE__rsv1__MASK_INV     = ~_VERTEX_PROBE__rsv1__MASK,                           // 1111 1111 1111 0000 1111 1111 1111 1111
  _VERTEX_PROBE__rsv1__ENA          = __VTXPV_TEST_ENA  << _VERTEX_PROBE__rsv1__OFFSET,     // ---- ---- ---- 0001 ---- ---- ---- ----
 


  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* ** RESERVED2 **                  --M-----           */
  _VERTEX_PROBE_IDLIST_OFFSET       = 20,
  _VERTEX_PROBE_IDLIST_MASK         = VGX_VALUE_MASK << _VERTEX_PROBE_IDLIST_OFFSET,        // 0000 0000 1111 0000 0000 0000 0000 0000
  _VERTEX_PROBE_IDLIST_MASK_INV     = ~_VERTEX_PROBE_IDLIST_MASK,                           // 1111 1111 0000 1111 1111 1111 1111 1111
  _VERTEX_PROBE_IDLIST_ENA          = __VTXPV_TEST_ENA  << _VERTEX_PROBE_IDLIST_OFFSET,     // ---- ---- 0001 ---- ---- ---- ---- ----



  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* VERTEX ID                       -M------            */
  _VERTEX_PROBE_ID_OFFSET           = 24,
  _VERTEX_PROBE_ID_MASK             = VGX_VALUE_MASK << _VERTEX_PROBE_ID_OFFSET,            // 0000 1111 0000 0000 0000 0000 0000 0000
  _VERTEX_PROBE_ID_MASK_INV         = ~_VERTEX_PROBE_ID_MASK,                               // 1111 0000 1111 1111 1111 1111 1111 1111
  _VERTEX_PROBE_ID_ENA              = __VTXPV_TEST_ENA  << _VERTEX_PROBE_ID_OFFSET,         // ---- 0001 ---- ---- ---- ---- ---- ----
  _VERTEX_PROBE_ID_LTE              = VGX_VALUE_LTE  << _VERTEX_PROBE_ID_OFFSET,            // ---- 0100 ---- ---- ---- ---- ---- ---- ( ==prefix )
  _VERTEX_PROBE_ID_EQU              = VGX_VALUE_EQU  << _VERTEX_PROBE_ID_OFFSET,            // ---- 1100 ---- ---- ---- ---- ---- ---- ( ==obid )
  _VERTEX_PROBE_ID_NEQ              = VGX_VALUE_NEQ  << _VERTEX_PROBE_ID_OFFSET,            // ---- 1110 ---- ---- ---- ---- ---- ---- ( != obid )
  _VERTEX_PROBE_ID_GT               = VGX_VALUE_GT   << _VERTEX_PROBE_ID_OFFSET,            // ---- 0110 ---- ---- ---- ---- ---- ---- ( != prefix )
  _VERTEX_PROBE_ID_EXACT            = _VERTEX_PROBE_ID_EQU | _VERTEX_PROBE_ID_ENA,          // 0000 1101 0000 0000 0000 0000 0000 0000



  /* --------------------------------------------------------------------------------------------------------------------------------- */
  /* ADVANCED PROBE CONDITIONS        M-------            */
  _VERTEX_PROBE_ADVANCED_OFFSET     = 28,
  _VERTEX_PROBE_ADVANCED_MASK       = VGX_VALUE_MASK << _VERTEX_PROBE_ADVANCED_OFFSET,      // 1111 0000 0000 0000 0000 0000 0000 0000
  _VERTEX_PROBE_ADVANCED_MASK_INV   = ~_VERTEX_PROBE_ADVANCED_MASK,                         // 0000 1111 1111 1111 1111 1111 1111 1111
  _VERTEX_PROBE_ADVANCED_ENA        = __VTXPV_TEST_ENA  << _VERTEX_PROBE_ADVANCED_OFFSET,   // 0001 ---- ---- ---- ---- ---- ---- ----
  /*  When advanced conditions enabled, a secondary level of vertex condition function(s) will be
      called for each candidate.
  */

  _VERTEX_PROBE_SIMPLE_MASK         = 0x0FFFFFFF,

  _VERTEX_PROBE_ANY_ENA             = _VERTEX_PROBE_TYPE_ENA   | _VERTEX_PROBE_ANY_DEGREE_ENA | 
                                      _VERTEX_PROBE__rsv1__ENA | _VERTEX_PROBE_IDLIST_ENA     |
                                      _VERTEX_PROBE_ID_ENA     | _VERTEX_PROBE_ADVANCED_ENA

} vgx_vertex_probe_spec;



__inline static void __append_string( char **dest, const char *src ) {
  while( *src && (*(*dest)++ = *src++) != '\0' );
}

__inline static char * _vgx_probe_spec_as_new_string( const vgx_vertex_probe_spec spec ) {
#define APPEND( Source )        __append_string( &dest, Source )
#define VCOMP( ISpec, Offset )  _vgx_vcomp_as_string( (vgx_value_comparison) (((ISpec) >> (Offset)) & VGX_VALUE_MASK) )

  char *buf = (char *) calloc( 256, 1 );
  if( buf ) {
    char *dest = buf;
    int ispec = (int)spec;

    if( ispec & _VERTEX_PROBE_ADVANCED_ENA ) {
      APPEND( "(ADV " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_ADVANCED_OFFSET ) );
      APPEND( ") " );
    }

    if( ispec & _VERTEX_PROBE_ID_ENA ) {
      APPEND( "(ID " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_ID_OFFSET ) );
      APPEND( ") " );
    }

    if( ispec & _VERTEX_PROBE_OUTDEGREE_ENA ) {
      APPEND( "(ODEG " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_OUTDEGREE_OFFSET ) );
      APPEND( ") " );
    }

    if( ispec & _VERTEX_PROBE_INDEGREE_ENA ) {
      APPEND( "(IDEG " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_INDEGREE_OFFSET ) );
      APPEND( ") " );
    }

    if( ispec & _VERTEX_PROBE_DEGREE_ENA ) {
      APPEND( "(DEG " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_DEGREE_OFFSET ) );
      APPEND( ") " );
    }

    if( ispec & _VERTEX_PROBE_TYPE_ENA ) {
      APPEND( "(TYPE " );
      APPEND( VCOMP( ispec, _VERTEX_PROBE_TYPE_OFFSET ) );
      APPEND( ") " );
    }
  }
  // WARNING!!! CALLER WILL OWN MEMORY
  return buf;
}



/*******************************************************************//**
 *
 *
 *
 ***********************************************************************
 */
typedef enum e_vgx_sortspec_t {
  VGX_SORTBY_NONE                    = 0x0000, //  0000 0000 0000 0000

  //                                   -----D      ---- ---- ---- --DD
  _VGX_SORT_DIRECTION_MASK           = 0x0003, //  0000 0000 0000 0011
  VGX_SORT_DIRECTION_UNSPECIFIED     = 0x0000, //  0000 0000 0000 0000
  VGX_SORT_DIRECTION_ASCENDING       = 0x0001, //  0000 0000 0000 0001
  VGX_SORT_DIRECTION_DESCENDING      = 0x0002, //  0000 0000 0000 0010
  VGX_SORT_DIRECTION_INVALID         = 0x0003, //  0000 0000 0000 0011

  //                                   ---SS-      ---- SSSS SSSS ----
  //                            (deref)       D
  //                           (no deref)     n
  _VGX_SORTBY_MASK                   = 0x0FF0, //  0000 1111 1111 0000
  __VGX_SORTBY_FIRST                 = 0x0010, //  0000 0000 0001 0000
  //                                                            v<------ special, only 1 for predicator sort
  VGX_SORTBY_PREDICATOR              = 0x0010, //  0000 0000 0001 0000
  VGX_SORTBY_MEMADDRESS              = 0x0020, //  0000 0000 0010 0000
  VGX_SORTBY_ANCHOR_OBID             = 0x0040, //  0000 0000 0100 0000
  VGX_SORTBY_ANCHOR_ID               = 0x0060, //  0000 0000 0110 0000
  VGX_SORTBY_NATIVE                  = 0x0080, //  0000 0000 1000 0000
  VGX_SORTBY_RANDOM                  = 0x00A0, //  0000 0000 1010 0000
  VGX_SORTBY_RANKING                 = 0x00C0, //  0000 0000 1100 0000 (note: no deref by default, but deref may still occur after analysis)
  VGX_SORTBY_INTERNALID              = 0x0100, //  0000 0001 0000 0000
  VGX_SORTBY_IDSTRING                = 0x0200, //  0000 0010 0000 0000
  VGX_SORTBY_DEGREE                  = 0x0300, //  0000 0011 0000 0000
  VGX_SORTBY_INDEGREE                = 0x0400, //  0000 0100 0000 0000
  VGX_SORTBY_OUTDEGREE               = 0x0500, //  0000 0101 0000 0000
  VGX_SORTBY_HAMDIST                 = 0x0600, //  0000 0110 0000 0000
  VGX_SORTBY_SIMSCORE                = 0x0700, //  0000 0111 0000 0000
  VGX_SORTBY_TMC                     = 0x0800, //  0000 1000 0000 0000
  VGX_SORTBY_TMM                     = 0x0900, //  0000 1001 0000 0000
  VGX_SORTBY_TMX                     = 0x0A00, //  0000 1010 0000 0000
  __VGX_SORTBY_LAST                  = 0x0F00, //  0000 1111 0000 0000

  //                                   --A---  //  ---A ---- ---- ----
  _VGX_AGGREGATE_MASK                = 0xF000, //  1111 0000 0000 0000
  VGX_AGGREGATE_ARC_FIRST_VALUE      = 0x1000, //  0001 0000 0000 0000
  VGX_AGGREGATE_ARC_MAX_VALUE        = 0x2000, //  0010 0000 0000 0000
  VGX_AGGREGATE_ARC_MIN_VALUE        = 0x3000, //  0011 0000 0000 0000
  VGX_AGGREGATE_ARC_AVERAGE_VALUE    = 0x4000, //  0100 0000 0000 0000
  VGX_AGGREGATE_ARC_COUNT            = 0x6000, //  0110 0000 0000 0000
  VGX_AGGREGATE_ARC_ADD_VALUES       = 0x8000, //  1000 0000 0000 0000
  VGX_AGGREGATE_ARC_ADD_SQ_VALUES    = 0x9000, //  1001 0000 0000 0000
  VGX_AGGREGATE_ARC_MULTIPLY_VALUES  = 0xA000, //  1010 0000 0000 0000

  //
  _VGX_SORTBY_DEREF_MASK             = 0x0F00, //  0000 1111 0000 0000

  // 
  //
  _VGX_SORTBY_PREDICATOR_ASCENDING   = VGX_SORTBY_PREDICATOR  | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_PREDICATOR_DESCENDING  = VGX_SORTBY_PREDICATOR  | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_MEMADDRESS_ASCENDING   = VGX_SORTBY_MEMADDRESS  | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_MEMADDRESS_DESCENDING  = VGX_SORTBY_MEMADDRESS  | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_INTERNALID_ASCENDING   = VGX_SORTBY_INTERNALID  | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_INTERNALID_DESCENDING  = VGX_SORTBY_INTERNALID  | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_ANCHOR_OBID_ASCENDING  = VGX_SORTBY_ANCHOR_OBID | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_ANCHOR_OBID_DESCENDING = VGX_SORTBY_ANCHOR_OBID | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_IDSTRING_ASCENDING     = VGX_SORTBY_IDSTRING    | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_IDSTRING_DESCENDING    = VGX_SORTBY_IDSTRING    | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_ANCHOR_ID_ASCENDING    = VGX_SORTBY_ANCHOR_ID   | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_ANCHOR_ID_DESCENDING   = VGX_SORTBY_ANCHOR_ID   | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_DEGREE_ASCENDING       = VGX_SORTBY_DEGREE      | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_DEGREE_DESCENDING      = VGX_SORTBY_DEGREE      | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_INDEGREE_ASCENDING     = VGX_SORTBY_INDEGREE    | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_INDEGREE_DESCENDING    = VGX_SORTBY_INDEGREE    | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_OUTDEGREE_ASCENDING    = VGX_SORTBY_OUTDEGREE   | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_OUTDEGREE_DESCENDING   = VGX_SORTBY_OUTDEGREE   | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_HAMDIST_ASCENDING      = VGX_SORTBY_HAMDIST     | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_HAMDIST_DESCENDING     = VGX_SORTBY_HAMDIST     | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_SIMSCORE_ASCENDING     = VGX_SORTBY_SIMSCORE    | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_SIMSCORE_DESCENDING    = VGX_SORTBY_SIMSCORE    | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_RANKING_ASCENDING      = VGX_SORTBY_RANKING     | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_RANKING_DESCENDING     = VGX_SORTBY_RANKING     | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_TMC_ASCENDING          = VGX_SORTBY_TMC         | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_TMC_DESCENDING         = VGX_SORTBY_TMC         | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_TMM_ASCENDING          = VGX_SORTBY_TMM         | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_TMM_DESCENDING         = VGX_SORTBY_TMM         | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_TMX_ASCENDING          = VGX_SORTBY_TMX         | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_TMX_DESCENDING         = VGX_SORTBY_TMX         | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_NATIVE_ASCENDING       = VGX_SORTBY_NATIVE      | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_NATIVE_DESCENDING      = VGX_SORTBY_NATIVE      | VGX_SORT_DIRECTION_DESCENDING,
  _VGX_SORTBY_RANDOM_ASCENDING       = VGX_SORTBY_RANDOM      | VGX_SORT_DIRECTION_ASCENDING,
  _VGX_SORTBY_RADNOM_DESCENDING      = VGX_SORTBY_RANDOM      | VGX_SORT_DIRECTION_DESCENDING

} vgx_sortspec_t;



__inline static vgx_sortspec_t _vgx_sort_direction( const vgx_sortspec_t spec ) {
  return (vgx_sortspec_t)((int)spec & _VGX_SORT_DIRECTION_MASK);
}

__inline static vgx_sortspec_t _vgx_sortby( const vgx_sortspec_t spec ) {
  return (vgx_sortspec_t)((int)spec & _VGX_SORTBY_MASK);
}

__inline static vgx_sortspec_t _vgx_aggregate( const vgx_sortspec_t spec ) {
  return (vgx_sortspec_t)((int)spec & _VGX_AGGREGATE_MASK);
}


__inline static vgx_sortspec_t _vgx_set_sort_direction( vgx_sortspec_t *sortspec ) {

  // Set default sort direction if not specified
  if( *sortspec != VGX_SORTBY_NONE && _vgx_sort_direction( *sortspec ) == VGX_SORT_DIRECTION_UNSPECIFIED ) {
    // Certain numeric values are sorted descending by default
    switch( _vgx_sortby( *sortspec ) ) {
    case VGX_SORTBY_PREDICATOR:
      /* FALLTHRU */
    case VGX_SORTBY_DEGREE:
      /* FALLTHRU */
    case VGX_SORTBY_INDEGREE:
      /* FALLTHRU */
    case VGX_SORTBY_OUTDEGREE:
      /* FALLTHRU */
    case VGX_SORTBY_SIMSCORE:
      /* FALLTHRU */
    case VGX_SORTBY_RANKING:
      *sortspec |= VGX_SORT_DIRECTION_DESCENDING;
      break;

    // All others are sorted ascending by default
    default:
      *sortspec |= VGX_SORT_DIRECTION_ASCENDING;
    }
  }

  return *sortspec;
}



__inline static int _vgx_sortspec_valid( vgx_sortspec_t spec ) {
  if( _vgx_sortby( spec ) ) {
    if( _vgx_sort_direction( spec ) == VGX_SORT_DIRECTION_INVALID ) {
      return 0;
    }
    switch( _vgx_sortby( spec ) ) {
    case VGX_SORTBY_PREDICATOR:
    case VGX_SORTBY_MEMADDRESS:
    case VGX_SORTBY_ANCHOR_OBID:
    case VGX_SORTBY_ANCHOR_ID:
    case VGX_SORTBY_NATIVE:
    case VGX_SORTBY_RANDOM:
    case VGX_SORTBY_RANKING:
    case VGX_SORTBY_INTERNALID:
    case VGX_SORTBY_IDSTRING:
    case VGX_SORTBY_DEGREE:
    case VGX_SORTBY_INDEGREE:
    case VGX_SORTBY_OUTDEGREE:
    case VGX_SORTBY_HAMDIST:
    case VGX_SORTBY_SIMSCORE:
    case VGX_SORTBY_TMC:
    case VGX_SORTBY_TMM:
    case VGX_SORTBY_TMX:
      /* FALLTHRU */
      break;
    default:
      return 0;
    }
  }

  if( _vgx_aggregate( spec ) ) {
    vgx_sortspec_t aspec = _vgx_aggregate( spec );
    switch( aspec ) {
    case VGX_AGGREGATE_ARC_FIRST_VALUE:
    case VGX_AGGREGATE_ARC_MAX_VALUE:
    case VGX_AGGREGATE_ARC_MIN_VALUE:
    case VGX_AGGREGATE_ARC_AVERAGE_VALUE:
    case VGX_AGGREGATE_ARC_COUNT:
    case VGX_AGGREGATE_ARC_ADD_VALUES:
    case VGX_AGGREGATE_ARC_ADD_SQ_VALUES:
    case VGX_AGGREGATE_ARC_MULTIPLY_VALUES:
      /* FALLTHRU */
      break; // ok
    default:
      return 0;
    }
  }

  if( spec != VGX_SORTBY_NONE && (((_VGX_SORTBY_MASK | _VGX_AGGREGATE_MASK) & spec) == 0) ) {
    return 0;
  }


  return 1;
}



__inline static int _vgx_sortspec_numeric( vgx_sortspec_t spec ) {
  switch( _vgx_sortby( spec ) ) {
  case VGX_SORTBY_PREDICATOR:
  case VGX_SORTBY_MEMADDRESS:
  case VGX_SORTBY_RANKING:
  case VGX_SORTBY_DEGREE:
  case VGX_SORTBY_INDEGREE:
  case VGX_SORTBY_OUTDEGREE:
  case VGX_SORTBY_HAMDIST:
  case VGX_SORTBY_SIMSCORE:
  case VGX_SORTBY_TMC:
  case VGX_SORTBY_TMM:
  case VGX_SORTBY_TMX:
    return 1;
  default:
    return 0;
  }
}



__inline static int _vgx_sortspec_integer( vgx_sortspec_t spec ) {
  switch( _vgx_sortby( spec ) ) {
  case VGX_SORTBY_PREDICATOR:
  case VGX_SORTBY_MEMADDRESS:
  case VGX_SORTBY_RANKING:
  case VGX_SORTBY_DEGREE:
  case VGX_SORTBY_INDEGREE:
  case VGX_SORTBY_OUTDEGREE:
  case VGX_SORTBY_HAMDIST:
  case VGX_SORTBY_SIMSCORE:
  case VGX_SORTBY_TMC:
  case VGX_SORTBY_TMM:
  case VGX_SORTBY_TMX:
    return 1;
  default:
    return 0;
  }
}




__inline static int _vgx_sortspec_string( vgx_sortspec_t spec ) {
  switch( _vgx_sortby( spec ) ) {
    case VGX_SORTBY_ANCHOR_OBID:
    case VGX_SORTBY_ANCHOR_ID:
    case VGX_SORTBY_INTERNALID:
    case VGX_SORTBY_IDSTRING:
    return 1;
  default:
    return 0;
  }
}



__inline static int _vgx_sortspec_dontcare( vgx_sortspec_t spec ) {
  switch( _vgx_sortby( spec ) ) {
  case VGX_SORTBY_NATIVE:
  case VGX_SORTBY_RANDOM:
    return 1;
  default:
    return 0;
  }
}





static const char *__reverse_sortspec_map[] = {
  "S_NONE",         // 0x00
  "S_VAL",          // 0x01
  "S_ADDR",         // 0x02
  "S_???",          // 0x03
  "S_ANCHOR_OBID",  // 0x04
  "S_???",          // 0x05
  "S_ANCHOR",       // 0x06
  "S_???",          // 0x07
  "S_NATIVE",       // 0x08
  "S_???",          // 0x09
  "S_RANDOM",       // 0x0A
  "S_???",          // 0x0B
  "S_RANK",         // 0x0C
  "S_???",          // 0x0D
  "S_???",          // 0x0E
  "S_???",          // 0x0F
  "S_???",          // 0x10
  "S_OBID",         // 0x11
  "S_ID",           // 0x12
  "S_DEG",          // 0x13
  "S_IDEG",         // 0x14
  "S_ODEG",         // 0x15
  "S_HAM",          // 0x16
  "S_SIM",          // 0x17
  "S_TMC",          // 0x18
  "S_TMM",          // 0x19
  "S_TMX",          // 0x1A
  "S_???",          // 0x1B
  "S_???",          // 0x1C
  "S_???",          // 0x1D
  "S_???",          // 0x1E
  "S_???"           // 0x1F
};


__inline static const char * _vgx_sortspec_as_string( const vgx_sortspec_t sortby ) {
  int code = ((int)(sortby & _VGX_SORTBY_MASK)) >> 4;
  int idx = 0;
  // low
  if( code & 0x0F ) {
    idx = code;
  }
  // high
  else if( code & 0xF0 ) {
    idx = 0x10 | (code >> 4);
  }
  
  return __reverse_sortspec_map[ idx & 0x1F ];
}


static const char *__reverse_sortspec_direction_map[] = {
  "",       // 0x00
  "S_ASC",  // 0x01
  "S_DESC", // 0x02
  "S_???"   // 0x03
};


__inline static const char * _vgx_sort_direction_as_string( const vgx_sortspec_t sortby ) {
  return __reverse_sortspec_direction_map[ (int)_vgx_sort_direction( sortby ) & 0x3 ];
}



__inline static int _vgx_vertex_condition_has_vertextype( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_TYPE_ENA;
}

__inline static int _vgx_vertex_condition_has_degree( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_DEGREE_ENA;
}

__inline static int _vgx_vertex_condition_has_indegree( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_INDEGREE_ENA;
}

__inline static int _vgx_vertex_condition_has_outdegree( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_OUTDEGREE_ENA;
}

__inline static int _vgx_vertex_condition_has_any_degree( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_ANY_DEGREE_ENA;
}

__inline static int _vgx_vertex_condition_has_id( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_ID_ENA;
}

__inline static int _vgx_vertex_condition_has_id_exact( const vgx_vertex_probe_spec spec ) {
  // True if spec contains == or != for exact ID
  return (spec & _VERTEX_PROBE_ID_ENA) && ((spec & _VERTEX_PROBE_ID_EQU) == _VERTEX_PROBE_ID_EQU);
}

__inline static int _vgx_vertex_condition_has_id_prefix( const vgx_vertex_probe_spec spec ) {
  // True if spec contains == or != for ID prefix
  return (spec & _VERTEX_PROBE_ID_ENA) && ((spec & _VERTEX_PROBE_ID_EQU) == _VERTEX_PROBE_ID_LTE);
}

__inline static int _vgx_vertex_condition_has_advanced( const vgx_vertex_probe_spec spec ) {
  return spec & _VERTEX_PROBE_ADVANCED_ENA;
}

/*******************************************************************//**
 * Non-zero if the vertex probe spec includes a condition for exact match on obid
 ***********************************************************************
 */
__inline static int _vgx_vertex_condition_has_obid_match( vgx_vertex_probe_spec spec ) {
  return (spec & _VERTEX_PROBE_ID_MASK) == (_VERTEX_PROBE_ID_ENA | _VERTEX_PROBE_ID_EQU);
}

/*******************************************************************//**
 * Non-zero if the vertex probe spec includes a condition for NOT exact match on obid
 ***********************************************************************
 */
__inline static int _vgx_vertex_condition_has_obid_not_match( vgx_vertex_probe_spec spec ) {
  return (spec & _VERTEX_PROBE_ID_MASK) == (_VERTEX_PROBE_ID_ENA | _VERTEX_PROBE_ID_NEQ);
}


__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_vertextype( vgx_vertex_probe_spec *spec, const vgx_value_comparison value_test ) {
  int ret = ((int)value_test << (int)_VERTEX_PROBE_TYPE_OFFSET) | (int)_VERTEX_PROBE_TYPE_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_degree( vgx_vertex_probe_spec *spec, const vgx_value_comparison value_test ) {
  int ret = ((int)value_test << (int)_VERTEX_PROBE_DEGREE_OFFSET) | (int)_VERTEX_PROBE_DEGREE_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_indegree( vgx_vertex_probe_spec *spec, const vgx_value_comparison value_test ) {
  int ret = ((int)value_test << (int)_VERTEX_PROBE_INDEGREE_OFFSET) | (int)_VERTEX_PROBE_INDEGREE_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_outdegree( vgx_vertex_probe_spec *spec, const vgx_value_comparison value_test ) {
  int ret = ((int)value_test << (int)_VERTEX_PROBE_OUTDEGREE_OFFSET) | (int)_VERTEX_PROBE_OUTDEGREE_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_id( vgx_vertex_probe_spec *spec, const vgx_value_comparison value_test ) {
  int ret = ((int)value_test << (int)_VERTEX_PROBE_ID_OFFSET) | (int)_VERTEX_PROBE_ID_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_idlist( vgx_vertex_probe_spec *spec ) {
  static const vgx_value_comparison equ = VGX_VALUE_EQU;
  _vgx_vertex_condition_add_id( spec, equ );
  int ret = ((int)equ << (int)_VERTEX_PROBE_IDLIST_OFFSET) | (int)_VERTEX_PROBE_IDLIST_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}

__inline static vgx_vertex_probe_spec _vgx_vertex_condition_add_advanced( vgx_vertex_probe_spec *spec ) {
  int ret = ((int)VGX_VALUE_ANY << (int)_VERTEX_PROBE_ADVANCED_OFFSET) | (int)_VERTEX_PROBE_ADVANCED_ENA | (int)*spec;
  *spec = (vgx_vertex_probe_spec)ret;
  return *spec;
}




__inline static void _vgx_vertex_condition_clear_vertextype( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_TYPE_MASK | *spec) ^ _VERTEX_PROBE_TYPE_MASK);
}

__inline static void _vgx_vertex_condition_clear_degree( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_DEGREE_MASK | *spec) ^ _VERTEX_PROBE_DEGREE_MASK);
}

__inline static void _vgx_vertex_condition_clear_indegree( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_INDEGREE_MASK | *spec) ^ _VERTEX_PROBE_INDEGREE_MASK);
}

__inline static void _vgx_vertex_condition_clear_outdegree( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_OUTDEGREE_MASK | *spec) ^ _VERTEX_PROBE_OUTDEGREE_MASK);
}

__inline static void _vgx_vertex_condition_clear_id( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_IDLIST_MASK | *spec) ^ _VERTEX_PROBE_IDLIST_MASK);
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_ID_MASK | *spec) ^ _VERTEX_PROBE_ID_MASK);
}

__inline static void _vgx_vertex_condition_clear_advanced( vgx_vertex_probe_spec *spec ) {
  *spec = (vgx_vertex_probe_spec)((_VERTEX_PROBE_ADVANCED_MASK | *spec) ^ _VERTEX_PROBE_ADVANCED_MASK);
}



/*******************************************************************//**
 * vgx_VertexAccessMode_t
 * 
 ***********************************************************************
 */
typedef enum e_vgx_VertexAccessMode_t {
  VGX_VERTEX_ACCESS_UNSPECIFIED,
  VGX_VERTEX_ACCESS_WRITABLE,
  VGX_VERTEX_ACCESS_WRITABLE_NOCREATE,
  VGX_VERTEX_ACCESS_READONLY
} vgx_VertexAccessMode_t;



/*******************************************************************//**
 * vgx_AccessReason_t
 * 
 ***********************************************************************
 */
typedef enum e_vgx_AccessReason_t {
  __VGX_ACCESS_REASON_MASK             = 0x0F0,
  __VGX_ACCESS_REASON_CATMASK          = 0xF00,
  
  // ------------
  // NO CATEGEORY
  // ------------
  __VGX_ACCESS_REASON_NOCATS           = 0x000,

  // NO REASON
  VGX_ACCESS_REASON_NONE               = 0x000,

  // LOOKUP REASONS
  __VGX_ACCESS_REASON_LOOKUP           = 0x040,
  VGX_ACCESS_REASON_NOEXIST            = 0x040,
  VGX_ACCESS_REASON_NOEXIST_MSG        = 0x041,

  // CREATION REASONS
  __VGX_ACCESS_REASON_CREATION         = 0x080,
  VGX_ACCESS_REASON_NOCREATE           = 0x080,
  
  // TRANSIENT ACCESS REASONS
  __VGX_ACCESS_REASON_TRANSIENT        = 0x090,
  VGX_ACCESS_REASON_LOCKED             = 0x098,  // Failed to acquire (no timeout)
  VGX_ACCESS_REASON_TIMEOUT            = 0x099,  // Failed to acquire after timeout
  VGX_ACCESS_REASON_OPFAIL             = 0x09A,  // Failed to open operation
  VGX_ACCESS_REASON_EXECUTION_TIMEOUT  = 0x09E,  // Long running execution timed out

  // VERTEX REASONS
  __VGX_ACCESS_REASON_VERTEX           = 0x0D0,
  VGX_ACCESS_REASON_NO_VERTEX_VECTOR   = 0x0D0,  // Vertex has no vector
  VGX_ACCESS_REASON_VERTEX_ARC_ERROR   = 0x0DA,  // Arc error


  // ---------------
  // SUCCESS REASONS
  // ---------------
  __VGX_ACCESS_REASON_SUCCESS          = 0x100,
  VGX_ACCESS_REASON_OBJECT_ACQUIRED    = 0x101,  // Success
  VGX_ACCESS_REASON_OBJECT_CREATED     = 0x102,

  // -------------------
  // GRAPH STATE REASONS
  // -------------------
  __VGX_ACCESS_REASON_READONLY         = 0x300,
  VGX_ACCESS_REASON_READONLY_GRAPH     = 0x311,  // Graph is readonly
  VGX_ACCESS_REASON_READONLY_PENDING   = 0x312,  // Graph readonly request is pending
  
  // -------------
  // ERROR REASONS
  // -------------
  __VGX_ACCESS_REASON_ERRORS           = 0xE00,
  VGX_ACCESS_REASON_INVALID            = 0xE01,
  VGX_ACCESS_REASON_ENUM_NOTYPESPACE   = 0xE02,
  VGX_ACCESS_REASON_TYPEMISMATCH       = 0xE03,
  VGX_ACCESS_REASON_SEMAPHORE          = 0xE04,
  VGX_ACCESS_REASON_BAD_CONTEXT        = 0xE05,  // Access request is not valid for current vertex lock state
  VGX_ACCESS_REASON_RO_DISALLOWED      = 0xE06,  // Access request is denied when readonly vertices are held
  VGX_ACCESS_REASON_ERROR              = 0xEEE,
} vgx_AccessReason_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static void __set_access_reason( vgx_AccessReason_t *dest, vgx_AccessReason_t reason ) {
  if( dest ) {
    *dest = reason;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __has_access_reason( vgx_AccessReason_t *rptr ) {
  return rptr != NULL && *rptr != VGX_ACCESS_REASON_NONE;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_noexist( vgx_AccessReason_t reason ) {
  return ((int)reason & __VGX_ACCESS_REASON_LOOKUP) == __VGX_ACCESS_REASON_LOOKUP;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_transient( vgx_AccessReason_t reason ) {
  return ((int)reason & __VGX_ACCESS_REASON_MASK) == __VGX_ACCESS_REASON_TRANSIENT;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_none_or_transient( vgx_AccessReason_t reason ) {
  return (reason == VGX_ACCESS_REASON_NONE) || (((int)reason & __VGX_ACCESS_REASON_MASK) == __VGX_ACCESS_REASON_TRANSIENT);
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_arc_error( vgx_AccessReason_t reason ) {
  return reason == VGX_ACCESS_REASON_VERTEX_ARC_ERROR;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_readonly( vgx_AccessReason_t reason ) {
  return  ((int)reason & __VGX_ACCESS_REASON_CATMASK) == __VGX_ACCESS_REASON_READONLY;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static int __is_access_reason_error( vgx_AccessReason_t reason ) {
  return  ((int)reason & __VGX_ACCESS_REASON_CATMASK) == __VGX_ACCESS_REASON_ERRORS;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */

typedef union u_vgx_ExecutionTimingBudgetFlags_t {
  uint64_t _bits;
  struct {
    int8_t is_halted;
    int8_t is_blocking;
    int8_t is_infinite;
    int8_t resource_blocked;
    int8_t is_exe_limited;
    int8_t is_exe_limit_exceeded;
    int8_t explicit_halt;
    int8_t _rsv8;
  };
} vgx_ExecutionTimingBudgetFlags_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_ExecutionTimingBudget_t {
  int64_t t_remain_ms;  // Milliseconds remaining for operation to allow executing
  int64_t t0_ms;        // Graph millisecond timestamp since 1970 when operation was started
  int64_t tt_ms;        // Graph millisecond timestamp since 1970 when operation is no longer allowed to execute
  int32_t timeout_ms;   // Total time allowed for operation
  vgx_ExecutionTimingBudgetFlags_t flags;
  void *resource;       // Address of blocked resource
  vgx_AccessReason_t reason;
} vgx_ExecutionTimingBudget_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_vgx_eventproc_event_enum {
  VGX_EVENTPROC_NO_EVENT                      = 0x00,
  VGX_EVENTPROC_VERTEX_EXPIRATION_EVENT       = 0x0E,
  VGX_EVENTPROC_VERTEX_REMOVE_SCHEDULE        = 0x0D
} vgx_eventproc_event_enum;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_vgx_VertexRefState {
  VGX_VERTEXREF_STATE_VIRGIN    = 0,
  VGX_VERTEXREF_STATE_AVAILABLE = 1,
  VGX_VERTEXREF_STATE_OCCUPIED  = 2,
  VGX_VERTEXREF_STATE_INVALID   = 9
} vgx_VertexRefState;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_vgx_collector_mode_t {
  //                                        -------T
  VGX_COLLECTOR_MODE_NONE_CONTINUE      = 0x00000000,
  VGX_COLLECTOR_MODE_NONE_STOP_AT_FIRST = 0x00000001,
  __VGX_COLLECTOR_MODE_COLLECT_MASK     = 0x000000F0,
  VGX_COLLECTOR_MODE_COLLECT_ARCS       = 0x00000010,
  VGX_COLLECTOR_MODE_COLLECT_VERTICES   = 0x00000020,
  VGX_COLLECTOR_MODE_COLLECT_AGGREGATE  = 0x00000040,
  __VGX_COLLECTOR_MODE_TYPE_MASK        = 0x000000FF,
  VGX_COLLECTOR_MODE_DEEP_COLLECT       = 0x00001000
} vgx_collector_mode_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_vgx_QueryType {

  /*                                                                                            */
  /*                                                                                            */
  /*                                                   TYPE CLCT RSLT EVAL COND FLTR ERR  TIME  */
  __VGX_QUERY_FEATURE_TIME_BUDGET   = 0x00000001,   /* ---- ---- ---- ---- ---- ---- ---- ---1  */
  __VGX_QUERY_FEATURE_TIME_EXEC     = 0x00000002,   /* ---- ---- ---- ---- ---- ---- ---- --1-  */
  __VGX_QUERY_FEATURE_TIME_rsv1     = 0x00000004,   /* ---- ---- ---- ---- ---- ---- ---- -1--  */
  __VGX_QUERY_FEATURE_TIME_rsv2     = 0x00000008,   /* ---- ---- ---- ---- ---- ---- ---- 1---  */

  __VGX_QUERY_FEATURE_ERR_STR       = 0x00000010,   /* ---- ---- ---- ---- ---- ---- ---1 ----  */
  __VGX_QUERY_FEATURE_ERR_REASON    = 0x00000020,   /* ---- ---- ---- ---- ---- ---- --1- ----  */
  __VGX_QUERY_FEATURE_ERR_rsv1      = 0x00000040,   /* ---- ---- ---- ---- ---- ---- -1-- ----  */
  __VGX_QUERY_FEATURE_ERR_rsv2      = 0x00000080,   /* ---- ---- ---- ---- ---- ---- 1--- ----  */

  __VGX_QUERY_FEATURE_FLTR_PRE      = 0x00000100,   /* ---- ---- ---- ---- ---- ---1 ---- ----  */
  __VGX_QUERY_FEATURE_FLTR_MAIN     = 0x00000200,   /* ---- ---- ---- ---- ---- --1- ---- ----  */
  __VGX_QUERY_FEATURE_FLTR_POST     = 0x00000400,   /* ---- ---- ---- ---- ---- -1-- ---- ----  */
  __VGX_QUERY_FEATURE_FLTR_rsv1     = 0x00000800,   /* ---- ---- ---- ---- ---- 1--- ---- ----  */

  __VGX_QUERY_FEATURE_COND_VERTEX   = 0x00001000,   /* ---- ---- ---- ---- ---1 ---- ---- ----  */
  __VGX_QUERY_FEATURE_COND_RANK     = 0x00002000,   /* ---- ---- ---- ---- --1- ---- ---- ----  */
  __VGX_QUERY_FEATURE_COND_ARC      = 0x00004000,   /* ---- ---- ---- ---- -1-- ---- ---- ----  */
  __VGX_QUERY_FEATURE_COND_COLLECT  = 0x00008000,   /* ---- ---- ---- ---- 1--- ---- ---- ----  */

  __VGX_QUERY_FEATURE_EVAL_MEMORY   = 0x00010000,   /* ---- ---- ---- ---1 ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_EVAL_SELECT   = 0x00020000,   /* ---- ---- ---- --1- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_EVAL_rsv1     = 0x00040000,   /* ---- ---- ---- -1-- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_EVAL_rsv2     = 0x00080000,   /* ---- ---- ---- 1--- ---- ---- ---- ----  */

  __VGX_QUERY_FEATURE_RSLT_OBJECT   = 0x00100000,   /* ---- ---- ---1 ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_RSLT_CNT_ARC  = 0x00200000,   /* ---- ---- --1- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_RSLT_CNT_VTX  = 0x00400000,   /* ---- ---- -1-- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_RSLT_CNT_OBJ  = 0x00800000,   /* ---- ---- 1--- ---- ---- ---- ---- ----  */

  __VGX_QUERY_FEATURE_CLCT_OBJECT   = 0x01000000,   /* ---- ---1 ---- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_CLCT_FIELDS   = 0x02000000,   /* ---- --1- ---- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_CLCT_AGGR     = 0x04000000,   /* ---- -1-- ---- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_CLCT_rsv1     = 0x08000000,   /* ---- 1--- ---- ---- ---- ---- ---- ----  */

  __VGX_QUERY_FEATURE_TYPE_ANCHOR   = 0x10000000,   /* ---1 ---- ---- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_TYPE_rsv1     = 0x20000000,   /* --1- ---- ---- ---- ---- ---- ---- ----  */
  __VGX_QUERY_FEATURE_TYPE_TYPE     = 0x40000000,   /* -1-- ---- ---- ---- ---- ---- ---- ----  */

  VGX_QUERY_TYPE_NONE               = 0x00000000,   /* 0000 0000 0000 0000 0000 0000 0000 0000  */

  __VGX_QUERY_FEATURE_SET_BASE          = __VGX_QUERY_FEATURE_TIME_BUDGET   | __VGX_QUERY_FEATURE_TIME_EXEC   |
                                          __VGX_QUERY_FEATURE_ERR_STR       |
                                          __VGX_QUERY_FEATURE_FLTR_PRE      | __VGX_QUERY_FEATURE_FLTR_MAIN   | __VGX_QUERY_FEATURE_FLTR_POST |
                                          __VGX_QUERY_FEATURE_COND_VERTEX   | __VGX_QUERY_FEATURE_COND_RANK   |
                                          __VGX_QUERY_FEATURE_EVAL_MEMORY   |
                                          __VGX_QUERY_FEATURE_RSLT_OBJECT,

  __VGX_QUERY_FEATURE_SET_ADJACENCY     = __VGX_QUERY_FEATURE_SET_BASE      |
                                          __VGX_QUERY_FEATURE_TYPE_ANCHOR   |
                                          __VGX_QUERY_FEATURE_ERR_REASON    |
                                          __VGX_QUERY_FEATURE_COND_ARC      |
                                          __VGX_QUERY_FEATURE_RSLT_CNT_ARC  | __VGX_QUERY_FEATURE_RSLT_CNT_VTX,

  __VGX_QUERY_FEATURE_SET_RESULT        = __VGX_QUERY_FEATURE_CLCT_OBJECT   | __VGX_QUERY_FEATURE_CLCT_FIELDS |
                                          __VGX_QUERY_FEATURE_EVAL_SELECT,

  __VGX_QUERY_FEATURE_SET_NEIGHBORHOOD  = __VGX_QUERY_FEATURE_SET_ADJACENCY |
                                          __VGX_QUERY_FEATURE_SET_RESULT    |
                                          __VGX_QUERY_FEATURE_COND_COLLECT,

  __VGX_QUERY_FEATURE_SET_GLOBAL        = __VGX_QUERY_FEATURE_SET_BASE      |
                                          __VGX_QUERY_FEATURE_SET_RESULT    |
                                          __VGX_QUERY_FEATURE_RSLT_CNT_OBJ,

  __VGX_QUERY_FEATURE_SET_AGGREGATOR    = __VGX_QUERY_FEATURE_SET_ADJACENCY |
                                          __VGX_QUERY_FEATURE_COND_COLLECT  |
                                          __VGX_QUERY_FEATURE_CLCT_AGGR,

  VGX_QUERY_TYPE_ADJACENCY              = __VGX_QUERY_FEATURE_TYPE_TYPE     | __VGX_QUERY_FEATURE_SET_ADJACENCY,
  VGX_QUERY_TYPE_NEIGHBORHOOD           = __VGX_QUERY_FEATURE_TYPE_TYPE     | __VGX_QUERY_FEATURE_SET_NEIGHBORHOOD,
  VGX_QUERY_TYPE_AGGREGATOR             = __VGX_QUERY_FEATURE_TYPE_TYPE     | __VGX_QUERY_FEATURE_SET_AGGREGATOR,
  VGX_QUERY_TYPE_GLOBAL                 = __VGX_QUERY_FEATURE_TYPE_TYPE     | __VGX_QUERY_FEATURE_SET_GLOBAL


} vgx_QueryType;



/******************************************************************************
 * vgx_ResponseAttrFastMask
 *
 ******************************************************************************
 */
typedef enum e_vgx_ResponseAttrFastMask {

  VGX_RESPONSE_ATTRS_MASK_NAME     = 0x0000000F,
  VGX_RESPONSE_ATTRS_MASK_DEGREE   = 0x000000F0,
  VGX_RESPONSE_ATTRS_MASK_PRED     = 0x00000F00,
  VGX_RESPONSE_ATTRS_MASK_PROP     = 0x0000F000,
  VGX_RESPONSE_ATTRS_MASK_REL      = 0x000F0000,
  VGX_RESPONSE_ATTRS_MASK_TIME     = 0x00F00000,
  VGX_RESPONSE_ATTRS_MASK_DETAIL   = 0x0F000000,

  // Names
  VGX_RESPONSE_ATTR_ANCHOR         = 0x00000001,
  VGX_RESPONSE_ATTR_ANCHOR_OBID    = 0x00000002,
  VGX_RESPONSE_ATTRS_ANCHOR        = 0x00000003,
  VGX_RESPONSE_ATTR_ID             = 0x00000004,
  VGX_RESPONSE_ATTR_OBID           = 0x00000008,
  VGX_RESPONSE_ATTRS_ID            = 0x0000000C,
  VGX_RESPONSE_ATTR_TYPENAME       = 0x00000010,
  VGX_RESPONSE_ATTRS_VERTICES      = VGX_RESPONSE_ATTR_ANCHOR | VGX_RESPONSE_ATTR_ID,
  VGX_RESPONSE_ATTRS_VERTICES_OBID = VGX_RESPONSE_ATTR_ANCHOR_OBID | VGX_RESPONSE_ATTR_OBID,
  VGX_RESPONSE_ATTRS_NAMES         = VGX_RESPONSE_ATTRS_VERTICES | VGX_RESPONSE_ATTRS_VERTICES_OBID | VGX_RESPONSE_ATTR_TYPENAME,
  // Degree
  VGX_RESPONSE_ATTR_DEGREE         = 0x00000020,
  VGX_RESPONSE_ATTR_INDEGREE       = 0x00000040,
  VGX_RESPONSE_ATTR_OUTDEGREE      = 0x00000080,
  VGX_RESPONSE_ATTRS_DEGREES       = VGX_RESPONSE_ATTR_DEGREE | VGX_RESPONSE_ATTR_INDEGREE | VGX_RESPONSE_ATTR_OUTDEGREE,
  // Predicator
  VGX_RESPONSE_ATTR_ARCDIR         = 0x00000100,  //  0001
  VGX_RESPONSE_ATTR_RELTYPE        = 0x00000200,  //  0010
  VGX_RESPONSE_ATTR_RELATIONSHIP   = 0x00000300,  //  0011 (two items: relationship 0010 and direction 0001)
  VGX_RESPONSE_ATTR_MODIFIER       = 0x00000400,  //  0100
  VGX_RESPONSE_ATTR_VALUE          = 0x00000800,  //  1000
  VGX_RESPONSE_ATTRS_PREDICATOR    = VGX_RESPONSE_ATTR_RELATIONSHIP | VGX_RESPONSE_ATTR_MODIFIER | VGX_RESPONSE_ATTR_VALUE,
  VGX_RESPONSE_ATTRS_ARC           = VGX_RESPONSE_ATTRS_PREDICATOR | VGX_RESPONSE_ATTR_ID,
  VGX_RESPONSE_ATTRS_ANCHORED_ARC  = VGX_RESPONSE_ATTR_ANCHOR | VGX_RESPONSE_ATTRS_ARC,
  // Properties
  VGX_RESPONSE_ATTR_VECTOR         = 0x00001000,
  VGX_RESPONSE_ATTR_PROPERTY       = 0x00002000,  // need property name supplied in addition to bitmask
  VGX_RESPONSE_ATTR__P_RSV         = 0x00004000,
  VGX_RESPONSE_ATTR_AS_ENUM        = 0x00008000,  // when present in the attrmask, do not decode enumerations
  VGX_RESPONSE_ATTRS_PROPERTIES    = VGX_RESPONSE_ATTR_VECTOR | VGX_RESPONSE_ATTR_PROPERTY,
  // Relevance
  VGX_RESPONSE_ATTR_RANKSCORE      = 0x00010000,
  VGX_RESPONSE_ATTR_SIMILARITY     = 0x00020000,
  VGX_RESPONSE_ATTR_HAMDIST        = 0x00040000,
  VGX_RESPONSE_ATTR__R_RSV         = 0x00080000,
  VGX_RESPONSE_ATTRS_RELEVANCE     = VGX_RESPONSE_ATTR_RANKSCORE | VGX_RESPONSE_ATTR_SIMILARITY | VGX_RESPONSE_ATTR_HAMDIST,
  // Timestamps
  VGX_RESPONSE_ATTR_TMC            = 0x00100000,
  VGX_RESPONSE_ATTR_TMM            = 0x00200000,
  VGX_RESPONSE_ATTR_TMX            = 0x00400000,
  VGX_RESPONSE_ATTR__T_RSV         = 0x00800000,
  VGX_RESPONSE_ATTRS_TIMESTAMP     = VGX_RESPONSE_ATTR_TMC | VGX_RESPONSE_ATTR_TMM | VGX_RESPONSE_ATTR_TMX,

  // ... more
  VGX_RESPONSE_ATTR_DESCRIPTOR     = 0x01000000,
  VGX_RESPONSE_ATTR_ADDRESS        = 0x02000000,
  VGX_RESPONSE_ATTR_HANDLE         = 0x04000000,
  VGX_RESPONSE_ATTR_RAW_VERTEX     = 0x08000000,
  VGX_RESPONSE_ATTRS_DETAILS       = VGX_RESPONSE_ATTR_DESCRIPTOR | VGX_RESPONSE_ATTR_ADDRESS | VGX_RESPONSE_ATTR_HANDLE | VGX_RESPONSE_ATTR_RAW_VERTEX,

  // Mask for determining whether vertex must be locked to render
  VGX_RESPONSE_ATTRS_TAIL_DEREF    = VGX_RESPONSE_ATTRS_ANCHOR,
  VGX_RESPONSE_ATTRS_HEAD_DEREF    = VGX_RESPONSE_ATTRS_ID | VGX_RESPONSE_ATTR_TYPENAME | VGX_RESPONSE_ATTRS_DEGREES | VGX_RESPONSE_ATTRS_PROPERTIES | VGX_RESPONSE_ATTRS_RELEVANCE | VGX_RESPONSE_ATTRS_TIMESTAMP | VGX_RESPONSE_ATTR_DESCRIPTOR | VGX_RESPONSE_ATTR_HANDLE | VGX_RESPONSE_ATTR_RAW_VERTEX,
  VGX_RESPONSE_ATTRS_DEREF         = VGX_RESPONSE_ATTRS_TAIL_DEREF | VGX_RESPONSE_ATTRS_HEAD_DEREF,

  // Safe to render without lock as long as vertex object remains allocated by allocator

  VGX_RESPONSE_ATTRS_MASK          = 0x0fffffff,

  VGX_RESPONSE_SHOW_AS_STRING      = 0x10000000,      //  0001
  VGX_RESPONSE_SHOW_AS_LIST        = 0x20000000,      //  0010
  VGX_RESPONSE_SHOW_AS_DICT        = 0x30000000,      //  0011
  VGX_RESPONSE_SHOW_AS_NONE        = 0x30000000,      //  0011
  VGX_RESPONSE_SHOW_WITH_NONE      = 0x00000000,      //  0000
  VGX_RESPONSE_SHOW_WITH_TIMING    = 0x40000000,      //  0100
  VGX_RESPONSE_SHOW_WITH_COUNTS    = 0x80000000,      //  1000
  VGX_RESPONSE_SHOW_WITH_METAS     = 0xC0000000,      //  1100
  VGX_RESPONSE_SHOW_AS_MASK        = 0x30000000,      //  0011
  VGX_RESPONSE_SHOW_WITH_MASK      = 0xC0000000,      //  1100
  VGX_RESPONSE_SHOW_MASK           = 0xF0000000,      //  1111

  VGX_RESPONSE_ATTRS_FULL          = VGX_RESPONSE_ATTRS_NAMES | VGX_RESPONSE_ATTRS_DEGREES | VGX_RESPONSE_ATTRS_PREDICATOR | VGX_RESPONSE_ATTRS_PROPERTIES | VGX_RESPONSE_ATTRS_RELEVANCE | VGX_RESPONSE_ATTRS_TIMESTAMP | VGX_RESPONSE_ATTRS_DETAILS,
  VGX_RESPONSE_ATTRS_NONE          = 0x00000000,      //

  VGX_RESPONSE_DEFAULT             = VGX_RESPONSE_SHOW_AS_STRING | VGX_RESPONSE_ATTR_ID


} vgx_ResponseAttrFastMask;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum e_vgx_query_debug {
  VGX_QUERY_DEBUG_QUERY_PRE   = 0x0001,
  VGX_QUERY_DEBUG_QUERY_POST  = 0x0002,
  VGX_QUERY_DEBUG_SEARCH_PRE  = 0x0010,
  VGX_QUERY_DEBUG_SEARCH_POST = 0x0020
} vgx_query_debug;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum _e_vgx_mapping_synchronization_t {
  /*                                           00000001    */
  VGX_MAPPING_SYNCHRONIZATION_MASK  = 0x01, // -------1
  VGX_MAPPING_SYNCHRONIZATION_NONE  = 0x00, // 00000000
  VGX_MAPPING_SYNCHRONIZATION_SYNC  = 0x01  // 00000001
} _vgx_mapping_synchronization_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum _e_vgx_mapping_keytype_t {
  /*                                           00001110    */
  VGX_MAPPING_KEYTYPE_MASK          = 0x0e, // ----111-
  VGX_MAPPING_KEYTYPE_QWORD         = 0x02, // 00000010
  VGX_MAPPING_KEYTYPE_STRING        = 0x04, // 00000100
  VGX_MAPPING_KEYTYPE_128BIT        = 0x08, // 00001000
} _vgx_mapping_keytype_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef enum _e_vgx_mapping_optimize_t {
  /*                                           00110000    */
  VGX_MAPPING_OPTIMIZE_MASK         = 0x30, // --11----
  VGX_MAPPING_OPTIMIZE_NORMAL       = 0x00, // 00000000
  VGX_MAPPING_OPTIMIZE_MEMORY       = 0x10, // 00010000
  VGX_MAPPING_OPTIMIZE_SPEED        = 0x20, // 00100000
  VGX_MAPPING_OPTIMIZE_BOTH         = 0x30, // 00110000
} _vgx_mapping_optimize_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum _e_vgx_ArcVector_VxD_tag {
  /*  VxD                                   N D T                                 */
  VGX_ARCVECTOR_VxD_EMPTY       = 0x4,  /*  1 0 0   NO ARC                        */
  VGX_ARCVECTOR_VxD_VERTEX      = 0x5,  /*  1 0 1   data 56_bit VERTEX OBJ56      */
  VGX_ARCVECTOR_VxD_DEGREE      = 0x7   /*  1 1 1   data 56-bit DEGREE (abuse D!) */
} _vgx_ArcVector_VxD_tag;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum _e_vgx_ArcVector_FxP_tag {
  /*  FxP                                  N D T                                  */
  VGX_ARCVECTOR_FxP_FRAME       = 0x0,  /*  0 0 0   ARRAY Pointer to Frame Top    */
  VGX_ARCVECTOR_FxP_EMPTY       = 0x4,  /*  1 0 0   NO ARC                        */
  VGX_ARCVECTOR_FxP_PREDICATOR  = 0x5   /*  1 0 1   data 56-bit PREDICATOR        */
} _vgx_ArcVector_FxP_tag;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum _e_vgx_ArcVector_cell_type {
  /* */
  VGX_ARCVECTOR_NO_ARCS                 = 0,
  VGX_ARCVECTOR_SIMPLE_ARC              = 1,
  VGX_ARCVECTOR_ARRAY_OF_ARCS           = 2,
  VGX_ARCVECTOR_MULTIPLE_ARC            = 3,
  VGX_ARCVECTOR_INDEGREE_COUNTER_ONLY   = 4,
  VGX_ARCVECTOR_INVALID                 = 5
} _vgx_ArcVector_cell_type;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_ArcFilter_match {
  VGX_ARC_FILTER_MATCH_ERROR = -1,
  VGX_ARC_FILTER_MATCH_MISS = 0,
  VGX_ARC_FILTER_MATCH_HIT = 1
} vgx_ArcFilter_match;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_ArcFilter_type {
  VGX_ARC_FILTER_TYPE_PASS                      = 0x0000,
  VGX_ARC_FILTER_TYPE_RELATIONSHIP              = 0x0010,
  VGX_ARC_FILTER_TYPE_MODIFIER                  = 0x0020,
  VGX_ARC_FILTER_TYPE_VALUE                     = 0x0040,
  VGX_ARC_FILTER_TYPE_SPECIFIC                  = 0x0070,
  VGX_ARC_FILTER_TYPE_RELATIONSHIP_VALUE        = 0x0100,
  VGX_ARC_FILTER_TYPE_MODIFIER_VALUE            = 0x0200,
  VGX_ARC_FILTER_TYPE_SPECIFIC_VALUE            = 0x0300,
  VGX_ARC_FILTER_TYPE_EVALUATOR                 = 0x0C00,
  
  VGX_ARC_FILTER_TYPE_GEN_NONE                  = 0x0F00,
  VGX_ARC_FILTER_TYPE_GEN_PRED                  = 0x0F10,
  VGX_ARC_FILTER_TYPE_GEN_LOCEVAL               = 0x0F20,
  VGX_ARC_FILTER_TYPE_GEN_PRED_LOCEVAL          = 0x0F30,
  VGX_ARC_FILTER_TYPE_GEN_TRAVEVAL              = 0x0F40,
  VGX_ARC_FILTER_TYPE_GEN_PRED_TRAVEVAL         = 0x0F50,
  VGX_ARC_FILTER_TYPE_GEN_VERTEX                = 0x0F80,
  VGX_ARC_FILTER_TYPE_GEN_PRED_VERTEX           = 0x0F90,
  VGX_ARC_FILTER_TYPE_GEN_LOCEVAL_VERTEX        = 0x0FA0,
  VGX_ARC_FILTER_TYPE_GEN_PRED_LOCEVAL_VERTEX   = 0x0FB0,
  VGX_ARC_FILTER_TYPE_GEN_TRAVEVAL_VERTEX       = 0x0FC0,
  VGX_ARC_FILTER_TYPE_GEN_PRED_TRAVEVAL_VERTEX  = 0x0FD0,

  VGX_ARC_FILTER_TYPE_GENERIC                   = 0x0FF0,
  __VGX_ARC_FILTER_TYPE_MASK_GENERIC            = 0xFF00,

  __VGX_ARC_FILTER_TYPE_MASK_GEN_PRED           = 0x0010,
  __VGX_ARC_FILTER_TYPE_MASK_GEN_LOCEVAL        = 0x0020,
  __VGX_ARC_FILTER_TYPE_MASK_GEN_TRAVEVAL       = 0x0040,
  __VGX_ARC_FILTER_TYPE_MASK_GEN_VERTEX         = 0x0080,

  __VGX_ARC_FILTER_TYPE_MASK_REQUIRE_TAIL       = 0x0800,
  VGX_ARC_FILTER_TYPE_STOP                      = 0xFFFF
} vgx_ArcFilter_type;



__inline static int _vgx_is_arcfilter_type_generic( const vgx_ArcFilter_type type ) {
  return (type & __VGX_ARC_FILTER_TYPE_MASK_GENERIC) == VGX_ARC_FILTER_TYPE_GEN_NONE;
}



__inline static int _vgx_arcfilter_has_vertex_probe( const vgx_ArcFilter_type type ) {
  return _vgx_is_arcfilter_type_generic( type ) && (type & __VGX_ARC_FILTER_TYPE_MASK_GEN_VERTEX) != 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_VertexFilter_type {
  VGX_VERTEX_FILTER_TYPE_PASS       = 0x0000,
  VGX_VERTEX_FILTER_TYPE_EVALUATOR  = 0x0C00,
  VGX_VERTEX_FILTER_TYPE_GENERIC    = 0x0FF0
} vgx_VertexFilter_type;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_StackItemType_t {
  //                              --TT
  STACK_ITEM_TYPE_INIT      = 0x00FFFF,   // 0000 0000 1111 1111 1111 1111
  STACK_ITEM_TYPE_NONE      = 0x000000,   // 0000 0000 0000 0000 0000 0000
  STACK_ITEM_TYPE_INTEGER   = 0x000001,   // 0000 0000 0000 0000 0000 0001
  STACK_ITEM_TYPE_REAL      = 0x000002,   // 0000 0000 0000 0000 0000 0010
  STACK_ITEM_TYPE_NAN       = 0x000003,   // 0000 0000 0000 0000 0000 0011 (special)
  STACK_ITEM_TYPE_VERTEX    = 0x000004,   // 0000 0000 0000 0000 0000 0100
  STACK_ITEM_TYPE_RANGE     = 0x000008,   // 0000 0000 0000 0000 0000 1000
  STACK_ITEM_TYPE_CSTRING   = 0x000010,   // 0000 0000 0000 0000 0001 0000
  STACK_ITEM_TYPE_VECTOR    = 0x000020,   // 0000 0000 0000 0000 0010 0000
  STACK_ITEM_TYPE_BITVECTOR = 0x000021,   // 0000 0000 0000 0000 0010 0001 (special)
  STACK_ITEM_TYPE_KEYVAL    = 0x000022,   // 0000 0000 0000 0000 0010 0010 (special)
  STACK_ITEM_TYPE_VERTEXID  = 0x000040,   // 0000 0000 0000 0000 0100 0000
  STACK_ITEM_TYPE_SET       = 0x000080,   // 0000 0000 0000 0000 1000 0000
  STACK_ITEM_TYPE_WILD      = 0x000077,   // 0000 0000 0000 0000 0111 0111
  
  __STACK_ITEM_TYPE_MASK    = 0x0000FF,   // 0000 0000 0000 0000 1111 1111
  __STACK_ITEM_SCALAR_MASK  = 0x000007,   // 0000 0000 0000 0000 0000 0111
  __STACK_ITEM_BITCMP_MASK  = STACK_ITEM_TYPE_INTEGER | STACK_ITEM_TYPE_VERTEX,
                                          // 0000 0000 0000 0000 0000 0101
  __STACK_ITEM_OBJECT_MASK  = 0x0000F8,   // 0000 0000 0000 0000 1111 1000

  // Used only for parsing support:
  __STACK_ITEM_REL_STR      = 0x000100 | STACK_ITEM_TYPE_CSTRING,
  __STACK_ITEM_VTX_STR      = 0x000200 | STACK_ITEM_TYPE_CSTRING,
  __STACK_ITEM_PROP_STR     = 0x000300 | STACK_ITEM_TYPE_CSTRING,
  __STACK_ITEM_DIM_STR      = 0x000400 | STACK_ITEM_TYPE_CSTRING,
  __STACK_ITEM_VARIABLE     = 0x000800 | STACK_ITEM_TYPE_INTEGER,

} vgx_StackItemType_t;





/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_item_type_bit_comparable( const vgx_StackItemType_t type ) {
  // INTEGER    01
  // VERTEX     04
  // BITVECTOR  21
  return (type & __STACK_ITEM_SCALAR_MASK) == STACK_ITEM_TYPE_INTEGER || type == STACK_ITEM_TYPE_VERTEX;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_item_type_integer_compatible( const vgx_StackItemType_t type ) {
  // INTEGER    01
  // BITVECTOR  21
  return (type & __STACK_ITEM_SCALAR_MASK) == STACK_ITEM_TYPE_INTEGER;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __are_stack_item_types_bit_comparable( const vgx_StackItemType_t t1, const vgx_StackItemType_t t2 ) {
  // INTEGER    01
  // VERTEX     04
  // BITVECTOR  21
  return __is_stack_item_type_bit_comparable( t1 ) && (t1 == t2 || __is_stack_item_type_bit_comparable( t2 ));
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_item_type_object( const vgx_StackItemType_t type ) {
  return (type & __STACK_ITEM_OBJECT_MASK) != 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_StackPairType_t {
  STACK_PAIR_NAN              = 0x000000, // NOTE: all zero bits, just use as a marker

  STACK_PAIR_YTYPE_INTEGER    = STACK_ITEM_TYPE_INTEGER,    // 0x000001
  STACK_PAIR_YTYPE_REAL       = STACK_ITEM_TYPE_REAL,       // 0x000002
  STACK_PAIR_YTYPE_NAN        = STACK_ITEM_TYPE_NAN,        // 0x000003
  STACK_PAIR_YTYPE_VERTEX     = STACK_ITEM_TYPE_VERTEX,     // 0x000004
  STACK_PAIR_YTYPE_RANGE      = STACK_ITEM_TYPE_RANGE,      // 0x000008
  STACK_PAIR_YTYPE_CSTRING    = STACK_ITEM_TYPE_CSTRING,    // 0x000010
  STACK_PAIR_YTYPE_VECTOR     = STACK_ITEM_TYPE_VECTOR,     // 0x000020
  STACK_PAIR_YTYPE_BITVECTOR  = STACK_ITEM_TYPE_BITVECTOR,  // 0x000021
  STACK_PAIR_YTYPE_KEYVAL     = STACK_ITEM_TYPE_KEYVAL,     // 0x000022
  STACK_PAIR_YTYPE_VERTEXID   = STACK_ITEM_TYPE_VERTEXID,   // 0x000040
  STACK_PAIR_YTYPE_SET        = STACK_ITEM_TYPE_SET,        // 0x000080
  STACK_PAIR_YTYPE_WILD       = STACK_ITEM_TYPE_WILD,       // 0x000077

  STACK_PAIR_XTYPE_INTEGER    = STACK_ITEM_TYPE_INTEGER << 8,    // 0x000100
  STACK_PAIR_XTYPE_REAL       = STACK_ITEM_TYPE_REAL << 8,       // 0x000200
  STACK_PAIR_XTYPE_NAN        = STACK_ITEM_TYPE_NAN << 8,        // 0x000300
  STACK_PAIR_XTYPE_VERTEX     = STACK_ITEM_TYPE_VERTEX << 8,     // 0x000400
  STACK_PAIR_XTYPE_RANGE      = STACK_ITEM_TYPE_RANGE << 8,      // 0x000800
  STACK_PAIR_XTYPE_CSTRING    = STACK_ITEM_TYPE_CSTRING << 8,    // 0x001000
  STACK_PAIR_XTYPE_VECTOR     = STACK_ITEM_TYPE_VECTOR << 8,     // 0x002000
  STACK_PAIR_XTYPE_BITVECTOR  = STACK_ITEM_TYPE_BITVECTOR << 8,  // 0x002100
  STACK_PAIR_XTYPE_KEYVAL     = STACK_ITEM_TYPE_KEYVAL << 8,     // 0x002200
  STACK_PAIR_XTYPE_VERTEXID   = STACK_ITEM_TYPE_VERTEXID << 8,   // 0x004000
  STACK_PAIR_XTYPE_SET        = STACK_ITEM_TYPE_SET << 8,        // 0x008000
  STACK_PAIR_XTYPE_WILD       = STACK_ITEM_TYPE_WILD << 8,       // 0x007700


  // x: integer
  STACK_PAIR_TYPE_XINT_YINT   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_INTEGER,
  STACK_PAIR_TYPE_XINT_YREA   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_REAL,
  STACK_PAIR_TYPE_XINT_YNAN   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_NAN         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YVTX   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_VERTEX,
  STACK_PAIR_TYPE_XINT_YRGE   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_RANGE       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YSTR   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_CSTRING     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YVEC   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_VECTOR      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YBTV   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_BITVECTOR,
  STACK_PAIR_TYPE_XINT_YKYV   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_KEYVAL,
  STACK_PAIR_TYPE_XINT_YVID   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_VERTEXID    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YSET   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_SET         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XINT_YWLD   = STACK_PAIR_XTYPE_INTEGER | STACK_PAIR_YTYPE_WILD        | STACK_PAIR_NAN,

  // x: real
  STACK_PAIR_TYPE_XREA_YINT   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_INTEGER,
  STACK_PAIR_TYPE_XREA_YREA   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_REAL,
  STACK_PAIR_TYPE_XREA_YNAN   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_NAN            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YVTX   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_VERTEX,
  STACK_PAIR_TYPE_XREA_YRGE   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_RANGE          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YSTR   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_CSTRING        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YVEC   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_VECTOR         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YBTV   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_BITVECTOR,
  STACK_PAIR_TYPE_XREA_YKYV   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_KEYVAL,
  STACK_PAIR_TYPE_XREA_YVID   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_VERTEXID       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YSET   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_SET            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XREA_YWLD   = STACK_PAIR_XTYPE_REAL | STACK_PAIR_YTYPE_WILD           | STACK_PAIR_NAN,

  // x: nan
  STACK_PAIR_TYPE_XNAN_YINT   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_INTEGER         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YREA   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_REAL            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YNAN   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_NAN             | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YVTX   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_VERTEX          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YRGE   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_RANGE           | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YSTR   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_CSTRING         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YVEC   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_VECTOR          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YBTV   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_BITVECTOR       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YKYV   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_KEYVAL          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YVID   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_VERTEXID        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YSET   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_SET             | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XNAN_YWLD   = STACK_PAIR_XTYPE_NAN | STACK_PAIR_YTYPE_WILD            | STACK_PAIR_NAN,

  // x: vertex
  STACK_PAIR_TYPE_XVTX_YINT   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_INTEGER,
  STACK_PAIR_TYPE_XVTX_YREA   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_REAL,
  STACK_PAIR_TYPE_XVTX_YNAN   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_NAN          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YVTX   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_VERTEX,
  STACK_PAIR_TYPE_XVTX_YRGE   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_RANGE        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YSTR   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_CSTRING      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YVEC   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_VECTOR       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YBTV   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_BITVECTOR,
  STACK_PAIR_TYPE_XVTX_YKYV   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_KEYVAL,
  STACK_PAIR_TYPE_XVTX_YVID   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_VERTEXID     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YSET   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_SET          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVTX_YWLD   = STACK_PAIR_XTYPE_VERTEX | STACK_PAIR_YTYPE_WILD         | STACK_PAIR_NAN,

  // x: range
  STACK_PAIR_TYPE_XRGE_YINT   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_INTEGER       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YREA   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_REAL          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YNAN   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_NAN           | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YVTX   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_VERTEX        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YRGE   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_RANGE         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YSTR   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_CSTRING       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YVEC   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_VECTOR        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YBTV   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_BITVECTOR     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YKYV   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_KEYVAL        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YVID   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_VERTEXID      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YSET   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_SET           | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XRGE_YWLD   = STACK_PAIR_XTYPE_RANGE | STACK_PAIR_YTYPE_WILD          | STACK_PAIR_NAN,

  // x: string
  STACK_PAIR_TYPE_XSTR_YINT   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_INTEGER     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YREA   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_REAL        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YNAN   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_NAN         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YVTX   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_VERTEX      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YRGE   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_RANGE       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YSTR   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_CSTRING     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YVEC   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_VECTOR      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YBTV   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_BITVECTOR   | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YKYV   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_KEYVAL      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YVID   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_VERTEXID    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YSET   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_SET         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSTR_YWLD   = STACK_PAIR_XTYPE_CSTRING | STACK_PAIR_YTYPE_WILD        | STACK_PAIR_NAN,

  // x: vector
  STACK_PAIR_TYPE_XVEC_YINT   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_INTEGER      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YREA   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_REAL         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YNAN   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_NAN          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YVTX   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_VERTEX       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YRGE   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_RANGE        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YSTR   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_CSTRING      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YVEC   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_VECTOR       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YBTV   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_BITVECTOR    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YKYV   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_KEYVAL       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YVID   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_VERTEXID     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YSET   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_SET          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVEC_YWLD   = STACK_PAIR_XTYPE_VECTOR | STACK_PAIR_YTYPE_WILD         | STACK_PAIR_NAN,

  // x: bitvector
  STACK_PAIR_TYPE_XBTV_YINT   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_INTEGER,
  STACK_PAIR_TYPE_XBTV_YREA   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_REAL,
  STACK_PAIR_TYPE_XBTV_YNAN   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_NAN       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YVTX   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_VERTEX,
  STACK_PAIR_TYPE_XBTV_YRGE   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_RANGE     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YSTR   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_CSTRING   | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YVEC   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_VECTOR    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YBTV   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_BITVECTOR,
  STACK_PAIR_TYPE_XBTV_YKYV   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_KEYVAL,
  STACK_PAIR_TYPE_XBTV_YVID   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_VERTEXID  | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YSET   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_SET       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XBTV_YWLD   = STACK_PAIR_XTYPE_BITVECTOR | STACK_PAIR_YTYPE_WILD      | STACK_PAIR_NAN,

  // x: keyval
  STACK_PAIR_TYPE_XKYV_YINT   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_INTEGER,
  STACK_PAIR_TYPE_XKYV_YREA   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_REAL,
  STACK_PAIR_TYPE_XKYV_YNAN   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_NAN          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YVTX   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_VERTEX,
  STACK_PAIR_TYPE_XKYV_YRGE   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_RANGE        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YSTR   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_CSTRING      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YVEC   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_VECTOR       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YBTV   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_BITVECTOR,
  STACK_PAIR_TYPE_XKYV_YKYV   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_KEYVAL,
  STACK_PAIR_TYPE_XKYV_YVID   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_VERTEXID     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YSET   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_SET          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XKYV_YWLD   = STACK_PAIR_XTYPE_KEYVAL | STACK_PAIR_YTYPE_WILD         | STACK_PAIR_NAN,

  // x: vertexid
  STACK_PAIR_TYPE_XVID_YINT   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_INTEGER    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YREA   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_REAL       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YNAN   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_NAN        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YVTX   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_VERTEX     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YRGE   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_RANGE      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YSTR   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_CSTRING    | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YVEC   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_VECTOR     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YBTV   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_BITVECTOR  | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YKYV   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_KEYVAL     | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YVID   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_VERTEXID   | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YSET   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_SET        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XVID_YWLD   = STACK_PAIR_XTYPE_VERTEXID | STACK_PAIR_YTYPE_WILD       | STACK_PAIR_NAN,

  // x: set
  STACK_PAIR_TYPE_XSET_YINT   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_INTEGER         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YREA   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_REAL            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YNAN   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_NAN             | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YVTX   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_VERTEX          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YRGE   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_RANGE           | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YSTR   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_CSTRING         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YVEC   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_VECTOR          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YBTV   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_BITVECTOR       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YKYV   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_KEYVAL          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YVID   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_VERTEXID        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YSET   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_SET             | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XSET_YWLD   = STACK_PAIR_XTYPE_SET | STACK_PAIR_YTYPE_WILD            | STACK_PAIR_NAN,

  // x: wild
  STACK_PAIR_TYPE_XWLD_YINT   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_INTEGER        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YREA   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_REAL           | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YNAN   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_NAN            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YVTX   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_VERTEX         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YRGE   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_RANGE          | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YSTR   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_CSTRING        | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YVEC   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_VECTOR         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YBTV   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_BITVECTOR      | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YKYV   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_KEYVAL         | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YVID   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_VERTEXID       | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YSET   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_SET            | STACK_PAIR_NAN,
  STACK_PAIR_TYPE_XWLD_YWLD   = STACK_PAIR_XTYPE_WILD | STACK_PAIR_YTYPE_WILD           | STACK_PAIR_NAN,



  STACK_PAIR_TYPE_XNUM_MASK   = 0x000F00,
  STACK_PAIR_TYPE_YNUM_MASK   = 0x00000F,
  STACK_PAIR_TYPE_X_MASK      = 0x00FF00,
  STACK_PAIR_TYPE_Y_MASK      = 0x0000FF,
  STACK_PAIR_TYPE_X_WILD      = 0x007700,
  STACK_PAIR_TYPE_Y_WILD      = 0x000077,

  STACK_PAIR_TYPE_SCALAR_MASK = 0x000707,
  STACK_PAIR_TYPE_INTEGERS    = 0x000101
} vgx_StackPairType_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_pair_type_numeric( const vgx_StackPairType_t pair ) {
  return (pair & STACK_PAIR_TYPE_XNUM_MASK) && (pair & STACK_PAIR_TYPE_YNUM_MASK);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_pair_type_integer_compatible( const vgx_StackPairType_t pair ) {
  return (pair & STACK_PAIR_TYPE_SCALAR_MASK) == STACK_PAIR_TYPE_INTEGERS;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __is_stack_pair_item_wild( const vgx_StackPairType_t pair ) {
  return (pair & STACK_PAIR_TYPE_Y_MASK) == STACK_PAIR_TYPE_Y_WILD || (pair & STACK_PAIR_TYPE_X_MASK) == STACK_PAIR_TYPE_X_WILD;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_ExpressEvalMemRegister {
  EXPRESS_EVAL_MEM_REGISTER_R1  =   -1,
  EXPRESS_EVAL_MEM_REGISTER_R2  =   -2,
  EXPRESS_EVAL_MEM_REGISTER_R3  =   -3,
  EXPRESS_EVAL_MEM_REGISTER_R4  =   -4,
  __EXPRESS_EVAL_MEM_SPTOP      =   -5
} vgx_ExpressEvalMemRegister;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_CollectorType_t {
  //                                                           GNI   SU   AV   LA
  //                                                          ---- ---- ---- ----
  __VGX_COLLECTOR_INDIVIDUAL                    = 0x1000, //  0001 0000 0000 0000
  __VGX_COLLECTOR_NEIGHBORHOOD_SUMMARY          = 0x2000, //  0010 0000 0000 0000
  __VGX_COLLECTOR_GLOBAL_SUMMARY                = 0x4000, //  0100 0000 0000 0000
  __VGX_COLLECTOR_SORTED                        = 0x0200, //  0000 0010 0000 0000
  __VGX_COLLECTOR_UNSORTED                      = 0x0100, //  0000 0001 0000 0000
  __VGX_COLLECTOR_ARC                           = 0x0020, //  0000 0000 0010 0000
  __VGX_COLLECTOR_VERTEX                        = 0x0010, //  0000 0000 0001 0000
  __VGX_COLLECTOR_LIST                          = 0x0002, //  0000 0000 0000 0010
  __VGX_COLLECTOR_AGGREGATION                   = 0x0001, //  0000 0000 0000 0001

  __VGX_COLLECTOR_TYPE_MASK                     = 0xF000, //  1111 0000 0000 0000
  __VGX_COLLECTOR_SORT_MASK                     = 0x0F00, //  0000 1111 0000 0000
  __VGX_COLLECTOR_ITEM_MASK                     = 0x00F0, //  0000 0000 1111 0000
  __VGX_COLLECTOR_CONTAINER_MASK                = 0x000F, //  0000 0000 0000 1111

  VGX_COLLECTOR_TYPE_NONE                       = 0x0000,

  VGX_COLLECTOR_TYPE_UNSORTED_ARC_LIST          = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_UNSORTED | __VGX_COLLECTOR_ARC    | __VGX_COLLECTOR_LIST,
  VGX_COLLECTOR_TYPE_SORTED_ARC_LIST            = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_SORTED   | __VGX_COLLECTOR_ARC    | __VGX_COLLECTOR_LIST,
  VGX_COLLECTOR_TYPE_UNSORTED_ARC_AGGREGATION   = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_UNSORTED | __VGX_COLLECTOR_ARC    | __VGX_COLLECTOR_AGGREGATION,
  VGX_COLLECTOR_TYPE_SORTED_ARC_AGGREGATION     = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_SORTED   | __VGX_COLLECTOR_ARC    | __VGX_COLLECTOR_AGGREGATION,
  VGX_COLLECTOR_TYPE_UNSORTED_VERTEX_LIST       = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_UNSORTED | __VGX_COLLECTOR_VERTEX | __VGX_COLLECTOR_LIST,
  VGX_COLLECTOR_TYPE_SORTED_VERTEX_LIST         = __VGX_COLLECTOR_INDIVIDUAL | __VGX_COLLECTOR_SORTED   | __VGX_COLLECTOR_VERTEX | __VGX_COLLECTOR_LIST,
  VGX_COLLECTOR_TYPE_NEIGHBORHOOD_AGGREGATION   = __VGX_COLLECTOR_NEIGHBORHOOD_SUMMARY | __VGX_COLLECTOR_AGGREGATION,
  VGX_COLLECTOR_TYPE_GLOBAL_AGGREGATION         = __VGX_COLLECTOR_GLOBAL_SUMMARY       | __VGX_COLLECTOR_AGGREGATION
} vgx_CollectorType_t;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_search_type {
  VGX_SEARCH_TYPE_NONE           = 0x00,   /* 0000 0000  */
  VGX_SEARCH_TYPE_ADJACENCY      = 0x01,   /* 0000 0001  */
  VGX_SEARCH_TYPE_NEIGHBORHOOD   = 0x03,   /* 0000 0011  */
  VGX_SEARCH_TYPE_AGGREGATOR     = 0x05,   /* 0000 0101  */
  VGX_SEARCH_TYPE_GLOBAL         = 0x10    /* 0001 0000  */
} vgx_search_type;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef enum e_vgx_vertex_record {
  VGX_VERTEX_RECORD_NONE          = 0x0000,   /* 0000 0000 0000 0000 */
  VGX_VERTEX_RECORD_ACQUISITION   = 0x0001,   /* 0000 0000 0000 0001 */
  VGX_VERTEX_RECORD_OPERATION     = 0x0010,   /* 0000 0000 0001 0000 */
  VGX_VERTEX_RECORD_ALL           = VGX_VERTEX_RECORD_ACQUISITION | VGX_VERTEX_RECORD_OPERATION,
  __VGX_VERTEX_RECORD_EVENT       = 0x0020,   /* 0000 0000 0010 0000 */
  VGX_VERTEX_RECORD_EVENTEXEC     = VGX_VERTEX_RECORD_OPERATION | __VGX_VERTEX_RECORD_EVENT
} vgx_vertex_record;



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __vgx_vertex_record_acquisition( const vgx_vertex_record record ) {
  return ((int)record & VGX_VERTEX_RECORD_ACQUISITION);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __vgx_vertex_record_operation( const vgx_vertex_record record ) {
  return ((int)record & VGX_VERTEX_RECORD_OPERATION);
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static int __vgx_vertex_record_event( const vgx_vertex_record record ) {
  return ((int)record & __VGX_VERTEX_RECORD_EVENT);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef struct s_vgx_URIQueryParameters_t {
  int sz;
  struct s_vgx_KeyVal_t *keyval;
  char *_buffer;
  void *_data;
} vgx_URIQueryParameters_t;



/*******************************************************************//**
 * 
 ***********************************************************************
 */
typedef struct s_vgx_URI_t {
  // -------- CL1 --------
  // [Q1.1.1.1]
  unsigned short port;
  // [Q1.1.1.2]
  WORD __rsv_1_1_1_2;
  // [Q1.1.2]
  int _fd;

  // [Q1.2]
  CXSOCKET _sock;

  // [Q1.3-4]
  struct {
    int *pfd;
    CXSOCKET *psock;
  } output;

  // [Q1.5]
  struct {
    CXSOCKET *psock;
  } input;

  // [Q1.6]
  QWORD __rsv_1_6;

  // [Q1.7]
  CString_t *CSTR__ip;

  // [Q1.8]
  CString_t *CSTR__nameinfo;

  // -------- CL2 --------
  // [Q2.1]
  CString_t *CSTR__uri;
  
  // [Q2.2]
  CString_t *CSTR__scheme;
  
  // [Q2.3]
  CString_t *CSTR__userinfo;
  
  // [Q2.4]
  CString_t *CSTR__host;
  
  // [Q2.5]
  CString_t *CSTR__port;
  
  // [Q2.6]
  CString_t *CSTR__path;
  
  // [Q2.7]
  CString_t *CSTR__query;

  // [Q2.8]
  CString_t *CSTR__fragment;

} vgx_URI_t;



typedef struct s_vgx_IURI_t {
  vgx_URI_t *  (*New)( const char *uri, CString_t **CSTR__error );
  vgx_URI_t *  (*NewElements)( const char *scheme, const char *userinfo, const char *host, unsigned short port, const char *path, const char *query, const char *fragment, CString_t **CSTR__error );
  vgx_URI_t *  (*Clone)( const vgx_URI_t *URI );
  void         (*Delete)( vgx_URI_t **URI );
  const char * (*URI)( const vgx_URI_t *URI );
  const char * (*Scheme)( const vgx_URI_t *URI );
  const char * (*UserInfo)( const vgx_URI_t *URI );
  const char * (*Host)( const vgx_URI_t *URI );
  const char * (*Port)( const vgx_URI_t *URI );
  unsigned short (*PortInt)( const vgx_URI_t *URI );
  const char * (*Path)( const vgx_URI_t *URI );
  int64_t      (*PathLength)( const vgx_URI_t *URI );
  const char * (*Query)( const vgx_URI_t *URI );
  int64_t      (*QueryLength)( const vgx_URI_t *URI );
  const char * (*Fragment)( const vgx_URI_t *URI );
  int64_t      (*FragmentLength)( const vgx_URI_t *URI );
  const char * (*HostIP)( vgx_URI_t *URI );
  const char * (*NameInfo)( vgx_URI_t *URI );
  bool         (*Match)( const vgx_URI_t *A, const vgx_URI_t *B );
  struct addrinfo * (*NewAddrInfo)( const vgx_URI_t *URI, CString_t **CSTR__error );
  void         (*DeleteAddrInfo)( struct addrinfo **address );
  int          (*Bind)( vgx_URI_t *URI, CString_t **CSTR__error );
  int          (*Listen)( vgx_URI_t *URI, int backlog, CString_t **CSTR__error );
  vgx_URI_t *  (*Accept)( vgx_URI_t *URI, const char *scheme, CString_t **CSTR__error );
  CXSOCKET *   (*ConnectInetStreamTCP)( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error, int *rerr );
  int          (*OpenFile)( vgx_URI_t *URI, CString_t **CSTR__error );
  int          (*OpenNull)( vgx_URI_t *URI, CString_t **CSTR__error );
  int          (*IsConnected)( const vgx_URI_t *URI );
  int          (*Connect)( vgx_URI_t *URI, int timeout_ms, CString_t **CSTR__error );
  void         (*Close)( vgx_URI_t *URI );
  CString_t *  (*NewFormat)( const vgx_URI_t *URI );
  CString_t *  (*NewFqdn)( void );
  struct {
    int        (*Error)( CString_t **CSTR__error, int err );
  } Address;
  struct {
    int (*GetDescriptor)( vgx_URI_t *URI );
    bool (*IsNullOutput)( vgx_URI_t *URI );
  } File;
  struct {
    struct {
      CXSOCKET * (*Get)( vgx_URI_t *URI );
    } Output;
    struct {
      CXSOCKET * (*Get)( vgx_URI_t *URI );
    } Input;
    int        (*Error)( vgx_URI_t *URI, CString_t **CSTR__error, int err, short revents );
    int        (*Busy)( int err );
  } Sock;
  struct {
    int        (*SetQueryParam)( const char *path, vgx_URIQueryParameters_t *param );
    void       (*ClearQueryParam)( vgx_URIQueryParameters_t *param );
  } Parse;
} vgx_IURI_t;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define URI_SCHEME__vgx( SchemeChars )    CharsEqualsConst( SchemeChars, "vgx" )
#define URI_SCHEME__http( SchemeChars )   CharsEqualsConst( SchemeChars, "http" )
#define URI_SCHEME__file( SchemeChars )   CharsEqualsConst( SchemeChars, "file" )
#define URI_SCHEME__null( SchemeChars )   CharsEqualsConst( SchemeChars, "null" )



/* Linked list object chain members */
#define VGX_LLIST_CHAIN_STRUCT( TYPE ) \
  struct {      \
    TYPE *prev; \
    TYPE *next; \
  } chain


/* Linked list head pointer is NULL */
#define VGX_LLIST_EMPTY( Parent ) \
  (Parent).head == NULL


/* Set linked list pointer (head or tail) to point to object */
#define VGX_LLIST_SET_ENDPOINT( Parent, Endpoint, Object ) \
  (Parent).Endpoint = Object


/* Set tail object chain's next pointer to NULL */
#define VGX_LLIST_TERMINATE( Parent ) \
  (Parent).tail->chain.next = NULL


/* Set linked list head to NULL */
#define VGX_LLIST_DETACH_HEAD( Parent ) \
  VGX_LLIST_SET_ENDPOINT( Parent, head, NULL )


/* Set linked list tail to NULL */
#define VGX_LLIST_DETACH_TAIL( Parent ) \
  VGX_LLIST_SET_ENDPOINT( Parent, tail, NULL )


/* Linked list initialized with a single object */
#define VGX_LLIST_SET_SINGLE( Parent, Object ) do { \
  VGX_LLIST_SET_ENDPOINT( Parent, head, Object );   \
  VGX_LLIST_SET_ENDPOINT( Parent, tail, Object );   \
} WHILE_ZERO


/* Linked list gets another object appended to its end */
#define VGX_LLIST_ATTACH_DOWNLINK( Parent, Object ) do { \
  (Parent).tail->chain.next = Object;       \
  (Object)->chain.prev = (Parent).tail;     \
  (Parent).tail = Object;                   \
} WHILE_ZERO


/* Linked list object is not part of any list */
#define VGX_LLIST_IS_OBJECT_ISOLATED( Object )  \
  ((Object)->chain.next == (Object)->chain.prev)


/* Append object to (or set single object in) linked list if object is not already in a list */
#define VGX_LLIST_APPEND( Parent, Object ) do {     \
  if( VGX_LLIST_IS_OBJECT_ISOLATED( Object ) ) {    \
    if( VGX_LLIST_EMPTY( Parent ) ) {               \
      VGX_LLIST_SET_SINGLE( Parent, Object );       \
    }                                               \
    else {                                          \
      VGX_LLIST_ATTACH_DOWNLINK( Parent, Object );  \
    }                                               \
  }                                                 \
} WHILE_ZERO


/* Linked list object chain (prev or next) exists */
#define VGX_LLIST_HAS_LINK( Object, Link )  \
  (Object)->chain.Link != NULL


/* Remove object from one direction of the linked list chain */
#define VGX_LLIST_OBJECT_BYPASS( Object, SideA, SideB ) \
  (Object)->chain.SideA->chain.SideB = (Object)->chain.SideB


/* Truncate linked list uplink from and including object */
#define VGX_LLIST_TRUNCATE_UPLINK( Parent, Object ) \
  (Parent).head = (Object)->chain.next


/* Truncate linked list downlink from and including object */
#define VGX_LLIST_TRUNCATE_DOWNLINK( Parent, Object ) \
  (Parent).tail = (Object)->chain.prev


/* Bypass linked list object uplink */
#define VGX_LLIST_DETACH_UPLINK( Parent, Object ) do { \
  if( VGX_LLIST_HAS_LINK( Object, prev ) ) {        \
    VGX_LLIST_OBJECT_BYPASS( Object, prev, next );  \
  }                                                 \
  else {                                            \
    VGX_LLIST_TRUNCATE_UPLINK( Parent, Object );    \
  }                                                 \
} WHILE_ZERO


/* Bypass linked list object downlink */
#define VGX_LLIST_DETACH_DOWNLINK( Parent, Object ) do { \
  if( VGX_LLIST_HAS_LINK( Object, next ) ) {        \
    VGX_LLIST_OBJECT_BYPASS( Object, next, prev );  \
  }                                                 \
  else {                                            \
    VGX_LLIST_TRUNCATE_DOWNLINK( Parent, Object );  \
  }                                                 \
} WHILE_ZERO


/* Set linked list object pointer to NULL */
#define VGX_LLIST_OBJECT_UNLINK( Object, Link ) \
  (Object)->chain.Link = NULL


/* Set linked list object chain pointers to NULL */
#define VGX_LLIST_OBJECT_ISOLATE( Object ) do { \
  VGX_LLIST_OBJECT_UNLINK( Object, prev ); \
  VGX_LLIST_OBJECT_UNLINK( Object, next ); \
} WHILE_ZERO


/* Remove object from linked list */
#define VGX_LLIST_YANK( Parent, Object ) do {   \
  VGX_LLIST_DETACH_UPLINK( Parent, Object);     \
  VGX_LLIST_DETACH_DOWNLINK( Parent, Object );  \
  VGX_LLIST_OBJECT_ISOLATE( Object );           \
} WHILE_ZERO









// TODO: do it differently
void vgx_Fingerprinter_RegisterClass( void );
void vgx_Fingerprinter_UnregisterClass( void );
void vgx_Vector_RegisterClass( void );
void vgx_Vector_UnregisterClass( void );
void vgx_Similarity_RegisterClass( void );
void vgx_Similarity_UnregisterClass( void );
DLL_VISIBLE extern int vgx_SIM_INIT( void ); 
DLL_VISIBLE extern void vgx_SIM_DESTROY( void ); 













#endif




