/*
###################################################
#
# File:   _probe.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXEVAL_MODULES_PROBE_H
#define _VGX_VXEVAL_MODULES_PROBE_H

#include "_memory.h"

/*******************************************************************//**
 *
 ***********************************************************************
 */
static void __eval_probe_probearray( vgx_Evaluator_t *self );
static void __eval_probe_probealtarray( vgx_Evaluator_t *self );
static void __eval_probe_probesuperarray( vgx_Evaluator_t *self );



/*******************************************************************//**
 *
 ***********************************************************************
 */
static int64_t __probearray_cstring( vgx_EvalStackItem_t *P1, vgx_EvalStackItem_t *P_end, const CString_t *CSTR__str, vgx_EvalStackItem_t *prefix_boost ) {
  const char *raw = CStringValue( CSTR__str );
  // Number of qwords in string
  int64_t nqw = VGX_CSTRING_ARRAY_LENGTH( CSTR__str );
  int64_t psz = P_end - P1;
  // Check probe size
  if( psz <= nqw ) {
    // Compare string data as integer array
    if( CStringAttributes( CSTR__str ) & CSTRING_ATTR_ARRAY_INT ) {
      QWORD *qwords = (QWORD*)raw;
      QWORD *end_qw = qwords + nqw; 
      QWORD *q0 = qwords;
      QWORD *qx = q0;
      vgx_EvalStackItem_t *ax = P1;

      QWORD posvec = 1;
      QWORD unmatched_posvec = ~0ULL;

      // Optimistic start
      while( ax < P_end ) {
        // Probe element is bitvector, which needs a 1 in position corresponding to current qword
        if( ax->type == STACK_ITEM_TYPE_BITVECTOR ) {
          if( !(posvec & ax->bits) ) {
            goto NOT_PREFIX; // bitvector mismatch
          }
        }
        // Probe element is integer (assumed, not checked!)
        else if( ax->bits != *qx ) {
          goto NOT_PREFIX; // token mismatch
        }
        // Next
        ++ax;
        ++qx;
        unmatched_posvec ^= posvec; // turn off bit in matched position
        posvec <<= 1;
      }

      // Prefix match!
      if( prefix_boost->type == STACK_ITEM_TYPE_REAL ) {
        return (int64_t)prefix_boost->real;
      }
      else {
        return prefix_boost->integer;
      }

    NOT_PREFIX:
      // Continue in non-prefix mode


      // Move qwords start to already matched prefix
      q0 = qx;

      // Initialize the posvec reset position to where prefix match left off
      QWORD posvec_reset = posvec;

      // Continue probe from where prefix match failed
      while( ax < P_end ) {
        if( ax->type == STACK_ITEM_TYPE_BITVECTOR ) {
          QWORD bv_match = ax->bits & unmatched_posvec;
          if( bv_match ) {
            // turn off bit in lowest matched position
            // unmatched_posvec ^= (bv_match & (~bv_match + 1));
            unmatched_posvec ^= ( bv_match & (-(int64_t)bv_match) );
            goto NEXT_AX;
          }
        }
        else if( ax->type == STACK_ITEM_TYPE_INTEGER ) {
          while( qx < end_qw ) {
            if( ax->bits == *qx && (posvec & unmatched_posvec) != 0 ) {
              unmatched_posvec ^= posvec; // turn off bit in matched position
              goto NEXT_AX;
            }
            ++qx;
            posvec <<= 1;
          }
        }
        
        goto MISS;

      NEXT_AX:
        ++ax;
        qx = q0;
        posvec = posvec_reset;
      }

      // Set match!
      return 1;
    }
  }

MISS:
  return 0;
}



/*******************************************************************//**
 * probearray( P1, Pn, array, prefix_boost )
 *
 *
 *
 ***********************************************************************
 */
static void __eval_probe_probearray( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t prefix_boost = POP_ITEM( self );
  vgx_EvalStackItem_t arr = POP_ITEM( self );

  vgx_EvalStackItem_t *P1, *Pn;
  int64_t nP = __slice( self, &P1, &Pn );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_INTEGER;

  if( nP > 0 ) {
    switch( arr.type ) {
    case STACK_ITEM_TYPE_CSTRING:
      px->integer = __probearray_cstring( P1, Pn, arr.CSTR__str, &prefix_boost );
      return;
    default:
      break;
    }
  }

  px->integer = 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __probealtarray_cstring( vgx_EvalStackItem_t *P1, vgx_EvalStackItem_t *P_end, const CString_t *CSTR__altarray, vgx_EvalStackItem_t *prefix_boost, vgx_EvalStackItem_t *alt_deboost ) {
  int n_primary = 0;
  int n_secondary = 0;
  const char *bytes = CStringValue( CSTR__altarray );
  // Number of qwords in altarray string
  int64_t nqw = VGX_CSTRING_ARRAY_LENGTH( CSTR__altarray );
  int64_t psz = P_end - P1;
  // Check probe size
  if( psz <= nqw ) {
    // Altarray data: Dual 32-bit integer array (primary and secondary)
    //
    //    Array entry expected format: 
    //      PRIMARY   SECONDARY
    //      32 MSB    32 LSB
    //
    //
    // Query contains one or more item which be one of two things:
    //
    // 1) Integer with same format as array entries, i.e. dual 32-bit integer.
    //    Each query item is matched against array until first match, or until
    //    exhausted with no match. If no match query is a miss and terminates.
    //    For each query item match the primary segment (32MSB) is checked first.
    //    If match, the probe continues with the next query item. If miss, the 
    //    secondary segment (32LSB) is checked. If match a penalty is flagged
    //    which will incorporate the alt_deboost when producing final score.
    //    If secondary is also miss the next array item is compared. If no match
    //    is found for query item in array the probe is a miss and terminates.
    //    If all query items find a match the probe is successful. Note that a
    //    match "consumes" the matched array entry, ensuring that subsequent 
    //    identical query items will not match the same array entry twice.
    //    If query items align exactly in order with array entries a prefix boost
    //    is incorporated into the final score. Otherwise no prefix boost.
    //
    // 2) 32-bit bitvector where positions of 1s correspond to array indexes that
    //    should be considered matches.
    //  
    //    Bitvector expected format:
    //      PRIMARY    SECONDARY
    //      16 MSB     16 LSB
    // 
    //    No array scanning is performed since the bitvector already asserts
    //    array matches exist in the positions corresponding to 1s in the bitvector.
    //    The first unconsumed array entry lining up with a 1 in the bitvector is 
    //    then consumed and the probe continues with the next query item. If no
    //    bitvector 1s align with unconsumed array items the probe is a miss and
    //    terminates.
    //    Bitvector matching is first performed using the primary (16MSB) segment.
    //    If miss, the secondary (16LSB) segment is used instead, and a penalty is
    //    flagged to incorporate alt_deboost in the final score. If secondary is also
    //    a miss the probe terminates with a miss.
    //    NOTE: The maximum array length that is matchable by bitvectors is 16.
    //
    //
    //
    //
    if( CStringAttributes( CSTR__altarray ) & CSTRING_ATTR_ARRAY_INT ) {
      QWORD *qwords = (QWORD*)bytes;
      QWORD *end_qw = qwords + nqw; 
      QWORD *q0 = qwords;
      QWORD *qx = q0;
      vgx_EvalStackItem_t *ax = P1;

      // 16 match positions
      uint16_t posvec = 1;
      uint16_t unmatched_posvec = 0xFFFF;
      
      // Optimistic start
      while( ax < P_end ) {
        // Probe element is bitvector, which needs a 1 in position corresponding to current qword
        if( ax->type == STACK_ITEM_TYPE_BITVECTOR ) {
          // Match in primary segment (16MSB)
          if( posvec & (ax->bits >> 16) ) {
            ++n_primary;
          }
          // Match in secondary segment (16LSB)
          else if( posvec & ax->bits ) {
            ++n_secondary;
          }
          // MISS
          else {
            goto NOT_PREFIX;
          }
        }
        // Probe element is dual 32-bit integer (assumed, not checked!): match primary or secondary
        else {
          // Compare  
          QWORD diff = ax->bits ^ *qx;
          
          // MSB are clear: Primary Match
          if( diff < 0x100000000ULL ) {
            ++n_primary;
          }
          // LSB are clear: Secondary Match
          else if( !(diff & 0xFFFFFFFFULL) ) {
            ++n_secondary;
          }
          // MISS
          else {
            goto NOT_PREFIX;
          }
        }

        // Next
        ++ax;
        ++qx;
        unmatched_posvec ^= posvec; // turn off bit in matched position
        posvec <<= 1;
      }

      // Prefix match!
      //
      double boost = prefix_boost->type == STACK_ITEM_TYPE_REAL ? prefix_boost->real : (double)prefix_boost->integer;
      // Primary match
      if( n_secondary == 0 ) {
        return boost;
      }
      // Secondary match
      else {
        return boost * (alt_deboost->type == STACK_ITEM_TYPE_REAL ? alt_deboost->real : (double)alt_deboost->integer);
      }
      //
      // ------------


    NOT_PREFIX:
      // Continue in non-prefix mode


      // Move qwords start to already matched prefix
      q0 = qx;

      // Initialize the posvec reset position to where prefix match left off
      uint16_t posvec_reset = posvec;
      uint16_t bv_match;

      // Continue probe from where prefix match failed
      while( ax < P_end ) {
        if( ax->type == STACK_ITEM_TYPE_BITVECTOR ) {

          // Match in primary segment (16MSB)
          if( (bv_match = (ax->bits >> 16) & unmatched_posvec) != 0 ) {
            ++n_primary;
          }
          // Match in secondary segment (16LSB)
          else if( (bv_match = ax->bits & unmatched_posvec) != 0 ) {
            ++n_secondary;
          }
          else {
            goto MISS;
          }

          // Turn off bit in lowest matched position
          unmatched_posvec ^= ( bv_match & (-(int16_t)bv_match) );
          goto NEXT_AX;

        }
        else if( ax->type == STACK_ITEM_TYPE_INTEGER ) {
          uint16_t secondary_posvec = 0;
          while( qx < end_qw ) {
            // Position must not already be matched by another token
            if( (posvec & unmatched_posvec) != 0 ) {
              // Compare  
              QWORD diff = ax->bits ^ *qx;
              // MSB are clear: Primary Match
              if( diff < 0x100000000ULL ) {
                ++n_primary;
                unmatched_posvec ^= posvec; // turn off bit in matched position
                goto NEXT_AX;
              }
              // LSB are clear: Secondary Match
              else if( !(diff & 0xFFFFFFFFULL) ) {
                // Not conclusive until all positions have been evaluated for primary match
                if( !secondary_posvec ) {
                  // Register the first secondary match to be used if all primary miss
                  secondary_posvec = posvec;
                }
              }
            }
            ++qx;
            posvec <<= 1;
          }
          // Primary miss, check if any secondary match
          if( secondary_posvec ) {
            ++n_secondary;
            unmatched_posvec ^= secondary_posvec; // turn off bit in matched position
            goto NEXT_AX;
          }
        }
        
        goto MISS;

      NEXT_AX:
        ++ax;
        qx = q0;
        posvec = posvec_reset;
      }

      // Match (but not prefix)
      //
      // Primary match
      if( n_secondary == 0 ) {
        return 1.0;
      }
      // Secondary match
      else {
        return (alt_deboost->type == STACK_ITEM_TYPE_REAL ? alt_deboost->real : (double)alt_deboost->integer);
      }
      //
      // ------------

    }
  }

MISS:
  return 0.0;
}



/*******************************************************************//**
 * probealtarray( P1, Pn, altarray, prefix_boost, alt_deboost )
 *
 *
 *
 ***********************************************************************
 */
static void __eval_probe_probealtarray( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t alt_deboost = POP_ITEM( self );
  vgx_EvalStackItem_t prefix_boost = POP_ITEM( self );
  vgx_EvalStackItem_t altarray = POP_ITEM( self );

  vgx_EvalStackItem_t *probe_0, *probe_end;
  int64_t nP = __slice( self, &probe_0, &probe_end );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_REAL;

  if( nP > 0 ) {
    switch( altarray.type ) {
    case STACK_ITEM_TYPE_CSTRING:
      px->real = __probealtarray_cstring( probe_0, probe_end, altarray.CSTR__str, &prefix_boost, &alt_deboost );
      return;
    default:
      break;
    }
  }

  px->real = 0;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static uint16_t __supertoken_mask( const QWORD *altarray, const QWORD *altarray_x, const QWORD *end_altarray, const QWORD segment_mask ) {
  uint16_t supertoken_mask = 0;
  
  // Find start of supertoken
  const QWORD *super = altarray_x;
  uint16_t super_pos = 1U << (int)(super - altarray);
  while( super > altarray && !(*super & segment_mask) ) { // <- we're at a dontcare position, continue searching for start
    --super;
    super_pos >>= 1;
  }

  // Build supertoken bitmask
  do {
    supertoken_mask |= super_pos;
    super_pos <<= 1;
    ++super;
  } while( super < end_altarray && !(*super & segment_mask) ); // boundary check: don't continue when altarray end is reached

  return supertoken_mask;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static uint16_t __consume_primary_supertoken( uint16_t *primary_unconsumed, const QWORD *altarray, const QWORD *altarray_x, const QWORD *end_altarray ) {
  *primary_unconsumed &= ~__supertoken_mask( altarray, altarray_x, end_altarray, 0xFFFFFFFF00000000ULL );
  return *primary_unconsumed;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static uint16_t __consume_secondary_supertoken( uint16_t *secondary_unconsumed, const QWORD *altarray, const QWORD *altarray_x, const QWORD *end_altarray ) {
  *secondary_unconsumed &= ~__supertoken_mask( altarray, altarray_x, end_altarray, 0x00000000FFFFFFFFULL );
  return *secondary_unconsumed;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
static double __probesuperaltarray_cstring( vgx_EvalStackItem_t *probe_0, vgx_EvalStackItem_t *probe_end, const CString_t *CSTR__altarray, vgx_EvalStackItem_t *prefix_boost, vgx_EvalStackItem_t *alt_deboost ) {
  int n_secondary = 0;
  const QWORD *altarray = CStringValueAsQwords( CSTR__altarray );
  // Number of qwords in altarray string
  int64_t len_altarray = VGX_CSTRING_ARRAY_LENGTH( CSTR__altarray );
  int64_t len_probe = probe_end - probe_0;
  // Check probe size
  if( len_probe <= len_altarray ) {
    // Altarray data: Dual 32-bit integer array (primary and secondary)
    //
    //    Array entry expected format: 
    //      PRIMARY   SECONDARY
    //      32 MSB    32 LSB
    //
    //
    // Query contains one or more item which be one of two things:
    //
    // 1) Integer with same format as array entries, i.e. dual 32-bit integer.
    //    Each query item is matched against array until first match, or until
    //    exhausted with no match. If no match query is a miss and terminates.
    //    For each query item match the primary segment (32MSB) is checked first.
    //    If match, the probe continues with the next query item. If miss, the 
    //    secondary segment (32LSB) is checked. If match a penalty is flagged
    //    which will incorporate the alt_deboost when producing final score.
    //    If secondary is also miss the next array item is compared. If no match
    //    is found for query item in array the probe is a miss and terminates.
    //    If all query items find a match the probe is successful. Note that a
    //    match "consumes" the matched array entry, ensuring that subsequent 
    //    identical query items will not match the same array entry twice.
    //    If query items align exactly in order with array entries a prefix boost
    //    is incorporated into the final score. Otherwise no prefix boost.
    //
    // 2) 32-bit bitvector where positions of 1s correspond to array indexes that
    //    should be considered matches.
    //  
    //    Bitvector expected format:
    //      PRIMARY    SECONDARY
    //      16 MSB     16 LSB
    // 
    //    No array scanning is performed since the bitvector already asserts
    //    array matches exist in the positions corresponding to 1s in the bitvector.
    //    The first unconsumed array entry lining up with a 1 in the bitvector is 
    //    then consumed and the probe continues with the next query item. If no
    //    bitvector 1s align with unconsumed array items the probe is a miss and
    //    terminates.
    //    Bitvector matching is first performed using the primary (16MSB) segment.
    //    If miss, the secondary (16LSB) segment is used instead, and a penalty is
    //    flagged to incorporate alt_deboost in the final score. If secondary is also
    //    a miss the probe terminates with a miss.
    //    NOTE: The maximum array length that is matchable by bitvectors is 16.
    //
    //
    //
    //
    if( CStringAttributes( CSTR__altarray ) & CSTRING_ATTR_ARRAY_INT ) {
      const QWORD *end_altarray = altarray + len_altarray; 
      const QWORD *altarray_0 = altarray;
      const QWORD *altarray_x = altarray_0;
      vgx_EvalStackItem_t *probe_x = probe_0;

      // 16 match positions
      uint16_t posvec = 1;

      uint16_t primary_unconsumed = 0xFFFF;
      uint16_t secondary_unconsumed = 0xFFFF;
      
      // Optimistic start
      while( probe_x < probe_end ) {
        // Probe element is bitvector, which needs a 1 in position corresponding to current qword
        if( probe_x->type == STACK_ITEM_TYPE_BITVECTOR ) {
          // Match in primary segment (16MSB)
          if( (primary_unconsumed & posvec) & (probe_x->bits >> 16) ) {

          }
          // Match in secondary segment (16LSB)
          else if( (secondary_unconsumed & posvec) & probe_x->bits ) {
            ++n_secondary;
          }
          // MISS
          else {
            goto NOT_PREFIX;
          }
          // Consume this position for both arrays
          primary_unconsumed &= ~posvec;
          secondary_unconsumed &= ~posvec;
        }
        // Probe element is dual 32-bit integer (assumed, not checked!): match primary or secondary
        else {
          // Compare  
          QWORD diff = probe_x->bits ^ *altarray_x;
          
          // MSB are clear: Primary Match
          if( (primary_unconsumed & posvec) && diff < 0x100000000ULL ) {
            // Consume primary token in this position
            primary_unconsumed &= ~posvec;
            // Consume secondary supertoken covering this position
            __consume_secondary_supertoken( &secondary_unconsumed, altarray, altarray_x, end_altarray );
          }
          // LSB are clear: Secondary Match
          else if( (secondary_unconsumed & posvec) && !(diff & 0xFFFFFFFFULL) ) {
            // Secondary match
            ++n_secondary;
            // Consume secondary token in this position
            secondary_unconsumed &= ~posvec;
            // Consume primary supertoken covering this position
            __consume_primary_supertoken( &primary_unconsumed, altarray, altarray_x, end_altarray );
          }
          // MISS
          else {
            goto NOT_PREFIX;
          }
        }

        // Next
        ++probe_x;
        ++altarray_x;
        posvec <<= 1;
      }

      // Prefix match!
      //
      double boost = prefix_boost->type == STACK_ITEM_TYPE_REAL ? prefix_boost->real : (double)prefix_boost->integer;
      // Primary match
      if( n_secondary == 0 ) {
        return boost;
      }
      // Secondary match
      else {
        return boost * (alt_deboost->type == STACK_ITEM_TYPE_REAL ? alt_deboost->real : (double)alt_deboost->integer);
      }
      //
      // ------------


    NOT_PREFIX:
      // Continue in non-prefix mode


      // Move qwords start to already matched prefix
      altarray_0 = altarray_x;

      // Initialize the posvec reset position to where prefix match left off
      uint16_t posvec_reset = posvec;
      uint16_t bv_match;
      uint16_t low_zero;

      // Continue probe from where prefix match failed
      while( probe_x < probe_end ) {
        if( probe_x->type == STACK_ITEM_TYPE_BITVECTOR ) {
          // Match in primary segment (16MSB)
          if( (bv_match = (probe_x->bits >> 16) & primary_unconsumed) != 0 ) {
            
          }
          // Match in secondary segment (16LSB)
          else if( (bv_match = probe_x->bits & secondary_unconsumed) != 0 ) {
            ++n_secondary;
          }
          else {
            goto MISS;
          }

          // Set zero in lowest matched position
          low_zero = ~(bv_match & (-(int16_t)bv_match));

          // Turn off bit in lowest matched position for both arrays
          primary_unconsumed &= low_zero;
          secondary_unconsumed &= low_zero;

          goto NEXT_AX;

        }
        else if( probe_x->type == STACK_ITEM_TYPE_INTEGER ) {
          const QWORD *secondary_x = NULL;
          while( altarray_x < end_altarray ) {
            // Compare  
            QWORD diff = probe_x->bits ^ *altarray_x;
            // MSB are clear: Primary Match
            if( (primary_unconsumed & posvec) && diff < 0x100000000ULL ) {
              // Consume primary token in this position
              primary_unconsumed &= ~posvec;
              // Consume secondary supertoken covering this position
              __consume_secondary_supertoken( &secondary_unconsumed, altarray, altarray_x, end_altarray );

              goto NEXT_AX;
            }
            // LSB are clear: Secondary Match
            else if( (secondary_unconsumed & posvec) && !(diff & 0xFFFFFFFFULL) ) {
              // Not conclusive until all positions have been evaluated for primary match
              if( !secondary_x ) {
                // Register the first secondary match to be used if all primary miss
                secondary_x = altarray_x;
              }
            }
            ++altarray_x;
            posvec <<= 1;
          }
          // Primary miss, check if any secondary match
          if( secondary_x ) {
            ++n_secondary;
            // Consume secondary token in this position
            secondary_unconsumed &= ~( 1U << (int)(secondary_x - altarray) );
            // Consume primary supertoken covering this position
            __consume_primary_supertoken( &primary_unconsumed, altarray, secondary_x, end_altarray );

            goto NEXT_AX;
          }
        }
        
        goto MISS;

      NEXT_AX:
        ++probe_x;
        altarray_x = altarray_0;
        posvec = posvec_reset;
      }

      // Match (but not prefix)
      //
      // Primary match
      if( n_secondary == 0 ) {
        return 1.0;
      }
      // Secondary match
      else {
        return (alt_deboost->type == STACK_ITEM_TYPE_REAL ? alt_deboost->real : (double)alt_deboost->integer);
      }
      //
      // ------------

    }
  }

MISS:
  return 0.0;
}



/*******************************************************************//**
 * probesuperarray( P1, Pn, superarray, prefix_boost, alt_deboost )
 *
 *
 *
 ***********************************************************************
 */
static void __eval_probe_probesuperarray( vgx_Evaluator_t *self ) {
  vgx_EvalStackItem_t alt_deboost = POP_ITEM( self );
  vgx_EvalStackItem_t prefix_boost = POP_ITEM( self );
  vgx_EvalStackItem_t altarray = POP_ITEM( self );

  vgx_EvalStackItem_t *probe_0, *probe_end;
  int64_t nP = __slice( self, &probe_0, &probe_end );

  vgx_EvalStackItem_t *px = NEXT_PITEM( self );
  px->type = STACK_ITEM_TYPE_REAL;

  if( nP > 0 ) {
    switch( altarray.type ) {
    case STACK_ITEM_TYPE_CSTRING:
      px->real = __probesuperaltarray_cstring( probe_0, probe_end, altarray.CSTR__str, &prefix_boost, &alt_deboost );
      return;
    default:
      break;
    }
  }

  px->real = 0;
}



#endif
