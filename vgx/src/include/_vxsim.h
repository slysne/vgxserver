/*
###################################################
#
# File:   _vxsim.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXSIM_H
#define _VGX_VXSIM_H


#include "_vxalloc.h"

/*

>>> def decode( mag ):
...   return struct.unpack( "f", struct.pack("I", (((int(mag)&0x3f) << 20) | 0x3c000000)) )[0]


>>> def encode( weight ):
...   bits = struct.unpack( "I", struct.pack("f", weight) )[0]
...   high = bits & 0xfc000000
...   if high == 0x3c000000:
...     return (bits & 0x03f00000) >> 20
...   elif high > 0x3c000000:
...     return 63
...   else:
...     return 0
...


*/



/*******************************************************************//**
 * Pseudo-float with 6-bit resolution and limited range represented in
 * a compact lookup table. Generated directly from the binary format of
 * single precision floating point (little-endian).
 *
 *   |        |        |        |        |
 *   |seeeeeee|efffffff|ffffffff|ffffffff|
 *   |        |        |        |        |
 *    001111^^ ^^^^0000 00000000 00000000
 *          -------
 *
 * >>> def cast(x):
 * ...   return struct.unpack( "f", struct.pack("I", mask | (x << shift)) )[0]
 *
 * >>> for n in range(64):
 * ...   print int(round((cast(n)**1) * (136**1))),
 *
 * The external floating point weight range goes from 0.078125 to 1.875. Six bits
 * are extracted from the middle of the float DWORD (3 from the exponent, 3 from the
 * fraction part) to represent the number in a low-resolution integer form. The map
 * pivots around a unity value of 136. This is done to maximize the resolution of
 * a resulting 8-bit lookup value, a choice made in order to fit the entire table
 * into a single cache line. I.e. the maximum representable weight of 1.875 is extracted
 * from the float as "111111" and maps to 255 in the table. The unity value 1.0 is
 * extracted as "111000" and maps to 136 in the table.
 *
 * Here are some sample points:
 * 17  = 0.125
 * 34  = 0.25
 * 68  = 0.5
 * 136 = 1.0     <= UNITY
 * 204 = 1.5
 * 255 = 1.875
 *
 * 64 bytes table = 1 CL
 ***********************************************************************
 */
CALIGNED_ static uint8_t _vxsim_magnitude_map[] = {
    1,   1,   1,   1,   2,   2,   2,   2,
    2,   2,   3,   3,   3,   3,   4,   4,
    4,   5,   5,   6,   6,   7,   7,   8,
    9,  10,  11,  12,  13,  14,  15,  16,
   17,  19,  21,  23,  26,  28,  30,  32,
   34,  38,  43,  47,  51,  55,  60,  64,
   68,  77,  85,  94, 102, 111, 119, 128,
  136,  // <= UNITY
        153, 170, 187, 204, 221, 238, 255
};

static const int _vxsim_magnitude_unity = 136;  // 0x88  0b10001000
static const int _vxsim_magnitude_max = 255;    // 0xFF  0b11111111



/*******************************************************************//**
 *
 * 128 bytes table = 2 CL
 ***********************************************************************
 */
CALIGNED_ static uint16_t _vxsim_square_magnitude_map[] = {
      1,     1,     2,     2,     3,     3,     3,     4,
      5,     6,     7,     9,    10,    12,    14,    16,
     18,    23,    28,    34,    41,    48,    55,    64,
     72,    91,   113,   137,   163,   191,   221,   254,
    289,   366,   452,   546,   650,   763,   885,  1016,
   1156,  1463,  1806,  2186,  2601,  3053,  3540,  4064,
   4624,  5852,  7225,  8742, 10404, 12210, 14161, 16256,
  18496, // <= UNITY
         23409, 28900, 34969, 41616, 48841, 56644, 65025
};

static const int _vxsim_square_magnitude_unity = 18496;  // 0x4840  0b0100100001000000
static const int _vxsim_square_magnitude_max = 65025;    // 0xFE01  0b1111111000000001





static const __float_bits_t _vxsim_vmag_inrange = { .f = 1.0f };


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static feature_vector_magnitude_enum _vxsim_weight_as_magnitude( float weight ) {
  // Convert single precision floating point (little-endian assumed) directly
  // to an 6-bit magnitude code.
  // float format:
  // |        |        |        |        |
  // |seeeeeee|efffffff|ffffffff|ffffffff|
  // |        |        |        |        |
  //  001111^^ ^^^^0000 00000000 00000000
  //        -------
  // The indicated variable bits are used directly, with the other
  // bits assumed constant as shows. This encodes the range
  // 0.0078125 - 1.875 (non-linear) as bits 000000 - 111111.
  //
  __float_bits_t bits = { .f = weight };

  if( bits.vmag.high == _vxsim_vmag_inrange.vmag.high ) {
    return (feature_vector_magnitude_enum)bits.vmag.code;
  }
  else if( bits.vmag.high > _vxsim_vmag_inrange.vmag.high ) {
    return FEATURE_VECTOR_MAGNITUDE_MAX;
  }
  else {
    return FEATURE_VECTOR_MAGNITUDE_ZERO;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_magnitude_as_weight( feature_vector_magnitude_enum mag ) {
  // Decode magnitude to a single precision float.
  __float_bits_t bits = _vxsim_vmag_inrange;
  bits.vmag.code = mag;
  return bits.f;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_quantize_weight( float weight ) {
  return _vxsim_magnitude_as_weight( _vxsim_weight_as_magnitude( weight ) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_magnitude_from_imag( uint32_t imag ) {
  const float unity = (float)_vxsim_magnitude_unity;
  __float_bits_t magnitude = { .f = imag / unity };
  magnitude.trunc.discard = 0;
  return magnitude.f;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_square_magnitude_from_isqmag( uint32_t isqmag ) {
  const float sq_unity = (float)_vxsim_square_magnitude_unity;
  __float_bits_t magnitude = { .f = isqmag / sq_unity };
  magnitude.trunc.discard = 0;
  return magnitude.f;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_multiply_quantized( float x, float y ) {
  __float_bits_t product = { .f = x * y };
  product.trunc.discard = 0;
  return product.f;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float _vxsim_divide_quantized( float x, float y ) {
  __float_bits_t ratio = { .f = x / y };
  ratio.trunc.discard = 0;
  return ratio.f;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static double __euclidean_scaling_factor( const float *farray, int sz ) {
  const float *fxelem = farray;
  const float *fxend = farray + sz;

  // Find max element absolute value and compute normalization scaling factor
  double maxabs = 0;
  while( fxelem < fxend ) {
    double a = fabs( *fxelem++ );
    if( a > maxabs ) {
      maxabs = a;
    }
  }

  return maxabs / 127.0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static double __euclidean_scaling_factor_dbl( const double *darray, int sz ) {
  const double *dxelem = darray;
  const double *dxend = darray + sz;

  // Find max element absolute value and compute normalization scaling factor
  double maxabs = 0;
  while( dxelem < dxend ) {
    double a = fabs( *dxelem++ );
    if( a > maxabs ) {
      maxabs = a;
    }
  }

  return maxabs / 127.0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char __encode_float_to_char( float x, double inv_scale ) {
  // Assumption: abs(x) <= 1
  return (char)round( inv_scale * x );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static char __encode_double_to_char( double x, double inv_scale ) {
  // Assumption: abs(x) <= 1
  return (char)round( inv_scale * x );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static float __decode_char_to_float( char x, double factor ) {
  return (float)(factor * x);
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static double __decode_char_to_double( char x, double factor ) {
  return factor * x;
}







#endif
