/*######################################################################
 *#
 *# framemath.c
 *#
 *#
 *######################################################################
 */


#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_FRAMEHASH );


typedef struct {
  int64_t n;
  double sum;
  double sqsum;
} __cell_accumulator_t;


// MULTIPLY
static int64_t __f_process_math_fmul( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_imul( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// ADD
static int64_t __f_process_math_fadd( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_iadd( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_sqrt( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// POWER
static int64_t __f_process_math_pow( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_square( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_cube( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// LOG
static int64_t __f_process_math_log( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_log2( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// EXP
static int64_t __f_process_math_exp_base( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_exp( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// DECAY
static int64_t __f_process_math_decay( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// SET
static int64_t __f_process_math_set( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// RANDOMIZE
static int64_t __f_process_math_randomize( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// CAST
static int64_t __f_process_math_int( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_float( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// MISC
static int64_t __f_process_math_abs( framehash_processing_context_t * const context, framehash_cell_t * const cell );

// SUM
static int64_t __f_process_math_sum( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_avg( framehash_processing_context_t * const context, framehash_cell_t * const cell );
static int64_t __f_process_math_stdev( framehash_processing_context_t * const context, framehash_cell_t * const cell );


/*******************************************************************//**
 * mul
 *
 ***********************************************************************
 */
// CELL PROCESSOR: floating point multiplication
static int64_t __f_process_math_fmul( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double factor = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, _CELL_AS_UNSIGNED( cell ) * factor );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, _CELL_AS_INTEGER( cell ) * factor );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, _CELL_GET_REAL( cell ) * factor );
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: integer multiplication
static int64_t __f_process_math_imul( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  int32_t factor = (int32_t)(*(double*)(context->processor.input));
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_UNSIGNED( cell, _CELL_AS_UNSIGNED( cell ) * factor );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_INTEGER( cell, _CELL_AS_INTEGER( cell ) * factor );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, _CELL_GET_REAL( cell ) * factor );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: multiplication
DLL_HIDDEN int64_t _framehash_framemath__mul( framehash_t * const self, double factor ) {
  f_framehash_cell_processor_t mulfunc;
  if( floor(factor) == factor && factor < CXLIB_LONG_MAX && factor > CXLIB_LONG_MIN ) {
    mulfunc = __f_process_math_imul;
  }
  else {
    mulfunc = __f_process_math_fmul;
  }
  framehash_processing_context_t math_mul = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, mulfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &math_mul, &factor, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_mul );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_mul );
  return _framehash_processor__process( &math_mul );
}



/*******************************************************************//**
 * add
 *
 ***********************************************************************
 */
// CELL PROCESSOR: floating point addition
static int64_t __f_process_math_fadd( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double value = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, _CELL_AS_UNSIGNED( cell ) + value );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, _CELL_AS_INTEGER( cell ) + value );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, _CELL_GET_REAL( cell ) + value );
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: integer addition
static int64_t __f_process_math_iadd( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  int32_t value = (int32_t)(*(double*)(context->processor.input));
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_UNSIGNED( cell, _CELL_AS_UNSIGNED( cell ) + value );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_INTEGER( cell, _CELL_AS_INTEGER( cell ) + value );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, _CELL_GET_REAL( cell ) + value );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: addition
DLL_HIDDEN int64_t _framehash_framemath__add( framehash_t * const self, double value ) {
  f_framehash_cell_processor_t addfunc;
  if( floor(value) == value && value < CXLIB_LONG_MAX && value > CXLIB_LONG_MIN ) {
    addfunc = __f_process_math_iadd;
  }
  else {
    addfunc = __f_process_math_fadd;
  }
  framehash_processing_context_t math_add = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, addfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &math_add, &value, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_add );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_add );
  return _framehash_processor__process( &math_add );
}



/*******************************************************************//**
 * sqrt
 *
 ***********************************************************************
 */
// CELL PROCESSOR: square root
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_sqrt( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, sqrt( (double)_CELL_AS_UNSIGNED( cell ) ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, sqrt( (double)_CELL_AS_INTEGER( cell ) ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, sqrt(_CELL_GET_REAL( cell )) );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: square root
DLL_HIDDEN int64_t _framehash_framemath__sqrt( framehash_t * const self ) {
  framehash_processing_context_t math_sqrt = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_sqrt );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_sqrt );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_sqrt );
  return _framehash_processor__process( &math_sqrt );
}



/*******************************************************************//**
 * pow
 *
 ***********************************************************************
 */
// CELL PROCESSOR: power
static int64_t __f_process_math_pow( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double exponent = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, pow( (double)_CELL_AS_UNSIGNED( cell ), exponent ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, pow( (double)_CELL_AS_INTEGER( cell ), exponent ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, pow( _CELL_GET_REAL( cell ), exponent ) );
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: square
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_square( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  uint64_t ival;
  double fval;
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      ival = _CELL_AS_UNSIGNED( cell );
      _CELL_SET_UNSIGNED( cell, ival * ival );
      return 1;
    case TAGGED_DTYPE_INT56:
      ival = _CELL_AS_INTEGER( cell );
      _CELL_SET_INTEGER( cell, ival * ival );
      return 1;
    case TAGGED_DTYPE_REAL56:
      fval = _CELL_GET_REAL( cell );
      _CELL_SET_REAL( cell, fval * fval );
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: cube
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_cube( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  uint64_t ival;
  double fval;
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      ival = _CELL_AS_UNSIGNED( cell );
      _CELL_SET_UNSIGNED( cell, ival * ival * ival );
      return 1;
    case TAGGED_DTYPE_INT56:
      ival = _CELL_AS_INTEGER( cell );
      _CELL_SET_INTEGER( cell, ival * ival * ival );
      return 1;
    case TAGGED_DTYPE_REAL56:
      fval = _CELL_GET_REAL( cell );
      _CELL_SET_REAL( cell, fval * fval * fval );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: power
DLL_HIDDEN int64_t _framehash_framemath__pow( framehash_t * const self, double exponent ) {
  f_framehash_cell_processor_t powfunc;
  if( exponent == 2.0 ) {
    powfunc = __f_process_math_square;
  }
  else if( exponent == 3.0 ) {
    powfunc = __f_process_math_cube;
  }
  else {
    powfunc = __f_process_math_pow;
  }
  framehash_processing_context_t math_pow = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, powfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &math_pow, &exponent, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_pow );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_pow );
  return _framehash_processor__process( &math_pow );
}



/*******************************************************************//**
 * log
 *
 ***********************************************************************
 */
// CELL PROCESSOR: logarithm (any base)
static int64_t __f_process_math_log( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double base_factor = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, log( (double)_CELL_AS_UNSIGNED( cell ) ) * base_factor );
      return 1;
    case TAGGED_DTYPE_INT56:
      if( _CELL_AS_INTEGER( cell ) < 0 ) {
        _CELL_SET_REAL( cell, NAN );
      }
      else {
        _CELL_SET_REAL( cell, log( (double)_CELL_AS_INTEGER( cell ) ) * base_factor );
      }
      return 1;
    case TAGGED_DTYPE_REAL56:
      if( _CELL_GET_REAL( cell ) < 0 ) {
        _CELL_SET_REAL( cell, NAN );
      }
      else {
        _CELL_SET_REAL( cell, log( _CELL_GET_REAL( cell ) ) * base_factor );
      }
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: logarithm (base 2)
#define __log2inv 1.4426950408889634
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_log2( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_UNSIGNED( cell, ilog2( _CELL_AS_UNSIGNED( cell ) ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_INTEGER( cell, ilog2( _CELL_AS_INTEGER( cell ) ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      if( _CELL_GET_REAL( cell ) < 0 ) {
        _CELL_SET_REAL( cell, NAN );
      }
      else {
        _CELL_SET_REAL( cell, log( _CELL_GET_REAL( cell ) ) * __log2inv );
      }
      return 1;
    }
  }
  return 0;
}

// INTERFACE: logarithm
DLL_HIDDEN int64_t _framehash_framemath__log( framehash_t * const self, double base ) {
  double base_factor;
  f_framehash_cell_processor_t logfunc;
  if( base == 2.0 ) {
    logfunc = __f_process_math_log2;
  }
  else if( base > 0 ) {
    base_factor = 1.0 / log(base);
    logfunc = __f_process_math_log;
  }
  else {
    return -1;
  }

  framehash_processing_context_t math_log = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, logfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &math_log, &base_factor, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_log );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_log );
  return _framehash_processor__process( &math_log );
}



/*******************************************************************//**
 * exp
 *
 ***********************************************************************
 */
// CELL PROCESSOR: exponentiation (any base)
static int64_t __f_process_math_exp_base( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double base = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, pow( base, (double)_CELL_AS_UNSIGNED( cell ) ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, pow( base, (double)_CELL_AS_INTEGER( cell ) ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, pow( base, _CELL_GET_REAL( cell ) ) );
      return 1;
    }
  }
  return 0;
}

// CELL PROCESSOR: exponentiation (base = e)
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_exp( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, exp( (double)_CELL_AS_UNSIGNED( cell ) ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, exp( (double)_CELL_AS_INTEGER( cell ) ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, exp( _CELL_GET_REAL( cell ) ) );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: exponentiation
DLL_HIDDEN int64_t _framehash_framemath__exp( framehash_t * const self, double base ) {
  f_framehash_cell_processor_t expfunc;
  if( base == 0.0 ) { // base=0 is shorthand for base=e, but e is hard to check for and 0 is meaningless anyway
    expfunc = __f_process_math_exp;
  }
  else if( base > 0 ) {
    expfunc = __f_process_math_exp_base;
  }
  else {
    return -1;
  }
  framehash_processing_context_t math_exp = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, expfunc );
  FRAMEHASH_PROCESSOR_SET_IO( &math_exp, &base, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_exp );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_exp );
  return _framehash_processor__process( &math_exp );
}



/*******************************************************************//**
 * decay
 *
 ***********************************************************************
 */
// CELL PROCESSOR: exponential decay
static int64_t __f_process_math_decay( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double decay_factor = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, _CELL_AS_UNSIGNED( cell ) * decay_factor );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, _CELL_AS_INTEGER( cell ) * decay_factor );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, _CELL_GET_REAL( cell ) * decay_factor );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: exponential decay
DLL_HIDDEN int64_t _framehash_framemath__decay( framehash_t * const self, double exponent ) {
  double factor = exp( exponent );
  framehash_processing_context_t math_decay = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_decay );
  FRAMEHASH_PROCESSOR_SET_IO( &math_decay, &factor, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_decay );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_decay );
  return _framehash_processor__process( &math_decay );
}



/*******************************************************************//**
 * set
 *
 ***********************************************************************
 */
// CELL PROCESSOR: set cell value (preserve existing data type)
static int64_t __f_process_math_set( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double value = *(double*)(context->processor.input);
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_UNSIGNED( cell, iround( value ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_INTEGER( cell, iround( value ) );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, value );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: set already populated cells to the same value, preserving existing data type in cells
DLL_HIDDEN int64_t _framehash_framemath__set( framehash_t * const self, double value ) {
  framehash_processing_context_t math_set = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_set );
  FRAMEHASH_PROCESSOR_SET_IO( &math_set, &value, NULL );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_set );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_set );
  return _framehash_processor__process( &math_set );
}



/*******************************************************************//**
 * randomize
 *
 ***********************************************************************
 */
// CELL PROCESSOR: set cell to a random value
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_randomize( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_UNSIGNED( cell, rand56() );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_INTEGER( cell, (int64_t)rand55() );
      return 1;
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_REAL( cell, rand40()/(double)ULLONG_MAX );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: set already populated cells to a random value, preserving existing data type in cells
DLL_HIDDEN int64_t _framehash_framemath__randomize( framehash_t * const self ) {
  framehash_processing_context_t math_randomize = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_randomize );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_randomize );
  return _framehash_processor__process( &math_randomize );
}


/*******************************************************************//**
 * cast to int
 *
 ***********************************************************************
 */
// CELL PROCESSOR: cast to int
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_int( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_REAL56:
      _CELL_SET_INTEGER( cell, iround(_CELL_GET_REAL( cell )) );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: cast to int
DLL_HIDDEN int64_t _framehash_framemath__int( framehash_t * const self ) {
  framehash_processing_context_t math_int = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_int );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_int );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_int );
  return _framehash_processor__process( &math_int );
}


/*******************************************************************//**
 * cast to float
 *
 ***********************************************************************
 */
// CELL PROCESSOR: cast to float
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_float( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_UINT56:
      _CELL_SET_REAL( cell, (double)_CELL_AS_UNSIGNED( cell ) );
      return 1;
    case TAGGED_DTYPE_INT56:
      _CELL_SET_REAL( cell, (double)_CELL_AS_INTEGER( cell ) );
      return 1;
    }
  }
  return 0;
}


// INTERFACE: cast to float
DLL_HIDDEN int64_t _framehash_framemath__float( framehash_t * const self ) {
  framehash_processing_context_t math_float = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_float );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_float );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_float );
  return _framehash_processor__process( &math_float );
}


/*******************************************************************//**
 * absolute value
 *
 ***********************************************************************
 */
// CELL PROCESSOR: absolute value
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
static int64_t __f_process_math_abs( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
      int64_t i;
      double r;
    case TAGGED_DTYPE_INT56:
      i = _CELL_AS_INTEGER( cell );
      if( i < 0 ) {
        _CELL_SET_INTEGER( cell, -i );
      }
      return 1;
    case TAGGED_DTYPE_REAL56:
      r = _CELL_GET_REAL( cell );
      if( r < 0 ) {
        _CELL_SET_REAL( cell, -r );
      }
      return 1;
    }
  }
  return 0;
}


// INTERFACE: absolute value
DLL_HIDDEN int64_t _framehash_framemath__abs( framehash_t * const self ) {
  framehash_processing_context_t math_abs = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_abs );
  FRAMEHASH_PROCESSOR_MAY_MODIFY( &math_abs );
  FRAMEHASH_PROCESSOR_PRESERVE_CACHE( &math_abs );
  return _framehash_processor__process( &math_abs );
}


/*******************************************************************//**
 * sum
 *
 ***********************************************************************
 */
// CELL PROCESSOR: sum of cell values
static int64_t __f_process_math_sum( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  double *accumulator = (double*)context->processor.output;
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_BOOL:
      *accumulator += _CELL_GET_BOOLEAN( cell );
      return 1;
    case TAGGED_DTYPE_UINT56:
      *accumulator += _CELL_AS_UNSIGNED( cell );
      return 1;
    case TAGGED_DTYPE_INT56:
      *accumulator += _CELL_AS_INTEGER( cell );
      return 1;
    case TAGGED_DTYPE_REAL56:
      *accumulator += _CELL_GET_REAL( cell );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: sum of cell values
DLL_HIDDEN int64_t _framehash_framemath__sum( framehash_t * const self, double *sum ) {
  framehash_processing_context_t math_sum = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_sum );
  FRAMEHASH_PROCESSOR_SET_IO( &math_sum, NULL, sum );
  return _framehash_processor__process( &math_sum );
}



/*******************************************************************//**
 * avg
 *
 ***********************************************************************
 */
// CELL PROCESSOR: average cell value
static int64_t __f_process_math_avg( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  __cell_accumulator_t *accumulator = (__cell_accumulator_t*)context->processor.output;
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_BOOL:
      accumulator->sum += _CELL_GET_BOOLEAN( cell );
      return 1;
    case TAGGED_DTYPE_UINT56:
      accumulator->sum += _CELL_AS_UNSIGNED( cell );
      return 1;
    case TAGGED_DTYPE_INT56:
      accumulator->sum += _CELL_AS_INTEGER( cell );
      return 1;
    case TAGGED_DTYPE_REAL56:
      accumulator->sum += _CELL_GET_REAL( cell );
      return 1;
    }
  }
  return 0;
}

// INTERFACE: average cell value
DLL_HIDDEN int64_t _framehash_framemath__avg( framehash_t * const self, double *avg ) {
  __cell_accumulator_t acc = {0};
  framehash_processing_context_t math_avg = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_avg );
  FRAMEHASH_PROCESSOR_SET_IO( &math_avg, NULL, &acc );
  if( (acc.n = _framehash_processor__process( &math_avg )) < 0 ) {
    return -1;
  }
  else {
    if( acc.n > 0 ) {
      *avg = acc.sum / acc.n;
    }
    else {
      *avg = 0.0;
    }
    return acc.n;
  }
}



/*******************************************************************//**
 * stdev
 *
 ***********************************************************************
 */
// CELL PROCESSOR: cell standard deviation
static int64_t __f_process_math_stdev( framehash_processing_context_t * const context, framehash_cell_t * const cell ) {
  uint64_t u;
  int64_t i;
  double d;
  __cell_accumulator_t *accumulator = (__cell_accumulator_t*)context->processor.output;
  if( _CELL_IS_VALUE( cell ) ) {
    switch( _CELL_VALUE_DTYPE( cell ) ) {
    case TAGGED_DTYPE_BOOL:
      u = _CELL_GET_BOOLEAN( cell );
      accumulator->sum += u;
      accumulator->sqsum += u*u;
      return 1;
    case TAGGED_DTYPE_UINT56:
      u = _CELL_AS_UNSIGNED( cell );
      accumulator->sum += u;
      accumulator->sqsum += u*u;
      return 1;
    case TAGGED_DTYPE_INT56:
      i = _CELL_AS_INTEGER( cell );
      accumulator->sum += i;
      accumulator->sqsum += i*i;
      return 1;
    case TAGGED_DTYPE_REAL56:
      d = _CELL_GET_REAL( cell );
      accumulator->sum += d;
      accumulator->sqsum += d*d;
      return 1;
    }
  }
  return 0;
}

// INTERFACE: cell standard deviation
DLL_HIDDEN int64_t _framehash_framemath__stdev( framehash_t * const self, double *stdev ) {
  __cell_accumulator_t acc = {0};
  framehash_processing_context_t math_stdev = FRAMEHASH_PROCESSOR_NEW_CONTEXT( &self->_topframe, &self->_dynamic, __f_process_math_stdev );
  FRAMEHASH_PROCESSOR_SET_IO( &math_stdev, NULL, &acc );
  if( (acc.n = _framehash_processor__process( &math_stdev )) < 0 ) {
    return -1;
  }
  else {
    if( acc.n > 0 ) {
      double avg_sq = acc.sqsum / acc.n;
      double avg = acc.sum / acc.n;
      *stdev = sqrt( avg_sq - avg*avg );
    }
    else {
      *stdev = 0.0;
    }
    return acc.n;
  }
}



DLL_HIDDEN IMathCellProcessors_t _framehash_framemath__iMathCellProcessors = {
  // MULTIPLY
  .math_fmul      = __f_process_math_fmul,
  .math_imul      = __f_process_math_imul,
  // ADD
  .math_fadd      = __f_process_math_fadd,
  .math_iadd      = __f_process_math_iadd,
  .math_sqrt      = __f_process_math_sqrt,
  // POWER
  .math_pow       = __f_process_math_pow,
  .math_square    = __f_process_math_square,
  .math_cube      = __f_process_math_cube,
  // LOG
  .math_log       = __f_process_math_log,
  .math_log2      = __f_process_math_log2,
  // EXP
  .math_exp_base  = __f_process_math_exp_base,
  .math_exp       = __f_process_math_exp,
  // DECAY
  .math_decay     = __f_process_math_decay,
  // SET
  .math_set       = __f_process_math_set,
  // RANDOMIZE
  .math_randomize = __f_process_math_randomize,
  // CAST
  .math_int       = __f_process_math_int,
  .math_float     = __f_process_math_float,
  // MISC
  .math_abs       = __f_process_math_abs,
  // SUM
  .math_sum       = __f_process_math_sum,
  .math_avg       = __f_process_math_avg,
  __f_process_math_stdev
};




#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_framehash_framemath.h"

DLL_HIDDEN test_descriptor_t _framehash_framemath_tests[] = {
  { "framemath",   __utest_framehash_framemath },
  {NULL}
};
#endif

