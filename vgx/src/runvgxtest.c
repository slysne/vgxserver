#define _FORCE_VGX_IMPORT
#define _FORCE_FRAMEHASH_IMPORT

#include "_vgx.h"
#include "_framehash.h"

SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_SELFTEST );


static int PrintLine( const char *format, ... ) {
  va_list args;
  char tbuf[32];
  char *pmilli = tbuf+20; // (see why below)
  struct tm *now;
  int64_t milliseconds_since_epoch = __MILLISECONDS_SINCE_1970();
  time_t seconds_since_epoch = milliseconds_since_epoch / 1000;
  now = localtime( &seconds_since_epoch );
  strftime( tbuf, 31, "%Y-%m-%d %H:%M:%S.", now ); 
  //                 2016-07-07 12:34:56.---
  //                 0123456789..........^
  //                                     20
  sprintf( pmilli, "%03lld", milliseconds_since_epoch % 1000 ); // since system start (not epoch) but good enough for logging, we just want more resolution between seconds
  va_start( args, format );
  fprintf( stdout, "[%s]    ", tbuf );
  vfprintf( stdout, format, args );
  fprintf( stdout, "\n" );
  va_end( args );
  fflush( stdout );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int print_test_names( void ) {

  typedef char ** (*f_get)(void);

  typedef struct s__slot {
    const char *libname;
    f_get getter;
  } __slot;

  __slot slots[] = {
    { "comlib",     comlib_get_unit_test_names },
    { "framehash",  iFramehash.test.GetTestNames },
    { "vgx",        vgx_get_unit_test_names },
    { NULL,      NULL }
  };

  __slot *slot = slots;
  
  while( slot->getter ) {
    char **names = slot->getter();
    if( names ) {
      char **cursor = names, *name;
      while( (name = *cursor++) != NULL ) {
        PrintLine( "%s:%s", slot->libname, name );
      }
      free( names );
    }
    ++slot;
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static char ** get_selected_tests( const char *prefix, const char **names ) {
  char **selected = NULL;

  XTRY {
    int n = 0;
    const char **cursor = names;
    const char *name;
    while( (name = *cursor++) != NULL ) {
      if( CharsStartsWithConst( name, prefix ) ) {
        ++n;
      }
    }
    if( n == 0 ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_GENERAL, 0x001, "No matching tests for '%s'", prefix );
    }

    if( (selected = calloc( n+1, sizeof( char* ) )) == NULL ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_MEMORY, 0x002, "Memory error" );
    }

    const char **src = names;
    char **dest = selected;
    while( (name = *src++) != NULL ) {
      if( CharsStartsWithConst( name, prefix ) ) {
        *dest++ = (char*)name;
      }
    }

  }
  XCATCH( errcode ) {
    if( selected ) {
      free( selected );
      selected = NULL;
    }
  }
  XFINALLY {
  }
  return selected;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int run_selected_test( const char *testroot, const char *prefix ) {
  int ret = 0;

  char **names = NULL;
  char **selected = NULL;

  const char *query;

  static const char *vgx_prefix = "vgx:";
  static const char *comlib_prefix = "comlib:";
  static const char *framehash_prefix = "framehash:";

  XTRY {
    if( CharsStartsWithConst( prefix, vgx_prefix ) ) {
      query = prefix + strlen( vgx_prefix );
      if( (names = vgx_get_unit_test_names()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
      }
      if( (selected = get_selected_tests( query, (const char**)names )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }
      if( vgx_unit_tests( (const char**)selected, testroot ) != 0 ) {
        THROW_ERROR( CXLIB_ERR_ASSERTION, 0x003 );
      }
    }
    else if( CharsStartsWithConst( prefix, comlib_prefix ) ) {
      query = prefix + strlen( comlib_prefix );
      if( (names = comlib_get_unit_test_names()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }
      if( (selected = get_selected_tests( query, (const char**)names )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }
      if( comlib_unit_tests( (const char**)selected, testroot ) != 0 ) {
        THROW_ERROR( CXLIB_ERR_ASSERTION, 0x006 );
      }
    }
    else if( CharsStartsWithConst( prefix, framehash_prefix ) ) {
      query = prefix + strlen( framehash_prefix );
      if( (names = iFramehash.test.GetTestNames()) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x007 );
      }
      if( (selected = get_selected_tests( query, (const char**)names )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x008 );
      }
      if( iFramehash.test.RunUnitTests( (const char**)selected, testroot ) != 0 ) {
        THROW_ERROR( CXLIB_ERR_ASSERTION, 0x009 );
      }
    }


  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    free( selected );
    free( names );
  }
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int run_all_vgx_tests( const char *testroot ) {

  // VGX
  if( vgx_unit_tests( NULL, testroot ) != 0 ) {
    return -1;
  }

  // OK
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int run_all_comlib_tests( const char *testroot ) {

  // COMLIB
  if( comlib_unit_tests( NULL, testroot ) != 0 ) {
    return -1;
  }

  // OK
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int run_all_framehash_tests( const char *testroot ) {

  // FRAMEHASH
  if( iFramehash.test.RunUnitTests( NULL, testroot ) != 0 ) {
    return -1;
  }

  // OK
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int run_all_tests( const char *testroot ) {

  // COMLIB
  if( run_all_comlib_tests( testroot ) < 0 ) {
    return -1;
  }

  // FRAMEHASH
  if( run_all_framehash_tests( testroot ) < 0 ) {
    return -1;
  }

  // VGX
  if( run_all_vgx_tests( testroot ) < 0 ) {
    return -1;
  }

  // OK
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int main( int argc, const char *argv[] ) {


#if defined CXPLAT_WINDOWS_X64
  const char *default_testroot = "C:/vgxroot/utest";
#else
  const char *default_testroot = "/usr/vgxroot/utest";
#endif
  const char *testroot = default_testroot;;
  int ret = 0;

  if( argc > 1 ) {
    SET_EXCEPTION_CONTEXT
    vgx_context_t *vgx = vgx_INIT( NULL );
    const char *selected = argv[1];
    if( argc > 2 ) {
      testroot = argv[2];
    }

    PrintLine( "Any key to begin test (dir='%s')", testroot );
    if( getc( stdin ) ) {}

    if( dir_exists( testroot ) ) {
      // Require manual deletion of testdir unless default dir is used (to avoid accidents)
      if( testroot != default_testroot || delete_dir( testroot ) < 0 ) {
        PrintLine( "Remove directory '%s' before running unit tests", testroot );
        return -1;
      }
    }

    // Run full set of tests (vgx and deeper components)
    if( CharsEqualsConst( selected, "*" ) ) {
      ret = run_all_tests( testroot );
    }
    // Run full set of vgx tests only
    else if( CharsEqualsConst( selected, "vgx*" ) ) {
      ret = run_all_vgx_tests( testroot );
    }
    // Run full set of comlib tests only
    else if( CharsEqualsConst( selected, "comlib*" ) ) {
      ret = run_all_comlib_tests( testroot );
    }
    // Run full set of framehash tests only
    else if( CharsEqualsConst( selected, "framehash*" ) ) {
      ret = run_all_framehash_tests( testroot );
    }
    // Run specific test
    else {
      ret = run_selected_test( testroot, selected );
    }

    vgx_DESTROY( &vgx );
    sleep_milliseconds( 2000 ); 
    if( ret == 0 ) {
      PrintLine( "============" );
      PrintLine( "== PASSED ==" );
      PrintLine( "============" );
      return 0;
    }
    else {
      PrintLine( "============" );
      PrintLine( "== FAILED ==" );
      PrintLine( "============" );
      return -1;
    }
  }
  else {
    PrintLine( "===============" );
    PrintLine( "Available tests" );
    PrintLine( "===============" );
    print_test_names();
  }
    
  return 0;
}
