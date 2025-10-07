/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    __utest_cxcstring.h
 * Author:  Stian Lysne slysne.dev@gmail.com
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

#ifndef __UTEST_CXCSTRING_H
#define __UTEST_CXCSTRING_H


#include "comlibsequence/__comlibtest_macro.h"




/**************************************************************************//**
 * random_char_string
 *
 ******************************************************************************
 */
static char * random_char_string( size_t sz, char min, char max ) {
  char range = max - min;
  if( range < 0 ) {
    return NULL;
  }
  char *str = malloc( sz + 1 );
  if( str ) {
    char *p = str;
    for( size_t n=0; n<sz; n++ ) {
      *p++ = (char)((rand32() % (range+1)) & 0x7F) + min;
    }
    *p = '\0';
  }
  return str;
}

/*
static void delete_list_of_strings( CString_t ***CSTR__list, int32_t len ) {
  if( CSTR__list && *CSTR__list ) {
    for( int32_t i=0; i<len; i++ ) {
      if( (*CSTR__list)[i] ) {
        CStringDelete( (*CSTR__list)[i] );
      }
    }
    free( *CSTR__list );
    *CSTR__list = NULL;
  }
}
*/



static CString_t * set_string( CString_t **CSTR__ptr, const char *data ) {
  if( CSTR__ptr ) {
    if( *CSTR__ptr ) {
      CStringDelete( *CSTR__ptr );
    }
    *CSTR__ptr = CStringNew( data );
    return *CSTR__ptr;
  }
  else {
    return NULL;
  }
}


#define CONSISTENCY( CString )  TEST_ASSERTION( __is_consistent( CString ), "consistency" );



static bool ENABLED = true;


/*******************************************************************//**
 *
 ***********************************************************************
 */
BEGIN_UNIT_TEST( __utest_cxcstring_basic ) {

  /*******************************************************************//**
   * Test char string macros
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "Char string macros" ) {

    const char empty_1[] = "";
    const char empty_2[] = "";
    const char a_1[] = "a";
    const char a_2[] = "a";
    const char ab_1[] = "ab";
    const char ab_2[] = "ab";
    const char abc_1[] = "abc";
    const char abc_2[] = "abc";


    TEST_ASSERTION(  CharsEqualsConst( empty_1, empty_2 ),     "empty strings are equal" );
    TEST_ASSERTION(  CharsEqualsConst( a_1,     a_2  ),        "expect equal strings" );
    TEST_ASSERTION(  CharsEqualsConst( ab_1,    ab_2 ),        "expect equal strings" );
    TEST_ASSERTION( !CharsEqualsConst( ab_1,    a_2  ),        "expect different strings" );
    TEST_ASSERTION( !CharsEqualsConst( ab_1,    empty_2 ),     "expect different strings" );
    TEST_ASSERTION( !CharsEqualsConst( a_1,     ab_2 ),        "expect different strings" );
    TEST_ASSERTION( !CharsEqualsConst( empty_1, ab_2 ),        "expect different strings" );

    TEST_ASSERTION(  CharsStartsWithConst( empty_1,   empty_2 ),  "empty string is a prefix of empty string" );
    TEST_ASSERTION(  CharsStartsWithConst( abc_1,     empty_2 ),  "empty string is a prefix of string" );
    TEST_ASSERTION(  CharsStartsWithConst( abc_1,     abc_2   ),  "string is prefix of itself" );
    TEST_ASSERTION(  CharsStartsWithConst( abc_1,     ab_2    ),  "string is prefix" );
    TEST_ASSERTION( !CharsStartsWithConst( abc_1,     "bc"    ),  "inner string is not prefix" );
    TEST_ASSERTION( !CharsStartsWithConst( abc_1,     "aa"    ),  "string is not prefix" );
    TEST_ASSERTION( !CharsStartsWithConst( abc_1,     "abcd"  ),  "longer string is not prefix" );

    TEST_ASSERTION(  CharsContainsConst( empty_1,     empty_2 ),     "empty string contains empty string" );
    TEST_ASSERTION(  CharsContainsConst( abc_1,       empty_2    ),     "string contains empty string" );
    TEST_ASSERTION(  CharsContainsConst( abc_1,       "a"   ),     "first char is a substring" );
    TEST_ASSERTION(  CharsContainsConst( abc_1,       "b"   ),     "inner char is a substring" );
    TEST_ASSERTION(  CharsContainsConst( abc_1,       "c"   ),     "last char is a substring" );
    TEST_ASSERTION( !CharsContainsConst( abc_1,       "ac"  ),     "not a substring" );
    TEST_ASSERTION( !CharsContainsConst( a_1,         "ab"  ),     "not a substring" );
    TEST_ASSERTION(  CharsContainsConst( abc_1,       "bc"  ),     "substring" );

    TEST_ASSERTION( !CharsOccConstWithin( "abcd*efgh",  "d*e", 0 ),   "substring, but not at start" );
    TEST_ASSERTION( !CharsOccConstWithin( "abcd*efgh",  "d*e", 2 ),   "substring, but not within 2 chars of start" );
    TEST_ASSERTION(  CharsOccConstWithin( "abcd*efgh",  "d*e", 3 ),   "substring is within 2 chars of start" );
    TEST_ASSERTION( !CharsOccConstWithin( "abcd*efgh",  "x", 1000 ),  "not a substring anywhere" );

    TEST_ASSERTION(  CharsEndsWithConst( "abcd*efgh",  "" ),          "empty string is a suffix" );
    TEST_ASSERTION( !CharsEndsWithConst( "abcd*efgh",  "abc" ),       "prefix is not a suffix" );
    TEST_ASSERTION( !CharsEndsWithConst( "abcd*efgh",  "d*e" ),       "inner string is not a suffix" );
    TEST_ASSERTION( !CharsEndsWithConst( "abcd*efgh",  "g" ),         "penultimate string is not a suffix" );
    TEST_ASSERTION(  CharsEndsWithConst( "abcd*efgh",  "h" ),         "last char is a suffix" );
    TEST_ASSERTION(  CharsEndsWithConst( "abcd*efgh",  "gh" ),        "two last chars is a suffix" );
    TEST_ASSERTION(  CharsEndsWithConst( abc_1,        abc_2 ),       "string is its own suffix" );
    TEST_ASSERTION( !CharsEndsWithConst( "abcd*efgh",  "ghi" ),       "not a suffix" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test the CString constructor
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "Basic CString constructor" ) {
    CString_t *CSTR__test = NULL;
    char buffer[27] = {'\0'};
    for( char n=0; n<=26; n++ ) {
      if( n > 0 ) {
        buffer[n-1] = 'a' + n-1;
      }
      TEST_ASSERTION( (CSTR__test = CStringNew(buffer)) != NULL,        "string '%s' created", buffer );
      // Test init property
      TEST_ASSERTION( CSTR__test->meta.flags.state.priv.init == 1,                "string should be initialized" );
      // Test size property
      TEST_ASSERTION( CSTR__test->meta.size == n,                            "size should be %d", n );
      // Test allocator_context property
      TEST_ASSERTION( CSTR__test->allocator_context == NULL,            "allocator_context should be NULL" );
      // Test short string property
      if( qwcount(n+1) > __CSTRING_MAX_SHORT_STRING_QWORDS ) {
        TEST_ASSERTION( CSTR__test->meta.flags.state.priv.shrt == 0,              "string should not be short" );
        TEST_ASSERTION( (uintptr_t)CStringValue( CSTR__test ) > (uintptr_t)CSTR__test,        "string data should be stored in the second cacheline" );
      }
      else {
        TEST_ASSERTION( CSTR__test->meta.flags.state.priv.shrt == 1,              "string should be short" );
        TEST_ASSERTION( (uintptr_t)CStringValue( CSTR__test ) < (uintptr_t)CSTR__test,        "string data should be stored early in the first cacheline" );
      }
      // Test obid has not been set yet
      TEST_ASSERTION( idnone( POBID_FROM_OBJECT( CSTR__test ) ) == 1,   "obid should not be set" );
      // Get the obid
      TEST_ASSERTION( CStringObid( CSTR__test ) != NULL,                "obid should be returned" );
      // Test obid has been set after it was initially requested
      TEST_ASSERTION( idnone( POBID_FROM_OBJECT( CSTR__test ) ) == 0,   "obid should be set" );

      CStringDelete( CSTR__test );
    }

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Test basic functionality
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "Basic functionality" ) {
    const char *strings[] = {
      "",
      "a",
      "short",
      "this string does not fit in the first cacheline",
      NULL
    };
    const char **cursor = strings;
    const char *str;
    CString_t *CSTR__str = NULL;

    while( (str = *cursor++) != NULL ) {
      size_t sz = strlen( str );
      char *prefix = malloc( sz + 1 );
      char *suffix = malloc( sz + 1 );
      TEST_ASSERTION( prefix && suffix,                                     "malloc should work" );
      strcpy( prefix, str );
      strcpy( suffix, str );
      TEST_ASSERTION( (CSTR__str = CStringNew( str )) != NULL,              "string created" );
      
      // Value
      TEST_ASSERTION( strcmp( CStringValue( CSTR__str ), str ) == 0,        "CString value should match initializer" );
      
      // Length
      TEST_ASSERTION( (size_t)CStringLength( CSTR__str ) == sz,                     "CString length does not match initializer length" );
      
      // Prefix
      TEST_ASSERTION( CStringStartsWith( CSTR__str, "" ) == true,           "Empty prefix should match" );
      TEST_ASSERTION( CStringStartsWith( CSTR__str, str ) == true,          "Entire string should match as prefix" );
      if( sz > 0 ) {
        for( size_t n=sz-1; n>0; n-- ) {
          prefix[n] = '\0'; // keep shortening the prefix (to min size 1) to test each possibility
          TEST_ASSERTION( CStringStartsWith( CSTR__str, prefix ) == true,     "Prefix '%s' should match", prefix );
          TEST_ASSERTION( CStringEqualsChars( CSTR__str, prefix ) == false,   "Prefix '%s' should not match equality test", prefix );
          if( n > 1 ) {
            const char *inner = prefix + 1;
            TEST_ASSERTION( CStringStartsWith( CSTR__str, inner ) == false,  "Non-prefix '%s' should not match", inner );
          }
        }
      }

      // Suffix
      TEST_ASSERTION( CStringEndsWith( CSTR__str, "" ) == true,             "Empty suffix should match" );
      TEST_ASSERTION( CStringEndsWith( CSTR__str, str ) == true,            "Entire string should match as suffix" );
      if( sz > 0 ) {
        const char *tail = suffix+1;
        while( tail ) {
          TEST_ASSERTION( CStringEndsWith( CSTR__str, tail ) == true,       "Suffix '%s' should match", tail );
          TEST_ASSERTION( CStringEqualsChars( CSTR__str, tail ) == false,   "Suffix '%s' should not match equality test", tail );
          // Keep shortening the suffix to test each possibility
          if( *tail++ == '\0' ) {
            tail = NULL;
          }
        }
        suffix[ sz-1 ] = '\0'; // truncate to check non-match
        tail = suffix+1;
        while( tail ) {
          if( *tail != '\0' ) {
            TEST_ASSERTION( CStringEndsWith( CSTR__str, tail ) == false,        "Non-suffix '%s' should not match", tail );
          }
          // Keep shortening the suffix to test each possibility
          if( *tail++ == '\0' ) {
            tail = NULL;
          }
        }
      }

      // Equality
      TEST_ASSERTION( CStringEqualsChars( CSTR__str, str ) == true,         "CString should equal initializer chars" );
      if( sz > 0 ) {
        TEST_ASSERTION( CStringEqualsChars( CSTR__str, "" ) == false,         "CString should not equal empty string" );
      }

      // Other??
      // TBD

      CStringDelete( CSTR__str );
      CSTR__str = NULL;
      free( prefix );
      free( suffix );
    }
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test CString creation and values for many string sizes
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "Different CString sizes from random initializer" ) {
    for( uint32_t n=0; n<10000; n++ ) {
      char *data = random_char_string( n, 'a', 'z' );
      TEST_ASSERTION( data != NULL,         "char string initializer should be created" );
      TEST_ASSERTION( strlen( data ) == n,  "char string initializer should have specified length" );
      CString_t *CSTR__str = CStringNew( data );
      TEST_ASSERTION( CSTR__str != NULL,                              "CString should be created from initializer" );
      CONSISTENCY( CSTR__str )
      // Length
      uint32_t len = CStringLength( CSTR__str );
      TEST_ASSERTION( len == n,                                       "CString length should be %lu, got %lu", n, len );
      // Equality
      TEST_ASSERTION( strcmp( CStringValue( CSTR__str ), data ) == 0, "CString value does not match initializer" );

      free( data );
      CStringDelete( CSTR__str );
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test CString equality and cloning
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringClone" ) {
    CString_t *CSTR__A = NULL;
    CString_t *CSTR__B = NULL;
    CString_t *CSTR__C = NULL;

    CString_t *CSTR__empty = CStringNew( "" );
    TEST_ASSERTION( CSTR__empty != NULL, "Empty string should be created" );
    CONSISTENCY( CSTR__empty )
    CString_t *CSTR__control = CStringNew( "NEGATIVE CONTROL" );
    TEST_ASSERTION( CSTR__control != NULL, "Control string should be created" );
    CONSISTENCY( CSTR__control )
    TEST_ASSERTION( CStringEquals( CSTR__control, CSTR__empty ) == false,   "Control string should not equal empty string" );

    for( size_t n=0; n<10000000; n = (size_t)(n*1.5f + 191) ) {
      // Create random initializer
      char *data = random_char_string( n, 'a', 'z' );
      TEST_ASSERTION( data != NULL,                                     "char string initializer should be created" );
      TEST_ASSERTION( strlen( data ) == n,                              "char string initializer should have specified length" );

      // String A
      TEST_ASSERTION( (CSTR__A = CStringNew( data )) != NULL,           "String A should be constructed" );
      CONSISTENCY( CSTR__A )
      // Test self-equality
      TEST_ASSERTION( CStringEquals( CSTR__A, CSTR__A ),                "String should be equal to itself" );
      // Test not equal to control
      TEST_ASSERTION( CStringEquals( CSTR__A, CSTR__control ) == false, "String should not equal control" );
      if( n > 0 ) {
        TEST_ASSERTION( CStringEquals( CSTR__A, CSTR__empty ) == false,   "String should not equal empty" );
      }

      // String B
      TEST_ASSERTION( (CSTR__B = CStringNew( data )) != NULL,           "String B should be constructed" );
      CONSISTENCY( CSTR__B )
      // Test equality to A
      TEST_ASSERTION( CStringEquals( CSTR__B, CSTR__A ),                "String B should match string A" );

      // Make clone C from B
      TEST_ASSERTION( (CSTR__C = CStringClone( CSTR__B )) != NULL,      "String C should be created as a clone of B" );
      CONSISTENCY( CSTR__C )
      // Test equality to B
      TEST_ASSERTION( CStringEquals( CSTR__C, CSTR__B ),                "Clone C should match string B" );

      // Clean up
      CStringDelete( CSTR__A );
      CStringDelete( CSTR__B );
      CStringDelete( CSTR__C );

      free( data );

    }

    CStringDelete( CSTR__empty );
    CStringDelete( CSTR__control );


  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test OBID
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "String OBID" ) {
    CString_t *CSTR__str = NULL;

    for( size_t n=0; n<10000000; n = (size_t)(n*1.5f + 191) ) {
      // Create random initializer
      char *data = random_char_string( n, 'a', 'z' );
      TEST_ASSERTION( data != NULL,                                     "char string initializer should be created" );
      TEST_ASSERTION( strlen( data ) == n,                              "char string initializer should have specified length" );

      objectid_t obid = obid_from_string_len( data, (unsigned int)n );

      // Create string
      TEST_ASSERTION( (CSTR__str = CStringNew( data )) != NULL,         "String should be constructed" );
      CONSISTENCY( CSTR__str )

      // Test OBID several times
      TEST_ASSERTION( idmatch( CStringObid( CSTR__str ), &obid ),       "Obid should match" );
      TEST_ASSERTION( idmatch( CStringObid( CSTR__str ), &obid ),       "Obid should match" );
      TEST_ASSERTION( idmatch( CStringObid( CSTR__str ), &obid ),       "Obid should match" );

      CStringDelete( CSTR__str );

      free( data );
    }

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test substring probes
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "Test substring probes" ) {
    const char data[] = "This is a string containing characters we will search for.";
    int sz = (int)strlen(data);

    CString_t *CSTR__data = CStringNew( data );
     
    TEST_ASSERTION( CSTR__data != NULL,                             "Data string created" );
    CONSISTENCY( CSTR__data )

    // Test Contains
    TEST_ASSERTION( CStringContains( CSTR__data, "This" ),          "Should contain substring (at beginning)" );
    TEST_ASSERTION( CStringContains( CSTR__data, "for." ),          "Should contain substring (at end)" );
    TEST_ASSERTION( CStringContains( CSTR__data, "string" ),        "Should contain substring (in the middle)" );
    TEST_ASSERTION( CStringContains( CSTR__data, "" ),              "Should contain the empty string" );
    TEST_ASSERTION( CStringContains( CSTR__data, "NO!" ) == false,  "Should not contain a non-substring" );

    // Test Find
    TEST_ASSERTION( CStringFind( CSTR__data, "This", 0 ) == 0,      "Should find substring at beginning" );
    TEST_ASSERTION( CStringFind( CSTR__data, "is", 0 ) == 2,        "Should find first occurrence of substring" );
    TEST_ASSERTION( CStringFind( CSTR__data, " is", 0 ) == 4,       "Should find first occurrence of substring" );
    TEST_ASSERTION( CStringFind( CSTR__data, "is", 3 ) == 2,        "Should find second occurrence of substring" );
    TEST_ASSERTION( CStringFind( CSTR__data, ".", 0 ) == sz-1,      "Should find substring at the end" );
    TEST_ASSERTION( CStringFind( CSTR__data, "for.", 0 ) == sz-4,   "Should find substring at the end" );
    TEST_ASSERTION( CStringFind( CSTR__data, "", 0 ) == 0,          "Should find the empty string" );
    TEST_ASSERTION( CStringFind( CSTR__data, "", 17 ) == 0,         "Should find the empty string at start index" );
    TEST_ASSERTION( CStringFind( CSTR__data, "NO!", 0 ) < 0,        "Should not find a non-substring" );

    CStringDelete( CSTR__data );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test Prefix
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringPrefix" ) {
    const char data[] = "This is a string we will use to generate prefix from.";
    int sz = (int)strlen(data);
    char prefix[sizeof(data)+1] = {'\0'};

    CString_t *CSTR__data = CStringNew( data );
    const CString_t *CSTR__prefix = NULL;
     
    TEST_ASSERTION( CSTR__data != NULL,                                           "Data string created" );
    CONSISTENCY( CSTR__data )

    for( int n=0; n<=sz; n++ ) {
      TEST_ASSERTION( (CSTR__prefix = CStringPrefix( CSTR__data, n )) != NULL,    "Prefix of size %d created", n );
      CONSISTENCY( CSTR__prefix )

      // Test Prefix
      TEST_ASSERTION( CStringEqualsChars( CSTR__prefix, prefix ),                 "Prefix string len=%d should match prefix buffer '%s'", n, prefix );

      // Build up control prefix buffer from source data, for next iteration
      prefix[n] = data[n];

      CStringDelete( CSTR__prefix );
    }

    CStringDelete( CSTR__data );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test Update
   ***********************************************************************
   */
   /*
  NEXT_TEST_SCENARIO( true, "CStringUpdate" ) {
    const char short_string[] = "Not much here.";
    const char normal_string[] = "This is a string we will use to test update."; // more than 26 characters
    
    CString_t *CSTR__short = NULL;
    CString_t *CSTR__normal = NULL;
    TEST_ASSERTION( (CSTR__short = CStringNew( short_string )),         "Short string created" );
    CONSISTENCY( CSTR__short )
    TEST_ASSERTION( (CSTR__normal = CStringNew( normal_string )),       "Normal string created" );
    CONSISTENCY( CSTR__normal )

    char update_data[27] = {'\0'};
    for( char n=0; n<=26; n++ ) {
      if( n > 0 ) {
        update_data[n-1] = 'a' + n-1;
      }

      // Update with increasing update data
      CStringUpdate( CSTR__short, update_data );
      CONSISTENCY( CSTR__short )
      CStringUpdate( CSTR__normal, update_data );
      CONSISTENCY( CSTR__normal )

      // Ensure short strings get truncated
      if( qwcount(n+1) > __CSTRING_MAX_SHORT_STRING_QWORDS ) {
        TEST_ASSERTION( CStringEqualsChars( CSTR__short, update_data ) == false, "short string should be truncated" );
      }
      else {
        TEST_ASSERTION( CStringEqualsChars( CSTR__short, update_data ) == true, "short string should match update data" );
      }
      TEST_ASSERTION( CStringEqualsChars( CSTR__normal, update_data ) == true, "normal string should match update data" );
    }


    CStringDelete( CSTR__short );
    CStringDelete( CSTR__normal );


  } END_TEST_SCENARIO
  */


  /*******************************************************************//**
   * Test CStringNewFormat
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringNewFormat" ) {

    CString_t *CSTR__formatted = NULL;
    const char format[] = {"[ %s | %d ]"};

    // Initially empty
    CSTR__formatted = CStringNew( "" ); 
    CONSISTENCY( CSTR__formatted )

    // Keep create longer and longer strings based on previous string
    for( int n=0; n<10000; n++ ) {
      CString_t *CSTR__previous = CSTR__formatted;
      const char *previous = CStringValue( CSTR__previous );
      CSTR__formatted = CStringNewFormat( format, previous, n );
      TEST_ASSERTION( CSTR__formatted != NULL,                                              "Formatted string should be created" );
      CONSISTENCY( CSTR__formatted )
      TEST_ASSERTION( CStringLength( CSTR__formatted ) > CStringLength( CSTR__previous ),   "Formatted string should be longer than previous" );
     
      TEST_ASSERTION( CStringStartsWith( CSTR__formatted, "[ " ),                           "Formatted should start with [" );
      TEST_ASSERTION( CStringEndsWith( CSTR__formatted, " ]" ),                             "Formatted should end with ]" );

      CStringDelete( CSTR__previous );
    }


    CStringDelete( CSTR__formatted );


  } END_TEST_SCENARIO

 
   
  /*******************************************************************//**
   * Test CStringSlice
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringSlice" ) {

    CString_t *CSTR__slice = NULL;

    //                              11111111112222222222333333333
    //                    012345678901234567890123456789012345678
    const char data[] =  "THIS is a string with SOME DATA in CAPS";
    //                  - 333333333322222222221111111111
    //                    987654321098765432109876543210987654321
    const int sz_data = (int)strlen( data );


    CString_t *CSTR__data = CStringNew( data );
    CONSISTENCY( CSTR__data )

    int32_t start, end;
    int32_t *pstart = NULL, *pend = NULL;
    char slice[64];   // buffer
    char testbuf[64]; // comparison buffer

    // Test [:] [0:] [-39:] [:39] [0:39] [-39:39] [-99:] [:99] should all be clones of the original string
    for( int i=1; i<=8; i++ ) {
      switch( i ) {
      case 1: strcpy(slice, "[:]");                       pstart = NULL;                pend = NULL;  break;
      case 2: strcpy(slice, "[0:]" );       start = 0;    pstart = &start;              pend = NULL;  break;
      case 3: strcpy(slice, "[-39:]" );     start = -39;  pstart = &start;              pend = NULL;  break;
      case 4: strcpy(slice, "[:39]" );                    pstart = NULL;    end = 39;   pend = &end;  break;
      case 5: strcpy(slice, "[0:39]" );     start = 0;    pstart = &start;  end = 39;   pend = &end;  break;
      case 6: strcpy(slice, "[-39:39]" );   start = -39;  pstart = &start;  end = 39;   pend = &end;  break;
      case 7: strcpy(slice, "[-99:]" );     start = -99;  pstart = &start;              pend = NULL;  break;
      case 8: strcpy(slice, "[:99]" );                    pstart = NULL;    end = 99;   pend = &end;  break;
      }

      CSTR__slice = CStringSlice( CSTR__data, pstart, pend );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                "Slice %s should be a different instance", slice );
      TEST_ASSERTION( CSTR__slice != NULL,                      "Slice %s should be created", slice );
      TEST_ASSERTION( CStringEquals( CSTR__slice, CSTR__data ), "Slice %s should be a clone of the original string", slice );
      CStringDelete( CSTR__slice );
    }

    // Test [:i] for all i in range negative beyond start and positive beyond end
    for( int i=-50; i<50; i++ ) {
      int sz = i <= -sz_data ? 0 : i < 0 ? i + sz_data : i <= sz_data ? i : sz_data;
      memset( testbuf, 0, 64 );
      strncpy( testbuf, data, sz );
      end = i;
      CSTR__slice = CStringSlice( CSTR__data, NULL, &end );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                  "Slice [:%d] should be a different instance", i );
      TEST_ASSERTION( CSTR__slice != NULL,                        "Slice [:%d] should be created", i );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, testbuf),  "Slice [:%d] should equal '%s'", i, testbuf );
    }

    // Test [i:] for all i in range negative beyond start and positive beyond end
    for( int i=-50; i<50; i++ ) {
      int sz = i <= -sz_data ? sz_data : i < 0 ? -i : i <= sz_data ? sz_data - i : 0;
      const char *dp = data + (sz_data - sz);
      memset( testbuf, 0, 64 );
      strncpy( testbuf, dp, sz );
      start = i;
      CSTR__slice = CStringSlice( CSTR__data, &start, NULL );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                  "Slice [:%d] should be a different instance", i );
      TEST_ASSERTION( CSTR__slice != NULL,                        "Slice [:%d] should be created", i );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, testbuf),  "Slice [:%d] should equal '%s'", i, testbuf );
    }

    // Test [:4] [0:4] [-39:4] [:-35] [0:-35] [-39:-35] -> "THIS"
    for( int i=1; i<=6; i++ ) {
      switch( i ) {
      case 1: strcpy(slice, "[:4]" );                     pstart = NULL;    end = 4;    pend = &end;  break;
      case 2: strcpy(slice, "[0:4]" );      start = 0;    pstart = &start;  end = 4;    pend = &end;  break;
      case 3: strcpy(slice, "[-39:4]" );    start = -39;  pstart = &start;  end = 4;    pend = &end;  break;
      case 4: strcpy(slice, "[:-35]" );                   pstart = NULL;    end = -35;  pend = &end;  break;
      case 5: strcpy(slice, "[0:-35]" );    start = 0;    pstart = &start;  end = -35;  pend = &end;  break;
      case 6: strcpy(slice, "[-39:-35]" );  start = -39;  pstart = &start;  end = -35;  pend = &end;  break;
      }
      CSTR__slice = CStringSlice( CSTR__data, pstart, pend );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                "Slice %s should be a different instance", slice );
      TEST_ASSERTION( CSTR__slice != NULL,                      "Slice %s should be created", slice );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, "THIS"), "Slice %s should equal 'THIS'", slice );
      CStringDelete( CSTR__slice );
    }

    // Test [35:] [35:-39] [-4:] [-4:-39] -> "CAPS"
    for( int i=1; i<=4; i++ ) {
      switch( i ) {
      case 1: strcpy(slice, "[35:]" );      start = 35;   pstart = &start;              pend = NULL;  break;
      case 2: strcpy(slice, "[35:39]" );    start = 35;   pstart = &start;  end = 39;   pend = &end;  break;
      case 3: strcpy(slice, "[-4:]" );      start = -4;   pstart = &start;              pend = NULL;  break;
      case 4: strcpy(slice, "[-4:39]" );    start = -4;   pstart = &start;  end = 39;   pend = &end;  break;
      }
      CSTR__slice = CStringSlice( CSTR__data, pstart, pend );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                "Slice %s should be a different instance", slice );
      TEST_ASSERTION( CSTR__slice != NULL,                      "Slice %s should be created", slice );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, "CAPS"), "Slice %s should equal 'CAPS'", slice );
      CStringDelete( CSTR__slice );
    }

    // Test [22:31] [22:-8] [-17:31] [-17:-8] -> "SOME DATA"
    for( int i=1; i<=4; i++ ) {
      switch( i ) {
      case 1: strcpy(slice, "[22:31]" );    start = 22;   pstart = &start;  end = 31;  pend = &end;  break;
      case 2: strcpy(slice, "[22:-8]" );    start = 22;   pstart = &start;  end = -8;  pend = &end;  break;
      case 3: strcpy(slice, "[-17:31]" );   start = -17;  pstart = &start;  end = 31;  pend = &end;  break;
      case 4: strcpy(slice, "[-17:-8]" );   start = -17;  pstart = &start;  end = -8;  pend = &end;  break;
      }
      CSTR__slice = CStringSlice( CSTR__data, pstart, pend );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                      "Slice %s should be a different instance", slice );
      TEST_ASSERTION( CSTR__slice != NULL,                            "Slice %s should be created", slice );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, "SOME DATA"),  "Slice %s should equal 'SOME DATA'", slice );
      CStringDelete( CSTR__slice );
    }

    // Test empty slice
    for( int i=1; i<=9; i++ ) {
      switch( i ) {
      case 1: strcpy(slice, "[0:0]" );      start = 0;    pstart = &start;  end = 0;   pend = &end;  break;
      case 2: strcpy(slice, "[-1:0]" );     start = -1;   pstart = &start;  end = 0;   pend = &end;  break;
      case 3: strcpy(slice, "[-1:-1]" );    start = -1;   pstart = &start;  end = -1;  pend = &end;  break;
      case 4: strcpy(slice, "[1:0]" );      start = 1;    pstart = &start;  end = 0;   pend = &end;  break;
      case 5: strcpy(slice, "[-99:0]" );    start = -99;  pstart = &start;  end = 0;   pend = &end;  break;
      case 6: strcpy(slice, "[-99:-99]" );  start = -99;  pstart = &start;  end = -99; pend = &end;  break;
      case 7: strcpy(slice, "[0:-99]" );    start = 0;    pstart = &start;  end = -99; pend = &end;  break;
      case 8: strcpy(slice, "[5:-34]" );    start = 5;    pstart = &start;  end = -34; pend = &end;  break;
      case 9: strcpy(slice, "[5:-35]" );    start = 5;    pstart = &start;  end = -35; pend = &end;  break;
      }
      CSTR__slice = CStringSlice( CSTR__data, pstart, pend );
      CONSISTENCY( CSTR__slice )
      TEST_ASSERTION( CSTR__slice != CSTR__data,                      "Slice %s should be a different instance", slice );
      TEST_ASSERTION( CSTR__slice != NULL,                            "Slice %s should be created", slice );
      TEST_ASSERTION( CStringEqualsChars( CSTR__slice, ""),           "Slice %s should be empty", slice );
      CStringDelete( CSTR__slice );
    }

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Test CStringReplace
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringReplace" ) {

    const char data[] = "This is a string containing characters,   like \\* and \\**   that we will replace.";

    CString_t *CSTR__data = CStringNew( data );
    CONSISTENCY( CSTR__data )
    CString_t *CSTR__replaced;
    
    // Replaced string is smaller than original
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "This is a", "A" )) != NULL,    "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringStartsWith( CSTR__replaced, "A string" ),                    "Expected replacement at start of string" );
    TEST_ASSERTION( CStringEndsWith( CSTR__replaced, "will replace." ),                 "Expected rest of string." );
    CStringDelete( CSTR__replaced );

    // Replaced string is larger than original
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "This", "What we have here" )) != NULL,   "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringStartsWith( CSTR__replaced, "What we have here is a string" ),         "Expected replacement at start of string" );
    TEST_ASSERTION( CStringEndsWith( CSTR__replaced, "will replace." ),                           "Expected rest of string." );
    CStringDelete( CSTR__replaced );

    // Replace multiple chunks
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "s", "S" )) != NULL,            "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringStartsWith( CSTR__replaced, "ThiS iS a String" ),            "Expected replacements at start of string" );
    TEST_ASSERTION( CStringEndsWith( CSTR__replaced, "will replace." ),                 "Expected rest of string." );
    CStringDelete( CSTR__replaced );

    // Remove all spaces
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, " ", "" )) != NULL,             "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringContains( CSTR__replaced, " " ) == false,                    "All spaces should be removed" );
    TEST_ASSERTION( CStringStartsWith( CSTR__replaced, "Thisisa" ),                     "Expected replacements at start of string" );
    TEST_ASSERTION( CStringEndsWith( CSTR__replaced, "willreplace." ),                  "Expected replacements at end of string" );
    CStringDelete( CSTR__replaced );

    // Replace with same
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "s", "s" )) != NULL,            "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringEquals( CSTR__replaced, CSTR__data ),                        "Resulting string should be the same as original" );
    CStringDelete( CSTR__replaced );

    // Replace everywhere
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "", "_" )) != NULL,             "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( strlen(CStringValue(CSTR__replaced)) == (size_t)CSTR__replaced->meta.size,       "Replaced string's size should match its data");
    TEST_ASSERTION( CStringStartsWith( CSTR__replaced, "_T_h_i_s_ _i_s_ " ),            "Resulting string should have subst inserted in all locations" );
    TEST_ASSERTION( CStringEndsWith( CSTR__replaced, " _r_e_p_l_a_c_e_._" ),            "Resulting string should have subst inserted in all locations" );
    CStringDelete( CSTR__replaced );

    // Remove the asterisks
    TEST_ASSERTION( (CSTR__replaced = CStringReplace( CSTR__data, "\\*", "ASTERISK" )) != NULL,   "Replaced string should be created" );
    CONSISTENCY( CSTR__replaced )
    TEST_ASSERTION( CStringContains( CSTR__replaced, "\\*" ) == false,                  "Escaped asterisks should be removed" );
    TEST_ASSERTION( CStringContains( CSTR__replaced, "ASTERISK" ) == true,              "Escaped asterisks should have replacement" );
    TEST_ASSERTION( CStringContains( CSTR__replaced, "*" ) == true,                     "Unescaped asterisks should not be removed" );
    CStringDelete( CSTR__replaced );

    CStringDelete( CSTR__data );

    // Make a new source string with lots of random characters
    int sz_rstr = 1000000;
    char *rstr = random_char_string( sz_rstr, 'a', 'z' );
    CString_t *CSTR__shorter = CStringNew( rstr );
    CONSISTENCY( CSTR__shorter )
    CString_t *CSTR__longer = CStringNew( rstr );
    CONSISTENCY( CSTR__longer )
    free( rstr );

    // Keep replacing everything
    for( char r[2]={'0'}, c='a'; c <= 'z'; c++ ) {
      *r = c;
      CString_t *CSTR__prev;

      CSTR__prev = CSTR__shorter;
      CSTR__shorter = CStringReplace( CSTR__prev, r, "" );
      CONSISTENCY( CSTR__shorter )
      TEST_ASSERTION( CStringLength( CSTR__shorter ) < CStringLength( CSTR__prev ),     "Replaced string should be shorter" );
      CStringDelete( CSTR__prev );

      CSTR__prev = CSTR__longer;
      CSTR__longer = CStringReplace( CSTR__prev, r, "STUFF" );
      CONSISTENCY( CSTR__longer )
      TEST_ASSERTION( CStringLength( CSTR__longer ) > CStringLength( CSTR__prev ),      "Replaced string should be longer" );
      CStringDelete( CSTR__prev );

    }

    TEST_ASSERTION( CStringLength( CSTR__shorter ) == 0,                                "Every character should be removed" );
    CStringDelete( CSTR__shorter );

    TEST_ASSERTION( CStringLength( CSTR__longer ) == 5*sz_rstr,                         "Every character should be replaced by something 5x longer" );
    CStringDelete( CSTR__longer );

    // Run a torture test with random things
    char *rinit = random_char_string( 50000, 'a', 'z' );
    CSTR__data = CStringNew( rinit );
    CONSISTENCY( CSTR__data )
    // initial probe size range
    int min_plen = 1, max_plen = 4;
    // initial subst size range
    int min_slen = 0, max_slen = 4;
    int32_t sz;
    do {
      int plen = (int)((rand32() % (max_plen+1-min_plen)) + min_plen );
      int slen = (int)((rand32() % (max_slen+1-min_slen)) + min_slen );

      char *rprobe = random_char_string( plen, 'a', 'z' );
      char *rsubst = random_char_string( slen, 'a', 'z' );

      int64_t u0 = __GET_CURRENT_MICROSECOND_TICK();
      CString_t *CSTR__str = CStringReplace( CSTR__data, rprobe, rsubst );
      CONSISTENCY( CSTR__str )
      int64_t u = __GET_CURRENT_MICROSECOND_TICK() - u0;
      sz = CStringLength( CSTR__str );
      int64_t rate = sz/(u?u:1);

      if( sz > 10000000 ) {
        max_slen = 0; // only subst empty strings from now on, to shrink to zero
        min_plen = 2;
        max_plen = 2;
      }
      else if( sz < 10000 ) {
        if( sz > 5000 ) {
          min_plen = 3;
          max_plen = 3;
        }
        else if( sz > 2000 ) {
          min_plen = 4;
          max_plen = 4;
        }
        else if( sz > 1000 ) {
          min_plen = 5;
          max_plen = 5;
        }
        else if( sz > 5 ) {
          min_plen = 1;
        }
      }

      if( CStringContains( CSTR__data, rprobe ) ) {
        TEST_ASSERTION( CStringContains( CSTR__str, rsubst ),         "Substitution should exist" );
        printf( "\r(%d) %s -> %s (%llu us, %llu chars/us)            ", CStringLength(CSTR__str), rprobe, rsubst, u, rate );
      }
      else {
        TEST_ASSERTION( CStringEquals( CSTR__str, CSTR__data ),       "No substitution should be made" );
        //printf( "(%lu) NO %s (%llu us, %llu chars/us)\n", CStringLength(CSTR__str), rprobe, u, rate );
      }

      CStringDelete( CSTR__data );
      CSTR__data = CSTR__str;

    } while( sz > 1 );
    printf( "\n" );

    CStringDelete( CSTR__data );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * Test CStringSplit
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringSplit" ) {
    const char data[] = "this is a string    that will be split into smaller pieces.";

    CString_t *CSTR__data = CStringNew( data );
    int32_t len = 0;
    CString_t **CSTR__list = NULL;

    // Split on whitespace
    CSTR__list = CStringSplit( CSTR__data, NULL, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 11,                            "Split on whitespace should generate 11 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "this" ),     "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], "is" ),       "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[2], "a" ),        "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[3], "string" ),   "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[4], "that" ),     "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[5], "will" ),     "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[6], "be" ),       "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[7], "split" ),    "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[8], "into" ),     "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[9], "smaller" ),  "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[10], "pieces." ), "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split on 's'
    const char *split_on_s = "s";
    CSTR__list = CStringSplit( CSTR__data, split_on_s, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 7,                             "Split on 's' should generate 7 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "thi" ),                      "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], " i" ),                       "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[2], " a " ),                      "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[3], "tring    that will be " ),   "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[4], "plit into " ),               "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[5], "maller piece" ),             "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[6], "." ),                       "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split on 'is'
    const char *split_on_is = "is";
    CSTR__list = CStringSplit( CSTR__data, split_on_is, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 3,                             "Split on 'is' should generate 3 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "th" ),                                                   "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], " " ),                                                    "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[2], " a string    that will be split into smaller pieces." ), "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split on 't'
    const char *split_on_t = "t";
    CSTR__list = CStringSplit( CSTR__data, split_on_t, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 7,                             "Split on 't' should generate 7 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "" ),                       "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], "his is a s" ),             "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[2], "ring    " ),               "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[3], "ha" ),                     "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[4], " will be spli" ),          "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[5], " in" ),                    "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[6], "o smaller pieces." ),      "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split on '.'
    const char *split_on_period = ".";
    CSTR__list = CStringSplit( CSTR__data, split_on_period, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 2,                             "Split on '.' should generate 2 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "this is a string    that will be split into smaller pieces" ),   "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], "" ),                                                             "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split on 'k' (does not exist)
    const char *split_on_k = "k";
    CSTR__list = CStringSplit( CSTR__data, split_on_k, &len );
    TEST_ASSERTION( CSTR__list != NULL,                   "Split should generate list" );
    TEST_ASSERTION( len == 1,                             "Split on 'k' should generate 1 token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "this is a string    that will be split into smaller pieces." ),  "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    CStringDelete( CSTR__data );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Test CStringSplit #2
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( ENABLED, "CStringSplit #2" ) {

    CString_t *CSTR__data = NULL;
    int32_t len = 0;
    CString_t **CSTR__list = NULL;

    // Split empty string on nothing
    TEST_ASSERTION( set_string( &CSTR__data, "" ),        "Set empty string" );
    CSTR__list = CStringSplit( CSTR__data, NULL, &len );
    TEST_ASSERTION( CSTR__list != NULL && len == 0,       "Split should generate empty list" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split empty string on empty string
    TEST_ASSERTION( set_string( &CSTR__data, "" ),        "Set empty string" );
    CSTR__list = CStringSplit( CSTR__data, "", &len );
    TEST_ASSERTION( CSTR__list != NULL && len == 0,       "Split should generate empty list" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split whitespace string on empty string
    TEST_ASSERTION( set_string( &CSTR__data, "    " ),    "Set whitespace string" );
    CSTR__list = CStringSplit( CSTR__data, "", &len );
    TEST_ASSERTION( CSTR__list != NULL && len == 0,       "Split should generate empty list" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split string on itself
    TEST_ASSERTION( set_string( &CSTR__data, "x" ),       "Set simple string" );
    CSTR__list = CStringSplit( CSTR__data, "x", &len );
    TEST_ASSERTION( CSTR__list != NULL && len == 2,           "Split should generate list of 2 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "" ),  "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], "" ),  "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    // Split string on itself v2
    TEST_ASSERTION( set_string( &CSTR__data, "yy" ),       "Set simple string" );
    CSTR__list = CStringSplit( CSTR__data, "yy", &len );
    TEST_ASSERTION( CSTR__list != NULL && len == 2,           "Split should generate list of 2 tokens" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[0], "" ),  "Unexpected token" );
    TEST_ASSERTION( CStringEqualsChars( CSTR__list[1], "" ),  "Unexpected token" );
    CStringDeleteList( &CSTR__list );
    //delete_list_of_strings( &CSTR__list, len );

    CStringDelete( CSTR__data );

  } END_TEST_SCENARIO

} END_UNIT_TEST



#endif
