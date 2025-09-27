/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxlock.c
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

#include "cxlock.h"


#if defined CXPLAT_WINDOWS_X64
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_NANOSECOND_TICK( void ) {
  static __THREAD uint64_t nano_factor = 0;
  static __THREAD uint64_t clocks_0 = 0;

  if( nano_factor == 0 ) {
    LARGE_INTEGER freq = {0};
    LARGE_INTEGER tstamp = {0};
    QueryPerformanceFrequency( &freq );
    QueryPerformanceCounter( &tstamp );
    uint64_t clocks_per_second = freq.QuadPart;
    // Expect frequency to be 10MHz on Windows, but compute the nano factor still.
    // We create this factor as a multiplier to be followed by bitshift to avoid division
    nano_factor = ( 1000000000ULL << 2 ) / clocks_per_second; // add 2 bits of resolution just in case not 10MHz
    // Clocks since boot time
    clocks_0 = tstamp.QuadPart;
  }
  
  LARGE_INTEGER tstamp = {0};
  QueryPerformanceCounter( &tstamp );

  // Number of perf counter clocks since thread initialization
  uint64_t clocks = tstamp.QuadPart - clocks_0;

  // 10MHz perf counter leads to a count 31,536,000,000,000,000 after 100 years
  // (10,000,000 * 3600 * 24 * 365 * 100)
  // log2( 31536000000000000 ) = 54.8 bits
  // nano_factor should normally be 400 (i.e. 10MHz perf counter + 2 bits)
  // log2(10,000,000 * 3600 * 24 * 365 * 100 * 400) = 63.5 bits after 100 years
  //

  uint64_t ns_ticks = (clocks * nano_factor) >> 2;
  
  return (int64_t)ns_ticks;
}

#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_NANOSECOND_TICK( void ) {
  struct timespec ts;
  if( clock_gettime( CLOCK_MONOTONIC, &ts ) == 0 ) {
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
  }
  return 0;
}

#endif



#if defined CXPLAT_WINDOWS_X64
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_MICROSECOND_TICK( void ) {
  return __GET_CURRENT_NANOSECOND_TICK() / 1000;
}

#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_MICROSECOND_TICK( void ) {
  struct timespec ts;
  if( clock_gettime( CLOCK_MONOTONIC, &ts ) == 0 ) {
    return (int64_t)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000LL;
  }
  return 0;
}

#endif



#if defined CXPLAT_WINDOWS_X64
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_MILLISECOND_TICK( void ) {
  return GetTickCount64();
}

#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __GET_CURRENT_MILLISECOND_TICK( void ) {
  struct timespec ts;
  if( clock_gettime( CLOCK_MONOTONIC, &ts ) == 0 ) {
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
  }
  return 0;
}

#endif



#if defined CXPLAT_WINDOWS_X64
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
bool __TRY_CRITICAL_SECTION( LPCRITICAL_SECTION pcs ) {
  // This call returns immediately
  if( TryEnterCriticalSection( pcs ) == 0 ) {
    // Failure - already locked and we can't enter
    return false;
  }
  else {
    // Success - we have entered
    return true;
  }
}

#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
bool __TRY_CRITICAL_SECTION( pthread_mutex_t *mutex ) {
  // This call returns immediately
  if( pthread_mutex_trylock( mutex ) == 0 ) {
    // Success - we have entered
    return true;
  }
  else {
    // Failure - already locked and we can't enter
    return false;
  }
}

#endif



#if defined CXPLAT_WINDOWS_X64
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __TIMED_WAIT_CONDITION_CS( PCONDITION_VARIABLE pcond, PCRITICAL_SECTION pcs, DWORD milliseconds ) {
  int64_t t0 = __GET_CURRENT_NANOSECOND_TICK();
  SleepConditionVariableCS( pcond, pcs, milliseconds );
  return __GET_CURRENT_NANOSECOND_TICK() - t0;
}

#elif defined(CXPLAT_LINUX_ANY)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __TIMED_WAIT_CONDITION_CS( pthread_cond_t *cond, pthread_mutex_t *mutex, DWORD milliseconds ) {

  int64_t t0 = __GET_CURRENT_NANOSECOND_TICK();
  struct timespec ts;

  if( milliseconds == 0xFFFFFFFF ) { // INFINITE
    pthread_cond_wait( cond, mutex );
  }
  else if( clock_gettime( CLOCK_MONOTONIC, &ts ) == 0 ) {
    int64_t wait_ns = milliseconds * 1000000LL;   // total ns to wait
    int64_t nsec = (int64_t)ts.tv_nsec + wait_ns; // timespec ns (possibly with overflow)
    // Add wait time to timespec
    ts.tv_nsec = nsec % 1000000000LL;
    ts.tv_sec += nsec / 1000000000LL;

    pthread_cond_timedwait( cond, mutex, &ts );
  }

  return __GET_CURRENT_NANOSECOND_TICK() - t0;
}

#elif defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __TIMED_WAIT_CONDITION_CS( pthread_cond_t *cond, pthread_mutex_t *mutex, DWORD milliseconds ) {
  static useconds_t default_usleep = 10;

  // Total max nanoseconds to wait
  int64_t wait_ns = milliseconds * 1000000LL;

  // Start/end time for this function
  int64_t t0, t1;

  // Get start time
  struct timespec ts;
  if( clock_gettime( CLOCK_REALTIME, &ts ) != 0 ) {
    goto default_micronap;
  }

  // Keep track of our starting point in nanoseconds 
  t0 = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

  if( milliseconds == 0xFFFFFFFF ) { // INFINITE
    pthread_cond_wait( cond, mutex );
  }
  else {
    int64_t nsec = (int64_t)ts.tv_nsec + wait_ns; // timespec ns (possibly with overflow)
    // Add wait time to timespec
    ts.tv_nsec = nsec % 1000000000LL;
    ts.tv_sec += nsec / 1000000000LL;
    // Wait for condition or timeout
    pthread_cond_timedwait( cond, mutex, &ts );
  }
  
  // Get end time
  if( clock_gettime( CLOCK_REALTIME, &ts ) != 0 ) {
    return wait_ns; // Failed to get end time, just return the requested timeout in nanoseconds
  }

  // Keep track of our starting point in nanoseconds 
  t1 = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

  // Return nanoseconds spent in this function
  return t1 - t0;

default_micronap:

  usleep( default_usleep ); // it didn't work, take a micronap instead
  return default_usleep * 1000LL;

}


#endif



#if defined CXPLAT_WINDOWS_X64

#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
void __CXLOCK_MUTEX_SPINLOCK( pthread_mutex_t *mutex ) {
  // Optimistic
  if( pthread_mutex_trylock( mutex ) == 0 ) {
    return;
  }

  uint8_t yield_counter = 0;
  int i = 0;
  do {
  
    // 256 times around the loop with pause before yielding
    if( ++yield_counter != 0 ) {
      #if defined CXPLAT_ARCH_X64
      _mm_pause();
      #elif defined CXPLAT_ARCH_ARM64
      __yield();
      #endif
    }
    else {
      sched_yield();
    }

    // Try the lock again
    if( pthread_mutex_trylock( mutex ) == 0 ) {
      return;
    }

  } while( i++ < 4096 );
  
  // Fallback
  pthread_mutex_lock( mutex );
}

#endif




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __ELAPSED_MILLISECONDS( int64_t since_t0 ) {
  int64_t now_t1 = __GET_CURRENT_MILLISECOND_TICK();
  return now_t1 - since_t0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
int64_t __MILLISECONDS_SINCE_1970( void ) {
  // NOTE: This is set independently for each dynamic library that uses the static cxlib!
  static int64_t __epoch_ms_ref = 0;

  if( __epoch_ms_ref == 0 ) {
    time_t ltime;
    time( &ltime );
    __epoch_ms_ref = __GET_CURRENT_MILLISECOND_TICK() - ltime * 1000;
  }
  
  return __ELAPSED_MILLISECONDS( __epoch_ms_ref );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
uint32_t __SECONDS_SINCE_1970( void ) {
  int64_t ms = __MILLISECONDS_SINCE_1970();
  int64_t s = ms / 1000;
  return s & 0xFFFFFFFF;
}
