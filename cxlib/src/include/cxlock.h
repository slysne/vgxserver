/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxlock.h
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

#ifndef CXLIB_CXLOCK_H
#define CXLIB_CXLOCK_H

#include "cxplat.h"

#ifdef __cplusplus
extern "C" {
#endif

/* critical section */
#define EXPECTED_SIZEOF_CS_LOCK 64 /* Because of Apple's pthread_mutex_t */

/* conditions */
#define EXPECTED_SIZEOF_CS_COND 48



#if defined CXPLAT_WINDOWS_X64
#include <process.h>
#define cpu_yield() SwitchToThread()

#define CXLOCK_TYPE CRITICAL_SECTION

typedef struct _CS_LOCK {
  CXLOCK_TYPE lock;
  char __pad[EXPECTED_SIZEOF_CS_LOCK-sizeof(CXLOCK_TYPE)];
} CS_LOCK;

#define CXCOND_TYPE CONDITION_VARIABLE

typedef struct _CS_COND {
  CXCOND_TYPE cond;
  char __pad[EXPECTED_SIZEOF_CS_COND-sizeof(CXCOND_TYPE)];
} CS_COND;

#define INIT_SPINNING_CRITICAL_SECTION( pcslock, SpinCount )   do { if(!InitializeCriticalSectionAndSpinCount( (pcslock), SpinCount )) { *((int*)NULL)=0; /* goodbye */ } } WHILE_ZERO
#define INIT_CRITICAL_SECTION( pcslock )   InitializeCriticalSection( (pcslock) )
#define DEL_CRITICAL_SECTION( pcslock )    DeleteCriticalSection( (pcslock) )
#define ENTER_CRITICAL_SECTION( pcslock )  EnterCriticalSection( (pcslock) )
extern bool __TRY_CRITICAL_SECTION( LPCRITICAL_SECTION pcs );
#define TRY_CRITICAL_SECTION( pcslock )    __TRY_CRITICAL_SECTION( (pcslock) )
#define LEAVE_CRITICAL_SECTION( pcslock )  LeaveCriticalSection( (pcslock) )

extern int64_t __GET_CURRENT_NANOSECOND_TICK( void );
extern int64_t __GET_CURRENT_MICROSECOND_TICK( void );
extern int64_t __GET_CURRENT_MILLISECOND_TICK( void );
extern int64_t __ELAPSED_MILLISECONDS( int64_t since_t0 );
extern int64_t __MILLISECONDS_SINCE_1970( void );
extern uint32_t __SECONDS_SINCE_1970( void );
#define INIT_CONDITION_VARIABLE( pcscond ) InitializeConditionVariable( (pcscond) )
#define DEL_CONDITION_VARIABLE( pcscond )  {}
#define WAIT_CONDITION( pcscond, pcslock )  SleepConditionVariableCS( (pcscond), (pcslock), INFINITE )
extern int64_t __TIMED_WAIT_CONDITION_CS( PCONDITION_VARIABLE pcond, PCRITICAL_SECTION pcs, DWORD milliseconds );
// Return the number of nanoseconds slept
#define TIMED_WAIT_CONDITION_CS( pcscond, pcslock, milliseconds )  __TIMED_WAIT_CONDITION_CS( (pcscond), (pcslock), milliseconds )
#define SIGNAL_ALL_CONDITION( pcscond )    WakeAllConditionVariable( (pcscond) )
#define SIGNAL_ONE_CONDITION( pcscond )    WakeConditionVariable( (pcscond) )

#define ATOMIC_VOLATILE_i32       __declspec(align(4)) volatile LONG
#define ATOMIC_VOLATILE_u32       __declspec(align(4)) volatile ULONG
#define ATOMIC_VOLATILE_i64       __declspec(align(8)) volatile int64_t
#define ATOMIC_VOLATILE_u64       __declspec(align(8)) volatile uint64_t


#define ATOMIC_INCREMENT_i32(ptr)   InterlockedIncrement( ptr )
#define ATOMIC_DECREMENT_i32(ptr)   InterlockedDecrement( ptr )
#define ATOMIC_ADD_i32(ptr, N)      InterlockedAdd( ptr, N )
#define ATOMIC_SUB_i32(ptr, N)      InterlockedAdd( ptr, -(N) )
#define ATOMIC_READ_i32(ptr)        InterlockedCompareExchange( ptr, 0, 0)
#define ATOMIC_ASSIGN_i32(ptr, val) InterlockedExchange( ptr, val )



/**************************************************************************//**
 * InterlockedIncrement_u32
 *
 ******************************************************************************
 */
__inline static ULONG InterlockedIncrement_u32( ULONG volatile * _Addend ) {
  return (ULONG)InterlockedIncrement( (LONG*)_Addend );
}


/**************************************************************************//**
 * InterlockedDecrement_u32
 *
 ******************************************************************************
 */
__inline static ULONG InterlockedDecrement_u32( ULONG volatile * _Addend ) {
  return (ULONG)InterlockedDecrement( (LONG*)_Addend );
}


/**************************************************************************//**
 * InterlockedAdd_u32
 *
 ******************************************************************************
 */
__inline static ULONG InterlockedAdd_u32( ULONG volatile * _Addend, ULONG value ) {
  return (ULONG)InterlockedAdd( (LONG*)_Addend, (LONG)value );
}


/**************************************************************************//**
 * InterlockedCompareExchange_u32
 *
 ******************************************************************************
 */
__inline static ULONG InterlockedCompareExchange_u32( ULONG volatile * _Destination, ULONG _Exchange, ULONG _Comparand ) {
  return (ULONG)InterlockedCompareExchange( (LONG*)_Destination, (LONG)_Exchange, (LONG)_Comparand );
}


/**************************************************************************//**
 * InterlockedExchange_u32
 *
 ******************************************************************************
 */
__inline static ULONG InterlockedExchange_u32( ULONG volatile * _Target, ULONG _Value ) {
  return (ULONG)InterlockedExchange( (LONG*)_Target, (LONG)_Value );
}


#define ATOMIC_INCREMENT_u32(ptr)   InterlockedIncrement_u32( ptr )
#define ATOMIC_DECREMENT_u32(ptr)   InterlockedDecrement_u32( ptr )
#define ATOMIC_ADD_u32(ptr, N)      InterlockedAdd_u32( ptr, N )
#define ATOMIC_SUB_u32(ptr, N)      InterlockedAdd_u32( ptr, -(N) )
#define ATOMIC_READ_u32(ptr)        InterlockedCompareExchange_u32( ptr, 0, 0)
#define ATOMIC_ASSIGN_u32(ptr, val) InterlockedExchange_u32( ptr, val )


#define ATOMIC_INCREMENT_i64(ptr)   InterlockedIncrement64( ptr )
#define ATOMIC_DECREMENT_i64(ptr)   InterlockedDecrement64( ptr )
#define ATOMIC_ADD_i64(ptr, N)      InterlockedAdd64( ptr, N )
#define ATOMIC_SUB_i64(ptr, N)      InterlockedAdd64( ptr, -(N) )
#define ATOMIC_READ_i64(ptr)        InterlockedCompareExchange64( ptr, 0, 0)
#define ATOMIC_ASSIGN_i64(ptr, val) InterlockedExchange64( ptr, val )



/**************************************************************************//**
 * InterlockedIncrement_u64
 *
 ******************************************************************************
 */
__inline static uint64_t InterlockedIncrement_u64( uint64_t volatile * _Addend ) {
  return (uint64_t)InterlockedIncrement64( (int64_t*)_Addend );
}


/**************************************************************************//**
 * InterlockedDecrement_u64
 *
 ******************************************************************************
 */
__inline static uint64_t InterlockedDecrement_u64( uint64_t volatile * _Addend ) {
  return (uint64_t)InterlockedDecrement64( (int64_t*)_Addend );
}


/**************************************************************************//**
 * InterlockedAdd_u64
 *
 ******************************************************************************
 */
__inline static uint64_t InterlockedAdd_u64( uint64_t volatile * _Addend, uint64_t value ) {
  return (uint64_t)InterlockedAdd64( (int64_t*)_Addend, (int64_t)value );
}


/**************************************************************************//**
 * InterlockedCompareExchange_u64
 *
 ******************************************************************************
 */
__inline static uint64_t InterlockedCompareExchange_u64( uint64_t volatile * _Destination, uint64_t _Exchange, uint64_t _Comparand ) {
  return (uint64_t)InterlockedCompareExchange64( (int64_t*)_Destination, (int64_t)_Exchange, (int64_t)_Comparand );
}


/**************************************************************************//**
 * InterlockedExchange_u64
 *
 ******************************************************************************
 */
__inline static uint64_t InterlockedExchange_u64( uint64_t volatile * _Target, uint64_t _Value ) {
  return (uint64_t)InterlockedExchange64( (int64_t*)_Target, (int64_t)_Value );
}

#define ATOMIC_INCREMENT_u64(ptr)   InterlockedIncrement_u64( ptr )
#define ATOMIC_DECREMENT_u64(ptr)   InterlockedDecrement_u64( ptr )
#define ATOMIC_ADD_u64(ptr, N)      InterlockedAdd_u64( ptr, N )
#define ATOMIC_SUB_u64(ptr, N)      InterlockedAdd_u64( ptr, -(N) )
#define ATOMIC_READ_u64(ptr)        InterlockedCompareExchange_u64( ptr, 0, 0)
#define ATOMIC_ASSIGN_u64(ptr, val) InterlockedExchange_u64( ptr, val )



#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)

#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <unistd.h>
#define cpu_yield() sched_yield()

#define CXLOCK_TYPE pthread_mutex_t

typedef struct _CS_LOCK {
  CXLOCK_TYPE lock;
  char __pad[EXPECTED_SIZEOF_CS_LOCK-sizeof(CXLOCK_TYPE)];
} CS_LOCK;

#define CXCOND_TYPE pthread_cond_t

typedef struct _CS_COND {
  CXCOND_TYPE cond;
  char __pad[EXPECTED_SIZEOF_CS_COND-sizeof(CXCOND_TYPE)];
} CS_COND;

#define INIT_SPINNING_CRITICAL_SECTION( pcslock, SpinCount )          \
  do {                                                                \
    pthread_mutexattr_t mutexattr;                                    \
    pthread_mutexattr_init( &mutexattr );                             \
    /* match the (only) Windows behavior: same thread can re-lock */  \
    pthread_mutexattr_settype( &mutexattr, PTHREAD_MUTEX_RECURSIVE ); \
    pthread_mutex_init( (pcslock), &mutexattr );                      \
  } WHILE_ZERO
#define INIT_CRITICAL_SECTION( pcslock )                              \
  do {                                                                \
    pthread_mutexattr_t mutexattr;                                    \
    pthread_mutexattr_init( &mutexattr );                             \
    /* match the (only) Windows behavior: same thread can re-lock */  \
    pthread_mutexattr_settype( &mutexattr, PTHREAD_MUTEX_RECURSIVE ); \
    pthread_mutex_init( (pcslock), &mutexattr );                      \
  } WHILE_ZERO


#define DEL_CRITICAL_SECTION( pcslock )    pthread_mutex_destroy( (pcslock) )
extern void __CXLOCK_MUTEX_SPINLOCK( pthread_mutex_t *mutex );
#define ENTER_CRITICAL_SECTION( pcslock )  __CXLOCK_MUTEX_SPINLOCK( (pcslock) )
extern bool __TRY_CRITICAL_SECTION( pthread_mutex_t *mutex );
#define TRY_CRITICAL_SECTION( pcslock )    __TRY_CRITICAL_SECTION( (pcslock) )
#define LEAVE_CRITICAL_SECTION( pcslock )  pthread_mutex_unlock( (pcslock) )

extern int64_t __GET_CURRENT_NANOSECOND_TICK( void );
extern int64_t __GET_CURRENT_MICROSECOND_TICK( void );
extern int64_t __GET_CURRENT_MILLISECOND_TICK( void );
extern int64_t __ELAPSED_MILLISECONDS( int64_t since_t0 );
extern int64_t __MILLISECONDS_SINCE_1970( void );
extern uint32_t __SECONDS_SINCE_1970( void );

#if defined CXPLAT_LINUX_ANY
#define INIT_CONDITION_VARIABLE( pcscond )                      \
  do {                                                          \
    pthread_condattr_t condattr;                                \
    pthread_condattr_init( &condattr );                         \
    pthread_condattr_setclock( &condattr, CLOCK_MONOTONIC );    \
    pthread_cond_init( (pcscond), &condattr );                  \
  } WHILE_ZERO
#else
#define INIT_CONDITION_VARIABLE( pcscond )                      \
  do {                                                          \
    pthread_cond_init( (pcscond), NULL );                       \
  } WHILE_ZERO
#endif

#define DEL_CONDITION_VARIABLE( pcscond )  pthread_cond_destroy( (pcscond) )
#define WAIT_CONDITION( pcscond, pcslock )  pthread_cond_wait( (pcscond), (pcslock) ) 
extern int64_t __TIMED_WAIT_CONDITION_CS( pthread_cond_t *cond, pthread_mutex_t *mutex, DWORD milliseconds );
// Return the number of nanoseconds slept
#define TIMED_WAIT_CONDITION_CS( pcscond, pcslock, milliseconds )  __TIMED_WAIT_CONDITION_CS( (pcscond), (pcslock), milliseconds )
#define SIGNAL_ALL_CONDITION( pcscond )    pthread_cond_broadcast( (pcscond) )
#define SIGNAL_ONE_CONDITION( pcscond )    pthread_cond_signal( (pcscond) )


#define ATOMIC_VOLATILE_i32       __attribute__ ((aligned(4))) volatile int32_t
#define ATOMIC_VOLATILE_u32       __attribute__ ((aligned(4))) volatile uint32_t
#define ATOMIC_VOLATILE_i64       __attribute__ ((aligned(8))) volatile int64_t
#define ATOMIC_VOLATILE_u64       __attribute__ ((aligned(8))) volatile uint64_t


#define __CXATOMIC_INCREMENT(ptr)   __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST)
#define __CXATOMIC_DECREMENT(ptr)   __atomic_sub_fetch(ptr, 1, __ATOMIC_SEQ_CST)
#define __CXATOMIC_ADD(ptr, N)      __atomic_add_fetch(ptr, N, __ATOMIC_SEQ_CST)
#define __CXATOMIC_SUB(ptr, N)      __atomic_sub_fetch(ptr, N, __ATOMIC_SEQ_CST)
#define __CXATOMIC_READ(ptr)        __atomic_load_n(ptr, __ATOMIC_SEQ_CST)
#define __CXATOMIC_ASSIGN(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_SEQ_CST)

#define ATOMIC_INCREMENT_i32(ptr)   __CXATOMIC_INCREMENT(ptr)
#define ATOMIC_DECREMENT_i32(ptr)   __CXATOMIC_DECREMENT(ptr)
#define ATOMIC_ADD_i32(ptr, N)      __CXATOMIC_ADD(ptr, N)
#define ATOMIC_SUB_i32(ptr, N)      __CXATOMIC_SUB(ptr, N)
#define ATOMIC_READ_i32(ptr)        __CXATOMIC_READ(ptr)
#define ATOMIC_ASSIGN_i32(ptr, val) __CXATOMIC_ASSIGN(ptr, val)

#define ATOMIC_INCREMENT_u32(ptr)   __CXATOMIC_INCREMENT(ptr)
#define ATOMIC_DECREMENT_u32(ptr)   __CXATOMIC_DECREMENT(ptr)
#define ATOMIC_ADD_u32(ptr, N)      __CXATOMIC_ADD(ptr, N)
#define ATOMIC_SUB_u32(ptr, N)      __CXATOMIC_SUB(ptr, N)
#define ATOMIC_READ_u32(ptr)        __CXATOMIC_READ(ptr)
#define ATOMIC_ASSIGN_u32(ptr, val) __CXATOMIC_ASSIGN(ptr, val)

#define ATOMIC_INCREMENT_i64(ptr)   __CXATOMIC_INCREMENT(ptr)
#define ATOMIC_DECREMENT_i64(ptr)   __CXATOMIC_DECREMENT(ptr)
#define ATOMIC_ADD_i64(ptr, N)      __CXATOMIC_ADD(ptr, N)
#define ATOMIC_SUB_i64(ptr, N)      __CXATOMIC_SUB(ptr, N)
#define ATOMIC_READ_i64(ptr)        __CXATOMIC_READ(ptr)
#define ATOMIC_ASSIGN_i64(ptr, val) __CXATOMIC_ASSIGN(ptr, val)

#define ATOMIC_INCREMENT_u64(ptr)   __CXATOMIC_INCREMENT(ptr)
#define ATOMIC_DECREMENT_u64(ptr)   __CXATOMIC_DECREMENT(ptr)
#define ATOMIC_ADD_u64(ptr, N)      __CXATOMIC_ADD(ptr, N)
#define ATOMIC_SUB_u64(ptr, N)      __CXATOMIC_SUB(ptr, N)
#define ATOMIC_READ_u64(ptr)        __CXATOMIC_READ(ptr)
#define ATOMIC_ASSIGN_u64(ptr, val) __CXATOMIC_ASSIGN(ptr, val)


#else
#error "Unsupported platform"
#endif
/* synchronize/release - make it more explicit, compile error for sloppy code */



/* synchronize on specified mutex */
#define SYNCHRONIZE_ON( CS )                \
do {                                        \
  CXLOCK_TYPE *__pcslock__ = &((CS).lock);  \
  ENTER_CRITICAL_SECTION( __pcslock__ );    \
  do                                        \
/*
    {
      CODE GOES HERE
    }
*/

/* synchronize on specified mutex pointer */
#define SYNCHRONIZE_ON_PTR( pCS )             \
do {                                          \
  CXLOCK_TYPE *__pcslock__ = (pCS) ? &((pCS)->lock) : NULL; \
  if( __pcslock__ ) {                         \
    ENTER_CRITICAL_SECTION( __pcslock__ );    \
  }                                           \
  do                                          \
/*
    {
      CODE GOES HERE
    }
*/


#define FORCE_RELEASE                       \
  LEAVE_CRITICAL_SECTION( __pcslock__ );    \
  __pcslock__ = NULL                        \
  /*               ^ expect ; in user code for consistency */
  /* better know what you're doing at this point            */


#define SUSPEND_SYNCH_ON( CS )              \
  do {                                      \
    CS_LOCK *__pcslock__ = &((CS).lock);    \
    LEAVE_CRITICAL_SECTION( __pcslock__ );  \
    do


#define SUSPEND_SYNCH                       \
  do {                                      \
    LEAVE_CRITICAL_SECTION( __pcslock__ );  \
    do


#define RESUME_SYNCH                        \
    WHILE_ZERO;                             \
    ENTER_CRITICAL_SECTION( __pcslock__ );  \
  } WHILE_ZERO


#define SUSPEND_SLEEP( Milliseconds )       \
  SUSPEND_SYNCH {                           \
    sleep_milliseconds( Milliseconds );     \
  } RESUME_SYNCH


#define SUSPEND_ON_SLEEP( CS, Milliseconds )  \
  SUSPEND_SYNCH_ON( CS ) {                    \
    sleep_milliseconds( Milliseconds );       \
  } RESUME_SYNCH





#define RELEASE                             \
  WHILE_ZERO;                               \
  SUPPRESS_WARNING_UNBALANCED_LOCK_RELEASE  \
  if( __pcslock__ ) {                       \
    LEAVE_CRITICAL_SECTION( __pcslock__ );  \
  }                                         \
}                                           \
WHILE_ZERO



typedef enum e_cxlib_thread_priority {
  CXLIB_THREAD_PRIORITY_LOWEST = 0,
  CXLIB_THREAD_PRIORITY_BELOW_NORMAL = 1,
  CXLIB_THREAD_PRIORITY_NORMAL = 2,
  CXLIB_THREAD_PRIORITY_ABOVE_NORMAL = 3,
  CXLIB_THREAD_PRIORITY_HIGHEST = 4,
  CXLIB_THREAD_PRIORITY_DEFAULT = 5
} cxlib_thread_priority;



#if defined CXPLAT_WINDOWS_X64



typedef HANDLE cxlib_thread_t;

typedef unsigned ( __stdcall *f_cxlib_thread_entrypoint )( void * );


/**************************************************************************//**
 * THREAD_START
 *
 ******************************************************************************
 */
static int THREAD_START( cxlib_thread_t *hThread, uint32_t *thread_id, f_cxlib_thread_entrypoint entrypoint, void *arg ) {
  int ret = 0;

  uintptr_t startcode;
  // Create a new thread in suspended state
  if( (startcode = _beginthreadex( NULL, 0, entrypoint, arg, CREATE_SUSPENDED, thread_id )) > 0 ) {
    *hThread = (HANDLE)startcode;
    // Success, now start the thread
    if( ResumeThread( *hThread ) == (DWORD)-1 ) {
      // Error starting thread
      ret = -1;
    }
  }
  else {
    ret = -1;
  }

  return ret;
}




/**************************************************************************//**
 * THREAD_SET_PRIORITY
 *
 ******************************************************************************
 */
static int THREAD_SET_PRIORITY( cxlib_thread_priority priority ) {
  static int priority_map[] = {
    THREAD_PRIORITY_LOWEST,
    THREAD_PRIORITY_BELOW_NORMAL,
    THREAD_PRIORITY_NORMAL,
    THREAD_PRIORITY_ABOVE_NORMAL,
    THREAD_PRIORITY_HIGHEST,
    0
  };
  return SetThreadPriority( GetCurrentThread(), priority_map[priority] ) ? 0 : -1;
}




/**************************************************************************//**
 * THREAD_JOIN
 *
 ******************************************************************************
 */
static void THREAD_JOIN( cxlib_thread_t hThread, uint32_t timeout_ms ) {
  WaitForSingleObject( hThread, timeout_ms );
}




/**************************************************************************//**
 * THREAD_ZERO
 *
 ******************************************************************************
 */
static void THREAD_ZERO( cxlib_thread_t *phThread ) {
  memset( phThread, 0, sizeof( cxlib_thread_t ) );
}




/**************************************************************************//**
 * THREAD_IS_ZERO
 *
 ******************************************************************************
 */
static int THREAD_IS_ZERO( cxlib_thread_t *phThread ) {
  static char nothread[ sizeof( cxlib_thread_t ) ] = {0};
  return memcmp( phThread, nothread, sizeof( cxlib_thread_t ) ) == 0;
}






#define __THREAD_DESCRIPTION_MAX 511
#define __THREAD_DESCRIPTION     __cxlib_thread_description__



/**************************************************************************//**
 * __append_thread_description
 *
 ******************************************************************************
 */
static errno_t __append_thread_description( wchar_t *description, const char *string, size_t max_sz ) {
  size_t sz_descr = wcslen( description );
  size_t cc = 0;
  wchar_t *dest = description + sz_descr;
  size_t remain_capacity = max_sz - sz_descr;
  size_t n = strlen( string );
  if( n > remain_capacity ) {
    n = remain_capacity;
  }
  errno_t ret = mbstowcs_s( &cc, dest, remain_capacity, string, n );
  if( ret == 0 ) {
    SetThreadDescription( GetCurrentThread(), description );
  }
  return ret;
}


#define APPEND_THREAD_NAME( Name )  __append_thread_description( __THREAD_DESCRIPTION, Name, __THREAD_DESCRIPTION_MAX )


#define DECLARE_THREAD_FUNCTION( FunctionName )  static unsigned __stdcall FunctionName( void *ArgPtr )


#define BEGIN_THREAD_FUNCTION( FunctionName, Description, ArgType, ArgPtrName )             \
static unsigned __stdcall FunctionName( void *ArgPtr ) {                                    \
  int64_t __cxthread_reference_ns__ = cxlib_thread_init();                                  \
  wchar_t __THREAD_DESCRIPTION[ (__THREAD_DESCRIPTION_MAX + 1) * sizeof( wchar_t ) ] = { 0 };   \
  ArgType *ArgPtrName = (ArgType*)ArgPtr;                                                   \
  if( __cxthread_reference_ns__ > 0 ) {                                                     \
    if( APPEND_THREAD_NAME( Description ) == 0 ) {                                          \
      do /* {
       ...
       Thread body goes here 
       ...
      } */
#define END_THREAD_FUNCTION                                         \
      WHILE_ZERO;                                                   \
    }                                                               \
  }                                                                 \
  cxlib_thread_clear();                                             \
  _endthreadex( 0 );                                                \
  return 0;                                                         \
}


#else

typedef pthread_t cxlib_thread_t;

typedef void * ( *f_cxlib_thread_entrypoint )( void * );


/**************************************************************************//**
 * THREAD_START
 *
 ******************************************************************************
 */
static int THREAD_START( cxlib_thread_t *thread, uint32_t *thread_id, f_cxlib_thread_entrypoint entrypoint, void *arg ) {
  int ret = 0;
  
  pthread_attr_t attr;

  // Initialize thread attributes
  if( (ret = pthread_attr_init( &attr )) == 0 ) {

    // Create and start a new thread
    if( (ret = pthread_create( thread, &attr, entrypoint, arg )) == 0 ) {
      // New thread now running.
      unsigned long int id = *((unsigned long int*)thread); // <= WARNING: Not portable - we're assuming pthread_t is typedef'ed as integer
      *thread_id = (uint32_t)id;
    }

    // Destroy thread attributes
    pthread_attr_destroy( &attr );
  }

  return ret;
}



#if defined(CXPLAT_LINUX_ANY)

/**************************************************************************//**
 * THREAD_SET_PRIORITY
 *
 ******************************************************************************
 */
static int THREAD_SET_PRIORITY( cxlib_thread_priority priority ) {
  /*
  * NOTE: For this to have any effect you will need to grant permission:
  * 
  * sudo setcap cap_sys_nice=eip /usr/local/bin/python3.12
  * 
  */
  static int nice_map[] = {
    10,   // LOWEST
    5,    // BELOW NORMAL
    0,    // NORMAL
    -5,   // ABOVE NORMAL
    -10,  // HIGHEST
    0     // DEFAULT
  };
  return setpriority( PRIO_PROCESS, gettid(), nice_map[priority] );
}
#elif defined(CXPLAT_MAC_ARM64)
#include <mach/thread_policy.h>
#include <mach/mach_init.h>
#include <mach/mach.h>


/**************************************************************************//**
 * _get_current_thread_importance
 *
 ******************************************************************************
 */
static int _get_current_thread_importance(void) {
    mach_port_t thread = pthread_mach_thread_np(pthread_self());

    thread_precedence_policy_data_t policy;
    mach_msg_type_number_t count = THREAD_PRECEDENCE_POLICY_COUNT;
    boolean_t get_default = FALSE;

    kern_return_t kr = thread_policy_get(thread,
                                         THREAD_PRECEDENCE_POLICY,
                                         (thread_policy_t)&policy,
                                         &count,
                                         &get_default);

    if (kr != KERN_SUCCESS) {
        return -1;
    }

    return policy.importance;
}



/**************************************************************************//**
 * THREAD_SET_PRIORITY
 *
 ******************************************************************************
 */
static int THREAD_SET_PRIORITY( cxlib_thread_priority priority ) {
  // NOTE: Call this only once per thread since 
  //       it will set priority relative to previous 
  //       setting.

  static int priority_map[] = {
      -10,  // LOWEST
      -5,   // BELOW NORMAL
      0,    // NORMAL
      5,    // ABOVE NORMAL
      10,   // HIGHEST
      0     // DEFAULT
    };

    int current_importance = _get_current_thread_importance();
    if( current_importance < 0 ) {
      return -1;
    }

    mach_port_t thread = pthread_mach_thread_np(pthread_self());

    thread_precedence_policy_data_t policy;
    policy.importance = current_importance + priority_map[priority];

    kern_return_t kr = thread_policy_set(thread,
                                         THREAD_PRECEDENCE_POLICY,
                                         (thread_policy_t)&policy,
                                         THREAD_PRECEDENCE_POLICY_COUNT);

    if( kr != KERN_SUCCESS ) {
      return -1;
    }

    return 0;
}

#else
#error "Unsupported platform"
#endif



#if defined CXPLAT_LINUX_ANY

/**************************************************************************//**
 * THREAD_JOIN
 *
 ******************************************************************************
 */
static void THREAD_JOIN( cxlib_thread_t thread, uint32_t timeout_ms ) {
  struct timespec ts = {0};
  int64_t now_ms = __MILLISECONDS_SINCE_1970();
  ts.tv_sec = (now_ms + timeout_ms) / 1000;
  pthread_timedjoin_np( thread, NULL, &ts );
}

#elif defined CXPLAT_MAC_ARM64

/**************************************************************************//**
 * THREAD_JOIN
 *
 ******************************************************************************
 */
static void THREAD_JOIN( cxlib_thread_t thread, uint32_t timeout_ms ) {
  struct timespec ts = {0};
  int64_t now_ms = __MILLISECONDS_SINCE_1970();
  ts.tv_sec = (now_ms + timeout_ms) / 1000;
  // TODO: Find a solution for timed join, which is not available on MacOS
  pthread_join( thread, NULL ); // Let's hope we don't block forever
}
#else
#error "Unsupported platform"
#endif



/**************************************************************************//**
 * THREAD_ZERO
 *
 ******************************************************************************
 */
static void THREAD_ZERO( cxlib_thread_t *p_thread ) {
  memset( p_thread, 0, sizeof( cxlib_thread_t ) );
}




/**************************************************************************//**
 * THREAD_IS_ZERO
 *
 ******************************************************************************
 */
static int THREAD_IS_ZERO( cxlib_thread_t *p_thread ) {
  static char nothread[ sizeof( cxlib_thread_t ) ] = {0};
  return memcmp( p_thread, nothread, sizeof( cxlib_thread_t ) ) == 0;
}


#define APPEND_THREAD_NAME( Name )  ((void)0)


#define DECLARE_THREAD_FUNCTION( FunctionName )  static void * FunctionName( void *ArgPtr )


#define BEGIN_THREAD_FUNCTION( FunctionName, Description, ArgType, ArgPtrName ) \
static void * FunctionName( void *ArgPtr ) {                       \
  int64_t __cxthread_reference_ns__ = cxlib_thread_init();         \
  const char *__description__ = Description;                       \
  ArgType *ArgPtrName = (ArgType*)ArgPtr;                          \
  if( __description__ && __cxthread_reference_ns__ > 0 )           \
/* ...
   Thread body goes here 
   ...
*/
#define END_THREAD_FUNCTION                                         \
  cxlib_thread_clear();                                             \
  int ret = 0;                                                      \
  pthread_exit( &ret );                                             \
}


#endif



#if defined CXPLAT_WINDOWS_X64

typedef HANDLE WAITABLE_TIMER;

#else

typedef void* WAITABLE_TIMER;

#endif



/*******************************************************************//**
 * Enter a loop and remain in loop until BlockCondition becomes false
 * or a timeout occurs. The TimeoutMilliseconds is specified as:
 * < 0    : no timeout (block indefinitely until BlockCondition is false)
 * 0      : run loop exactly once regardless of BlockCondition (immediate return)
 * > 0    : block for at most the number of specified milliseconds
 *
 ***********************************************************************
 */
#define BEGIN_TIME_LIMITED_WHILE( BlockCondition, TimeoutMilliseconds, HaltedPtr )    \
  do {                                                                                \
    int64_t __timeout__ = TimeoutMilliseconds;                                        \
    int64_t __t0__ = 0;                                                               \
    bool *__is_halted__ = HaltedPtr;                                                  \
    bool __cancel__ = false;                                                          \
    /* Stay in loop as long as the blocking condition is true and we have not */      \
    /* aborted the loop due to timeout.                                       */      \
    while( (BlockCondition) && !__cancel__ ) {                                        \
      /* Set the start time if a timeout is specified */                              \
      if( __timeout__ > 0 && __t0__ == 0 ) {                                          \
        __t0__ = __GET_CURRENT_MILLISECOND_TICK() + 1;                                \
      }                                                                               \
      do { /* LOOP BODY HERE */                                                       \

#define END_TIME_LIMITED_WHILE                                                        \
      } WHILE_ZERO;                                                                   \
      /* Break out of loop if we're set to return immediately or if we timed out */   \
      if( __timeout__ == 0                                                            \
          ||                                                                          \
          ( __timeout__ > 0 && __ELAPSED_MILLISECONDS( __t0__ ) > __timeout__ )       \
        ) {                                                                           \
        __cancel__ = true;                                                            \
        if( __is_halted__ ) {                                                         \
          *__is_halted__ = true;                                                      \
        }                                                                             \
      }                                                                               \
    }                                                                                 \
  } WHILE_ZERO


#define SYNCHRONIZED_WAIT_UNTIL( ContinueCondition, TimeoutMilliseconds )         \
  BEGIN_TIME_LIMITED_WHILE( !(ContinueCondition), TimeoutMilliseconds, NULL ) {   \
    SUSPEND_SLEEP( 10 );                                                          \
  } END_TIME_LIMITED_WHILE;                                                       \


#define SYNCHRONIZED_ON_WAIT_UNTIL( CS, ContinueCondition, TimeoutMilliseconds )    \
  do {                                                                              \
    CS_LOCK *__pcslock__ = &((CS).lock);                                            \
    BEGIN_TIME_LIMITED_WHILE( !(ContinueCondition), TimeoutMilliseconds, NULL ) {   \
      SUSPEND_SLEEP( 10 );                                                          \
    } END_TIME_LIMITED_WHILE;                                                       \
  } WHILE_ZERO





#ifdef __cplusplus
}
#endif

#endif
