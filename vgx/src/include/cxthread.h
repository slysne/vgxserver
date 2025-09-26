/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    cxthread.h
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

#ifndef CXTHREAD_INCLUDED_H
#define CXTHREAD_INCLUDED_H


#include "objectmodel.h"
#include "cxcstring.h"


DLL_COMLIB_PUBLIC extern const char * SET_CURRENT_THREAD_LABEL( const char *label );
DLL_COMLIB_PUBLIC extern const char * GET_CURRENT_THREAD_LABEL( char *dest );
DLL_COMLIB_PUBLIC extern uint32_t GET_CURRENT_THREAD_ID( void );
DLL_COMLIB_PUBLIC extern uint32_t GET_CURRENT_PROCESS_ID( void );

DLL_COMLIB_PUBLIC extern const void * COMLIB_TRAP( const void *obj );
DLL_COMLIB_PUBLIC extern void COMLIB__Spin( int microseconds );


struct s_comlib_task_t;

typedef unsigned (*f_comlib_task_initialize)( struct s_comlib_task_t *task );

typedef unsigned (*f_comlib_task_shutdown)( struct s_comlib_task_t *task );


typedef union u_comlib_task_state_t {
  int64_t _bits;
  struct {
    uint32_t _acquired_thread;    // Thread ID of thread that currently holds task acquired
    int8_t _n_acquired;           // Acquisition recursion
    int8_t _lock_recursion;       // Recursion counter of state mutex
    struct {
      uint8_t Run           : 1;  //
      uint8_t Stop          : 1;  //
      uint8_t Suspend       : 1;  //
      uint8_t Resume        : 1;  //
      uint8_t ForceExit     : 1;  //
      uint8_t _R6           : 1;  //
      uint8_t _R7           : 1;  //
      uint8_t _R8           : 1;  //
    } req;
    struct {
      uint8_t Alive         : 1;  //
      uint8_t Stopping      : 1;  //
      uint8_t Dead          : 1;  //
      uint8_t Suspending    : 1;  //
      uint8_t Suspended     : 1;  //
      uint8_t Resuming      : 1;  //
      uint8_t Busy          : 1;  //
      uint8_t Sleeping      : 1;  //
    } ack;
  };
} comlib_task_state_t;



#define COMLIB_TASK_HEAD                                \
  /* 1 --------------------------------------------- */ \
  union {                                               \
    cacheline_t _cl1;                                   \
    struct {                                            \
      /* [Q1.1] */                                      \
      cxlib_thread_t _THREAD;                           \
      /* [Q1.2.1] */                                    \
      uint32_t _thread_id;                              \
      /* [Q1.2.2] */                                    \
      int _retcode;                                     \
      /* [Q1.3] */                                      \
      f_cxlib_thread_entrypoint _entrypoint;            \
      /* [Q1.4] */                                      \
      void *_task_data;                                 \
      /* [Q1.5] */                                      \
      f_comlib_task_initialize _initialize;             \
      /* [Q1.6] */                                      \
      f_comlib_task_shutdown _shutdown;                 \
      /* [Q1.7] */                                      \
      int64_t _thread_t_start;                          \
      /* [Q1.8] */                                      \
      int64_t _thread_t_end;                            \
    };                                                  \
  };                                                    \
  /* 2 --------------------------------------------- */ \
  union {                                               \
    cacheline_t _cl2;                                   \
    /* [Q2] */                                          \
    CS_LOCK _lock;                                      \
  };                                                    \
  /* 3 --------------------------------------------- */ \
  union {                                               \
    cacheline_t _cl3;                                   \
    struct {                                            \
      /* [Q3.1] */                                      \
      comlib_task_state_t _state_TCS;                   \
      /* [Q3.2] */                                      \
      CString_t *CSTR__description;                     \
      /* [Q3.3] */                                      \
      CString_t *CSTR__error;                           \
      /* [Q3.4] */                                      \
      int64_t _thread_ns_reference;                     \
      /* [Q3.5] */                                      \
      int64_t total_sleep_ns;                           \
    };                                                  \
  };                                                    \
  /* 4 --------------------------------------------- */ \
  union {                                               \
    cacheline_t _cl4;                                   \
    struct {                                            \
      /* [Q4.1-6] */                                    \
      CS_COND _idle_event;                              \
      /* [Q4.7] */                                      \
      QWORD __rsv_4_7;                                  \
      /* [Q4.8] */                                      \
      QWORD __rsv_4_8;                                  \
    };                                                  \
  };                                                    \
  /* ----------------------------------------------- */ \
  union {                                               \
    cacheline_t _cl5;                                   \
    struct {                                            \
      /* [Q5.1-6] */                                    \
      CS_COND _wake_event;                              \
      /* [Q5.7] */                                      \
      QWORD __rsv_5_7;                                  \
      /* [Q5.8.1] */                                    \
      int delete_self_on_thread_exit;                   \
      /* [Q5.8.2] */                                    \
      int n_threads_suspended_waiting;                  \
    };                                                  \
  }




CALIGNED_TYPE(struct) s_comlib_task_t {
  COMLIB_TASK_HEAD;
} comlib_task_t;


typedef float (*f_comlib_task_progress)( comlib_task_t *task );


DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__Delete( comlib_task_t **task );
DLL_COMLIB_PUBLIC extern comlib_task_t * COMLIB_TASK__New( f_cxlib_thread_entrypoint entrypoint, f_comlib_task_initialize initialize, f_comlib_task_shutdown shutdown, void *task_data );
DLL_COMLIB_PUBLIC extern comlib_task_t * COMLIB_TASK__SetInitialize( comlib_task_t *task, f_comlib_task_initialize initialize );
DLL_COMLIB_PUBLIC extern comlib_task_t * COMLIB_TASK__SetShutdown( comlib_task_t *task, f_comlib_task_shutdown shutdown );
DLL_COMLIB_PUBLIC extern void            COMLIB_TASK__DeleteSelfOnThreadExit( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern comlib_task_t * COMLIB_TASK__SetData( comlib_task_t *task, void *task_data );
DLL_COMLIB_PUBLIC extern void *          COMLIB_TASK__GetData( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern uint32_t        COMLIB_TASK__ThreadId( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern const char *    COMLIB_TASK__Description( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern const char *    COMLIB_TASK__AppendDescription( comlib_task_t *task, const char *string );
DLL_COMLIB_PUBLIC extern CString_t *     COMLIB_TASK__SetError( comlib_task_t *task, const char *message );
DLL_COMLIB_PUBLIC extern const char *    COMLIB_TASK__GetError( comlib_task_t *task );

DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__Start( comlib_task_t *task, int timeout_ms );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__Stop( comlib_task_t *task, f_comlib_task_progress progressf, int timeout_ms );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__Suspend( comlib_task_t *task, f_comlib_task_progress progressf, int timeout_ms );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__Resume( comlib_task_t *task, int timeout_ms );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__ForceExit( comlib_task_t *task, int timeout_ms );
DLL_COMLIB_PUBLIC extern comlib_task_t * COMLIB_TASK__StartNew( f_cxlib_thread_entrypoint entrypoint, f_comlib_task_initialize initialize, f_comlib_task_shutdown shutdown, void *task_data, int timeout_ms );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__StopDelete( comlib_task_t **task );
DLL_COMLIB_PUBLIC extern int64_t         COMLIB_TASK__StartTime( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern int64_t         COMLIB_TASK__EndTime( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern int64_t         COMLIB_TASK__Age( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern double          COMLIB_TASK__AgeSeconds( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern double          COMLIB_TASK__LifetimeIdle( comlib_task_t *task );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__SetReturnCode( comlib_task_t *task, int retcode );
DLL_COMLIB_PUBLIC extern int             COMLIB_TASK__GetReturnCode( comlib_task_t *task );





/**************************************************************************//**
 * __enter_task_CS
 *
 ******************************************************************************
 */
__inline static int16_t __enter_task_CS( comlib_task_t *task ) {
  // UNSAFE HERE
  ENTER_CRITICAL_SECTION( &task->_lock.lock );
  // SAFE HERE
  return ++(task->_state_TCS._lock_recursion);
}



/**************************************************************************//**
 * __leave_task_CS
 *
 ******************************************************************************
 */
__inline static int16_t __leave_task_CS( comlib_task_t *task ) {
  // SAFE HERE
  int16_t c = --(task->_state_TCS._lock_recursion);
  LEAVE_CRITICAL_SECTION( &task->_lock.lock );
  // UNSAFE HERE
  return c;
}




#define COMLIB_TASK_LOCK( Task )      \
  do {                                \
    comlib_task_t *__ptask__ = Task;  \
    __enter_task_CS( __ptask__ );     \
    do


#define COMLIB_TASK_RELEASE           \
    WHILE_ZERO;                       \
    __leave_task_CS( __ptask__ );     \
  } WHILE_ZERO




/**************************************************************************//**
 * COMLIB_TASK_TRY_LOCK
 *
 ******************************************************************************
 */
__inline static bool COMLIB_TASK_TRY_LOCK( comlib_task_t *task ) {
  if( !TRY_CRITICAL_SECTION( &task->_lock.lock ) ) {
    return false;
  }
  // We have task lock
  ++(task->_state_TCS._lock_recursion);
  return true;
}



/**************************************************************************//**
 * COMLIB_TASK_RELEASE_LOCK
 *
 ******************************************************************************
 */
__inline static void COMLIB_TASK_RELEASE_LOCK( comlib_task_t *task ) {
  __leave_task_CS( task );
}



#define COMLIB_TASK_FORCE_RELEASE __leave_task_CS( __ptask__ )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int16_t __prewait_leave_CS( comlib_task_t *task ) {
  int16_t r = task->_state_TCS._lock_recursion;
  // Leave all but one recursion
  while( task->_state_TCS._lock_recursion > 1 ) {
    __leave_task_CS( task );
  }
  // Increment number of threads waiting
  task->n_threads_suspended_waiting++;
  // Down to 0
  task->_state_TCS._lock_recursion--;
  return r;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __postwait_reenter_CS( comlib_task_t *task, int16_t recursion ) {
  // Up to 1
  task->_state_TCS._lock_recursion++;
  // Decrement number of threads waiting
  task->n_threads_suspended_waiting--;
  // Re-enter to prewait recursion
  while( task->_state_TCS._lock_recursion < recursion ) {
    __enter_task_CS( task );
  }   
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_SUSPEND_LOCK( Task )                                  \
  do {                                                                    \
    /* SAFE HERE */                                                       \
    comlib_task_t *__ptask__ = Task;                                      \
    int16_t __presus_recursion__ = __ptask__->_state_TCS._lock_recursion;  \
    __ptask__->n_threads_suspended_waiting++;                             \
    while( __leave_task_CS( __ptask__ ) > 0 );                            \
    /* UNSAFE HERE */                                                     \
    do


#define COMLIB_TASK_RESUME_LOCK                                   \
    /* UNSAFE HERE */                                             \
    WHILE_ZERO;                                                   \
    while( __enter_task_CS( __ptask__ ) < __presus_recursion__ ); \
    /* SAFE HERE */                                               \
    __ptask__->n_threads_suspended_waiting--;                     \
  } WHILE_ZERO




#define COMLIB_TASK_SIGNAL_IDLE( Task )     SIGNAL_ALL_CONDITION( &((Task)->_idle_event.cond) )
#define COMLIB_TASK_SIGNAL_WAKE( Task )     SIGNAL_ALL_CONDITION( &((Task)->_wake_event.cond) )




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_WAIT_FOR_IDLE( Task, TimeoutMilliseconds )                \
  do {                                                                        \
    /* SAFE HERE */                                                           \
    comlib_task_t *__ptask__ = Task;                                          \
    int16_t __prewait_recursion = __prewait_leave_CS( __ptask__ );            \
    /* Release one lock and sleep until condition or timeout */               \
    TIMED_WAIT_CONDITION_CS( &(__ptask__->_idle_event.cond), &(__ptask__->_lock.lock), TimeoutMilliseconds );  \
    /* One lock now re-acquired */                                            \
    /* SAFE HERE */                                                           \
    __postwait_reenter_CS( __ptask__, __prewait_recursion );                  \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_WAIT_FOR_WAKE( Task, TimeoutMilliseconds )                \
  do {                                                                        \
    /* SAFE HERE */                                                           \
    comlib_task_t *__ptask__ = Task;                                          \
    int16_t __prewait_recursion = __prewait_leave_CS( __ptask__ );          \
    /* Release one lock and sleep until condition or timeout */               \
    TIMED_WAIT_CONDITION_CS( &(__ptask__->_wake_event.cond), &(__ptask__->_lock.lock), TimeoutMilliseconds );  \
    /* One lock now re-acquired */                                            \
    /* SAFE HERE */                                                           \
    __postwait_reenter_CS( __ptask__, __prewait_recursion );                 \
  } WHILE_ZERO




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_IDLE_SLEEP( Task, TimeoutMilliseconds )                   \
  do {                                                                        \
    /* SAFE HERE */                                                           \
    comlib_task_t *__ptask__ = Task;                                          \
    int16_t __prewait_recursion = __prewait_leave_CS( __ptask__ );          \
    COMLIB_TASK__SetState_Sleeping( __ptask__ );                              \
    /* Release one lock and sleep until condition or timeout */               \
    TIMED_WAIT_CONDITION_CS( &(__ptask__->_wake_event.cond), &(__ptask__->_lock.lock), TimeoutMilliseconds );  \
    COMLIB_TASK__ClearState_Sleeping( __ptask__ );                            \
    /* One lock now re-acquired */                                            \
    /* SAFE HERE */                                                           \
    __postwait_reenter_CS( __ptask__, __prewait_recursion );                 \
  } WHILE_ZERO




/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void COMLIB_TASK__SignalIdle( comlib_task_t *task ) {
  COMLIB_TASK_LOCK( task ) {
    COMLIB_TASK_SIGNAL_IDLE( task );
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void COMLIB_TASK__WaitForIdle( comlib_task_t *task, int milliseconds ) {
  COMLIB_TASK_LOCK( task ) {
    COMLIB_TASK_WAIT_FOR_IDLE( task, milliseconds );
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static void COMLIB_TASK__SignalWake( comlib_task_t *task ) {
  COMLIB_TASK_LOCK( task ) {
    COMLIB_TASK_SIGNAL_WAKE( task );
  } COMLIB_TASK_RELEASE;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __comlib_task_delete_waitable_timer( WAITABLE_TIMER *WT ) {
  if( WT ) {
#if defined CXPLAT_WINDOWS_X64
    CloseHandle( *WT );
    *WT = NULL;
#endif
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __comlib_task_new_waitable_timer( WAITABLE_TIMER *WT ) {
  int ret = 0;
#if defined CXPLAT_WINDOWS_X64
  *WT = CreateWaitableTimerExW( NULL,
                                NULL,
                                CREATE_WAITABLE_TIMER_MANUAL_RESET, 
                                DELETE | SYNCHRONIZE | EVENT_MODIFY_STATE );
  if( *WT == 0 ) {
    ret = -1;
  }
#endif
  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_SUSPENDED_MILLISECONDS( Task, Milliseconds )  \
  COMLIB_TASK_SUSPEND_LOCK( Task ) {                              \
    sleep_milliseconds( Milliseconds );                           \
  } COMLIB_TASK_RESUME_LOCK



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_SUSPENDED_SPIN( Task, Microseconds )  \
  COMLIB_TASK_SUSPEND_LOCK( Task ) {                      \
    COMLIB__Spin( Microseconds );                         \
  } COMLIB_TASK_RESUME_LOCK




/**************************************************************************//**
 * __sleep_microseconds_windows
 *
 ******************************************************************************
 */
__inline static void __sleep_microseconds_windows( WAITABLE_TIMER T, int64_t us ) {
  if( sleep_nanoseconds( T, 1000*us ) < 0 ) {
    COMLIB__Spin( (int)us ); // fallback
  }
}



/**************************************************************************//**
 * __sleep_microseconds_unix
 *
 ******************************************************************************
 */
__inline static void __sleep_microseconds_unix( WAITABLE_TIMER T, int64_t us ) {
  sleep_nanoseconds( T, us );
}



#ifdef CXPLAT_WINDOWS_X64
#define SleepMicroseconds( Timer, MicroSec ) __sleep_microseconds_windows( Timer, MicroSec )
#else
#define SleepMicroseconds( Timer, MicroSec ) __sleep_microseconds_unix( Timer, MicroSec )
#endif



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_SUSPENDED_SLEEP( Task, Timer, Microseconds )  \
  COMLIB_TASK_SUSPEND_LOCK( Task ) {                              \
    SleepMicroseconds( Timer, Microseconds );                     \
  } COMLIB_TASK_RESUME_LOCK



typedef int64_t comlib_task_delay_t;

/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_LOOP_DELAY( Milliseconds ) ((Milliseconds) * 1000000LL)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_LOOP_DELAY_MICROSECONDS( Microseconds ) ((Microseconds) * 1000)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_LOOP_DELAY_NANOSECONDS( Nanoseconds ) (Nanoseconds)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_SUSPEND_MILLISECONDS( Task, Milliseconds )  \
  do {                                                          \
    COMLIB_TASK_SUSPEND_LOCK( Task ) {                          \
      sleep_milliseconds( Milliseconds );                       \
    } COMLIB_TASK_RESUME_LOCK;                                  \
  } WHILE_ZERO



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static comlib_task_t * __comlib_task__acquire( comlib_task_t *task ) {
  task->_state_TCS._acquired_thread = GET_CURRENT_THREAD_ID();
  if( task->_state_TCS._n_acquired < 127 ) {
    task->_state_TCS._n_acquired++;
    return task;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __comlib_task__release( comlib_task_t *task ) {
  if( task->_state_TCS._n_acquired > 0 ) {
    if( --task->_state_TCS._n_acquired == 0 ) {
      task->_state_TCS._acquired_thread = 0;
    }
    return task->_state_TCS._n_acquired;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int __comlib_task__acquisitions( comlib_task_t *task ) {
  return task->_state_TCS._n_acquired;
}



/*******************************************************************//**
 * Acquire Task.
 * Return task pointer if acquired.
 * Return NULL if not acquired.
 ***********************************************************************
 */
__inline static comlib_task_t * COMLIB_TASK__Acquire( comlib_task_t *task, int timeout_ms ) {
  comlib_task_t *task_WL = NULL;
  uint32_t threadid = GET_CURRENT_THREAD_ID();
  COMLIB_TASK_LOCK( task ) {
    comlib_task_state_t *s = &task->_state_TCS;
    // Task is available for immediate acquisition
    if( s->_n_acquired == 0 || s->_acquired_thread == threadid ) {
      task_WL = __comlib_task__acquire( task );
    }
    // Not available for immediate acquisition, enter timeout loop
    else {
      BEGIN_TIME_LIMITED_WHILE( task_WL == NULL, timeout_ms, NULL ) {
        COMLIB_TASK_SUSPEND_MILLISECONDS( task, 5 );
        if( s->_n_acquired == 0 ) {
          task_WL = __comlib_task__acquire( task );
        }
      } END_TIME_LIMITED_WHILE;
    }
  } COMLIB_TASK_RELEASE;
  return task_WL;
}



/*******************************************************************//**
 * Release Task.
 * Return 0 if fully released
 * Return 1 or greater if thread still holds recursive acquisition(s)
 * Return -1 if task was not locked by calling thread
 ***********************************************************************
 */
__inline static int COMLIB_TASK__Release( comlib_task_t *task_WL ) {
  int r = -1;
  COMLIB_TASK_LOCK( task_WL ) {
    // Task is owned by current thread, proceed with release
    if( task_WL->_state_TCS._acquired_thread == GET_CURRENT_THREAD_ID() ) {
      r = __comlib_task__release( task_WL );
    }
  } COMLIB_TASK_RELEASE;
  return r;
}



/*******************************************************************//**
 * 
 * 
 *
 *
 ***********************************************************************
 */
__inline static int COMLIB_TASK__Acquisitions( comlib_task_t *task_WL ) {
  int r = -1;
  COMLIB_TASK_LOCK( task_WL ) {
    // Task is owned by current thread, get recursion count
    if( task_WL->_state_TCS._acquired_thread == GET_CURRENT_THREAD_ID() ) {
      r = __comlib_task__acquisitions( task_WL );
    }
  } COMLIB_TASK_RELEASE;
  return r;
}




#define __define__COMLIB_TASK__Request_X( X )                                 \
__inline static int COMLIB_TASK__Request_ ## X ( comlib_task_t *task ) {      \
  COMLIB_TASK_LOCK( task ) {                                                  \
    task->_state_TCS.req. X = 1;                                              \
  } COMLIB_TASK_RELEASE;                                                      \
  return 1;                                                                   \
}

#define __define__COMLIB_TASK__IsRequested_X( X )                             \
__inline static int COMLIB_TASK__IsRequested_ ## X ( comlib_task_t *task ) {  \
  int req;                                                                    \
  COMLIB_TASK_LOCK( task ) {                                                  \
    req = task->_state_TCS.req. X;                                            \
  } COMLIB_TASK_RELEASE;                                                      \
  return req;                                                                 \
}

#define __define__COMLIB_TASK__AcceptRequest_X( X )                             \
__inline static int COMLIB_TASK__AcceptRequest_ ## X ( comlib_task_t *task ) {  \
  COMLIB_TASK_LOCK( task ) {                                                    \
    task->_state_TCS.req. X = 0;                                                \
  } COMLIB_TASK_RELEASE;                                                        \
  return 0;                                                                     \
}



#define __define__COMLIB_TASK__SetState_X( X )                                \
__inline static int COMLIB_TASK__SetState_ ## X ( comlib_task_t *task ) {     \
  COMLIB_TASK_LOCK( task ) {                                                  \
    task->_state_TCS.ack. X = 1;                                              \
  } COMLIB_TASK_RELEASE;                                                      \
  return 1;                                                                   \
}

#define __define__COMLIB_TASK__IsX( X )                                       \
__inline static int COMLIB_TASK__Is ## X ( comlib_task_t *task ) {            \
  int ack;                                                                    \
  COMLIB_TASK_LOCK( task ) {                                                  \
    ack = task->_state_TCS.ack. X;                                            \
  } COMLIB_TASK_RELEASE;                                                      \
  return ack;                                                                 \
}

#define __define__COMLIB_TASK__IsNotX( X )                                    \
__inline static int COMLIB_TASK__IsNot ## X ( comlib_task_t *task ) {         \
  return !COMLIB_TASK__Is ## X ( task );                                      \
}


#define __define__COMLIB_TASK__ClearState_X( X )                              \
__inline static int COMLIB_TASK__ClearState_ ## X ( comlib_task_t *task ) {   \
  COMLIB_TASK_LOCK( task ) {                                                  \
    task->_state_TCS.ack. X = 0;                                              \
  } COMLIB_TASK_RELEASE;                                                      \
  return 0;                                                                   \
}





#define __define__COMLIB_TASK__req_flag_X( X )  \
  __define__COMLIB_TASK__Request_X( X )         \
  __define__COMLIB_TASK__IsRequested_X( X )     \
  __define__COMLIB_TASK__AcceptRequest_X( X )   \


 #define __define__COMLIB_TASK__ack_flag_X( X ) \
  __define__COMLIB_TASK__SetState_X( X )        \
  __define__COMLIB_TASK__IsX( X )               \
  __define__COMLIB_TASK__IsNotX( X )            \
  __define__COMLIB_TASK__ClearState_X( X )      \
  
 

__define__COMLIB_TASK__req_flag_X( Run )
__define__COMLIB_TASK__req_flag_X( Stop )
__define__COMLIB_TASK__req_flag_X( Suspend )
__define__COMLIB_TASK__req_flag_X( Resume )
__define__COMLIB_TASK__req_flag_X( ForceExit )
__define__COMLIB_TASK__req_flag_X( _R6 )
__define__COMLIB_TASK__req_flag_X( _R7 )
__define__COMLIB_TASK__req_flag_X( _R8 )


__define__COMLIB_TASK__ack_flag_X( Alive )
__define__COMLIB_TASK__ack_flag_X( Stopping )
__define__COMLIB_TASK__ack_flag_X( Dead )
__define__COMLIB_TASK__ack_flag_X( Suspending )
__define__COMLIB_TASK__ack_flag_X( Suspended )
__define__COMLIB_TASK__ack_flag_X( Resuming )
__define__COMLIB_TASK__ack_flag_X( Busy )
__define__COMLIB_TASK__ack_flag_X( Sleeping )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int COMLIB_TASK__IsStarting( comlib_task_t *task ) {
  int starting;
  COMLIB_TASK_LOCK( task ) {
    comlib_task_state_t *s = &task->_state_TCS;
    // Thread is starting if not yet alive and not dead.
    starting = !s->ack.Alive && !s->ack.Dead;
  } COMLIB_TASK_RELEASE;
  return starting;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int COMLIB_TASK__IsStateChangeRequested( comlib_task_t *task ) {
  int state_change;
  COMLIB_TASK_LOCK( task ) {
    comlib_task_state_t *s = &task->_state_TCS;
    // Thread is starting if not yet alive and not dead.
    state_change = s->req.Suspend || s->req.Resume || s->req.Stop;
  } COMLIB_TASK_RELEASE;
  return state_change;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int COMLIB_TASK__RunLoop( comlib_task_t *task ) {
  int run_loop = 0;
  COMLIB_TASK_LOCK( task ) {
    comlib_task_state_t *s = &task->_state_TCS;
    // Alive when thread is running and we have not requested a forced exit
    int alive = s->ack.Alive && !s->req.ForceExit;
    // Exit loop if stopping is acknowledged and the stop request has been accepted
    int stop = s->ack.Stopping && !s->req.Stop;
    // Run loop when alive and stop condition not met
    if( alive && !stop ) {
      run_loop = 1;
    }
  } COMLIB_TASK_RELEASE;
  return run_loop;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int COMLIB_TASK__IsDefunct( comlib_task_t *task ) {
  int defunct = 0;
  COMLIB_TASK_LOCK( task ) {
    comlib_task_state_t *s = &task->_state_TCS;
    // Defunct if 1) dead OR 2) not alive and no request to run
    if( s->ack.Dead || (!s->ack.Alive && !s->req.Run) ) {
      defunct = 1;
    }
  } COMLIB_TASK_RELEASE;
  return defunct;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_LOOP_SLEEP( DelayNanosec )  sleep_nanoseconds( _WT_, (int64_t)DelayNanosec )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_TIMER (&_WT_)


/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define COMLIB_TASK_NANOSECOND_REFERENCE( Task ) (Task->_thread_ns_reference)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define BEGIN_COMLIB_TASK_MAIN_LOOP( LoopDelay )    \
  COMLIB_TASK_LOCK( _CT_ ) {                        \
    comlib_task_t *__task__ = _CT_;                 \
    comlib_task_delay_t *__pdly__ = &(LoopDelay);   \
    do {                                            \
      COMLIB_TASK_SUSPEND_LOCK( __task__ ) {        \
        comlib_task_delay_t __dly__ = *__pdly__;    \
        if( __dly__ > 0 ) {                                 \
          int64_t __t0__ = __GET_CURRENT_NANOSECOND_TICK(); \
          COMLIB_TASK_LOOP_SLEEP( __dly__ );                \
          int64_t __t1__ = __GET_CURRENT_NANOSECOND_TICK(); \
          __task__->total_sleep_ns += __t1__ - __t0__;      \
        }                                                   \
        do /* {
          ...
          ...
          ...
          ...
        } */

#define END_COMLIB_TASK_MAIN_LOOP                   \
        WHILE_ZERO;                                 \
      } COMLIB_TASK_RESUME_LOCK;                    \
      /* Check state */                             \
      if( COMLIB_TASK__IsRequested_Stop( __task__ ) ) { \
        COMLIB_TASK__SetState_Stopping( __task__ ); \
      }                                             \
    } while( COMLIB_TASK__RunLoop( __task__ ) );    \
  } COMLIB_TASK_RELEASE




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __COMLIB_TASK__FreeResources( comlib_task_t *task ) {
  // Description
  if( task->CSTR__description ) {
    CStringDelete( task->CSTR__description );
    task->CSTR__description = NULL;
  }

  // Error
  if( task->CSTR__error ) {
    CStringDelete( task->CSTR__error );
    task->CSTR__error = NULL;
  }

  // Remove the thread lock
  DEL_CRITICAL_SECTION( &task->_lock.lock );

  // Remove the condition variables
  DEL_CONDITION_VARIABLE( &task->_idle_event.cond );
  DEL_CONDITION_VARIABLE( &task->_wake_event.cond );

  // Free
  free( task );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define DECLARE_COMLIB_TASK( TaskEntrypoint )  DECLARE_THREAD_FUNCTION( TaskEntrypoint )



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
#define BEGIN_COMLIB_TASK( Task, DataType, Data, TaskEntrypoint, TaskPriority, TaskDescription )  \
BEGIN_THREAD_FUNCTION( TaskEntrypoint, TaskDescription, comlib_task_t, Task ) {     \
  comlib_task_t *_CT_ = Task;                                                       \
  cxlib_thread_priority priority = TaskPriority;                                    \
  int free_on_exit = 0;                                                             \
  _CT_->_thread_ns_reference = __cxthread_reference_ns__;                           \
  _CT_->_thread_id = GET_CURRENT_THREAD_ID();                                       \
  __lfsr_init( _CT_->_thread_id );                                                  \
  COMLIB_TASK_LOCK( _CT_ ) {                                                        \
    /* Get a timer */                                                               \
    WAITABLE_TIMER _WT_ = 0;                                                        \
    if( __comlib_task_new_waitable_timer( &_WT_ ) < 0 ) {                           \
      _CT_->_retcode = -1;                                                          \
    }                                                                               \
    /* Accept request to run */                                                     \
    COMLIB_TASK__AcceptRequest_Run( _CT_ );                                         \
    /* Task start time */                                                           \
    _CT_->_thread_t_start = __MILLISECONDS_SINCE_1970();                            \
    /* Set task description */                                                      \
    if( (_CT_->CSTR__description = CStringNew( TaskDescription )) == NULL ) {       \
      _CT_->_retcode = -1;                                                          \
    }                                                                               \
    /* Proceed if no errors */                                                      \
    if( !_CT_->_retcode ) {                                                         \
      /* Set task data */                                                           \
      DataType *Data = (DataType*)_CT_->_task_data;                                 \
      /* Execute startup routine if one provided */                                 \
      unsigned __startup__ = 0;                                                     \
      if( _CT_->_initialize ) {                                                     \
        COMLIB_TASK_SUSPEND_LOCK( _CT_ ) {                                          \
          __startup__ = _CT_->_initialize( _CT_ );                                  \
        } COMLIB_TASK_RESUME_LOCK;                                                  \
      }                                                                             \
      /* Startup problem? */                                                        \
      if( __startup__ != 0 ) {                                                      \
        _CT_->_retcode = __startup__;                                               \
      }                                                                             \
      /* Startup OK: Run task */                                                    \
      else {                                                                        \
        /* Alive */                                                                 \
        COMLIB_TASK__SetState_Alive( _CT_ );                                        \
        /* Main Body */                                                             \
        COMLIB_TASK_SUSPEND_LOCK( _CT_ ) {                                          \
          /* Set Priority */                                                        \
          if( priority != CXLIB_THREAD_PRIORITY_DEFAULT ) {                         \
            THREAD_SET_PRIORITY( priority );                                        \
          }                                                                         \
          /* TASK MAIN LOOP */                                                      \
          do /* {
            ...
            ...
            ...
            ...
          } */


#define END_COMLIB_TASK                                           \
          WHILE_ZERO;                                             \
          if( priority != CXLIB_THREAD_PRIORITY_DEFAULT ) {       \
            THREAD_SET_PRIORITY( CXLIB_THREAD_PRIORITY_NORMAL );  \
          }                                                       \
        } COMLIB_TASK_RESUME_LOCK;                                \
        /* End main body */                                       \
        free_on_exit = _CT_->delete_self_on_thread_exit;    \
        /* Main loop completed */                           \
        COMLIB_TASK__SetState_Stopping( _CT_ );             \
        /* Execute shutdown routine if one provided */      \
        if( _CT_->_shutdown ) {                             \
          COMLIB_TASK_SUSPEND_LOCK( _CT_ ) {                \
            _CT_->_retcode = _CT_->_shutdown( _CT_ );       \
          } COMLIB_TASK_RESUME_LOCK;                        \
        }                                                   \
        else {                                              \
          _CT_->_retcode = 0;                               \
        }                                                   \
      }                                                     \
    }                                                       \
    /* End task */                                          \
    COMLIB_TASK__AcceptRequest_Stop( _CT_ );                \
    COMLIB_TASK__ClearState_Alive( _CT_ );                  \
    COMLIB_TASK__SetState_Dead( _CT_ );                     \
    __comlib_task_delete_waitable_timer( &_WT_ );           \
    _CT_->_thread_t_end = __MILLISECONDS_SINCE_1970();      \
    if( _CT_->_state_TCS._lock_recursion != 1 ) {           \
      FATAL( 0, "unbalanced task lock on thread exit" );    \
    }                                                       \
  } COMLIB_TASK_RELEASE;                                    \
  while( _CT_->n_threads_suspended_waiting > 0 ) {          \
    sleep_milliseconds( 10 );                               \
  }                                                         \
  if( free_on_exit ) {                                      \
    __COMLIB_TASK__FreeResources( _CT_ );                   \
    _CT_ = NULL;                                            \
  }                                                         \
} END_THREAD_FUNCTION





#endif
