/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  comlib
 * File:    cxthread.c
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

#include "_comlib.h"

#if defined CXPLAT_LINUX_ANY
#include <sys/syscall.h>
#elif defined CXPLAT_MAC_ARM64
#include <pthread.h>
#endif


SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );



static int64_t  __timeout_remain( int64_t t0, int64_t timeout_ms );
static int      __wait_for_idle( comlib_task_t *task, f_comlib_task_progress progressf, int64_t timeout_ms );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static __THREAD char __thread_label[16] = {0};



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const char * SET_CURRENT_THREAD_LABEL( const char *label ) {
#if defined CXPLAT_WINDOWS_X64
  wchar_t descr[ (128 + 1) * sizeof( wchar_t ) ] = { 0 };
  __append_thread_description( descr, label, 128 );
#endif
  strncpy( __thread_label, label, 15 );
  return __thread_label;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const char * GET_CURRENT_THREAD_LABEL( char *dest ) {
  strncpy( dest, __thread_label, 15 );
  return dest;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT uint32_t GET_CURRENT_THREAD_ID( void ) {
  static __THREAD uint32_t thread_id = 0;
  if( !thread_id ) {
#if defined CXPLAT_WINDOWS_X64
    thread_id = GetCurrentThreadId();
#elif defined CXPLAT_LINUX_ANY
    thread_id = (uint32_t)syscall(SYS_gettid);
#elif defined CXPLAT_MAC_ARM64
    uint64_t tid;
    pthread_threadid_np(NULL, &tid);
    thread_id = (uint32_t)tid;
#else
#error "Unsupported platform"
#endif
  }
  return thread_id;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT uint32_t GET_CURRENT_PROCESS_ID( void ) {
  static __THREAD uint32_t pid = 0;
  if( !pid ) {
#if defined CXPLAT_WINDOWS_X64
    pid = GetCurrentProcessId();
#elif defined CXPLAT_LINUX_ANY
    pid = (uint32_t)syscall(SYS_getpid);
#elif defined CXPLAT_MAC_ARM64
    pid = (uint32_t)getpid();
#else
#error "Unsupported platform"
#endif
  }
  return pid;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const void * COMLIB_TRAP( const void *obj ) {
  volatile const void *p = NULL;
  if( obj != NULL ) {
    p = obj;
  }
  else {
    p = NULL;
  }
  return (void*)p;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __measure_spinfactor( void ) {
  int64_t t0 = __GET_CURRENT_MICROSECOND_TICK();
  const int64_t baseline = 100000000;
  // Measure how long it takes to spin a large number of times
  int64_t x = 1;
  for( int64_t i=0; i<baseline; i++ ) {
    x = ihash64( x );
  }
  int64_t t1 = __GET_CURRENT_MICROSECOND_TICK();
  // It took delta microseconds to spin baseline number of times
  int64_t baseline_us = t1 - t0;
  if( baseline_us > 0 ) {
    // Estimate the spin count that will take 1 millisecond
    double spins_per_microsecond = (double)baseline / baseline_us;
    int64_t spin_factor = (int64_t)spins_per_microsecond;
    return spin_factor;
  }
  else {
    return 1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB__Spin( int microseconds ) {
  static int64_t spin_factor = -1;
  static __THREAD int64_t x = 1;
  if( spin_factor < 0 ) {
    spin_factor = __measure_spinfactor();
  }
  int64_t spincount = microseconds * spin_factor;
  for( int64_t i=0; i<spincount; i++ ) {
    x = ihash64( x );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__Delete( comlib_task_t **task ) {
  if( task && *task ) {
    // Proceed only if task is not alive
    if( COMLIB_TASK__IsDefunct( *task ) ) {

      // Clear any unconsumed run request
      COMLIB_TASK__AcceptRequest_Run( *task );

      // Join
      THREAD_JOIN( (*task)->_THREAD, 1000 );

      __COMLIB_TASK__FreeResources( *task );

      *task = NULL;

      // Task deleted
      return 1;
    }
    // Can't delete a running task!
    else {
      return -1;
    }
  }
  // Nothing to delete
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_task_t * COMLIB_TASK__New( f_cxlib_thread_entrypoint entrypoint, f_comlib_task_initialize initialize, f_comlib_task_shutdown shutdown, void *task_data ) {
  comlib_task_t *task = calloc( 1, sizeof( comlib_task_t ) );
  if( task && entrypoint ) {
    // Create the thread lock
    INIT_SPINNING_CRITICAL_SECTION( &task->_lock.lock, 4000 );

    // Initialize the condition variables
    INIT_CONDITION_VARIABLE( &task->_idle_event.cond );
    INIT_CONDITION_VARIABLE( &task->_wake_event.cond );

    // Set entrypoint
    task->_entrypoint = entrypoint;

    // Set startup hook
    task->_initialize = initialize;

    // Set shutdown hook
    task->_shutdown = shutdown;

    // Set task data
    task->_task_data = task_data;
  }
  return task;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_task_t * COMLIB_TASK__SetInitialize( comlib_task_t *task, f_comlib_task_initialize initialize ) {
  task->_initialize = initialize;
  return task;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_task_t * COMLIB_TASK__SetShutdown( comlib_task_t *task, f_comlib_task_shutdown shutdown ) {
  task->_shutdown = shutdown;
  return task;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void COMLIB_TASK__DeleteSelfOnThreadExit( comlib_task_t *task ) {
  task->delete_self_on_thread_exit = true;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_task_t * COMLIB_TASK__SetData( comlib_task_t *task, void *task_data ) {
  task->_task_data = task_data;
  return task;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT void * COMLIB_TASK__GetData( comlib_task_t *task ) {
  return task->_task_data;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT uint32_t COMLIB_TASK__ThreadId( comlib_task_t *task ) {
  return task->_thread_id;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const char * COMLIB_TASK__Description( comlib_task_t *task ) {
  return task->CSTR__description ? CStringValue( task->CSTR__description ) : "<none>";
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const char * COMLIB_TASK__AppendDescription( comlib_task_t *task, const char *string ) {
  const char *previous = "";
  if( task->CSTR__description ) {
    previous = CStringValue( task->CSTR__description );
  }
  CString_t *CSTR__new = CStringNewFormat( "%s%s", previous, string );
  if( task->CSTR__description ) {
    CStringDelete( task->CSTR__description );
  }
  task->CSTR__description = CSTR__new;
  return COMLIB_TASK__Description( task );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT CString_t * COMLIB_TASK__SetError( comlib_task_t *task, const char *message ) {
  if( task->CSTR__error ) {
    CStringDelete( task->CSTR__error );
  }
  task->CSTR__error = CStringNew( message );
  return task->CSTR__error;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT const char * COMLIB_TASK__GetError( comlib_task_t *task ) {
  return task->CSTR__error ? CStringValue( task->CSTR__error ) : "";
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__Start( comlib_task_t *task, int timeout_ms ) {
  int running = 0;
  COMLIB_TASK_LOCK( task ) {
    // Set run request
    COMLIB_TASK__Request_Run( task );
    // Start thread
    task->_thread_id = 0;
    uint32_t _ign;
    if( THREAD_START( &task->_THREAD, &_ign, task->_entrypoint, task ) == 0 ) {
      // Wait until alive (or dead) is confirmed
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsStarting( task ), timeout_ms , NULL ) {
        COMLIB_TASK_SUSPENDED_MILLISECONDS( task, 10 );
      } END_TIME_LIMITED_WHILE;
      // Running ?
      if( COMLIB_TASK__RunLoop( task ) ) {
        running = 1;
      }
      // Failed to start
      else {
        running = -1;
      }
    }
    else {
      running = -1;
    }
  } COMLIB_TASK_RELEASE;
  return running;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t __timeout_remain( int64_t t0, int64_t timeout_ms ) {
  int64_t t1 = __GET_CURRENT_MILLISECOND_TICK();
  int64_t elapsed = t1 - t0;
  // Infinite timeout
  if( timeout_ms < 0 ) {
    return LLONG_MAX;
  }
  // Remain
  else {
    return timeout_ms - elapsed;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __wait_for_idle( comlib_task_t *task, f_comlib_task_progress progressf, int64_t timeout_ms ) {
  const char *descr = COMLIB_TASK__Description( task );
  int64_t t0 = __GET_CURRENT_MILLISECOND_TICK(); 
  int64_t t1 = t0;
  int64_t t2 = t0;
  if( progressf ) {
    int64_t t_remain = 0;
    int64_t t_log = t0 + 5000;
    float last_pct = 100.0;
    while( COMLIB_TASK__IsBusy( task ) && (t_remain = __timeout_remain( t1, timeout_ms )) > 0 ) {
      t2 = __GET_CURRENT_MILLISECOND_TICK(); 
      float pct = progressf( task );
      if( pct < 0.0 ) {
        CRITICAL( 0x001, "Task '%s' progress interrupted after %lld seconds", descr, (t2-t0)/1000 );
        return -1;
      }
      else if( t2 > t_log ) {
        WARN( 0x002, "Task '%s' busy: %.1f%% ", descr, pct );
        t_log = t2 + 5000;
        // Progress is made, bump timeout base reference (i.e. reset and start timeout counter from the beginning)
        if( pct != last_pct ) {
          last_pct = pct;
          t1 = __GET_CURRENT_MILLISECOND_TICK();
        }
      }
      COMLIB_TASK_WAIT_FOR_IDLE( task, 100 );
    }
  }
  else {
    int64_t t_remain = __timeout_remain( t0, timeout_ms );
    BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsBusy( task ), t_remain, NULL ) {
      WARN( 0x003, "Task '%s' busy ", descr );
      COMLIB_TASK_SUSPENDED_MILLISECONDS( task, 5000 );
    } END_TIME_LIMITED_WHILE;
    if( COMLIB_TASK__IsBusy( task ) ) {
      t1 = __GET_CURRENT_MILLISECOND_TICK();
      CRITICAL( 0x004, "Task '%s' still busy after %lld seconds, forcing exit", descr, (t1-t0)/1000 );
      return -1;
    }
  }

  // Return 1 if idle at this point.
  // Return 0 if task still busy
  return COMLIB_TASK__IsNotBusy( task );

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__Stop( comlib_task_t *task, f_comlib_task_progress progressf, int timeout_ms ) {
  int stopped = 0;
  COMLIB_TASK_LOCK( task ) {
    if( !COMLIB_TASK__IsDefunct( task ) ) {
      int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();
      COMLIB_TASK__Request_Stop( task );
      const char *descr = COMLIB_TASK__Description( task );
      VERBOSE( 0x001, "Stopping task '%s'", descr );

      // Wait while task exits main loop (i.e. stopping not yet confirmed)
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotStopping( task ) && COMLIB_TASK__IsNotDead( task ), 10000, NULL ) {
        COMLIB_TASK_SUSPENDED_MILLISECONDS( task, 1 );
      } END_TIME_LIMITED_WHILE;

      // Main loop has not exited within timeout
      if( COMLIB_TASK__IsNotStopping( task ) && COMLIB_TASK__IsNotDead( task ) ) {
        WARN( 0x002, "Task '%s' did not respond to stop request", descr );
      }

      // Wait for idle
      if( __wait_for_idle( task, progressf, __timeout_remain( t0, timeout_ms ) ) < 1 ) {
        CRITICAL( 0x003, "Task '%s': Forcing exit", descr );
        COMLIB_TASK__Request_ForceExit( task );
      }

      // Update remaining time (plus a little extra)
      int64_t t_remain = __timeout_remain( t0, timeout_ms ) + 1000;

      // Wait for thread to die
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotDead( task ), t_remain, NULL ) {
        COMLIB_TASK_SUSPENDED_MILLISECONDS( task, 1 );
      } END_TIME_LIMITED_WHILE;

      // Still not dead?
      if( COMLIB_TASK__IsNotDead( task ) ) {
        CRITICAL( 0x004, "Task '%s' is not dead after stop attempt", descr );
      }
      else {
        VERBOSE( 0x005, "Task '%s' stopped", descr );
      }

      //
      // Stopped if task is dead
      //
      if( COMLIB_TASK__IsDead( task ) ) {
        stopped = 1;
      }
      // Failed to stop
      else {
        stopped = -1;
      }
    }
  } COMLIB_TASK_RELEASE;
  return stopped;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__Suspend( comlib_task_t *task, f_comlib_task_progress progressf, int timeout_ms ) {
  int suspended = 0;

  COMLIB_TASK_LOCK( task ) {
    if( !COMLIB_TASK__IsDefunct( task ) ) {
      int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();

      COMLIB_TASK__Request_Suspend( task );
      COMLIB_TASK__SignalWake( task );

      const char *descr = COMLIB_TASK__Description( task );
      VERBOSE( 0x001, "Suspending task '%s'", descr );

      // Short spin while task responds to suspend request
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotSuspending( task ) && COMLIB_TASK__IsNotSuspended( task ), 10000, NULL ) {
        COMLIB_TASK_WAIT_FOR_IDLE( task, 10 );
      } END_TIME_LIMITED_WHILE;

      // Main loop has not entered suspended state within timeout
      if( COMLIB_TASK__IsNotSuspending( task ) && COMLIB_TASK__IsNotSuspended( task ) ) {
        WARN( 0x002, "Task '%s' did not respond to suspend request", descr );
      }

      // Wait for idle
      int64_t t_remain = __timeout_remain( t0, timeout_ms );
      if( t_remain < 0 && timeout_ms >= 0 ) {
        t_remain = 0;
      }

      if( __wait_for_idle( task, progressf, t_remain ) < 1 ) {
        WARN( 0x003, "Task '%s': Could not confirm suspend (timeout)", descr );
        COMLIB_TASK__AcceptRequest_Suspend( task );
        COMLIB_TASK__ClearState_Suspending( task );
        COMLIB_TASK__Request_Resume( task ); // Just in case...
        suspended = -1;
      }
      // Idle, now wait for main loop to suspend
      else {
        // Update remaining time (with short extra margin)
        t_remain = __timeout_remain( t0, timeout_ms );
        if( t_remain < 5000 && timeout_ms >= 0 ) {
          t_remain = 5000;
        }

        BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotSuspended( task ), t_remain, NULL ) {
          COMLIB_TASK_WAIT_FOR_IDLE( task, 10 );
        } END_TIME_LIMITED_WHILE;
        // Suspended confirmed
        if( COMLIB_TASK__IsSuspended( task ) ) {
          VERBOSE( 0x007, "Task '%s' suspended", descr );
          suspended = 1;
        }
        // Shutdown while waiting to suspend
        else if( COMLIB_TASK__IsNotAlive( task ) ) {
          WARN( 0x008, "Task '%s' terminated while waiting to suspend", descr );
          suspended = 0;
        }
        // Cannot suspend
        else {
          CRITICAL( 0x009, "Task '%s' could not be suspended", descr );
          COMLIB_TASK__AcceptRequest_Suspend( task );
          suspended = -1;
        }
      }
    }
  } COMLIB_TASK_RELEASE;
  return suspended;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__Resume( comlib_task_t *task, int timeout_ms ) {
  int resumed = 0;

  COMLIB_TASK_LOCK( task ) {

    if( COMLIB_TASK__IsAlive( task ) && COMLIB_TASK__IsSuspended( task ) ) {
      int64_t t0 = __GET_CURRENT_MILLISECOND_TICK();

      // Set resume request
      COMLIB_TASK__Request_Resume( task );
      COMLIB_TASK__SignalWake( task );

      const char *descr = COMLIB_TASK__Description( task );
      VERBOSE( 0x001, "Resuming task '%s'", descr );

      // Wait while task is suspended
      int64_t t_remain = __timeout_remain( t0, timeout_ms ) + 1000;
      BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsSuspended( task ), t_remain, NULL ) {
        COMLIB_TASK_WAIT_FOR_IDLE( task, 10 );
      } END_TIME_LIMITED_WHILE;

      // Resume confirmed
      if( COMLIB_TASK__IsAlive( task ) && COMLIB_TASK__IsNotSuspended( task ) ) {
        VERBOSE( 0x002, "Task '%s' resumed", descr );
        resumed = 1;
      }
      else {
        WARN( 0x003, "Task '%s' could not be resumed", descr );
        COMLIB_TASK__AcceptRequest_Resume( task );
        resumed = -1;
      }
    }
  } COMLIB_TASK_RELEASE;
  return resumed;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__ForceExit( comlib_task_t *task, int timeout_ms ) {
  int dead = 0;
  COMLIB_TASK_LOCK( task ) {
    COMLIB_TASK__Request_ForceExit( task );
    BEGIN_TIME_LIMITED_WHILE( COMLIB_TASK__IsNotDead( task ), timeout_ms, NULL ) {
      COMLIB_TASK_SUSPEND_MILLISECONDS( task, 100 );
    } END_TIME_LIMITED_WHILE;
    if( COMLIB_TASK__IsDead( task ) ) {
      dead = 1;
    }
    else {
      dead = -1;
    }
  } COMLIB_TASK_RELEASE;
  return dead;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT comlib_task_t * COMLIB_TASK__StartNew( f_cxlib_thread_entrypoint entrypoint, f_comlib_task_initialize initialize, f_comlib_task_shutdown shutdown, void *task_data, int timeout_ms ) {
  // Create task structure
  comlib_task_t *task = COMLIB_TASK__New( entrypoint, initialize, shutdown, task_data );
  if( task ) {
    VERBOSE( 0x001, "Starting task with entrypoint '%s' at %p", cxlib_get_symbol_name( (uintptr_t)entrypoint ).value, entrypoint );
    // Start thread
    if( COMLIB_TASK__Start( task, timeout_ms ) < 1 ) {
      REASON( 0x002, "Failed to start task with entrypoint '%s' at %p", cxlib_get_symbol_name( (uintptr_t)entrypoint ).value, entrypoint );
      COMLIB_TASK__Delete( &task );
    }
  }
  return task;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__StopDelete( comlib_task_t **task ) {
  int timeout_ms = 60000;
  int stopped = 0;
  if( task && *task ) {

    // Shutdown
    if( COMLIB_TASK__Stop( *task, NULL, timeout_ms ) < 0 ) {
      CRITICAL( 0x001, "Task '%s' failed to stop", COMLIB_TASK__Description( *task ) );
      COMLIB_TASK__ClearState_Alive( *task );
      COMLIB_TASK__SetState_Dead( *task );
      --stopped;
    }

    //
    if( COMLIB_TASK__Delete( task ) != 1 ) {
      CRITICAL( 0x002, "Task '%s' could not be deleted", COMLIB_TASK__Description( *task ) );
      --stopped;
    }

    //
  }
  return stopped;
}



/*******************************************************************//**
 * Return the starttimestamp of task in milliseconds since 1970
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_TASK__StartTime( comlib_task_t *task ) {
  int64_t start;
  COMLIB_TASK_LOCK( task ) {
    start = task->_thread_t_start;
  } COMLIB_TASK_RELEASE;
  return start;
}


/*******************************************************************//**
 * Return the end timestamp of task in milliseconds since 1970
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_TASK__EndTime( comlib_task_t *task ) {
  int64_t end;
  COMLIB_TASK_LOCK( task ) {
    end = task->_thread_t_end;
  } COMLIB_TASK_RELEASE;
  return end;
}



/*******************************************************************//**
 * Return the number of milliseconds the task has been alive
 *
 ***********************************************************************
 */
DLL_EXPORT int64_t COMLIB_TASK__Age( comlib_task_t *task ) {
  int64_t t_start_ms;
  COMLIB_TASK_LOCK( task ) {
    t_start_ms = task->_thread_t_start;
  } COMLIB_TASK_RELEASE;
  int64_t age = __MILLISECONDS_SINCE_1970() - t_start_ms;
  return age;
}



/*******************************************************************//**
 * Return the number of milliseconds the task has been alive
 *
 ***********************************************************************
 */
DLL_EXPORT double COMLIB_TASK__AgeSeconds( comlib_task_t *task ) {
  return COMLIB_TASK__Age( task ) / 1000.0;
}



/*******************************************************************//**
 * Return the fraction (0.0 - 1.0) of time the task has been sleeping
 *
 ***********************************************************************
 */
DLL_EXPORT double COMLIB_TASK__LifetimeIdle( comlib_task_t *task ) {
  int64_t t_start_ms;
  int64_t t_sleep_ns;
  COMLIB_TASK_LOCK( task ) {
    t_start_ms = task->_thread_t_start;
    t_sleep_ns = task->total_sleep_ns;
  } COMLIB_TASK_RELEASE;
  int64_t t_now_ms = __MILLISECONDS_SINCE_1970();
  int64_t t_alive_ns = (t_now_ms - t_start_ms) * 1000000LL;
  double idle = 1.0;
  if( t_alive_ns > 0 && t_sleep_ns < t_alive_ns ) {
    idle = t_sleep_ns / (double)t_alive_ns;
  }
  return idle;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__SetReturnCode( comlib_task_t *task, int retcode ) {
  COMLIB_TASK_LOCK( task ) {
    task->_retcode = retcode;
  } COMLIB_TASK_RELEASE;
  return retcode;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB_TASK__GetReturnCode( comlib_task_t *task ) {
  return task->_retcode;
}
