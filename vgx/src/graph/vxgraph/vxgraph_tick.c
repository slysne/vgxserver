/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vxgraph_tick.c
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

#include "_vgx.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




#define VGX_TICK_RESOLUTION_MS 50
#define VGX_TICK_TIMER_SLEEP_NANOSEC_MAX  (VGX_TICK_RESOLUTION_MS * 1000000LL * 73 / 100)
#define VGX_TICK_TIMER_SLEEP_MILLISEC_MAX (VGX_TICK_RESOLUTION_MS * 73 / 100)



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __TIC_initialize( comlib_task_t *self ) {
  vgx_GraphTimer_t *Timer = COMLIB_TASK__GetData( self );

  int64_t tms_now = __MILLISECONDS_SINCE_1970();
  uint32_t ts_now = (uint32_t)(tms_now / 1000);

  // Start the clock
  // [Q1.4.2]
  ATOMIC_ASSIGN_u32( &Timer->t0_atomic, ts_now );
  // [Q1.4.1]
  ATOMIC_ASSIGN_u32( &Timer->ts_atomic, ts_now );
  // [Q1.3]
  ATOMIC_ASSIGN_i64( &Timer->tms_atomic, tms_now );
  // [Q1.5.1]
  ATOMIC_ASSIGN_i32( &Timer->offset_tms_atomic, 0 );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned __TIC_shutdown( comlib_task_t *self ) {
  vgx_GraphTimer_t *Timer = COMLIB_TASK__GetData( self );

  // Stop the clock
  int64_t tms_now = __MILLISECONDS_SINCE_1970(); // time of death
  uint32_t ts_now = (uint32_t)(tms_now / 1000);
  ATOMIC_ASSIGN_i64( &Timer->tms_atomic, tms_now );
  ATOMIC_ASSIGN_u32( &Timer->ts_atomic, ts_now );

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_GraphTimer_t,
                   Timer,
                   __TIC_entrypoint,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "graph_timer/" )
{

  vgx_Graph_t *graph = Timer->graph;
  const char *name = CStringValue( CALLABLE( graph )->Name( graph ) );

  APPEND_THREAD_NAME( name );
  COMLIB_TASK__AppendDescription( self, name );

  int64_t tms_next = 0;
  int64_t tms_now = __MILLISECONDS_SINCE_1970();
  int offset = 0;
  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( VGX_TICK_TIMER_SLEEP_MILLISEC_MAX );

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {

    // Advance tick? (deadline passed)
    if( (tms_now = __MILLISECONDS_SINCE_1970()) >= tms_next ) {

      // Next tick
      tms_next += VGX_TICK_RESOLUTION_MS;

      // Safeguard: if we're lagging behind, fast forward
      if( tms_next < tms_now ) {
        tms_next = tms_now + VGX_TICK_RESOLUTION_MS;
      }

      int64_t tms = tms_now + offset;

      // Seconds
      uint32_t ts = (uint32_t)(tms / 1000);

      // Update graph
      ATOMIC_ASSIGN_i64( &Timer->tms_atomic, tms );
      ATOMIC_ASSIGN_u32( &Timer->ts_atomic, ts );
      offset = ATOMIC_READ_i32( &Timer->offset_tms_atomic );

      // Sleep for most of the time until next tick
      loop_delay = COMLIB_TASK_LOOP_DELAY( VGX_TICK_TIMER_SLEEP_MILLISEC_MAX );

      // Monitor exit condition
      if( COMLIB_TASK__IsStopping( self ) ) {
        COMLIB_TASK__AcceptRequest_Stop( self );
      }

    }
    // Shorter sleep
    else {
      // Sleep ~75% of the time until next tick (at least 1us)
      int64_t remain_microsec_75pct = (3000 * (tms_next - tms_now)) >> 2;
      loop_delay = COMLIB_TASK_LOOP_DELAY_MICROSECONDS( 1 + remain_microsec_75pct );

    }
  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tick__synchronize( vgx_Graph_t *self, int64_t external_tms_ref ) {
  vgx_GraphTimer_t *Timer = &self->TIC;
  int64_t tms_now = __MILLISECONDS_SINCE_1970();
  int32_t offset_tms = (int32_t)(external_tms_ref - tms_now);
  ATOMIC_ASSIGN_i32( &Timer->offset_tms_atomic, offset_tms );
  return offset_tms;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tick__initialize_CS( vgx_Graph_t *self, uint32_t inception_t0 ) {
  int ret = 0;

  XTRY {
    vgx_GraphTimer_t *Timer = &self->TIC;

    // Reset data
    memset( Timer, 0, sizeof( vgx_GraphTimer_t ) );

    // -----------------
    // START TICK THREAD
    // -----------------

    if( Timer->TASK != NULL ) {
      THROW_ERROR( CXLIB_ERR_INITIALIZATION, 0x001 );
    }

    memset( Timer, 0, sizeof( vgx_GraphTimer_t ) );

    // [Q1.2]
    Timer->graph = self;

    comlib_task_t *task;

    GRAPH_SUSPEND_LOCK( self ) {
      // Start new graph timer
      if( (task = COMLIB_TASK__StartNew( __TIC_entrypoint, __TIC_initialize, __TIC_shutdown, Timer, 10000 )) == NULL ) {
        ret = -1;
      }
      else {
        ret = 1;
      }
    } GRAPH_RESUME_LOCK;

    if( ret < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
    }

    // [Q1.1]
    Timer->TASK = task;

    // [Q1.5.2]
    ATOMIC_ASSIGN_u32( &Timer->inception_t0_atomic, inception_t0 );
  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
  }

  return ret;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int _vxgraph_tick__destroy_CS( vgx_Graph_t *self ) {
  int ret = 0;
  vgx_GraphTimer_t *Timer = &self->TIC;
  comlib_task_t *task = Timer->TASK;

  GRAPH_SUSPEND_LOCK( self ) {
    ret = COMLIB_TASK__StopDelete( &task );
  } GRAPH_RESUME_LOCK;

  // Failed to stop
  if( ret < 0 ) {
    return -1;
  }

  // Timer stopped OK
  memset( Timer, 0, sizeof( vgx_GraphTimer_t ) );
  return 0;

}



#ifdef INCLUDE_UNIT_TESTS
#include "tests/__utest_vxgraph_tick.h"

test_descriptor_t _vgx_vxgraph_tick_tests[] = {
  { "Graph Internal Timer Tests", __utest_vxgraph_tick },

  {NULL}
};
#endif
