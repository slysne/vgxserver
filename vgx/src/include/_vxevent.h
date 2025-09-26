/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    _vxevent.h
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

#ifndef _VGX_VXEVENT_H
#define _VGX_VXEVENT_H


#include "_vgx.h"

extern int __create_eventproc_task( vgx_EventProcessor_t *processor );
extern int __start_eventproc( vgx_EventProcessor_t *processor, bool enable );
extern int __stop_delete_eventproc( vgx_EventProcessor_t *processor );

extern int64_t __length_Qapi( vgx_Graph_t *self );

extern vgx_EventProcessor_t * __acquire_eventproc( vgx_EventProcessor_t *processor, int timeout_ms );
extern int __release_eventproc( vgx_EventProcessor_t **processor_WL );

extern void __set_req_flush_WL( vgx_EventProcessor_t *processor_WL, int timeout_ms );
extern int  __is_req_flush_WL( vgx_EventProcessor_t *processor_WL );
extern int  __consume_is_req_flush_WL( vgx_EventProcessor_t *processor_WL );

extern void __set_ack_flushed_WL( vgx_EventProcessor_t *processor_WL );
extern int  __is_ack_flushed_WL( vgx_EventProcessor_t *processor_WL );
extern int  __consume_is_ack_flushed_WL( vgx_EventProcessor_t *processor_WL );



typedef enum __e_eventproc_state_enum {
  VGX_EVENTPROC_STATE_INITIALIZE        = 0,
  VGX_EVENTPROC_STATE_IDLE              = 1,
  VGX_EVENTPROC_STATE_BUSY              = 2,
  VGX_EVENTPROC_STATE_SCHEDULE          = 3,
  VGX_EVENTPROC_STATE_FLUSH             = 4,
  VGX_EVENTPROC_STATE_MIGRATE_LONG      = 5,
  VGX_EVENTPROC_STATE_MIGRATE_MEDIUM    = 6,
  VGX_EVENTPROC_STATE_EXECUTE           = 7,
  VGX_EVENTPROC_STATE_CANCEL_EXECUTION  = 8,
  VGX_EVENTPROC_STATE_DESTROY           = 9,
  VGX_EVENTPROC_STATE_FINALIZED         = 10
} __eventproc_state_enum;


static const char *vgx_eventproc_state_name[] = {
  "INITIALIZE",
  "IDLE",
  "BUSY",
  "SCHEDULE",
  "FLUSH",
  "MIGRATE_LONG",
  "MIGRATE_MEDIUM",
  "EXECUTE",
  "CANCEL_EXECUTION",
  "DESTROY",
  "FINALIZED",
  "DEFUNCT",
  "???",
  "???",
  "???",
  "???"
};



/**************************************************************************//**
 * __update_eventproc_state_WL
 *
 ******************************************************************************
 */
static void __update_eventproc_state_WL( vgx_EventProcessor_t *processor_WL, vgx_eventproc_state_t state ) {
  processor_WL->params.task_WL.state = state;
}



/*******************************************************************//**
 * Set the processor defunct.
 ***********************************************************************
 */
static int __eventproc_state_set_defunct( vgx_EventProcessor_t *processor, int timeout_ms ) {
  vgx_EventProcessor_t *processor_WL = __acquire_eventproc( processor, timeout_ms );
  if( processor_WL ) {
    processor_WL->params.task_WL.flags.defunct = true;
    __release_eventproc( &processor_WL );
    return 0;
  }
  else {
    return -1;
  }
}



/*******************************************************************//**
 * Return true if processor is defunct
 ***********************************************************************
 */
static bool __eventproc_state_is_defunct_WL( vgx_EventProcessor_t *processor_WL ) {
  return processor_WL->params.task_WL.flags.defunct;
}



/*******************************************************************//**
 * Return true if processor is operational
 ***********************************************************************
 */
static bool __eventproc_state_is_operational_WL( vgx_EventProcessor_t *processor_WL ) {
  return !processor_WL->params.task_WL.flags.defunct;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
typedef enum __e_map_term {
  LONG_TERM,
  MEDIUM_TERM,
  SHORT_TERM
} __map_term;



extern vgx_EventParamInfo_t *g_pevent_param;



/*******************************************************************//**
 *
 ***********************************************************************
 */
typedef struct __s_cutoff_timespec {
  uint32_t cutoff_ts;
  uint32_t now_ts;
} __cutoff_timespec;




/**************************************************************************//**
 * __get_executable_cutoff
 *
 ******************************************************************************
 */
__inline static __cutoff_timespec __get_executable_cutoff( void ) {
  __cutoff_timespec T;
  int64_t now_tms = __MILLISECONDS_SINCE_1970();
  T.now_ts = (uint32_t)(now_tms / 1000);
  T.cutoff_ts = (uint32_t) ((now_tms + g_pevent_param->Executor.insertion_threshold_tms) / 1000);
  return T;
}




extern int __schedule_events_WL( vgx_EventProcessor_t *processor_WL, vgx_ExecutionJobDescriptor_t **job );
extern vgx_VertexEventQueue_t * __eventexec_cancel_execution_job_WL( vgx_ExecutionJobDescriptor_t **job, int timeout_ms );
extern vgx_ExecutionJobDescriptor_t * __eventexec_new_execution_job_WL( vgx_EventProcessor_t *processor_WL );


// Queue

/**************************************************************************//**
 * __APPEND_QUEUE_VERTEX_EVENT_NOLOCK
 *
 ******************************************************************************
 */
__inline static int __APPEND_QUEUE_VERTEX_EVENT_NOLOCK( vgx_VertexEventQueue_t *VertexEventQueue, const vgx_VertexStorableEvent_t *VertexEvent ) {
  return CALLABLE( VertexEventQueue )->AppendNolock( VertexEventQueue, &VertexEvent->m128 ) == 1;
}


/**************************************************************************//**
 * __NEXT_QUEUE_VERTEX_EVENT_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __NEXT_QUEUE_VERTEX_EVENT_NOLOCK( vgx_VertexEventQueue_t *VertexEventQueue, vgx_VertexStorableEvent_t *VertexEvent ) {
  return CALLABLE( VertexEventQueue )->NextNolock( VertexEventQueue, &VertexEvent->m128 );
}


/**************************************************************************//**
 * __LENGTH_QUEUE_VERTEX_EVENTS
 *
 ******************************************************************************
 */
__inline static int64_t __LENGTH_QUEUE_VERTEX_EVENTS( vgx_VertexEventQueue_t *VertexEventQueue ) {
  return CALLABLE( VertexEventQueue )->Length( VertexEventQueue );
}


/**************************************************************************//**
 * __ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK( vgx_VertexEventQueue_t *DestEvents, vgx_VertexEventQueue_t *SrcEvents, int64_t N ) {
  return CALLABLE( DestEvents )->AbsorbNolock( DestEvents, SrcEvents, N );
}

#define APPEND_QUEUE_VERTEX_EVENT_NOLOCK        __APPEND_QUEUE_VERTEX_EVENT_NOLOCK
#define NEXT_QUEUE_VERTEX_EVENT_NOLOCK          __NEXT_QUEUE_VERTEX_EVENT_NOLOCK
#define LENGTH_QUEUE_VERTEX_EVENTS              __LENGTH_QUEUE_VERTEX_EVENTS
#define ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK       __ABSORB_QUEUE_VERTEX_EVENTS_NOLOCK



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __cmp_event( const vgx_VertexStorableEvent_t *ev1, const vgx_VertexStorableEvent_t *ev2 ) {
  uint32_t a = ev1->event_val.ts_exec;
  uint32_t b = ev2->event_val.ts_exec;
  return (int)( (b > a) - (b < a) );
}

// Heap

/**************************************************************************//**
 * __PUSH_HEAP_VERTEX_EVENT_NOLOCK
 *
 ******************************************************************************
 */
__inline static int __PUSH_HEAP_VERTEX_EVENT_NOLOCK( vgx_VertexEventHeap_t *VertexEventHeap, const vgx_VertexStorableEvent_t *VertexEvent ) {
  return CALLABLE( VertexEventHeap )->HeapPush( VertexEventHeap, &VertexEvent->m128 ) == 1;
}


/**************************************************************************//**
 * __POP_HEAP_VERTEX_EVENT_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __POP_HEAP_VERTEX_EVENT_NOLOCK( vgx_VertexEventHeap_t *VertexEventHeap, vgx_VertexStorableEvent_t *VertexEvent ) {
  return CALLABLE( VertexEventHeap )->HeapPop( VertexEventHeap, &VertexEvent->m128 );
}


/**************************************************************************//**
 * __LENGTH_HEAP_VERTEX_EVENTS
 *
 ******************************************************************************
 */
__inline static int64_t __LENGTH_HEAP_VERTEX_EVENTS( vgx_VertexEventHeap_t *VertexEventHeap ) {
  return CALLABLE( VertexEventHeap )->Length( VertexEventHeap );
}


/**************************************************************************//**
 * __ABSORB_HEAP_VERTEX_EVENTS_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __ABSORB_HEAP_VERTEX_EVENTS_NOLOCK( vgx_VertexEventHeap_t *DestEvents, vgx_VertexEventHeap_t *SrcEvents, int64_t N ) {
  int64_t n = 0;
  vgx_VertexStorableEvent_t e;
  Cm128iHeap_vtable_t *iHeap = CALLABLE( SrcEvents );
  if( N < 0 ) {
    N = LLONG_MAX;
  }
  while( n < N && iHeap->HeapPop( SrcEvents, &e.m128 ) == 1 ) {
    if( iHeap->HeapPush( DestEvents, &e.m128 ) == 1 ) {
      ++n;
    }
    else {
      return -1;
    }
  }
  return n;
}


/**************************************************************************//**
 * __ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK( vgx_VertexEventHeap_t *DestEvents, vgx_VertexEventQueue_t *SrcEvents, int64_t N ) {
  int64_t n = 0;
  vgx_VertexStorableEvent_t e;
  Cm128iQueue_vtable_t *iQueue = CALLABLE( SrcEvents );
  Cm128iHeap_vtable_t *iHeap = CALLABLE( DestEvents );
  if( N < 0 ) {
    N = LLONG_MAX;
  }
  while( n < N && iQueue->NextNolock( SrcEvents, &e.m128 ) == 1 ) {
    if( iHeap->HeapPush( DestEvents, &e.m128 ) == 1 ) {
      ++n;
    }
    else {
      return -1;
    }
  }
  return n;
}


/**************************************************************************//**
 * __ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK
 *
 ******************************************************************************
 */
__inline static int64_t __ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK( vgx_VertexEventQueue_t *DestEvents, vgx_VertexEventHeap_t *SrcEvents, uint32_t t_thres, int64_t N ) {
  int64_t n = 0;
  vgx_VertexStorableEvent_t e;
  Cm128iHeap_vtable_t *iHeap = CALLABLE( SrcEvents );
  Cm128iQueue_vtable_t *iQueue = CALLABLE( DestEvents );
  if( N < 0 ) {
    N = LLONG_MAX;
  }
  while( n < N && iHeap->HeapTop( SrcEvents, &e.m128 ) && e.event_val.ts_exec <= t_thres && iHeap->HeapPop( SrcEvents, &e.m128 ) == 1 ) {
    if( iQueue->AppendNolock( DestEvents, &e.m128 ) == 1 ) {
      ++n;
    }
    else {
      return -1;
    }
  }
  return n;
}



#define PUSH_HEAP_VERTEX_EVENT_NOLOCK               __PUSH_HEAP_VERTEX_EVENT_NOLOCK
#define POP_HEAP_VERTEX_EVENT_NOLOCK                __POP_HEAP_VERTEX_EVENT_NOLOCK
#define LENGTH_HEAP_VERTEX_EVENTS                   __LENGTH_HEAP_VERTEX_EVENTS
#define ABSORB_HEAP_VERTEX_EVENTS_NOLOCK            __ABSORB_HEAP_VERTEX_EVENTS_NOLOCK
#define ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK   __ABSORB_QUEUE_TO_HEAP_VERTEX_EVENTS_NOLOCK
#define ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK            __ENQUEUE_DUE_VERTEX_EVENTS_NOLOCK





/*******************************************************************//**
 * 
 ***********************************************************************
 */
__inline static vgx_VertexStorableEvent_t __get_vertex_expiration_event( vgx_Vertex_t *vertex_WL, uint32_t ts_exec ) {
  vgx_VertexStorableEvent_t ev = {0};
  ev.event_key = _cxmalloc_object_as_handle( vertex_WL ),   // Convert vertex pointer to handle so we can store it
  ev.event_val.ts_exec = ts_exec;
  ev.event_val.type = VGX_EVENTPROC_VERTEX_EXPIRATION_EVENT;
  return ev;
}



/**************************************************************************//**
 * __event_key_as_qword
 *
 ******************************************************************************
 */
__inline static QWORD __event_key_as_qword( vgx_VertexStorableEvent_t *ev ) {
  return ev->event_key.allocdata;
}


/**************************************************************************//**
 * __event_key_as_vertex
 *
 ******************************************************************************
 */
__inline static vgx_Vertex_t * __event_key_as_vertex( vgx_VertexStorableEvent_t *ev, cxmalloc_family_t *vertex_allocator ) {
  return (vgx_Vertex_t*)CALLABLE( vertex_allocator )->HandleAsObjectNolock( vertex_allocator, ev->event_key ); 
}


/**************************************************************************//**
 * __event_value_as_qword
 *
 ******************************************************************************
 */
__inline static QWORD __event_value_as_qword( vgx_VertexStorableEvent_t *ev ) {
  return ev->event_val.bits;
}



/**************************************************************************//**
 * __EVENTMAP_SIZE
 *
 ******************************************************************************
 */
__inline static int64_t __EVENTMAP_SIZE( framehash_t *Map ) {
  return CALLABLE( Map )->Items( Map );
}



/**************************************************************************//**
 * __EVENTMAP_SERIALIZE
 *
 ******************************************************************************
 */
__inline static int64_t __EVENTMAP_SERIALIZE( framehash_t *Map, bool force ) {
  return CALLABLE( Map )->BulkSerialize( Map, force );
}


/*******************************************************************//**
 * Return 1 on success, 0 on failure
 ***********************************************************************
 */
__inline static int __EVENTMAP_DELETE( framehash_t *Map, vgx_VertexStorableEvent_t *ev ) {
  return CALLABLE( Map )->DelKey64( Map, CELL_KEY_TYPE_PLAIN64, __event_key_as_qword( ev ) ) == CELL_VALUE_TYPE_INTEGER;
}


/*******************************************************************//**
 * Return 1 on success, 0 on failure
 ***********************************************************************
 */
__inline static int __EVENTMAP_SET( framehash_t *Map, vgx_VertexStorableEvent_t *ev ) {
  return CALLABLE( Map )->SetInt56( Map, CELL_KEY_TYPE_PLAIN64, __event_key_as_qword( ev ), __event_value_as_qword( ev ) ) == CELL_VALUE_TYPE_INTEGER;
}


/*******************************************************************//**
 * Return 1 on success, 0 on failure
 ***********************************************************************
 */
__inline static int __EVENTMAP_ENSURE_KEY( framehash_t *Map, QWORD key, int64_t value ) {
  framehash_vtable_t *iFH = CALLABLE( Map );
  if( iFH->HasKey64( Map, CELL_KEY_TYPE_PLAIN64, key ) != CELL_VALUE_TYPE_INTEGER ) {
    return iFH->SetInt56( Map, CELL_KEY_TYPE_PLAIN64, key, value ) == CELL_VALUE_TYPE_INTEGER;
  }
  else {
    return 1;
  }
}



/*******************************************************************//**
 *
 ***********************************************************************
 */
__inline static vgx_EventValue_t __EVENTMAP_GET( framehash_t *Map, vgx_VertexStorableEvent_t *ev ) {
  vgx_EventValue_t ev_value = {0};
  if( CALLABLE( Map )->GetInt56( Map, CELL_KEY_TYPE_PLAIN64, __event_key_as_qword( ev ), &ev_value.ival ) != CELL_VALUE_TYPE_INTEGER ) {
    ev_value.type = VGX_EVENTPROC_NO_EVENT;
  }
  return ev_value;
}


/*******************************************************************//**
 * Return 1 if map has key, 0 otherwise
 ***********************************************************************
 */
__inline static int __EVENTMAP_CONTAINS( framehash_t *Map, vgx_VertexStorableEvent_t *ev ) {
  return CALLABLE( Map )->HasKey64( Map, CELL_KEY_TYPE_PLAIN64, __event_key_as_qword( ev ) ) == CELL_VALUE_TYPE_INTEGER;
}



#define EVENTMAP_SIZE       __EVENTMAP_SIZE
#define EVENTMAP_SERIALIZE  __EVENTMAP_SERIALIZE
#define EVENTMAP_DELETE     __EVENTMAP_DELETE
#define EVENTMAP_SET        __EVENTMAP_SET
#define EVENTMAP_ENSURE_KEY __EVENTMAP_ENSURE_KEY
#define EVENTMAP_GET        __EVENTMAP_GET
#define EVENTMAP_CONTAINS   __EVENTMAP_CONTAINS






#endif
