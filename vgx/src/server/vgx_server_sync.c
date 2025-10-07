/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_sync.c
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

#include "_vgx.h"
#include "_vxserver.h"



/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static int              __sync__get_keyval_string( const vgx_KeyVal_t *kv, const char **value, CString_t **CSTR__error );
static int              __sync__get_keyval_integer( const vgx_KeyVal_t *kv, int64_t *value, CString_t **CSTR__error );
static CString_t *      __sync__subscribe_new_URI_string( vgx_URIQueryParameters_t *params, int *timeout_ms, bool *hardsync, CString_t **CSTR__error );
static int              __sync__start_subscriber_synchronizer( vgx_Graph_t *SYSTEM, CString_t **CSTR__new_subscriber_uri, vgx_StringList_t **subscribers_URIs, bool sync_hard, bool resume_tx_input );

DECLARE_COMLIB_TASK(    __sync__task_synchronize_new_subscriber );





/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __sync__get_keyval_string( const vgx_KeyVal_t *kv, const char **value, CString_t **CSTR__error ) {
  static __THREAD char sbuf[32];
  if( kv->val.type == VGX_VALUE_TYPE_STRING ) {
    *value = kv->val.data.simple.string;
    return 0;
  }
  else if( kv->val.type == VGX_VALUE_TYPE_INTEGER ) {
    snprintf( sbuf, 7, "%lld", kv->val.data.simple.integer );
    *value = sbuf;
    return 0;
  }
  else {
    __format_error_string( CSTR__error, "%s value must be string", kv->key ); 
    return -1;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __sync__get_keyval_integer( const vgx_KeyVal_t *kv, int64_t *value, CString_t **CSTR__error ) {
  if( kv->val.type == VGX_VALUE_TYPE_INTEGER ) {
    *value = kv->val.data.simple.integer;
    return 0;
  }
  else {
    if( kv->val.type == VGX_VALUE_TYPE_STRING ) {
      char *e = NULL;
      int64_t v = strtol( kv->val.data.simple.string, &e, 0 );
      if( e && *e != '\0' ) {
        __format_error_string( CSTR__error, "%s value must be integer", kv->key ); 
        return -1;
      }
      *value = v;
      return 0;
    }
    else {
      __set_error_string( CSTR__error, "timeout must be string or integer" ); 
      return -1;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static CString_t * __sync__subscribe_new_URI_string( vgx_URIQueryParameters_t *params, int *timeout_ms, bool *hardsync, CString_t **CSTR__error ) {
  CString_t *CSTR__uri = NULL;

  XTRY {
    vgx_KeyVal_t *cursor = params->keyval;
    vgx_KeyVal_t *kv_end = cursor + params->sz;
    const char *uri = NULL;

    // Extract parameters
    while( cursor < kv_end ) {
      vgx_KeyVal_t *kv = cursor++;
      // uri
      if( CharsEqualsConst( kv->key, "uri" ) ) {
        if( __sync__get_keyval_string( kv, &uri, CSTR__error ) < 0 ) { 
          THROW_SILENT( CXLIB_ERR_API, 0x001 );
        }
      }
      else if( CharsEqualsConst( kv->key, "hardsync" ) ) {
        int64_t i;
        if( __sync__get_keyval_integer( kv, &i, CSTR__error ) < 0 ) { 
          THROW_SILENT( CXLIB_ERR_API, 0x004 );
        }
        if( i > 0 ) {
          *hardsync = true;
        }
      }
      // timeout
      else if( CharsEqualsConst( kv->key, "timeout" ) ) {
        int64_t i;
        if( __sync__get_keyval_integer( kv, &i, CSTR__error ) < 0 ) { 
          THROW_SILENT( CXLIB_ERR_API, 0x005 );
        }
        *timeout_ms = (int)i;
      }
    }

    // URI provided
    if( uri ) {
      // Create URI string
      if( (CSTR__uri = CStringNew( uri )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }
    }
    else {
      __set_error_string( CSTR__error, "missing uri" ); 
      THROW_SILENT( CXLIB_ERR_API, 0x007 );
    }

    // Make sure the URI string is valid
    vgx_URI_t *URI = iURI.New( CStringValue( CSTR__uri ), CSTR__error );
    if( URI == NULL ) {
      THROW_SILENT( CXLIB_ERR_API, 0x009 );
    }
    iURI.Delete( &URI );

  }
  XCATCH( errcode ) {
    iString.Discard( &CSTR__uri );
  }
  XFINALLY {
  }

  return CSTR__uri;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   vgx_SubscriberSynchronizer_t,
                   synchronizer,
                   __sync__task_synchronize_new_subscriber,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "task_synchronize_new_subscriber" )
{


  COMLIB_TASK_LOCK( self ) {
    // We are ready
    synchronizer->stage_TCS.state.ready_to_sync = true;

    // Wait for green light
    while( synchronizer->stage_TCS.command.perform_sync == false ) {
      COMLIB_TASK_SUSPEND_MILLISECONDS( self, 100 );
    }

  } COMLIB_TASK_RELEASE;

  CString_t *CSTR__error = NULL;

  // Synchronize (single)
  COMLIB_TASK_LOCK( self ) {
    synchronizer->stage_TCS.state.performing_sync = true;
  } COMLIB_TASK_RELEASE;

  int timeout_ms = 30000;

  // Disable events for all user graphs
  if( igraphfactory.SuspendEvents( timeout_ms ) < 0 ) {
    CRITICAL( 0x001, "SubscriberSynchronizer failed to suspend events" );
  }
  else {
    BEGIN_OPSYS_STATE_CHANGE( synchronizer->sysgraph, timeout_ms ) {
      // --------------------------------------------------
      // Now synchronize the new subscriber.
      // This may take a long time.
      // 
      // Assumption: All existing subscribers are detached
      // at this point.
      // 
      // --------------------------------------------------
      XTRY {
        if( igraphfactory.SyncAllGraphs( synchronizer->sync_hard, timeout_ms, &CSTR__error ) != 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }

        // Detach (single)
        if( iSystem.DetachOutput( NULL, true, false, timeout_ms, &CSTR__error ) != 1 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }

        // Add the new subscriber to our list of subscribers
        if( iString.List.AppendSteal( synchronizer->subscribers_URIs, &synchronizer->CSTR__new_subscriber_uri ) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
        }

      }
      XCATCH( errcode ) {
        const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown error";
        CRITICAL( 0x005, "SubscriberSynchronizer: %s", serr );
        iString.Discard( &CSTR__error );
        iSystem.DetachOutput( NULL, true, false, timeout_ms, &CSTR__error );
        iString.Discard( &CSTR__error );
      }
      XFINALLY {
        iString.Discard( &synchronizer->CSTR__new_subscriber_uri );
        // (Re)attach (all)
        // If this fails we have no way of restoring proper operation.
        // All we can do is log errors.
        if( iSystem.AttachOutput( synchronizer->subscribers_URIs, TX_ATTACH_MODE_NORMAL, true, timeout_ms, &CSTR__error ) != 1 ) {
          const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "unknown error";
          CRITICAL( 0x006, "SubscriberSynchronizer failed to reattach subscribers after synchronization: %s", serr );
          iString.Discard( &CSTR__error );
        }
      }
    } END_OPSYS_STATE_CHANGE;

    if( igraphfactory.ResumeEvents() < 0 ) {
      CRITICAL( 0x007, "SubscriberSynchronizer failed to resume events" );
    }
  }


  COMLIB_TASK_LOCK( self ) {
    // Taks complete
    synchronizer->stage_TCS.state.sync_complete = true;

    // Get cleanup flag
    if( synchronizer->stage_TCS.command.perform_cleanup ) {
      iString.Discard( &synchronizer->CSTR__new_subscriber_uri );
      iString.List.Discard( &synchronizer->subscribers_URIs );
      // On thread exit the thread will delete all task resources before terminating.
      COMLIB_TASK__DeleteSelfOnThreadExit( synchronizer->TASK );
    }

    // Wait until task is allowed to exit
    while( synchronizer->stage_TCS.command.terminate_on_completion == false ) {
      COMLIB_TASK_SUSPEND_MILLISECONDS( self, 100 );
    }
  } COMLIB_TASK_RELEASE;

  if( synchronizer->resume_tx_input ) {
    if( iOperation.System_OPEN.ConsumerService.ResumeExecution( synchronizer->sysgraph, timeout_ms ) != 1 ) {
      REASON( 0x008, "Failed to resume transaction input service after subscriber synchronization" );
    }
  }



} END_COMLIB_TASK;




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __sync__start_subscriber_synchronizer( vgx_Graph_t *SYSTEM, CString_t **CSTR__new_subscriber_uri, vgx_StringList_t **subscribers_URIs, bool sync_hard, bool resume_tx_input ) {
  int ret = -1;
  vgx_SubscriberSynchronizer_t *synchronizer = NULL;
  // Allocate
  if( (synchronizer = calloc( 1, sizeof( vgx_SubscriberSynchronizer_t ) )) != NULL ) {

    synchronizer->sysgraph = SYSTEM;

    // Hard sync requested by subscriber
    synchronizer->sync_hard = sync_hard;

    // Resume transaction input after synchronization completes
    synchronizer->resume_tx_input = resume_tx_input;

    // Create and start thread
    if( (synchronizer->TASK = COMLIB_TASK__StartNew( __sync__task_synchronize_new_subscriber, NULL, NULL, synchronizer, 5000 )) == NULL ) {
      free( synchronizer );
      synchronizer = NULL;
    }
    // Thread running
    else {
      COMLIB_TASK_LOCK( synchronizer->TASK ) {
        // Steal the provided subscriber information
        synchronizer->CSTR__new_subscriber_uri = *CSTR__new_subscriber_uri;
        *CSTR__new_subscriber_uri = NULL;
        synchronizer->subscribers_URIs = *subscribers_URIs;
        *subscribers_URIs = NULL;

        // Give green light to start synchronization
        synchronizer->stage_TCS.command.perform_sync = true;

        // Wait for confirmation that sync has started
        int64_t t0 = __MILLISECONDS_SINCE_1970();
        int64_t deadline = t0 + 5000;
        while( synchronizer->stage_TCS.state.performing_sync == false ) {
          COMLIB_TASK_SUSPEND_MILLISECONDS( synchronizer->TASK, 100 );
          if( __MILLISECONDS_SINCE_1970() > deadline ) {
            break;
          }
        }

        if( synchronizer->stage_TCS.state.performing_sync == false ) {
          CRITICAL( 0x001, "Failed to confirm startup of SubscriberSynchronizer" );
        }

        // Instruct synchronizer thread to clean up all data and self-terminate on completion
        synchronizer->stage_TCS.command.terminate_on_completion = true;
        synchronizer->stage_TCS.command.perform_cleanup = true;
      } COMLIB_TASK_RELEASE;

      // ok
      ret = 0;
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
SUPPRESS_WARNING_UNREFERENCED_FORMAL_PARAMETER
DLL_HIDDEN int vgx_server_sync__subscribe_and_sync( vgx_Graph_t *SYSTEM, vgx_URIQueryParameters_t *params, vgx_StreamBuffer_t *request_content, vgx_StreamBuffer_t *response_body, vgx_MediaType mediatype, CString_t **CSTR__error ) {

  static int sync_requested_SYS_CS = false;

  int timeout_ms = 10000;
  bool hardsync = false;
  // Get the operation destination URI for the new subscriber
  CString_t *CSTR__new_subscriber_uri = __sync__subscribe_new_URI_string( params, &timeout_ms, &hardsync, CSTR__error );
  if( CSTR__new_subscriber_uri == NULL ) {
    return -1;
  }

  int ret = 0;
  GRAPH_LOCK( SYSTEM ) {
    if( sync_requested_SYS_CS ) {
      ret = -1;
    }
    else {
      sync_requested_SYS_CS = true;
    }
  } GRAPH_RELEASE;

  if( ret < 0 ) {
    mediatype = MEDIA_TYPE__text_plain;
    iStreamBuffer.Write( response_body, "BUSY", 4 );
    return -1;
  }

  vgx_StringList_t *subscribers_URIs = NULL;

  BEGIN_OPSYS_STATE_CHANGE( SYSTEM, timeout_ms ) {

    int tx_input_suspended_here = 0;
    if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
      if( iOperation.System_OPEN.ConsumerService.IsExecutionSuspended( SYSTEM ) == 0 ) {
        if( (tx_input_suspended_here = iOperation.System_OPEN.ConsumerService.SuspendExecution( SYSTEM, timeout_ms )) < 1 ) {
          mediatype = MEDIA_TYPE__text_plain;
          iStreamBuffer.Write( response_body, "BUSY", 4 );
          ret = -1;
        }
      }
    }

    XTRY {
      if( ret < 0 ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      // Get list of any existing subscribers (0 or more)
      if( (subscribers_URIs = iSystem.AttachedOutputs( NULL, timeout_ms )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
      }


      const char *new_subscriber = CStringValue( CSTR__new_subscriber_uri );

      // Remove new subscriber if already on the list of existing subscribers.
      // It will be re-added later.
      // TODO Weakness: The same subscriber could possibly be entered by different hostnames that
      //                all resolve to the same host. For example mixing ip addresses and host names
      //                would escape this check.
      int already = iString.List.RemoveItem( subscribers_URIs, new_subscriber, true );
      if( already ) {
        VERBOSE( 0x003, "Already a subscriber: %s. Will refresh.", new_subscriber );
      }

      // Disconnect all existing subscribers so we can perform the sync only to the new subscriber.
      if( iSystem.DetachOutput( NULL, false, false, timeout_ms, CSTR__error ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
      }

      // Try to attach the new subscriber and check
      vgx_StringList_t *singleton_URIs = iString.List.New( NULL, 1 );
      iString.List.SetItem( singleton_URIs, 0, new_subscriber );
      int attached = iSystem.AttachOutput( singleton_URIs, TX_ATTACH_MODE_SYNC_NEW_SUBSCRIBER, true, timeout_ms, CSTR__error );
      iString.List.Discard( &singleton_URIs );
      if( attached != 1 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
      }

      // At this point we are fairly certain the sync will work.
      // Now hand the job over to a new thread and respond to 
      // subscriber that sync is in progress and should complete
      // on its own some time in the future.
      if (__sync__start_subscriber_synchronizer( SYSTEM, &CSTR__new_subscriber_uri, &subscribers_URIs, hardsync, tx_input_suspended_here ) < 0 ) {
        THROW_ERROR( CXLIB_ERR_GENERAL, 0x006 );
      }

      mediatype = MEDIA_TYPE__text_plain;
      iStreamBuffer.Write( response_body, "OK", 2 );

    }
    XCATCH( errcode ) {
      ret = -1;
      if( tx_input_suspended_here ) {
        if( iOperation.System_OPEN.ConsumerService.BoundPort( SYSTEM ) ) {
          if( iOperation.System_OPEN.ConsumerService.ResumeExecution( SYSTEM, timeout_ms ) != 1 ) {
            REASON( 0x007, "Failed to resume transaction input service after subscriber synchronization" );
          }
        }
      }
    }
    XFINALLY {
      iString.List.Discard( &subscribers_URIs );
      iString.Discard( &CSTR__new_subscriber_uri );
    }

  } END_OPSYS_STATE_CHANGE;

  GRAPH_LOCK( SYSTEM ) {
    sync_requested_SYS_CS = false;
  } GRAPH_RELEASE;


  return ret;
  
}
