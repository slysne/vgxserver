/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  vgx
 * File:    vgx_server_io.c
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
#include "_vxserver.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );


static int      __io__handle_exception( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, struct pollfd *pfd, int err );
static void     __io__pollinit_client( vgx_VGXServer_t *server, int n, vgx_VGXServerClient_t *client, bool poll_writable );
static void     __io__pollinit_pollfd( struct pollfd *pollable, CXSOCKET *psock, bool poll_writable );
static void     __io__pollinit_readsock( vgx_VGXServer_t *server, int n, CXSOCKET *psocket );
static bool     __io__client_has_request_data( vgx_VGXServerClient_t *client );
static bool     __io__client_has_response_data( vgx_VGXServerClient_t *client );
static int      __io__poll_any_with_dispatcher( vgx_VGXServer_t *server, int timeout_ms );
static int      __io__poll_any_front_only( vgx_VGXServer_t *server, int timeout_ms );
static bool     __io__poll( vgx_VGXServer_t *server );
static void     __io__perform_pending_front_io( vgx_VGXServer_t *server );
static void     __io__handle_yielded_front_clients( vgx_VGXServer_t *server );
static void     __io__perform_pending_dispatcher_io( vgx_VGXServer_t *server );
static void     __io__handle_yielded_dispatcher_channels( vgx_VGXServer_t *server );
static void     __io__not_accepted( vgx_VGXServer_t *server, vgx_URI_t *ClientURI, HTTPStatus status );
static int      __io__try_accept( vgx_VGXServer_t *server );
static int      __io__recv( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );
static int64_t  __io__send_segment( CXSOCKET *psock, vgx_StreamBuffer_t *outbuf, int64_t send_limit );
static int64_t  __io__get_input_buffer( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer );
static int      __io__get_output_buffer( vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer );
static int      __io__transition_request_state( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client );



#define __SERVERIO_MESSAGE( LEVEL, VGXServer, Code, Format, ... ) LEVEL( Code, "IO::NET::%c(%s): " Format, __ident_letter( VGXServer ), __full_path( VGXServer ), ##__VA_ARGS__ )

#define SERVERIO_VERBOSE( VGXServer, Code, Format, ... )   __SERVERIO_MESSAGE( VERBOSE, VGXServer, Code, Format, ##__VA_ARGS__ )
#define SERVERIO_INFO( VGXServer, Code, Format, ... )      __SERVERIO_MESSAGE( INFO, VGXServer, Code, Format, ##__VA_ARGS__ )
#define SERVERIO_WARNING( VGXServer, Code, Format, ... )   __SERVERIO_MESSAGE( WARN, VGXServer, Code, Format, ##__VA_ARGS__ )
#define SERVERIO_REASON( VGXServer, Code, Format, ... )    __SERVERIO_MESSAGE( REASON, VGXServer, Code, Format, ##__VA_ARGS__ )
#define SERVERIO_CRITICAL( VGXServer, Code, Format, ... )  __SERVERIO_MESSAGE( CRITICAL, VGXServer, Code, Format, ##__VA_ARGS__ )
#define SERVERIO_FATAL( VGXServer, Code, Format, ... )     __SERVERIO_MESSAGE( FATAL, VGXServer, Code, Format, ##__VA_ARGS__ )



/*******************************************************************//**
 * Returns true (poll blocked)
 *
 ***********************************************************************
 */
__inline static bool __io__set_blocked( vgx_VGXServerExecutorCompletion_t *completion ) {
  if( !ATOMIC_READ_i32( &completion->poll_blocked_atomic ) ) {
    ATOMIC_INCREMENT_i32( &completion->poll_blocked_atomic );
  }
  return true;
}



/*******************************************************************//**
 * Returns:
 *    true if empty completion queue
 *    false if completion queue contains responses
 ***********************************************************************
 */
__inline static bool __io__set_blocked_if_none_completed( vgx_VGXServerExecutorCompletion_t *completion ) {
  if( ATOMIC_READ_i32( &completion->length_atomic ) > 0 ) {
    return false;
  }
  return __io__set_blocked( completion );
}



/*******************************************************************//**
 * Returns false (poll not blocked)
 *
 ***********************************************************************
 */
__inline static bool __io__clear_blocked( vgx_VGXServerExecutorCompletion_t *completion ) {
  if( ATOMIC_READ_i32( &completion->poll_blocked_atomic ) > 0 ) {
    ATOMIC_DECREMENT_i32( &completion->poll_blocked_atomic );
  }
  return false;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__handle_exception( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, struct pollfd *pfd, int err ) {
  TRAP_ILLEGAL_CLIENT_ACCESS( server, client );

  CString_t *CSTR__error = NULL;
  const char *client_uri = "";
  if( client->URI ) {
    client_uri = iURI.URI( client->URI );
    if( err == 0 ) {
      err = iURI.Sock.Error( client->URI, &CSTR__error, -1, 0 );
    }
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
    if( pfd && pfd->revents == POLLHUP ) {
      SERVERIO_VERBOSE( server, 0x001, "Disconnected: %s", client_uri );
    }
    else if( (pfd && pfd->revents == POLLERR) || err == ECONNRESET ) {
      SERVERIO_VERBOSE( server, 0x002, "Disconnected: %s", serr );
    }
    else if( err == 0 ) {
      SERVERIO_INFO( server, 0x003, "%s", serr );
    }
    else {
      SERVERIO_REASON( server, 0x004, "I/O Exception %s", serr );
    }
    iString.Discard( &CSTR__error );
    return vgx_server_client__close( server, client );
  }

  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __io__pollinit_pollfd( struct pollfd *pollable, CXSOCKET *psock, bool poll_writable ) {
  cxpollfd_pollinit( pollable, psock, true, poll_writable );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __io__pollinit_client( vgx_VGXServer_t *server, int n, vgx_VGXServerClient_t *client, bool poll_writable ) {
  CXSOCKET *psocket = iURI.Sock.Input.Get( client->URI );
  if( psocket ) {
    cxpollfd_pollinit( server->io.pollfd_list + n, psocket, true, poll_writable );
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __io__pollinit_readsock( vgx_VGXServer_t *server, int n, CXSOCKET *psocket ) {
  cxpollfd_pollinit( server->io.pollfd_list + n, psocket, true, false );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __io__client_has_request_data( vgx_VGXServerClient_t *client ) {
  return iStreamBuffer.IsReadable( client->request.buffers.stream ) || iStreamBuffer.IsReadable( client->request.buffers.content );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static bool __io__client_has_response_data( vgx_VGXServerClient_t *client ) {
  return iStreamBuffer.IsReadable( client->response.buffers.stream ) || iStreamBuffer.IsReadable( client->response.buffers.content );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static struct pollfd * __io__pollinit_front( vgx_VGXServer_t *server, struct pollfd *pollable ) {
  vgx_VGXServerClient_t *front_io_client = server->pool.clients.iolist.head;
  CXSOCKET *psock;
  while( front_io_client ) {
    if( (psock = iURI.Sock.Input.Get( front_io_client->URI )) != NULL ) {
      // Any response data ready to be written?
      bool has_writable_data = __io__client_has_response_data( front_io_client );
      // Monitor READABLE, and possibly WRITABLE
      cxpollfd_pollinit( pollable++, psock, true, has_writable_data );
    }
    // Next client
    front_io_client = front_io_client->chain.next;
  }
  return pollable;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__ready_front( vgx_VGXServer_t *server, struct pollfd *polled, int n_max ) {
  vgx_VGXServerFrontIOReady_t *ready_front = server->io.ready.front;
  vgx_VGXServerClient_t *front_io_client = server->pool.clients.iolist.head;
  int n_front = 0;
  while( front_io_client && n_front < n_max ) {
    // Populate slot in ioready list if this polled fd has events
    if( cxpollfd_any_valid( polled ) ) {
      ready_front->client = front_io_client;
      ready_front->pfd = polled;
      ++ready_front; // advance to next slot in ready list
      ++n_front; //
    }
    ++polled; // next polled fd
    front_io_client = front_io_client->chain.next;
  }
  // Terminate front ready list
  ready_front->client = NULL;
  // Return number of ready file descriptors in front
  return n_front;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static struct pollfd * __io__pollinit_dispatcher( vgx_VGXServer_t *server, struct pollfd *pollable ) {
  vgx_VGXServerClient_t *dispatcher_io_client = server->matrix.active.head;
  while( dispatcher_io_client ) {
    vgx_VGXServerDispatcherChannel_t *channel = dispatcher_io_client->dispatcher.channels.head;
    while( channel ) {
      // Read OR write (never both)
      cxpollfd_pollinit( pollable++, &channel->socket, CHANNEL_INBOUND( channel ), CHANNEL_OUTBOUND( channel ) );

      // Next channel in client
      channel = channel->chain.next;
    }
    // Next client in matrix
    dispatcher_io_client = dispatcher_io_client->chain.next;
  }
  return pollable;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__ready_dispatcher( vgx_VGXServer_t *server, struct pollfd *polled, int n_max ) {
  vgx_VGXServerDispatcherIOReady_t *ready_dispatcher = server->io.ready.dispatcher;
  vgx_VGXServerClient_t *dispatcher_io_client = server->matrix.active.head;
  int n_disp = 0;
  while( dispatcher_io_client && n_disp < n_max ) {
    vgx_VGXServerDispatcherChannel_t *channel = dispatcher_io_client->dispatcher.channels.head;
    while( channel && n_disp < n_max ) {

      if( cxpollfd_any_valid( polled ) ) {
        ready_dispatcher->channel = channel;
        ready_dispatcher->pfd = polled;
        ++ready_dispatcher; // advance to next slot in ready list
        ++n_disp;
      }

      // Next polled fd
      ++polled;
      // Next channel in client
      channel = channel->chain.next;
    }
    // Next client in matrix
    dispatcher_io_client = dispatcher_io_client->chain.next;
  }
  // Terminate dispatcher ready list
  ready_dispatcher->channel = NULL;
  // Return number of ready file descriptors in dispatcher
  return n_disp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__poll_any_with_dispatcher( vgx_VGXServer_t *server, int timeout_ms ) {
  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;
  // Get Listen socket
  CXSOCKET *plisten = iURI.Sock.Input.Get( server->Listen );
  if( plisten == NULL ) {
    return -1;
  }

#ifndef VGXSERVER_USE_LINUX_EVENTFD
  // Get Wake Monitor socket
  CXSOCKET *pwake = iURI.Sock.Input.Get( completion->monitor );
  if( pwake == NULL ) {
    return -1;
  }
#endif

  struct pollfd *start_poll = server->io.pollfd_list;
  struct pollfd *listen_poll = start_poll + LISTEN_FD_SLOT;
  struct pollfd *wake_poll = start_poll + WAKE_FD_SLOT;
  struct pollfd *start_front = start_poll + CLIENT_FD_START;
  struct pollfd *start_dispatcher;
  struct pollfd *end_poll;

  // Monitor READABLE on Listen socket
  // pollfd:  [L                       ]
  cxpollfd_pollinit( listen_poll, plisten, true, false );
  
#ifdef VGXSERVER_USE_LINUX_EVENTFD
  wake_poll->fd = completion->efd;
  wake_poll->events = POLLIN;
#else
  // Monitor READABLE on Wake Monitor socket
  // pollfd:  [LM                      ]
  cxpollfd_pollinit( wake_poll, pwake, true, false );
#endif

  // Monitor READABLE and WRITABLE on front client sockets
  // pollfd:  [LMUUUUUUU               ]
  start_dispatcher = __io__pollinit_front( server, start_front );

  // Monitor WRITABLE or READABLE on dispatcher channel sockets
  // pollfd:  [LMUUUUUUUDDDDDDDDDDDD   ]
  end_poll = __io__pollinit_dispatcher( server, start_dispatcher );

  // ------------------------------------------------------
  // Perform poll and return number of sockets on poll list
  // ------------------------------------------------------
  // pollfd:  [LMUUUUUUUDDDDDDDDDDDD   ]
  //           0                    n
  int nfd;
  int n_front = 0;
  int n_disp = 0;
  server->io.ready.front[0].client = NULL;
  server->io.ready.dispatcher[0].channel = NULL;
  uint32_t n_pollable = (uint32_t)(end_poll - start_poll);
  if( (nfd = cxpoll( server->io.pollfd_list, n_pollable, timeout_ms )) == 0 ) {
    goto stop_iteration;
  }

  // Accept new connection
  //
  if( cxpollfd_readable( listen_poll ) ) {
    __io__try_accept( server );
    if( --nfd == 0 ) {
      goto stop_iteration;
    }
  }

  // Consume any signals we may have from executors
  //
  if( cxpollfd_readable( wake_poll ) ) {
#ifdef VGXSERVER_USE_LINUX_EVENTFD
    uint64_t value;
    if( read( completion->efd, &value, sizeof(value) ) == -1 ) {
      // ignore
    }
#else
    char discard[ MAX_EXECUTOR_POOL_SIZE ];
    completion->n_blocked_poll_signals += cxrecv( pwake, discard, MAX_EXECUTOR_POOL_SIZE, 0 );
#endif
    if( --nfd == 0 ) {
      goto stop_iteration;
    }
  }

  // Populate front ready clients
  //
  if( (n_front = __io__ready_front( server, start_front, nfd )) == nfd ) {
    goto stop_iteration;
  }

  // Populate dispatcher ready channels
  //
  n_disp = __io__ready_dispatcher( server, start_dispatcher, nfd - n_front );

stop_iteration:
  // Return the number of clients and channels with events
  return n_front + n_disp;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__poll_any_front_only( vgx_VGXServer_t *server, int timeout_ms ) {
  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;
  // Get Listen socket
  CXSOCKET *plisten = iURI.Sock.Input.Get( server->Listen );
  if( plisten == NULL ) {
    return -1;
  }

#ifndef VGXSERVER_USE_LINUX_EVENTFD
  // Get Wake Monitor socket
  CXSOCKET *pwake = iURI.Sock.Input.Get( completion->monitor );
  if( pwake == NULL ) {
    return -1;
  }
#endif

  struct pollfd *start_poll = server->io.pollfd_list;
  struct pollfd *listen_poll = start_poll + LISTEN_FD_SLOT;
  struct pollfd *wake_poll = start_poll + WAKE_FD_SLOT;
  struct pollfd *start_front = start_poll + CLIENT_FD_START;
  struct pollfd *end_poll;

  // Monitor READABLE on Listen socket
  // pollfd:  [L                       ]
  cxpollfd_pollinit( listen_poll, plisten, true, false );
  
#ifdef VGXSERVER_USE_LINUX_EVENTFD
  wake_poll->fd = completion->efd;
  wake_poll->events = POLLIN;
#else
  // Monitor READABLE on Wake Monitor socket
  // pollfd:  [LM                      ]
  cxpollfd_pollinit( wake_poll, pwake, true, false );
#endif

  // Monitor READABLE and WRITABLE on front client sockets
  // pollfd:  [LMUUUUUUU               ]
  end_poll = __io__pollinit_front( server, start_front );

  // ------------------------------------------------------
  // Perform poll and return number of sockets on poll list
  // ------------------------------------------------------
  // pollfd:  [LMUUUUUUUDDDDDDDDDDDD   ]
  //           0                    n
  int nfd;
  server->io.ready.front[0].client = NULL;
  uint32_t n_pollable = (uint32_t)(end_poll - start_poll);
  if( (nfd = cxpoll( server->io.pollfd_list, n_pollable, timeout_ms )) == 0 ) {
    return 0;
  }

  // Accept new connection
  //
  if( cxpollfd_readable( listen_poll ) ) {
    __io__try_accept( server );
    if( --nfd == 0 ) {
      return 0;
    }
  }

  // Consume any signals we may have from executors
  //
  if( cxpollfd_readable( wake_poll ) ) {
#ifdef VGXSERVER_USE_LINUX_EVENTFD
    uint64_t value;
    if( read( completion->efd, &value, sizeof(value) ) == -1 ) {
      // ignore
    }
#else
    char discard[ MAX_EXECUTOR_POOL_SIZE ];
    completion->n_blocked_poll_signals += cxrecv( pwake, discard, MAX_EXECUTOR_POOL_SIZE, 0 );
#endif
    if( --nfd == 0 ) {
      return 0;
    }
  }

  // Populate front ready clients
  // Return the number of clients with events
  return __io__ready_front( server, start_front, nfd );
}



/*******************************************************************//**
 *
 * Returns:
 *        true:   poll did not block
 *        false:  poll blocked
 *
 ***********************************************************************
 */
__inline static bool __io__poll( vgx_VGXServer_t *server ) {
  static __THREAD int count = 0;
  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;

  int nfd = 0;
  int qsz = 0;
  while( (nfd = __io__poll_any_front_only( server, 0 )) == 0 && (qsz = ATOMIC_READ_i32( &completion->length_atomic )) == 0 && count++ < 8 ) {
    #if defined CXPLAT_ARCH_X64
    _mm_pause();
    #elif defined CXPLAT_ARCH_ARM64
    __yield();
    #endif
  }

  if( nfd > 0 || qsz > 0 ) {
    count = 0;
    return true;
  }

  count = 8;

  // No sockets on poll list, prepare blocking poll if no
  // clients on the completion queue.
  if( __io__set_blocked_if_none_completed( completion ) ) {
    // No I/O and no clients on the completion queue: Poll blocking
    __io__poll_any_front_only( server, 5 );

    // Clear flag after wakup
    __io__clear_blocked( completion );
    return false;
  }

  // Completion queue non-empty, don't block
  return true;

}



/*******************************************************************//**
 *
 * Returns:
 *        true:   poll did not block
 *        false:  poll blocked
 *
 ***********************************************************************
 */
__inline static bool __io__poll_with_dispatcher( vgx_VGXServer_t *server ) {
  static __THREAD int count = 0;

  vgx_VGXServerExecutorCompletion_t *completion = &server->dispatch.completion;
  int nfd = 0;
  int qsz = 0;
  while( (nfd = __io__poll_any_with_dispatcher( server, 0 )) == 0 && (qsz = ATOMIC_READ_i32( &completion->length_atomic )) == 0 && count++ < 8 ) {
    #if defined CXPLAT_ARCH_X64
    _mm_pause();
    #elif defined CXPLAT_ARCH_ARM64
    __yield();
    #endif
  }

  if( nfd > 0 || qsz > 0 ) {
    count = 0;
    return true;
  }

  count = 8;

  // No sockets on poll list, prepare blocking poll if no
  // clients on the completion queue.
  if( __io__set_blocked_if_none_completed( completion ) ) {
    // No I/O and no clients on the completion queue: Poll blocking
    __io__poll_any_with_dispatcher( server, 5 );

    // Clear flag after wakup
    __io__clear_blocked( completion );
    return false;
  }

  // Completion queue non-empty, don't block
  return true;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__perform_pending_front_io( vgx_VGXServer_t *server ) {

  vgx_VGXServerFrontIOReady_t *ready = server->io.ready.front;
  vgx_VGXServerClient_t *client;

  while( (client = ready->client) != NULL ) {

    // -----------------------------------
    // Send client data to writable socket
    // -----------------------------------
    if( cxpollfd_writable( ready->pfd ) ) {
      if( vgx_server_io__front_send( server, client ) < 0 ) {
        goto next_client;
      }
      if( CLIENT_STATE__IN_EXECUTOR( client ) ) {
        goto next_client;
      }
    }

    // -------------------------------------
    // Read client data from readable socket
    // and proceed to next client
    // -------------------------------------
    if( cxpollfd_readable( ready->pfd ) ) {
      __io__recv( server, client );
      goto next_client;
    }

    // ---------------------------
    // Client socket has exception
    // ---------------------------
    if( cxpollfd_exception( ready->pfd ) ) {
      __io__handle_exception( server, client, ready->pfd, 0 );
    }

  next_client:
    ++ready;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__handle_yielded_front_clients( vgx_VGXServer_t *server ) {
  if( server->pool.clients.n_yielded > 0 ) {
    int n_remain = server->pool.clients.n_yielded;
    vgx_VGXServerClient_t *front_client = server->pool.clients.iolist.head;
    while( front_client ) {
      if( CLIENT_YIELDED( front_client ) ) {
        vgx_server_request__handle( server, front_client );
        if( --n_remain == 0 ) {
          return;
        }
      }
      front_client = front_client->chain.next;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__perform_pending_dispatcher_io( vgx_VGXServer_t *server ) {

  vgx_VGXServerDispatcherIOReady_t *ready = server->io.ready.dispatcher;

  while( ready->channel != NULL ) {

    // ----------------------------
    // Channel socket has exception
    // ----------------------------
    if( cxpollfd_exception( ready->pfd ) ) {
      vgx_server_dispatcher_io__handle_exception( server, ready->channel, HTTP_STATUS__INTERNAL_SOCKET_ERROR, NULL );
      goto next_channel;
    }

    // ------------------------------------
    // Send channel data to writable socket
    // ------------------------------------
    if( cxpollfd_writable( ready->pfd ) ) {
      if( vgx_server_dispatcher_io__send( server, ready->channel ) < 0 ) {
        goto next_channel;
      }
    }

    // -------------------------------------
    // Read channel data from readable socket
    // and proceed to next channel
    // -------------------------------------
    if( cxpollfd_readable( ready->pfd ) ) {
      vgx_server_dispatcher_io__recv( server, ready->channel );
    }

  next_channel:
    ++ready;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__handle_yielded_dispatcher_channels( vgx_VGXServer_t *server ) {
  if( server->matrix.n_ch_yielded > 0 ) {
    int n_remain = server->matrix.n_ch_yielded;
    vgx_VGXServerClient_t *dispatcher_client = server->matrix.active.head;
    while( dispatcher_client ) {
      vgx_VGXServerDispatcherChannel_t *channel = dispatcher_client->dispatcher.channels.head;
      while( channel ) {
        if( CHANNEL_YIELDED( channel ) ) {
          vgx_server_dispatcher_response__handle( server, channel );
          if( --n_remain == 0 ) {
            return;
          }
        }
        channel = channel->chain.next;
      }
      dispatcher_client = dispatcher_client->chain.next;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_io__process_socket_events( vgx_VGXServer_t *server, int64_t max_ns ) {

  int64_t deadline_ns = __GET_CURRENT_NANOSECOND_TICK() + max_ns;

  // ---------------------------------------------
  // Allow uninterrupted socket i/o until deadline
  // ---------------------------------------------
  __io__clear_blocked( &server->dispatch.completion );

  // Matrix
  if( DISPATCHER_MATRIX_ENABLED( server ) ) {
    do {
      int loops_until_deadline_check = 1000;

      // Check exit trigger
      while( --loops_until_deadline_check > 0 ) {

        // Poll
        if( __io__poll_with_dispatcher( server ) == false ) {
          loops_until_deadline_check = 1;
        }

        // Monitor completed return from executors
        vgx_server_dispatch__executor_complete( server );

        // Perform Front IO
        __io__perform_pending_front_io( server );

        // Perform Dispatcher IO
        __io__perform_pending_dispatcher_io( server );

        // Process yielded clients
        __io__handle_yielded_front_clients( server );

        // Process yielded dispatcher channels
        __io__handle_yielded_dispatcher_channels( server );

      }

      // Continue IO until exit condition met
      if( __GET_CURRENT_NANOSECOND_TICK() > deadline_ns || server->control.flag_TCS.snapshot_request /* <- NOTE: we're not TCS */ ) {
        __io__set_blocked( &server->dispatch.completion );
        return;
      }
    } WHILE_ONE;
  }
  // Backend Engine
  else {
    do {
      int loops_until_deadline_check = 1000;

      // Check exit trigger
      while( --loops_until_deadline_check > 0 ) {

        // Poll
        if( __io__poll( server ) == false ) {
          loops_until_deadline_check = 1;
        }

        // Monitor completed return from executors
        vgx_server_dispatch__executor_complete( server );

        // Perform Front IO
        __io__perform_pending_front_io( server );

        // Process yielded clients
        __io__handle_yielded_front_clients( server );

      }

      // Continue IO until exit condition met
      if( __GET_CURRENT_NANOSECOND_TICK() > deadline_ns || server->control.flag_TCS.snapshot_request /* <- NOTE: we're not TCS */ ) {
        __io__set_blocked( &server->dispatch.completion );
        return;
      }
    } WHILE_ONE;
  }

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__not_accepted( vgx_VGXServer_t *server, vgx_URI_t *ClientURI, HTTPStatus status ) {
  // Use reserved client instance to respond with bad news
  vgx_VGXServerClient_t *reject_client = &server->pool.clients.clients[ LISTEN_FD_SLOT ];
  if( reject_client->URI ) {
    iURI.Delete( &reject_client->URI ); // just in case
  }
  // Temporarily enter reject client into iochain so we can respond properly
  vgx_server_client__accept_into_iochain( server, reject_client, ClientURI );
  // Clear response buffer and produce error
  vgx_server_response__client_response_reset( reject_client );
  vgx_server_response__produce_error( server, reject_client, status, "Cannot accept new connection at this time", false );
  // BEST EFFORT SEND ONLY
  if( vgx_server_io__front_send( server, reject_client ) == 0 ) {
    vgx_server_client__close( server, reject_client );
  }
  ASSERT_CLIENT_RESET( reject_client );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__try_accept( vgx_VGXServer_t *server ) {
  int ret = 0;

  vgx_URI_t *Listen = server->Listen;

  // ACCEPT AND LOG
  CString_t *CSTR__error = NULL;
  vgx_URI_t *ClientURI = iURI.Accept( Listen, "http", &CSTR__error );

  if( ClientURI ) {
    if( VGXSERVER_GLOBAL_IS_SERVING( server ) ) {
      // Register new client
      if( vgx_server_client__register( server, ClientURI ) ) {
        ret = 1; // one new client accepted
      }
      else {
        __io__not_accepted( server, ClientURI, HTTP_STATUS__TooManyRequests );
      }
    }
    else {
      __io__not_accepted( server, ClientURI, HTTP_STATUS__ServiceUnavailable );
    }
  }
  else {
    const char *serr = CSTR__error ? CStringValue( CSTR__error ) : "?";
    SERVERIO_REASON( server, 0x000, "Accept error: %s", serr );
    ret = -1;
  }

  iString.Discard( &CSTR__error );

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__recv( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  TRAP_ILLEGAL_CLIENT_ACCESS( server, client );

  // Get the client's socket
  CXSOCKET *psock;
  char *segment;
  int64_t sz_segment;
  int64_t n_recv;
  vgx_StreamBuffer_t *inbuf;
  int64_t recv_limit;

  if( client == NULL || (psock = iURI.Sock.Input.Get( client->URI )) == NULL ) {
    goto no_input_socket;
  }
  
  // Get the client's buffer to receive into
  if( (recv_limit = __io__get_input_buffer( server, client, &inbuf )) < 1 ) {
    return (int)recv_limit;
  }

  // Receive data from socket into buffer.
  // Each pass around the loop fills a linear region of the buffer with data read from socket.
  // We may perform one or more passes around the loop.
recv_segment:

  // Get a linear region of request buffer into which we are able to receive bytes from socket
  if( (segment = iStreamBuffer.WritableSegmentEx( inbuf, recv_limit, &sz_segment )) == NULL ) {
    goto payload_too_large;
  }

  // Receive bytes from socket into request buffer and advance the request buffer write pointer
  if( (n_recv = cxrecv( psock, segment, sz_segment, 0 )) > 0 ) {
    server->counters.perf->bytes_in += n_recv;

    // Move buffer's write pointer forward
    if( iStreamBuffer.AdvanceWrite( inbuf, n_recv ) != n_recv ) {
      goto buffer_error;
    }
   
    // We filled the entire segment and we still have room in our recv chunk byte budget.
    recv_limit -= n_recv;
    if( recv_limit > 0 && n_recv == sz_segment ) {
      // We will make another pass around the loop
      goto recv_segment;
    }
  }
  // Socket is no longer readable  because there is no data to read at the moment
  else if( n_recv < 0 && iURI.Sock.Busy( errno ) ) {
    goto handle_request;
  }
  else {
    goto socket_exception;
  }


handle_request:
  return vgx_server_request__handle( server, client );

no_input_socket:
  SERVERIO_REASON( server, 0x000, "no input socket" );
  return -1;

payload_too_large:
  SERVERIO_CRITICAL( server, 0x000, "request buffer cannot be expanded beyond %lld bytes capacity", iStreamBuffer.Capacity( inbuf ) );
  return vgx_server_response__produce_error( server, client, HTTP_STATUS__PayloadTooLarge, "Out of memory while receiving socket data", true );

buffer_error:
  SERVERIO_CRITICAL( server, 0x000, "internal request buffer error: %s", iURI.URI( client->URI ) );
  return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Unknown internal buffer error", true );

socket_exception:
  if( n_recv < 0 ) {
    __io__handle_exception( server, client, NULL, errno );
    return -1;
  }

  vgx_server_client__close( server, client );
  return 0;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __io__send_segment( CXSOCKET *psock, vgx_StreamBuffer_t *outbuf, int64_t send_limit ) {
  // Get a linear buffer segment to send
  const char *segment;
  int64_t sz_segment;
  int64_t n_sent;

  // Get linear segment
  if( (sz_segment = iStreamBuffer.ReadableSegment( outbuf, send_limit, &segment, NULL )) == 0 ) {
    return 0;
  }

  // Send linear segment to socket
  if( (n_sent = cxsend( psock, segment, sz_segment, 0 )) <= 0 ) {
    // Error not caused by socket temporarily unwritable
    if( n_sent < 0 && !iURI.Sock.Busy( errno ) ) {
      return -1;
    }
    return 0;
  }
    
  // At least one byte was sent to the socket
  iStreamBuffer.AdvanceRead( outbuf, n_sent );
  if( iStreamBuffer.Empty( outbuf ) ) {
    iStreamBuffer.Clear( outbuf );
  }

  return n_sent;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_io__front_send( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {

  CXSOCKET *psock;
  vgx_StreamBuffer_t *outbuf = NULL;
  int64_t n_sent1;
  int64_t n_sent2 = 0;

  if( client == NULL ) {
    return -1;
  }

  // Get the client's socket
  if( (psock = iURI.Sock.Input.Get( client->URI )) == NULL ) {
    goto error;
  }

  // Get the client's output buffer to send
  if( __io__get_output_buffer( client, &outbuf ) < 0 ) {
    goto error;
  }

  // Send one or two linear buffer segments
  if( (n_sent1 = __io__send_segment( psock, outbuf, SEND_CHUNK_SZ )) > 0 ) {
    n_sent2 = __io__send_segment( psock, outbuf, SEND_CHUNK_SZ - n_sent1 );
  }

  // Check error
  if( n_sent1 < 0 || n_sent2 < 0 ) {
    goto error;
  }

  // Increment output counter
  server->counters.perf->bytes_out += n_sent1 + n_sent2;

  // Check request state and transition as needed
  return __io__transition_request_state( server, client );

error:
  vgx_server_client__close( server, client );
  return -1;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __io__get_stream_buffer( vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer ) {
  // We don't know how much data we will need, receive up to max chunk size.
  *buffer = client->request.buffers.stream;
  return RECV_CHUNK_SZ;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __io__get_content_buffer( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer ) {
  // Receive up to the smaller of max chunk size and the known remaining data.
  *buffer = client->request.buffers.content;
  int64_t unfilled = __vgxserver_request_unfilled_content_buffer( &client->request );
  if( unfilled < RECV_CHUNK_SZ ) {
    if( unfilled < 0 ) {
      return vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Internal buffer corruption", true );
    }
    return unfilled;
  }
  return RECV_CHUNK_SZ;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static int64_t __io__get_next_stream_buffer( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer ) {
  // Previously received data already exists. It needs to be handled
  // outside of this function.
  if( __io__client_has_request_data( client ) ) {
    return 0;
  }

  // Server is suspended
  if( !VGXSERVER_GLOBAL_IS_SERVING( server ) ) {
    return vgx_server_response__produce_error( server, client, HTTP_STATUS__ServiceUnavailable, "Service out", false );
  }

  // Server is serving
  // No pre-existing request data buffered yet.
  // Reset the state machine and begin new request.
  vgx_server_request__client_request_ready( server, client );
  vgx_server_response__client_response_reset( client );
  return __io__get_stream_buffer( client, buffer );
}



/*******************************************************************//**
 *
 * Select the buffer to receive into
 *
 ***********************************************************************
 */
static int64_t __io__get_input_buffer( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer ) {
  // Client state: Fresh request or headers in progress
  if( __vgxserver_request_state_pre_content( &client->request ) ) {
    return __io__get_stream_buffer( client, buffer );
  }
  // Client state: Past request and headers, possibly sitting idle in a completed state
  else {
    switch( CLIENT_STATE_NOERROR( client ) ) {

    // Current request in progress
    // We receive directly into request body when we are in a state where request content is expected
    case VGXSERVER_CLIENT_STATE__EXPECT_CONTENT:
      return __io__get_content_buffer( server, client, buffer );

    // Idle client begin asked to begin a new request
    // Ready to receive and process next request
    case VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE:
      return __io__get_next_stream_buffer( server, client, buffer );

    // Client is either dispatched or completed but response not fully sent
    // so we have to wait before we can receive data for this client.
    default:
      return 0;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__get_output_buffer( vgx_VGXServerClient_t *client, vgx_StreamBuffer_t **buffer ) {
  int64_t sz_stream = iStreamBuffer.Size( client->response.buffers.stream );
  // We still need to send status and headers
  if( sz_stream > 0 ) {
    int64_t sz_content = iStreamBuffer.Size( client->response.buffers.content ); 
    // Entire response will fit in one chunk. Combine to avoid two send() with another poll() in between.
    // We may not be able to send the whole thing. In that case more sends are going to occur, but only
    // from one buffer since body will have been emptied.
    if( sz_content > 0 && sz_stream + sz_content <= SEND_CHUNK_SZ ) {
      if( iStreamBuffer.Absorb( client->response.buffers.stream, client->response.buffers.content, LLONG_MAX ) < 0 ) {
        return -1; // big server problem. No response just close.
      }
    }
    *buffer = client->response.buffers.stream;
  }
  // We are sending response body
  else {
    *buffer = client->response.buffers.content;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void __io__count_response_exception( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  // Count 400-527 http codes
  if( client->response.info.http_errcode >= 400 ) {
    int cx = ((int)client->response.info.http_errcode - 400) & 0x7f; // restrict to range 0-127 for safety
    if( server->counters.perf->__http_400_527[ cx ] < 0xffff ) {
      server->counters.perf->__http_400_527[ cx ]++; // max 65536
    }
  }
  if( client->response.info.error.svc_exe ) {
    server->counters.perf->error_count.service++;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __io__update_perf_counters( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  client->io_t1_ns = __GET_CURRENT_NANOSECOND_TICK();
  double request_duration_seconds = (client->io_t1_ns - client->io_t0_ns) / 1e9;
  server->counters.perf->request_count_total++;
  server->counters.perf->duration_total += request_duration_seconds;
  if( client->response.info.execution.plugin ) {
    server->counters.perf->request_count_plugin++;
  }
  
  // Increment counter for this duration's bucket
  int duration_bucket = vgx_server_counters__get_bucket_by_duration( request_duration_seconds );
  server->counters.perf->__duration_sample_buckets[ duration_bucket ]++;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
__inline static void __io__process_next_request( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  vgx_server_request__client_request_ready( server, client );
  vgx_server_response__client_response_reset( client );
  if( vgx_server_request__handle( server, client ) < 0 ) {
    // Fallback in case error was not set
    if( !CLIENT_STATE__HAS_ERROR( client ) ) {
      vgx_server_response__produce_error( server, client, HTTP_STATUS__InternalServerError, "Unknown internal error", true );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int __io__transition_request_state( vgx_VGXServer_t *server, vgx_VGXServerClient_t *client ) {
  bool err = CLIENT_STATE__HAS_ERROR( client );
  if( CLIENT_STATE__IS_COLLECTED( client ) || err ) {
    // Transition COLLECTED -> COMPLETE if both response buffers are empty
    // All data has been sent
    if( !__io__client_has_response_data( client ) ) {
      // Error counters
      if( err ) {
        server->counters.perf->error_count.http++;
      }
      if( client->response.info.http_errcode ) {
        __io__count_response_exception( server, client );
      }

      // Update performance counters (unless excluded from metrics)
      if( !client->response.info.execution.nometrics ) {
        __io__update_perf_counters( server, client );
      }

      // Request produced an error of some sort
      if( CLIENT_STATE__HAS_ERROR( client ) ) {
        // Connections that are not managed by a higher level VGX dispatcher will be closed at this point.
        if( client->request.accept_type != MEDIA_TYPE__application_x_vgx_partial ) {
          goto error;
        }
        // Release dispatcher resources
        if( client->dispatcher.streams ) {
          vgx_server_dispatcher_dispatch__complete( server, client );
        }
      }

      // Response complete
      CLIENT_STATE__UPDATE( client, VGXSERVER_CLIENT_STATE__RESPONSE_COMPLETE );

      // Check if client already has pipelined request data for the next request
      if( __io__client_has_request_data( client ) ) {
        __io__process_next_request( server, client );
      }
    }
  }

  return 0;

error:
  vgx_server_client__close( server, client );
  return -1;
}
