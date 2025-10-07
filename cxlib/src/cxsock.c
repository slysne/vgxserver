/******************************************************************************
 * 
 * VGX Server
 * Distributed engine for plugin-based graph and vector search
 * 
 * Module:  cxlib
 * File:    cxsock.c
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

#include "cxsock.h"
#include "cxexcept.h"


/*******************************************************************//**
 * 
 ***********************************************************************
 */
struct addrinfo * cxgetaddrinfo( const char *node, const char *service, int *err ) {
  struct addrinfo *address = NULL;
  struct addrinfo hints = {0};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_CANONNAME;
  if( (*err = getaddrinfo( node, service, &hints, &address )) == 0 ) {
    return address;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
struct addrinfo * cxlowaddr( struct addrinfo *address ) {
  struct addrinfo *low = address;
  
  struct addrinfo *next = low;

  while( (next=next->ai_next) != NULL ) {
    if( memcmp( next->ai_addr, low->ai_addr, minimum_value( next->ai_addrlen, low->ai_addrlen ) ) < 0 ) {
      low = next;
    }
  }

  return low;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
struct addrinfo * cxhiaddr( struct addrinfo *address ) {
  struct addrinfo *hi = address;
  
  struct addrinfo *next = hi;

  while( (next=next->ai_next) != NULL ) {
    if( memcmp( next->ai_addr, hi->ai_addr, maximum_value( next->ai_addrlen, hi->ai_addrlen ) ) > 0 ) {
      hi = next;
    }
  }

  return hi;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
void cxfreeaddrinfo( struct addrinfo **address ) {
  if( address && *address ) {
    freeaddrinfo( *address );
    *address = NULL;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
int cxsockaddrtohostport( struct sockaddr *addr, char **host, size_t sz_host, unsigned short *port ) {
  char *local = NULL;
  if( host ) {
    if( *host == NULL ) {
      sz_host = 63;
      if( (local = *host = calloc( sz_host + 1, sizeof(char) )) == NULL ) {
        return -1;
      }
    }
  }

  if( addr->sa_family == AF_INET ) {
    struct sockaddr_in *addr_in = (struct sockaddr_in*)addr;
    if( host ) {
      inet_ntop( AF_INET, &addr_in->sin_addr, *host, sz_host );
    }
    if( port ) {
      *port = addr_in->sin_port;
    }
    return 0;
  }
  else if( addr->sa_family == AF_INET6 ) {
    struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6*)addr;
    if( host ) {
      inet_ntop( AF_INET, &addr_in6->sin6_addr, *host, sz_host );
    }
    if( port ) {
      *port = addr_in6->sin6_port;
    }
    return 0;
  }
  else {
    if( local ) {
      free( local );
    }
    return -1;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
int cxislocalhost( struct sockaddr *addr ) {
  if( addr->sa_family == AF_INET ) {
    struct sockaddr_in *addr_in = (struct sockaddr_in*)addr;
    return addr_in->sin_addr.s_addr == htonl( INADDR_LOOPBACK );
  }
  else if( addr->sa_family == AF_INET6 ) {
    struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6*)addr;
    return IN6_IS_ADDR_LOOPBACK( &addr_in6->sin6_addr );
  }
  else {
    return 0;
  }
}



/*******************************************************************//**
 * 
 * 
 * 
 ***********************************************************************
 */
int cxaddrinfotohostport( struct addrinfo *address, char **host, unsigned short *port ) {
  if( address && address->ai_addrlen >= 6 ) {
    return cxsockaddrtohostport( address->ai_addr, host, 0, port );
  }
  return -1;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
char * cxgetip( const char *hostname ) {
  char *ip = NULL;
  char namebuf[256] = {0};
  // Get local hostname if not provided
  if( hostname == NULL ) {
    if( gethostname( namebuf, 255 ) == 0 ) {
      hostname = namebuf;
    }
  }
  if( hostname ) {
    int err = 0;
    struct addrinfo *address = cxgetaddrinfo( hostname, NULL, &err );
    if( address ) {
      struct addrinfo *last = cxlowaddr( address );
      // ip is initially NULL, will be malloced
      cxaddrinfotohostport( last, &ip, NULL );
      cxfreeaddrinfo( &address );
    }
  }
  // Caller owns string
  return ip;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
char ** cxgetip_list( const char *hostname ) {
  char **list = NULL;
  char namebuf[256] = {0};
  // Get local hostname if not provided
  if( hostname == NULL ) {
    if( gethostname( namebuf, 255 ) == 0 ) {
      hostname = namebuf;
    }
  }
  if( hostname ) {
    int err = 0;
    struct addrinfo *address = cxgetaddrinfo( hostname, NULL, &err );
    if( address ) {
      struct addrinfo *cursor = address;
      int N = 0;
      while( cursor != NULL ) {
        ++N;
        cursor = cursor->ai_next;
      }
      if( (list = calloc( N+1LL, sizeof(char*) )) != NULL ) {
        int n = 0;
        cursor = address;
        while( cursor != NULL && n < N ) {
          char *ip = NULL; // auto malloc
          cxaddrinfotohostport( cursor, &ip, NULL );
          if( ip ) {
            list[n++] = ip;
          }
          cursor = cursor->ai_next;
        }
      }
      cxfreeaddrinfo( &address );
    }
  }
  // Caller owns NULL-terminated list of strings
  return list;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxisvalidip( const char *ip ) {
  struct sockaddr_in addr = {0};
  return inet_pton( AF_INET, ip, &addr.sin_addr ) == 1;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
char * cxgetnameinfo( const char *ip ) {
  char *name = NULL;
  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  if( inet_pton( addr.sin_family, ip, &addr.sin_addr ) == 1 ) {
    if( (name = calloc( 1, 256 )) != NULL ) {
      if( getnameinfo( (struct sockaddr*)&addr, sizeof( struct sockaddr_in ), name, 255, NULL, 0, 0 ) != 0 ) {
        free( name );
        name = NULL;
      }
    }
  }
  return name;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
CXSOCKET * cxsocket( CXSOCKET *psock, int af, int type, int protocol, bool keepalive, int linger_seconds, bool reuseaddr ) {
  struct linger _linger = {0};
#ifdef CXPLAT_WINDOWS_X64
  DWORD bTrue = 1;
  u_short l_linger = (u_short)linger_seconds;
  if( (psock->s = socket( af, type, protocol )) != INVALID_SOCKET ) {
#else
  int bTrue = 1;
  int l_linger = linger_seconds;
  if( !((psock->s = socket( af, type, protocol )) < 0) ) {
#endif
    // SO_KEEPALIVE
    if( keepalive ) {
      int idle = 120, interval = 10, count = 3;
      setsockopt( psock->s, SOL_SOCKET, SO_KEEPALIVE, (char*)&bTrue, sizeof(bTrue) );
#if defined CXPLAT_MAC_ARM64
      setsockopt( psock->s, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle));
#else
      setsockopt( psock->s, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle) );
#endif
      setsockopt( psock->s, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval) );
      setsockopt( psock->s, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count) );
    }
    // SO_LINGER
    if( linger_seconds > 0 ) {
      _linger.l_onoff = 1;
      _linger.l_linger = l_linger;
    }
    setsockopt( psock->s, SOL_SOCKET, SO_LINGER, (char*)&_linger, sizeof( _linger ) );
    // SO_REUSEADDR
    if( reuseaddr ) {
      setsockopt( psock->s, SOL_SOCKET, SO_REUSEADDR, (char*)&bTrue, sizeof( bTrue ) );
    }
    return psock;
  }
  else {
    return NULL;
  }
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxsocket_valid( const CXSOCKET *psock ) {
#ifdef CXPLAT_WINDOWS_X64
  return psock->s != INVALID_SOCKET;
#else
  return !(psock->s < 0);
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
void cxsocket_invalidate( CXSOCKET *psock ) {
#ifdef CXPLAT_WINDOWS_X64
  psock->s = INVALID_SOCKET;
#else
  psock->s = -1;
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxsocket_set_nonblocking( CXSOCKET *psock ) {
  int nfds;
  int err;
#ifdef CXPLAT_WINDOWS_X64
  u_long Mode = 1; // nonblocking when != 0
  if( (err = ioctlsocket( psock->s, FIONBIO, &Mode )) < 0 ) {
    return err;
  }
  nfds = 0; // Ignored on Windows
#else
  if( (err = fcntl( psock->s, F_SETFL, O_NONBLOCK )) < 0 ) {
    return err;
  }
  // Set to the highest-numbered file descriptor plus 1
  nfds = psock->s + 1;
#endif
  return nfds;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxsocket_is_connected( const CXSOCKET *psock ) {
  if( (int64_t)psock->s > 0 ) {
    char buf;
    int r = recv( psock->s, &buf, 1, MSG_PEEK );

    // Socket valid with no data available or with data available
    if( (r < 0 && (
  #ifdef CXPLAT_WINDOWS_X64
        WSAGetLastError() == WSAEWOULDBLOCK
  #else
        errno == EWOULDBLOCK || errno == EAGAIN
  #endif
    )) || r > 0 )
    {
      return true;
    }
  }

  // Socket was either closed by peer or has other errors
  return false;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxbind( CXSOCKET *psock, int family, const char *address, uint16_t port ) {

  struct sockaddr_in service;
  // Family
  service.sin_family =
#ifdef CXPLAT_WINDOWS_X64
    (ADDRESS_FAMILY)family;
#else
    (sa_family_t)family;
#endif
  // Internet address
  if( address == NULL || !strcmp( address, "0.0.0.0" ) ) {
    service.sin_addr.s_addr = INADDR_ANY;
  }
  else {
    service.sin_addr.s_addr = inet_addr( address );
  }
  // Port
  service.sin_port = htons( port );

  // Bind
  return bind( psock->s, (struct sockaddr*)&service, sizeof( service ) );

}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxlisten( CXSOCKET *psock, int backlog ) {
  int ret;
#ifdef CXPLAT_WINDOWS_X64
  ret = listen( psock->s, backlog );
#else
  ret = listen( psock->s, backlog );
#endif
  return ret;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxaccept( CXSOCKET *paccepted, CXSOCKET *plisten, struct sockaddr *addr, int *addrlen ) {
  int ret = 0;

  if( addr && addrlen && *addrlen <= 0 ) {
    *addrlen = sizeof( struct sockaddr );
  }

  #if defined CXPLAT_MAC_ARM64
  socklen_t socklen = *addrlen;
  paccepted->s = accept( plisten->s, addr, &socklen );
  #else
  paccepted->s = accept( plisten->s, addr, addrlen );
  #endif

  if( cxsocket_set_nonblocking( paccepted ) < 0 ) {
    return -1;
  }

#ifdef CXPLAT_WINDOWS_X64
  if( paccepted->s == INVALID_SOCKET ) {
    ret = -1;
  }
#else
  if( paccepted->s < 0 ) {
    ret = -1;
  }
#endif
  return ret;
}



/*******************************************************************//**
 * Connect without waiting for handshake to complete if socket is
 * set to nonblocking.
 * 
 * Returns :  0 if connection was made (for blocking sockets)
 *         : -1 for nonblocking sockets
 ***********************************************************************
 */
static int __connect_async( CXSOCKET *psock, const struct sockaddr *addr, size_t addrlen ) {
  // Connect (will fail with socket error in nonblocking mode)
  return connect( psock->s, addr, (int)addrlen );
}



/*******************************************************************//**
 * Connect and wait up to timeout_ms for the connection to be established
 * 
 * Returns: 1 on successful connect
 *          0 on connect timeout
 *         <0 on error
 ***********************************************************************
 */
static int __connect_sync( CXSOCKET *psock, const struct sockaddr *addr, size_t addrlen, int timeout_ms, short *revents ) {

  // Connect (will fail with socket error in nonblocking mode)
  int ret = connect( psock->s, addr, (int)addrlen );
  if( ret < 0 ) {
    if(
#ifdef CXPLAT_WINDOWS_X64
      WSAGetLastError() == WSAEWOULDBLOCK
#else
      errno == EAGAIN || errno == EINPROGRESS
#endif
    )
    // Non-blocking socket - poll with timeout
    {
      struct pollfd pollable = {0};
      cxpollfd_pollinit( &pollable, psock, false, true );
      // Wait for connection to be established (or timeout)
      if( cxpoll( &pollable, 1, timeout_ms ) < 0 ) {
        goto error; 
      }

      // Error
      if( cxpollfd_exception( &pollable ) ) {
        *revents = pollable.revents;
        return -1;
      }
      // Connected
      else if( cxpollfd_writable( &pollable ) ) {
        return 1;
      }
      // Timeout
      else {
        return 0;
      }
    }
    // Blocking socket failed to connect
    else {
      return -1;
    }
  }

  // Connected
  return 1;

error:
  {
    // Not connected
    int err;
    int optlen = (int)sizeof( int );
#if defined CXPLAT_MAC_ARM64
    socklen_t socklen = optlen;
    getsockopt( psock->s, SOL_SOCKET, SO_ERROR, (char*)&err, &socklen );
#else
    getsockopt( psock->s, SOL_SOCKET, SO_ERROR, (char*)&err, &optlen );
#endif
#ifdef CXPLAT_WINDOWS_X64
    WSASetLastError( err );
#else
    errno = err;
#endif
  }
  return -1;
}



/*******************************************************************//**
 * Connect to address
 * 
 * If timeout_ms is 0 and the socket is set to nonblocking we return -1 
 * and the caller has to use poll() or select() to determine when the
 * socket can be used.
 * 
 * If timeout_ms is > 0 and the socket is set to nonblocking we wait
 * up to that number of milliseconds for the handshake to complete.
 * 
 * 
 * Returns: 1 on successful connect
 *          0 on connect timeout
 *        < 0 when timeout_ms is zero and socket is nonblocking
 ***********************************************************************
 */
int cxconnect( CXSOCKET *psock, const struct sockaddr *addr, size_t addrlen, int timeout_ms, short *revents ) {
  if( addrlen > INT_MAX ) {
    return -EINVAL;
  }
  if( timeout_ms == 0 ) {
    return __connect_async( psock, addr, addrlen );
  }
  else {
    return __connect_sync( psock, addr, addrlen, timeout_ms, revents );
  }
}



/*******************************************************************//**
 * 
 * Return  > 0  socket was open and is now successfully closed
 *           0  no action (socket was not open)
 *         < 0  failed to close socket (WSAGetLastError() or errno says why)
 * 
 ***********************************************************************
 */
int64_t cxclose( CXSOCKET **psock ) {
  int64_t ret = 0;
  int64_t fd = (psock && *psock) ? (int64_t)((*psock)->s) : 0;
  if( fd > 0 ) {
#ifdef CXPLAT_WINDOWS_X64
    char buf[512];
    int n = 1<<20; // max recv iterations
    shutdown( (*psock)->s, SD_SEND );
    while( recv( (*psock)->s, buf, 512, 0 ) > 0 && --n > 0 );
    ret = closesocket( (*psock)->s );
    (*psock)->s = INVALID_SOCKET;
#else
    shutdown( (*psock)->s, SHUT_WR );
    ret = close( (*psock)->s );
    (*psock)->s = -1;
#endif
    *psock = NULL;
    // No error
    if( ret == 0 ) {
      // indicate success by returning socket descriptor as (positive) integer
      ret = fd;
    }
  }
  return ret;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int64_t cxsend( CXSOCKET *psock, const char *sbuf, size_t lenbuf, int flags ) {
  int64_t n;
#ifdef CXPLAT_WINDOWS_X64
  if( lenbuf > INT_MAX ) {
    WSASetLastError( WSAENOBUFS );
    return SOCKET_ERROR;
  }
  n = send( psock->s, sbuf, (int)lenbuf, flags );
#else
  n = send( psock->s, sbuf, lenbuf, flags );
#endif
  return n;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int64_t cxsendall( CXSOCKET *psock, const char *data, int64_t sz, int timeout_ms ) {
  int err = 0;
#ifdef CXPLAT_WINDOWS_X64
#define __cxbusy( Err ) ((err=WSAGetLastError()) == WSAEWOULDBLOCK )
#define __cxconnaborted( Err ) ((Err) == WSAECONNABORTED)
#define __nfds( Socket ) 0 /* ignored on windows */
#else
#define __cxconnaborted( err ) (err == ECONNABORTED)
#define __cxbusy( Err ) ((err=(Err)) == EWOULDBLOCK || err == EAGAIN )
#define __nfds( Socket ) ((Socket)->s + 1) /* Set to the highest-numbered file descriptor plus 1 */
#endif
#define MAX_SEND_CHUNK 32768
  struct timeval timeout = {
    .tv_sec = timeout_ms / 1000,
    .tv_usec = (timeout_ms % 1000) * 1000
  };
  const char *p = data;
  int64_t nremain = sz;
  int64_t nsent;

  int64_t maxsend = minimum_value( nremain, MAX_SEND_CHUNK );
  if( (nsent = cxsend( psock, p, maxsend, 0 )) != nremain ) {
    if( nsent > 0 ) {
      nremain -= nsent;
    }
    while( nremain > 0 ) {
      fd_set Writable;
      FD_ZERO( &Writable );
      IGNORE_WARNING_EXPRESSION_BEFORE_COMMA_HAS_NO_EFFECT
      FD_SET( psock->s, &Writable );
      RESUME_WARNINGS
      int s = select( __nfds( psock ), NULL, &Writable, NULL, &timeout );
      if( s == 1 && FD_ISSET( psock->s, &Writable ) ) {
        maxsend = minimum_value( nremain, MAX_SEND_CHUNK );
        if( (nsent = cxsend( psock, p, maxsend, 0 )) > 0 ) {
          nremain -= nsent;
        }
        else if( !__cxbusy( errno ) ) {
          return -err;
        }
      }
      else {
        return -1;
      }
    }
  }

  return sz;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int64_t cxrecv( CXSOCKET *psock, char *rbuf, size_t lenbuf, int flags ) {
  int64_t n;
#ifdef CXPLAT_WINDOWS_X64
  if( lenbuf > INT_MAX ) {
    WSASetLastError( WSAEMSGSIZE );
    return SOCKET_ERROR;
  }
  n = recv( psock->s, rbuf, (int)lenbuf, flags );
#else
  n = recv( psock->s, rbuf, lenbuf, flags );
#endif
  return n;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
short cxpollfd_pollinit( struct pollfd *pfd, const CXSOCKET *psock, bool read, bool write ) {
  pfd->fd = psock->s;
  return pfd->events = (POLLIN*read) | (POLLOUT*write);
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
short cxpollfd_pollread( struct pollfd *pfd ) {
  pfd->events |= POLLIN;
  return pfd->events;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
short cxpollfd_pollwrite( struct pollfd *pfd ) {
  pfd->events |= POLLOUT;
  return pfd->events;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
int cxpoll( struct pollfd *fds, uint32_t nfds, int timeout ) {
#ifdef CXPLAT_WINDOWS_X64
  return WSAPoll( fds, nfds, timeout );
#else
  return poll( fds, nfds, timeout );
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_valid( const struct pollfd *pfd ) {
#ifdef CXPLAT_WINDOWS_X64
  return pfd->fd != INVALID_SOCKET;
#else
  return !(pfd->fd < 0);
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_invalid( const struct pollfd *pfd ) {
#ifdef CXPLAT_WINDOWS_X64
  return pfd->fd == INVALID_SOCKET;
#else
  return pfd->fd < 0;
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
void cxpollfd_invalidate( struct pollfd *pfd ) {
#ifdef CXPLAT_WINDOWS_X64
  pfd->fd = INVALID_SOCKET;
#else
  pfd->fd = -1;
#endif
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_readable( const struct pollfd *pfd ) {
  return (pfd->revents & POLLIN) != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_writable( const struct pollfd *pfd ) {
  return (pfd->revents & POLLOUT) != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_exception( const struct pollfd *pfd ) {
  static short mask = POLLHUP | POLLERR | POLLNVAL;
  return (pfd->revents & mask) != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_any( const struct pollfd *pfd ) {
  return pfd->revents != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
bool cxpollfd_any_valid( const struct pollfd *pfd ) {
  return pfd->fd
#ifdef CXPLAT_WINDOWS_X64
  != INVALID_SOCKET
#else
  >= 0
#endif
  && pfd->revents != 0;
}



/*******************************************************************//**
 * 
 ***********************************************************************
 */
void cxpollfd_clear( struct pollfd *pfd ) {
  pfd->events = 0;
  pfd->revents = 0;
}
