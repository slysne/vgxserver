/*
###################################################
#
# cxsock.h
#
###################################################
*/

#ifndef CXLIB_CXSOCK_H
#define CXLIB_CXSOCK_H

#include "cxplat.h"

#if defined CXPLAT_WINDOWS_X64
// ok
#elif defined(CXPLAT_LINUX_ANY) || defined(CXPLAT_MAC_ARM64)
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#ifndef INVALID_SOCKET
#define INVALID_SOCKET  (~0)
#endif
#else
#  error "Unsupported platform"
#endif

#if defined CXPLAT_LINUX_ANY
#include <sys/eventfd.h>
#define CXPLAT_FEATURE_EVENTFD
#elif defined CXPLAT_MAC_ARM64
// TODO: Replace eventfd usage on macOS with kqueue or pipe-based event signaling.
#endif


#ifdef __cplusplus
extern "C" {
#endif


typedef struct s_CXSOCKET {
#if defined  CXPLAT_WINDOWS_X64
  SOCKET s;
#else
  int s;
  int _rsv;
#endif
} CXSOCKET;


struct addrinfo *   cxgetaddrinfo( const char *node, const char *service, int *err );
struct addrinfo *   cxlowaddr( struct addrinfo *address );
struct addrinfo *   cxhiaddr( struct addrinfo *address );
void                cxfreeaddrinfo( struct addrinfo **address );
int                 cxsockaddrtohostport( struct sockaddr *addr, char **host, size_t sz_host, unsigned short *port );
int                 cxislocalhost( struct sockaddr *addr );
int                 cxaddrinfotohostport( struct addrinfo *address, char **host, unsigned short *port );
char *              cxgetip( const char *hostname );
char **             cxgetip_list( const char *hostname );
bool                cxisvalidip( const char *ip );
char *              cxgetnameinfo( const char *ip );
CXSOCKET *          cxsocket( CXSOCKET *psock, int af, int type, int protocol, bool keepalive, int linger_seconds, bool reuseaddr );
int                 cxsocket_valid( const CXSOCKET *psock );
void                cxsocket_invalidate( CXSOCKET *psock );
int                 cxsocket_set_nonblocking( CXSOCKET *psock );
bool                cxsocket_is_connected( const CXSOCKET *psock );
int                 cxbind( CXSOCKET *psock, int family, const char *address, uint16_t port );
int                 cxlisten( CXSOCKET *psock, int backlog );
int                 cxaccept( CXSOCKET *paccepted, CXSOCKET *plisten, struct sockaddr *addr, int *addrlen );
int                 cxconnect( CXSOCKET *psock, const struct sockaddr *addr, size_t addrlen, int timeout_ms, short *revents );
int64_t             cxclose( CXSOCKET **psock );
int64_t             cxsend( CXSOCKET *psock, const char *sbuf, size_t lenbuf, int flags );
int64_t             cxsendall( CXSOCKET *psock, const char *data, int64_t sz, int timeout_ms );
int64_t             cxrecv( CXSOCKET *psock, char *rbuf, size_t lenbuf, int flags );

short               cxpollfd_pollinit( struct pollfd *pfd, const CXSOCKET *psock, bool read, bool write );
short               cxpollfd_pollread( struct pollfd *pfd );
short               cxpollfd_pollwrite( struct pollfd *pfd );
int                 cxpoll( struct pollfd *fds, uint32_t nfds, int timeout );
bool                cxpollfd_valid( const struct pollfd *pfd );
bool                cxpollfd_invalid( const struct pollfd *pfd );
void                cxpollfd_invalidate( struct pollfd *pfd );
bool                cxpollfd_readable( const struct pollfd *pfd );
bool                cxpollfd_writable( const struct pollfd *pfd );
bool                cxpollfd_exception( const struct pollfd *pfd );
bool                cxpollfd_any( const struct pollfd *pfd );
bool                cxpollfd_any_valid( const struct pollfd *pfd );
void                cxpollfd_clear( struct pollfd *pfd );

#ifdef __cplusplus
}
#endif

#endif
