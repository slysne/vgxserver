/*######################################################################
 *#
 *# vgx_server_config.c
 *#
 *#
 *######################################################################
 */


#include "_vgx.h"
#include "_vxserver.h"

/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_VGX_GRAPH );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerConfig_t * vgx_server_config__new( void ) {

  vgx_VGXServerConfig_t *cf = NULL;
  XTRY {
    // Config object
    if( (cf = calloc( 1, sizeof( vgx_VGXServerConfig_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    
  }
  XCATCH( errcode ) {
    iVGXServer.Config.Delete( &cf );
  }
  XFINALLY {
  }

  return cf;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__delete( vgx_VGXServerConfig_t **cf ) {
  if( cf && *cf ) {
    // Delete front config
    iVGXServer.Config.Front.Delete( &(*cf)->front );

    // Delete dispatcher config
    iVGXServer.Config.Dispatcher.Delete( &(*cf)->dispatcher );

    // Delete
    free( *cf );
    *cf = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__set_front( vgx_VGXServerConfig_t *cf, vgx_VGXServerFrontConfig_t **cf_front ) {
  // Discard any previous front config
  iVGXServer.Config.Front.Delete( &cf->front );

  // Steal front config
  if( cf_front ) {
    cf->front = *cf_front;
    *cf_front = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__set_dispatcher( vgx_VGXServerConfig_t *cf, vgx_VGXServerDispatcherConfig_t **cf_dispatcher ) {
  // Discard any previous dispatcher config
  iVGXServer.Config.Dispatcher.Delete( &cf->dispatcher );

  // Steal dispatcher config
  if( cf_dispatcher ) {
    cf->dispatcher = *cf_dispatcher;
    *cf_dispatcher = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN extern const vgx_VGXServerConfig_t * vgx_server_config__get( vgx_VGXServer_t *server, uint16_t *rport, int *rwidth, int *rheight, bool *rallow_incomplete ) {
  vgx_VGXServerConfig_t *cf = NULL;
  if( server && server->sysgraph ) {
    GRAPH_LOCK( server->sysgraph ) {
      if( (cf = server->config.cf_server) != NULL ) {
        if( cf->front ) {
          if( rport ) {
            *rport = cf->front->port.base + cf->front->port.offset;
          }
        }
        if( cf->dispatcher ) {
          if( rwidth ) {
            *rwidth = cf->dispatcher->shape.width;
          }
          if( rheight ) {
            *rheight = cf->dispatcher->shape.height;
          }
          if( rallow_incomplete ) {
            *rallow_incomplete = cf->dispatcher->allow_incomplete;
          }
        }
      }
    } GRAPH_RELEASE;
  }
  return cf;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN extern vgx_VGXServerConfig_t * vgx_server_config__clone( vgx_VGXServer_t *server ) {
  vgx_VGXServerConfig_t *clone = NULL;
  if( server && server->sysgraph ) {
    if( (clone = iVGXServer.Config.New()) != NULL ) {
      GRAPH_LOCK( server->sysgraph ) {
        const vgx_VGXServerConfig_t *cf = server->config.cf_server;
        XTRY {
          // Front
          vgx_VGXServerFrontConfig_t *f = cf->front;
          const char *ip = f->CSTR__ip ? CStringValue( f->CSTR__ip ) : NULL;
          const char *prefix = f->CSTR__prefix ? CStringValue( f->CSTR__prefix ) : NULL;
          if( (clone->front = iVGXServer.Config.Front.New( ip, f->port.base, f->port.offset, prefix )) == NULL ) {
            THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
          }
          // Dispatcher (for base server only)
          if( f->port.offset == 0 && cf->dispatcher ) {
            vgx_VGXServerDispatcherConfig_t *d = cf->dispatcher;
            // New dispatcher config with same width/height
            if( (clone->dispatcher = iVGXServer.Config.Dispatcher.New( d->shape.width, d->shape.height )) == NULL ) {
              THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
            }
            // Copy allow incomplete flag
            clone->dispatcher->allow_incomplete = d->allow_incomplete;
            // Clone matrix
            for( int w=0; w<d->shape.width; w++ ) {
              vgx_VGXServerDispatcherConfigPartition_t *psrc = &d->partitions[w];
              vgx_VGXServerDispatcherConfigPartition_t *pdst = &clone->dispatcher->partitions[w];
              for( int h=0; h<d->shape.height; h++ ) {
                vgx_VGXServerDispatcherConfigReplica_t *rsrc = &psrc->replicas[h];
                vgx_VGXServerDispatcherConfigReplica_t *rdst = &pdst->replicas[h];
                rdst->uri = rsrc->uri ? iURI.Clone( rsrc->uri ) : NULL;
                rdst->__rsv_1_2 = rsrc->__rsv_1_2;
                rdst->port = rsrc->port;
                rdst->__rsv_1_3_1_2 = rsrc->__rsv_1_3_1_2;
                rdst->settings.bits = rsrc->settings.bits;
                rdst->__rsv_1_4 = rsrc->__rsv_1_4;
              }
            }
          }
          // Plugin Function
          clone->executor_pluginf = cf->executor_pluginf;
        }
        XCATCH( errcode ) {
          iVGXServer.Config.Delete( &clone );
        }
        XFINALLY {
        }
      } GRAPH_RELEASE;
    }
  }
  return clone;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__set_executor_plugin_entrypoint( vgx_VGXServerConfig_t *config, f_vgx_ServicePluginCall pluginf ) {
  config->executor_pluginf = pluginf; 
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerFrontConfig_t * vgx_server_config__front_new( const char *ip, uint16_t port, uint16_t offset, const char *prefix ) {

  vgx_VGXServerFrontConfig_t *cf = NULL;
  XTRY {
    // Front config object
    if( (cf = calloc( 1, sizeof( vgx_VGXServerFrontConfig_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }
    
    // Service base port
    cf->port.base = port;

    // Service port offset
    cf->port.offset = offset;

    // Service ip
    if( ip ) {
      if( !cxisvalidip( ip ) ) {
        THROW_ERROR( CXLIB_ERR_API, 0x002 );
      }
      if( (cf->CSTR__ip = CStringNew( ip )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
    }

    // Service prefix
    if( prefix ) {
      if( (cf->CSTR__prefix = CStringNew( prefix )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x004 );
      }
    }
    else {
      cf->CSTR__prefix = NULL;
    }
  }
  XCATCH( errcode ) {
    iVGXServer.Config.Front.Delete( &cf );
  }
  XFINALLY {
  }

  return cf;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__front_delete( vgx_VGXServerFrontConfig_t **cf ) {
  if( cf && *cf ) {

    iString.Discard( &(*cf)->CSTR__ip );
    iString.Discard( &(*cf)->CSTR__prefix );

    free( *cf );
    *cf = NULL;

  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN vgx_VGXServerDispatcherConfig_t * vgx_server_config__dispatcher_new( int width, int height ) {

  vgx_VGXServerDispatcherConfig_t *cf = NULL;

  XTRY {
    // Check params
    if( width < 1 || width > SERVER_MATRIX_WIDTH_MAX ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x000, "Invalid width: %d (must be 1 - %d)", width, SERVER_MATRIX_WIDTH_MAX );
    }
    if( height < 1 || height > SERVER_MATRIX_HEIGHT_MAX ) {
      THROW_ERROR_MESSAGE( CXLIB_ERR_API, 0x000, "Invalid height: %d (must be 1 - %d)", height, SERVER_MATRIX_HEIGHT_MAX );
    }

    // Matrix config object
    if( (cf = calloc( 1, sizeof( vgx_VGXServerDispatcherConfig_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    // Number of partitions in matrix
    cf->shape.width = (BYTE)width;

    // Number of replicas per partition
    cf->shape.height = (BYTE)height;

    // Partition config objects
    if( (cf->partitions = calloc( cf->shape.width, sizeof( vgx_VGXServerDispatcherConfigPartition_t ) )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x002 );
    }

    // Set each partition config to default
    for( int i=0; i<cf->shape.width; i++ ) {
      vgx_VGXServerDispatcherConfigPartition_t *cf_partition = &cf->partitions[i];
      // Number of replicas in partition
      cf_partition->height = cf->shape.height;

      // Partial?
      cf_partition->partial = cf->shape.width > 1;

      // Replica config objects
      if( (cf_partition->replicas = calloc( cf_partition->height, sizeof( vgx_VGXServerDispatcherConfigReplica_t ) )) == NULL ) {
        THROW_ERROR( CXLIB_ERR_MEMORY, 0x003 );
      }
      // Set each replica config to default
      for( int k=0; k<cf_partition->height; k++ ) {
        vgx_VGXServerDispatcherConfigReplica_t *cf_replica = &cf_partition->replicas[k];

        // [Q1.1] No host yet
        cf_replica->uri = NULL;
        // [Q1.2]
        cf_replica->__rsv_1_2 = 0;
        // [Q1.3.1.1] Must set port later
        cf_replica->port = 0;
        // [Q1.3.1.2]
        cf_replica->__rsv_1_3_1_2 = 0;
        // [Q1.3.2]
        // Default max number of open channels per replica (may override later)
        cf_replica->settings.channels = REPLICA_DEFAULT_CHANNELS;
        // Default replica priority (may override later)
        cf_replica->settings.priority = REPLICA_PRIORITY_DEFAULT;
        // Replica is searchable by default
        cf_replica->settings.flag.readable = true;
        // Replica is not writable by default
        cf_replica->settings.flag.writable = false;
        // [Q1.4]
        cf_replica->__rsv_1_4 = 0;
      }
    }

  }
  XCATCH( errcode ) {
    iVGXServer.Config.Dispatcher.Delete( &cf );
  }
  XFINALLY {
  }

  return cf;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__dispatcher_delete( vgx_VGXServerDispatcherConfig_t **cf ) {
  if( cf && *cf ) {
    // Partitions
    if( (*cf)->partitions ) {
      for( int i=0; i<(*cf)->shape.width; i++ ) {
        vgx_VGXServerDispatcherConfigPartition_t *cf_partition = &(*cf)->partitions[ i ];
        // Replicas
        if( cf_partition->replicas ) {
          for( int k=0; k<cf_partition->height; k++ ) {
            vgx_VGXServerDispatcherConfigReplica_t *cf_replica = &cf_partition->replicas[ k ];
            iURI.Delete( &cf_replica->uri );
          }
          free( cf_partition->replicas );
          cf_partition->replicas = NULL;
        }
      }
      free( (*cf)->partitions );
      (*cf)->partitions = NULL;
    }

    free( *cf );
    *cf = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerDispatcherConfigPartition_t * __dispatcher_get_partition( const vgx_VGXServerDispatcherConfig_t *cf, int partition_id ) {
  if( cf && cf->partitions ) {
    if( partition_id >= 0 && partition_id < cf->shape.width ) {
      return &cf->partitions[ partition_id ];
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static vgx_VGXServerDispatcherConfigReplica_t * __dispatcher_get_replica( const vgx_VGXServerDispatcherConfig_t *cf, int partition_id, int replica_id ) {
  vgx_VGXServerDispatcherConfigPartition_t *cf_partition = __dispatcher_get_partition( cf, partition_id );
  if( cf_partition ) {
    if( replica_id >= 0 && replica_id < cf_partition->height ) {
      return &cf_partition->replicas[ replica_id ];
    }
  }
  return NULL;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_config__dispatcher_set_replica_address( vgx_VGXServerDispatcherConfig_t *cf, int partition_id, int replica_id, const char *host, int port, CString_t **CSTR__error ) {
  int ret = 0;

  if( port < 1 || port > 65535 ) {
    return -1;
  }

  vgx_VGXServerDispatcherConfigReplica_t *cf_replica = NULL;
  struct addrinfo *ai = NULL;
  XTRY {
    // Get the replica from the partition
    if( (cf_replica = __dispatcher_get_replica( cf, partition_id, replica_id )) == NULL ) {
      THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
    }

    // Remove any previous URI and ip
    iURI.Delete( &cf_replica->uri );

    // [Q1.3.1.1]
    // Set port
    cf_replica->port = (uint16_t)port;

    // [Q1.1]
    // Make new URI
    if( (cf_replica->uri = iURI.NewElements( "http", NULL, host, cf_replica->port, NULL, NULL, NULL, CSTR__error )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {
    iURI.DeleteAddrInfo( &ai );
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_config__dispatcher_set_replica_access( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, bool writable ) {
  int ret = 0;

  vgx_VGXServerDispatcherConfigReplica_t *cf_replica = NULL;
  XTRY {

    // Access is configured identically for same replica id across all partitions
    for( int i=0; i<cf->shape.width; i++ ) {
      if( (cf_replica = __dispatcher_get_replica( cf, i, replica_id )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }

      // [Q1.3.2]
      // Always readable
      cf_replica->settings.flag.readable = true;

      // [Q1.3.2]
      // Writable if not readonly
      cf_replica->settings.flag.writable = writable;
    }
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
DLL_HIDDEN int vgx_server_config__dispatcher_set_replica_channels( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, int channels ) {
  int ret = 0;

  if( channels < REPLICA_MIN_CHANNELS || channels > REPLICA_MAX_CHANNELS ) {
    return -1;
  }

  vgx_VGXServerDispatcherConfigReplica_t *cf_replica = NULL;
  XTRY {

    // Number of channels is configured identically for same replica id across all partitions
    for( int i=0; i<cf->shape.width; i++ ) {
      if( (cf_replica = __dispatcher_get_replica( cf, i, replica_id )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
  
      // [Q1.3.2]
      cf_replica->settings.channels = (int8_t)channels;
    }
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
DLL_HIDDEN int vgx_server_config__dispatcher_set_replica_priority( vgx_VGXServerDispatcherConfig_t *cf, int replica_id, int priority ) {
  int ret = 0;

  if( priority < REPLICA_PRIORITY_HIGHEST || priority > REPLICA_PRIORITY_LOWEST ) {
    return -1;
  }

  vgx_VGXServerDispatcherConfigReplica_t *cf_replica = NULL;
  XTRY {

    // Priority is configured identically for same replica id across all partitions
    for( int i=0; i<cf->shape.width; i++ ) {
      if( (cf_replica = __dispatcher_get_replica( cf, i, replica_id )) == NULL ) {
        THROW_SILENT( CXLIB_ERR_GENERAL, 0x001 );
      }
  
      // [Q1.3.2]
      cf_replica->settings.priority = (int8_t)priority;
    }
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
DLL_HIDDEN bool vgx_server_config__dispatcher_verify( const vgx_VGXServerDispatcherConfig_t *cf, CString_t **CSTR__error ) {
#define ConfigError( Format, ... ) __format_error_string( CSTR__error, "VGX Server dispatcher configuration error: " Format, ##__VA_ARGS__ ); goto bad_config

  // Verify object
  if( cf == NULL ) {
    ConfigError( "not initialized" );
  }

  // Verify partitions list
  if( cf->partitions == NULL ) {
    ConfigError( "partitions not initialized" );
  }

  const int max_nondefunct_cost = REPLICA_DEFUNCT_COST_FLOOR - REPLICA_MAX_COST - 1;

  // Verify each partition
  for( int i=0; i<cf->shape.width; i++ ) {
    vgx_VGXServerDispatcherConfigPartition_t *cf_partition = &cf->partitions[ i ];

    // Verify replicas list for partition
    if( cf_partition->replicas == NULL ) {
      ConfigError( "replicas not initialized for partition %d", i );
    }

    // Verify each replica in partition
    for( int k=0; k<cf_partition->height; k++ ) {
      vgx_VGXServerDispatcherConfigReplica_t *cf_replica = &cf_partition->replicas[ k ];

      // Verify replica URI
      if( cf_replica->uri == NULL ) {
        ConfigError( "empty URI for replica %d in partition %d", k, i );
      }

      // Verify max channels setting for replica
      if( cf_replica->settings.channels < REPLICA_MIN_CHANNELS || cf_replica->settings.channels > REPLICA_MAX_CHANNELS ) {
        ConfigError( "invalid channel limit %d for replica %d in partition %d", (int)cf_replica->settings.channels, k, i );
      }

      // Verify priority setting for replica
      if( cf_replica->settings.priority < REPLICA_PRIORITY_HIGHEST || cf_replica->settings.priority > REPLICA_PRIORITY_LOWEST ) {
        ConfigError( "invalid priority %d for replica %d in partition %d", (int)cf_replica->settings.priority, k, i );
      }

      // Verify max replica cost
      int max_cost = (int)cf_replica->settings.priority * (int)cf_replica->settings.channels;
      if( max_cost > max_nondefunct_cost ) {
        ConfigError( "max cost %d x %d = %d exceeds %d for replica %d in partition %d", (int)cf_replica->settings.channels, (int)cf_replica->settings.priority, max_cost, max_nondefunct_cost, k, i );
      }

      // Verify replica access
      if( cf_replica->settings.flag.readable == 0 && cf_replica->settings.flag.writable == 0 ) {
        ConfigError( "no access for replica %d in partition %d", k, i );
      }

    }
  }

  return true;

bad_config:
  return false;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN int vgx_server_config__dispatcher_channels( const vgx_VGXServerDispatcherConfig_t *cf ) {
  int n = 0;
  for( int i=0; i<cf->shape.width; i++ ) {
    for( int k=0; k<cf->shape.height; k++ ) {
      n += (int)cf->partitions[i].replicas[k].settings.channels;
    }
  }
  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_HIDDEN void vgx_server_config__dispatcher_dump( const vgx_VGXServerDispatcherConfig_t *cf ) {

  BEGIN_CXLIB_OBJ_DUMP( vgx_VGXServerDispatcherConfig_t, cf ) {

    CXLIB_OSTREAM( "Matrix shape" );
    CXLIB_OSTREAM( "    Number of partitions (width)                = %d", cf->shape.width );
    CXLIB_OSTREAM( "    Number of replicas per partitions (height)  = %d", cf->shape.height );

    // Partitions
    if( cf->partitions ) {
      for( int i=0; i<cf->shape.width; i++ ) {
        CXLIB_OSTREAM( "Partition %d:", i );
        vgx_VGXServerDispatcherConfigPartition_t *cf_partition = &cf->partitions[ i ];
        // Replicas
        if( cf_partition->replicas ) {
          for( int k=0; k<cf_partition->height; k++ ) {
            CXLIB_OSTREAM( "    Replica %d:", k );
            vgx_VGXServerDispatcherConfigReplica_t *cf_replica = &cf_partition->replicas[ k ];
            const char *host = iURI.Host( cf_replica->uri );
            int port = iURI.PortInt( cf_replica->uri );
            int channels = cf_replica->settings.channels;
            int priority = cf_replica->settings.priority;
            const char *readable = cf_replica->settings.flag.readable ? "[readable]" : "";
            const char *writable = cf_replica->settings.flag.writable ? "[writable]" : "";
            CXLIB_OSTREAM( "        ADDRESS=(%s,%d) ACCESS=(%s%s) CHANNELS=%d PRIORITY=%d", host, port, readable, writable, channels, priority );
          }
        }
      }
    }
  } END_CXLIB_OBJ_DUMP;
}


