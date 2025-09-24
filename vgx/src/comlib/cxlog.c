/*
 * cxlog.c
 *
 *
*/

#include "cxlog.h"


/* exception module */
SET_EXCEPTION_MODULE( COMLIB_MSG_MOD_COMLIB );


static LogContext_t * cxlog_new_context_OPEN( void );
static void cxlog_close_file_XLOCK( LogContext_t *context, bool flush );
static void cxlog_clear_context_OPEN( LogContext_t *context );
static void cxlog_delete_context_OPEN( LogContext_t **context );
static int64_t cxlog_write_timestamp_output_OPEN( LogContext_t *context, int64_t ns_1970, CString_t *CSTR__msg );
static int cxlog_process_next_OPEN( LogContext_t *context, int wait );
static unsigned cxlog_task_initialize_OPEN( comlib_task_t *self );
static unsigned cxlog_task_shutdown_OPEN( comlib_task_t *self );
static int cxlog_force_rotate_XLOCK( LogContext_t *context );




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static LogContext_t * cxlog_new_context_OPEN( void ) {
  return calloc( 1, sizeof(LogContext_t) );
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void cxlog_close_file_XLOCK( LogContext_t *context, bool flush ) {
  if( context ) {
    // Flush all queue items
    if( flush && context->queue && context->fd > 0 ) {
      if( context->init ) {
        // Close queue input before flushing
        SYNCHRONIZE_ON( context->qlock ) {
          context->qready = false;
        } RELEASE;
        // Flush
        while( ComlibSequenceLength(context->queue) > 0 ) {
          cxlog_process_next_OPEN( context, 0 );
        }
      }
    }

    // Close file
    if( context->fd > 0 ) {
      CX_CLOSE( context->fd );
      context->fd = 0;
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void cxlog_drain_queue_OPEN( LogContext_t *context ) {
  // Drain
  Cx2tptrQueue_t *Q = context->queue;
  x2tptr_t ts_cstr = {0};
  CString_t *CSTR__data;
  while( ComlibSequenceLength(Q) > 0 ) {
    CALLABLE( Q )->NextNolock( Q, &ts_cstr );
    if( (CSTR__data = (CString_t*)ts_cstr.t_2.qword) != NULL ) {
      CStringDelete( CSTR__data );
    }
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void cxlog_clear_context_OPEN( LogContext_t *context ) {
  if( context ) {
    // Flush all items in queue and close file
    CS_LOCK *xlock = context->init ? &context->xlock : NULL;
    SYNCHRONIZE_ON_PTR( xlock ) {
      // Flush queue to file and mark queue input as disabled
      // Nothing new will come into the queue after this
      cxlog_close_file_XLOCK( context, true );
    } RELEASE;

    // Destroy queue
    if( context->queue ) {
      // Delete any queue contents that may remain (due to incomplete flush)
      cxlog_drain_queue_OPEN( context );
      // Destroy queue object
      COMLIB_OBJECT_DESTROY( context->queue );
      context->queue = NULL;
    }

    // Destroy locks and condition variable
    if( context->init ) {
      DEL_CRITICAL_SECTION( &context->xlock.lock );
      DEL_CRITICAL_SECTION( &context->qlock.lock );
      DEL_CONDITION_VARIABLE( &context->qwake.cond );
      context->init = 0;
    }

    // Delete filename
    if( context->CSTR__filename ) {
      COMLIB_OBJECT_DESTROY( context->CSTR__filename );
      context->CSTR__filename = NULL;
    }

    context->sz = 0;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static void cxlog_delete_context_OPEN( LogContext_t **context ) {
  if( context && *context ) {
    cxlog_clear_context_OPEN( *context );
    free( *context );
    *context = NULL;
  }
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t cxlog_write_timestamp_output_OPEN( LogContext_t *context, int64_t ns_1970, CString_t *CSTR__msg ) {
  int64_t nwritten = 0;
  char tbuf[64] = {0};    /* (a little more than we need for the 23-char "2016-07-07 12:34:56.789" ) */
  char *p = NULL;
  if( ns_1970 > 0 ) {
    int64_t milliseconds_since_epoch = ns_1970 < 0 ? __MILLISECONDS_SINCE_1970() : ns_1970 / 1000000;
    time_t seconds_since_epoch = milliseconds_since_epoch / 1000;
    struct tm *now;
    if( (now = localtime( &seconds_since_epoch )) != NULL ) {
      p = tbuf;
      *p++ = '[';
      size_t nt = strftime( p, 31, "%Y-%m-%d %H:%M:%S.", now );
      if( nt > 0 ) {
        p += nt;
      }
      /*                 2016-07-07 12:34:56.---  */
      /*                 0123456789..........^    */
      /*                                     20   */
      /* since system start (not epoch) but good enough for logging, we just want more resolution between seconds */
      int nm = sprintf( p, "%03lld", milliseconds_since_epoch % 1000 );
      if( nm > 0 ) {
        p += nm;
      }
      *p++ = ']';
      *p++ = ' ';
      *p++ = ' ';
      *p = '\0';
    }
  }

  SYNCHRONIZE_ON( context->xlock ) {
    if( context->fd > 0 ) {
      if( p ) {
        nwritten += CX_WRITE( tbuf, 1, p - tbuf, context->fd );
      }
      nwritten += CX_WRITE( CStringValue(CSTR__msg), 1, CStringLength(CSTR__msg), context->fd );
      nwritten += CX_WRITE( "\n", 1, 1, context->fd );
    }
  } RELEASE;

  return nwritten;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int64_t cxlog_write__OPEN( LogContext_t *context, int64_t ns_1970, CString_t *CSTR__msg ) {
  int64_t nwritten = 0;
  int64_t milliseconds_since_epoch = ns_1970 < 0 ? __MILLISECONDS_SINCE_1970() : ns_1970 / 1000000;
  time_t seconds_since_epoch = milliseconds_since_epoch / 1000;
  struct tm *now;
  char tbuf[64] = {0};    /* (a little more than we need for the 23-char "2016-07-07 12:34:56.789" ) */
  char *p = tbuf;
  if( (now = localtime( &seconds_since_epoch )) != NULL ) {
    *p++ = '[';
    size_t nt = strftime( p, 31, "%Y-%m-%d %H:%M:%S.", now );
    if( nt > 0 ) {
      p += nt;
    }
    /*                 2016-07-07 12:34:56.---  */
    /*                 0123456789..........^    */
    /*                                     20   */
    /* since system start (not epoch) but good enough for logging, we just want more resolution between seconds */
    int nm = sprintf( p, "%03lld", milliseconds_since_epoch % 1000 );
    if( nm > 0 ) {
      p += nm;
    }
    *p++ = ']';
    *p++ = ' ';
    *p++ = ' ';
    *p = '\0';
  }

  SYNCHRONIZE_ON( context->xlock ) {
    if( context->fd > 0 ) {
      nwritten += CX_WRITE( tbuf, 1, p - tbuf, context->fd );
      nwritten += CX_WRITE( CStringValue(CSTR__msg), 1, CStringLength(CSTR__msg), context->fd );
      nwritten += CX_WRITE( "\n", 1, 1, context->fd );
    }
  } RELEASE;

  return nwritten;
}




/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int cxlog_process_next_OPEN( LogContext_t *context, int wait ) {
  int n = 0;
  x2tptr_t ts_cstr = {0};
  Cx2tptrQueue_t *Q = context->queue;
  SYNCHRONIZE_ON( context->qlock ) {
    do {
      // We have data, extract one item
      if( ComlibSequenceLength(Q) > 0 ) {
        n = CALLABLE( Q )->NextNolock( Q, &ts_cstr );
        if( ts_cstr.t_2.qword == 0 ) { // end of thread marker
          n = -1;
        }
        break;
      }

      // No data, no wait
      if( wait == 0 ) {
        break;
      }

      // No data, wait for data signal
      TIMED_WAIT_CONDITION_CS( &(context->qwake.cond), &(context->qlock.lock), wait );
      // Break if no data, otherwise back to top and extract item
    } while( ComlibSequenceLength(Q) > 0 );
  } RELEASE;

  // Unpack and write to output
  if( n > 0 ) {
    int64_t ns_1970 = ts_cstr.t_1.int64;
    CString_t *CSTR__msg = (CString_t*)ts_cstr.t_2.qword;
    context->sz += cxlog_write_timestamp_output_OPEN( context, ns_1970, CSTR__msg );
    COMLIB_OBJECT_DESTROY( CSTR__msg );
  }

  return n;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned cxlog_task_initialize_OPEN( comlib_task_t *self ) {
  LogContext_t *context = COMLIB_TASK__GetData( self );
  Cx2tptrQueue_t *Q = NULL;
  if( context->init == 0 ) {
    Cx2tptrQueue_constructor_args_t queue_args = {
      .element_capacity = 64,
      .comparator = NULL
    };
    Q = context->queue = COMLIB_OBJECT_NEW( Cx2tptrQueue_t, NULL, &queue_args );
    INIT_CONDITION_VARIABLE( &context->qwake.cond );
    INIT_SPINNING_CRITICAL_SECTION( &context->qlock.lock, 4000 );
    context->fd = 0;
    INIT_SPINNING_CRITICAL_SECTION( &context->xlock.lock, 4000 );
    context->init = 1;
  }
  if( Q == NULL ) {
    return 1;
  }
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static unsigned cxlog_task_shutdown_OPEN( comlib_task_t *self ) {
  LogContext_t *context = COMLIB_TASK__GetData( self );
  cxlog_clear_context_OPEN( context );
  return 0;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
static int cxlog_force_rotate_XLOCK( LogContext_t *context ) {
  if( context == NULL || !context->init ) {
    return -1;
  }

  int ret = 0;
  // access.2025-05-08-070000

  // access.2025-05-08-070000._2
  const char *sep = "._";
  int n = 0;
  const CString_t **CSTR__parts = NULL;
  CString_t *CSTR__split = NULL;
  XTRY {
    if( (CSTR__split = CStringNew(sep)) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x001 );
    }
    
    if( context->CSTR__filename ) {
      CSTR__parts = (const CString_t**)CALLABLE(context->CSTR__filename)->Split(context->CSTR__filename, sep, &n );
    }
    if( CSTR__parts == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    CString_t *CSTR__next = NULL;

    // Found split extension, increment it
    if( n > 1 ) {
      // Sequence number
      const char *suffix = CStringValue( CSTR__parts[n-1] );
      char *end = NULL;
      int64_t seq = strtoll( suffix, &end, 10 );
      // Integer found
      if( end && *end == '\0' ) {
        // Replace sequence number with one higher
        CStringDelete(CSTR__parts[n-1]);
        if( (CSTR__parts[n-1] = CStringNewFormat( "%lld", seq+1 )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x003 );
        }

        // Reassemble filepath
        CSTR__next = CALLABLE(CSTR__split)->Join(CSTR__split, (const CString_t**)CSTR__parts);
      }
    }

    // No split extension yet, make first 
    if( CSTR__next == NULL ) {
      CSTR__next = CStringNewFormat( "%s%s1", CStringValue(context->CSTR__filename), sep );
    }

    if( CSTR__next == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x004 );
    }
    
    // Rotate
    cxlog_close_file_XLOCK( context, false );
    if( COMLIB__OpenLog( CStringValue( CSTR__next ), NULL, &context->fd, NULL ) < 0 ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x005 );
    }

    context->sz = context->fd > 0 ? CX_SEEK( context->fd, 0, SEEK_END ) : 0;

    // Replace with new filename
    CStringDelete( context->CSTR__filename );
    context->CSTR__filename = CSTR__next;

  }
  XCATCH( errcode ) {
    ret = -1;
  }
  XFINALLY {

    if( CSTR__split ) {
      CStringDelete( CSTR__split );
    }

    if( CSTR__parts ) {
      for( int i=0; i<n; i++ ) {
        if( CSTR__parts[i] ) {
          CStringDelete( CSTR__parts[i] );
        }
      }
      free( (void*)CSTR__parts );
    }
  }

  return ret;
}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
BEGIN_COMLIB_TASK( self,
                   LogContext_t,
                   context,
                   cxlog_task,
                   CXLIB_THREAD_PRIORITY_DEFAULT,
                   "accesslog/" )
{

  int64_t size_limit = 100LL << 20;

  comlib_task_delay_t loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
  bool running = true;

  BEGIN_COMLIB_TASK_MAIN_LOOP( loop_delay ) {
    if( running ) {
      SYNCHRONIZE_ON( context->xlock ) {
        // Force log rotation of current file too large
        if( context->sz > size_limit ) {
          cxlog_force_rotate_XLOCK( context );
        }

        // Only attempt output if a file is currently open
        if( context->fd > 0 ) {
          int batch = 100;
          int n;
          do {
            SUSPEND_SYNCH {
              n = cxlog_process_next_OPEN( context, 1000 );
            } RESUME_SYNCH;
            if( n < 0 ) {
              running = false; // end of thread
              break;
            }
          } while( n > 0 && --batch > 0 );
        }

        // No open file or end of thread, brief pause before next iteration
        if( context->fd <= 0 || running == false ) {
          loop_delay = COMLIB_TASK_LOOP_DELAY( 10 );
        }
        else {
          loop_delay = COMLIB_TASK_LOOP_DELAY( 0 );
        }

      } RELEASE;
    }

    if( COMLIB_TASK__IsStopping( self ) ) {
      COMLIB_TASK__AcceptRequest_Stop( self );
    }

  } END_COMLIB_TASK_MAIN_LOOP;

} END_COMLIB_TASK;



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT LogContext_t * COMLIB__NewLogContext( void ) {

  LogContext_t *context = NULL;

  XTRY {
    if( (context = cxlog_new_context_OPEN()) == NULL ) {
      THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
    }

    if( (context->task = COMLIB_TASK__StartNew( cxlog_task, cxlog_task_initialize_OPEN, cxlog_task_shutdown_OPEN, context, 10000 )) == NULL ) {
      THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
    }

    context->qready = true;

  }
  XCATCH( errcode ) {
    COMLIB__DeleteLogContext( &context );
  }
  XFINALLY {
  }

  return context;

}



/*******************************************************************//**
 *
 *
 ***********************************************************************
 */
DLL_EXPORT int COMLIB__DeleteLogContext( LogContext_t **context ) {
  int ret = 0;

  if( context && *context ) {
    LogContext_t *ctx = *context;

    // End of logging
    SYNCHRONIZE_ON( ctx->qlock ) {
      COMLIB__Log( ctx, 0, NULL );
      ctx->qready = false; // no more items allowed into queue
    } RELEASE;

    // Drain
    int64_t remain = 0;
    SYNCHRONIZE_ON( ctx->qlock ) {
      int draining = 5;
      int64_t last = remain = ComlibSequenceLength(ctx->queue);
      BEGIN_TIME_LIMITED_WHILE( draining > 0 && ComlibSequenceLength(ctx->queue) > 0, 600000, NULL ) {
        SUSPEND_SLEEP(1000);
        remain = ComlibSequenceLength(ctx->queue);
        if( remain < last ) {
          last = remain;
          draining = 5;
        }
        else {
          --draining;
        }
      } END_TIME_LIMITED_WHILE;
      remain = ComlibSequenceLength(ctx->queue);
    } RELEASE;

    // Failed to drain
    if( remain > 0 ) {
      return -1;
    }

    // Stop thread
    ret = COMLIB_TASK__StopDelete( &ctx->task );

    // Failed to stop
    if( ret < 0 ) {
      return -1;
    }

    cxlog_delete_context_OPEN( context );
  }

  return 0;
}



/******************************************************************************
 *
 *
 ******************************************************************************
 */
DLL_EXPORT int COMLIB__OpenLog( const char *filepath, FILE **rfile, int *rfd, CString_t **CSTR__error ) {
  int ret = 0;

  XTRY {
    if( filepath == NULL || strlen(filepath) > MAX_PATH ) {
      if( CSTR__error && *CSTR__error == NULL ) {
        *CSTR__error = CStringNewFormat( "Invalid filename '%s'", filepath ? filepath : "?" );
      }
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x001 );
    }
    
    // Establish file if not exists
    if( !file_exists( filepath ) ) {
      FILE *tmp = CX_FOPEN( filepath, "w" );
      if( tmp ) {
        CX_FCLOSE( tmp );
      }
      else {
        if( CSTR__error && *CSTR__error == NULL ) {
          *CSTR__error = CStringNewFormat( "Failed to open new writable file '%s': %s", filepath, strerror(errno) );
        }
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x002 );
      }
    }

    // Open file for writing
    if( file_exists( filepath ) ) {
      bool err = false;
      if( rfile ) {
        err = (*rfile = CX_FOPEN( filepath, "ab+" )) == NULL;
      }
      else if( rfd ) {
        err = OPEN_A_SEQ( rfd, filepath ) != 0;
      }
      if( err ) {
        if( CSTR__error && *CSTR__error == NULL ) {
          *CSTR__error = CStringNewFormat( "Could not open file '%s' for writing: %s", filepath, strerror(errno) );
        }
        THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x003 );
      }
    }
    else {
      if( CSTR__error && *CSTR__error == NULL ) {
        *CSTR__error = CStringNewFormat( "No such file: '%s'", filepath );
      }
      THROW_ERROR( CXLIB_ERR_FILESYSTEM, 0x004 );
    }
  }
  XCATCH( errcode ) {
    if( rfile && *rfile ) {
      CX_FCLOSE( *rfile );
      *rfile = NULL;
    }
    else if( rfd && *rfd > 0 ) {
      CX_CLOSE(*rfd);
      *rfd = 0;
    }
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
DLL_EXPORT int COMLIB__LogRotate( LogContext_t *context, const char *filepath, CString_t **CSTR__error ) {
  if( context == NULL || !context->init ) {
    return -1;
  }

  int err = 0;
  SYNCHRONIZE_ON( context->xlock ) {
    // Flush queue and close previous file if any
    cxlog_close_file_XLOCK( context, false );

    // Delete filename
    if( context->CSTR__filename ) {
      COMLIB_OBJECT_DESTROY( context->CSTR__filename );
      context->CSTR__filename = NULL;
    }

    XTRY {
      if( filepath != NULL ) {
        // Set the new filename
        if( (context->CSTR__filename = CStringNew( filepath )) == NULL ) {
          THROW_ERROR( CXLIB_ERR_MEMORY, 0x001 );
        }
        // Open file
        int ret = COMLIB__OpenLog( filepath, NULL, &context->fd, CSTR__error );
        if( context->fd > 0 ) {
          context->sz = CX_SEEK( context->fd, 0, SEEK_END );
        }
        if( ret < 0 ) {
          THROW_ERROR( CXLIB_ERR_GENERAL, 0x002 );
        }
      }
      // No log, drain queue without flushing and set not ready 
      else {
        SYNCHRONIZE_ON( context->qlock ) {
          context->qready = false;
        } RELEASE;
        cxlog_drain_queue_OPEN( context );
      }
    }
    XCATCH( errcode ) {
      COMLIB__LogRotate( context, NULL, CSTR__error );
      err = -1;
    }
    XFINALLY {
    }
  } RELEASE;

  return err;

}



/*******************************************************************//**
 *
 * NOTE: this steals the CSTR__msg instance!
 ***********************************************************************
 */
DLL_EXPORT int COMLIB__Log( LogContext_t *context, int64_t ns_1970, CString_t **CSTR__msg ) {
  if( context == NULL ) {
    return 0; // ignore
  }

  if( !context->init || !context->queue ) {
    return -1;
  }

  // may be NULL if this is the terminate thread message
  // STEAL: cstr instance will be discared by async thread
  uint64_t cstr_addr = CSTR__msg ? (uintptr_t)*CSTR__msg : 0;

  x2tptr_t ts_cstr = {
    .t_1 = {
      .int64 = ns_1970
    },
    .t_2 = {
      .qword = cstr_addr
    }
  };

  // Special case
  if( ts_cstr.t_1.int64 == 0 ) {
    ts_cstr.t_1.int64 = __MILLISECONDS_SINCE_1970() * 1000000LL;
  }

  int ret = 0;
  SYNCHRONIZE_ON( context->qlock ) {
    if( context->qready ) {
      Cx2tptrQueue_t *Q = context->queue;
      ret = CALLABLE(Q)->AppendNolock(Q, &ts_cstr);
      SIGNAL_ALL_CONDITION( &(context->qwake.cond) );
    }
  } RELEASE;

  // STOLEN
  if( ret > 0 && cstr_addr ) {
    *CSTR__msg = NULL;
  }

  return ret;
}

