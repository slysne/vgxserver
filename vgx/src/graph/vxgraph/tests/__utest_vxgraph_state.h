/*#####################################################################
 *#
 *# __utest_vxgraph_state.h
 *#
 *#
 *######################################################################
 */

#ifndef __UTEST_VXGRAPH_STATE_H
#define __UTEST_VXGRAPH_STATE_H

#include "__vxtest_macro.h"


#define BEGIN_OTHER_OWNER( VertexPtr )                        \
do {                                                          \
  vgx_Vertex_t *__vertex__ = VertexPtr;                       \
  DWORD __threadid__ = GET_CURRENT_THREAD_ID();               \
  /* Pretend to be a different thread and request read */     \
  TEST_ASSERTION( __threadid__ == __vertex_get_writer_threadid( __vertex__ ),      "Writer is current thread" ); \
  /* !!! START INNER BIT MANIPULATION !!! */                  \
  /* Pretend a different thread owns vertex as writable */    \
  __vertex__->descriptor.writer.threadid = __threadid__ + 1;  \
  do

#define END_OTHER_OWNER                                   \
  WHILE_ZERO;                                             \
  /* Restore correct threadid */                          \
  __vertex__->descriptor.writer.threadid = __threadid__;  \
  /* !!! END INNER BIT MANIPULATION !!! */                \
} WHILE_ZERO;

#ifdef CXPLAT_WINDOWS_X64
#pragma optimize( "", off )
#endif

BEGIN_UNIT_TEST( __utest_vxgraph_state ) {

  static const int64_t VERTEX_ONE_OWNER_REFCNT = VXTABLE_VERTEX_REFCOUNT + 1;

  const CString_t *CSTR__graph_path = CStringNew( TestName );
  const CString_t *CSTR__graph_name = CStringNew( "VGX_Graph" );

  TEST_ASSERTION( CSTR__graph_path && CSTR__graph_name, "graph_path and graph_name created" );

  const char *basedir = GetCurrentTestDirectory();
  bool INITIALIZED = __INITIALIZE_GRAPH_FACTORY( basedir, false );


  vgx_Graph_t *graph = NULL;
  vgx_Graph_vtable_t *igraph = NULL;
  const CString_t *CSTR__idA = NULL;
  const CString_t *CSTR__idB = NULL;
  const CString_t *CSTR__idC = NULL;
  const CString_t *CSTR__idD = NULL;
  const CString_t *CSTR__idE = NULL;
  const CString_t *CSTR__idF = NULL;
  const CString_t *CSTR__idG = NULL;



  /*******************************************************************//**
   * CREATE A GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create Graph" ) {
    graph = igraphfactory.NewGraph( CSTR__graph_path, CSTR__graph_name, true, NULL );
    TEST_ASSERTION( graph != NULL,              "Graph constructed, graph=%llp", graph );
    igraph = CALLABLE(graph);
    TEST_ASSERTION( igraph != NULL,             "Graph vtable exists" );

    TEST_ASSERTION( GraphOrder(graph) == 0,  "Graph has no vertices" );
    TEST_ASSERTION( GraphSize(graph) == 0,   "Graph has no edges" );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * DESCRIPTOR BITS
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Descriptor bits" ) {
    // Save original graph acquisition counts before we begin
    int64_t orig_graph_WL_count;
    int64_t orig_graph_RO_count;
    GRAPH_LOCK( graph ) {
      orig_graph_WL_count = _vgx_graph_get_vertex_WL_count_CS( graph );
      orig_graph_RO_count = _vgx_graph_get_vertex_RO_count_CS( graph );
    } GRAPH_RELEASE;

    vgx_VertexDescriptor_t descriptor;
    const CString_t *CSTR__id = icstringobject.New( graph->ephemeral_string_allocator_context, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    CString_t *CSTR__error = NULL;
    vgx_Vertex_t *V;
    vgx_Rank_t rank = vgx_Rank_INIT();
    GRAPH_LOCK( graph ) {
      V = NewVertex_CS( graph, &obid, CSTR__id, VERTEX_TYPE_ENUMERATION_VERTEX, rank, VERTEX_STATE_CONTEXT_MAN_REAL, &CSTR__error );
    } GRAPH_RELEASE;
    DWORD threadid = GET_CURRENT_THREAD_ID();

    // ----------------
    // BIT verification
    // ----------------
    descriptor.bits = 0;
    // writer.threadid
    descriptor.writer.threadid = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000001,                          "writer.threadid = 1" );
    descriptor.writer.threadid = UINT_MAX;
    TEST_ASSERTION( descriptor.bits == 0x00000000ffffffff,                          "writer.threadid = 0xffffffff" );
    descriptor.writer.threadid = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "writer.threadid = 0" );
    // property.index.main
    descriptor.property.index.main = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000000100000000,                          "property.index.main = 1" );
    descriptor.property.index.main = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.index.main = 0" );
    // property.index.type
    descriptor.property.index.type = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000000200000000,                          "property.index.type = 1" );
    descriptor.property.index.type = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.index.type = 0" );
    // property.scope.def
    descriptor.property.scope.def = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000000800000000,                          "property.scope.def = 1" );
    descriptor.property.scope.def = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.scope.def = 0" );
    // property.vector.vec
    descriptor.property.vector.vec = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000001000000000,                          "property.vector.vec = 1" );
    descriptor.property.vector.vec = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.vector.vec = 0" );
    // property.vector.ctr
    descriptor.property.vector.ctr = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000002000000000,                          "property.vector.ctr = 1" );
    descriptor.property.vector.ctr = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.vector.ctr = 0" );
    // property.degree.out
    descriptor.property.degree.out = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000004000000000,                          "property.degree.out = 1" );
    descriptor.property.degree.out = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.degree.out = 0" );
    // property.degree.in
    descriptor.property.degree.in = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000008000000000,                          "property.degree.in = 1" );
    descriptor.property.degree.in = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "property.degree.in = 0" );
    // state.lock.lck
    descriptor.state.lock.lck = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000010000000000,                          "state.lock.lck = 1" );
    descriptor.state.lock.lck = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.lock.lck = 0" );
    // state.lock.rwl
    descriptor.state.lock.rwl = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000020000000000,                          "state.lock.rwl = 1" );
    descriptor.state.lock.rwl = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.lock.rwl = 0" );
    // state.lock.wrq
    descriptor.state.lock.wrq = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000040000000000,                          "state.lock.wrq = 1" );
    descriptor.state.lock.wrq = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.lock.wrq = 0" );
    // state.lock.iny
    descriptor.state.lock.iny = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000080000000000,                          "state.lock.iny = 1" );
    descriptor.state.lock.iny = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.lock.iny = 0" );
    // state.lock.yib
    descriptor.state.lock.yib = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000100000000000,                          "state.lock.yib = 1" );
    descriptor.state.lock.yib = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.lock.yib = 0" );
    // state.context.sus
    descriptor.state.context.sus = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000200000000000,                          "state.context.sus = 1" );
    descriptor.state.context.sus = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.context.sus = 0" );
    // state.context.man
    descriptor.state.context.man = 1;
    TEST_ASSERTION( descriptor.bits == 0x0000400000000000,                          "state.context.man = 1" );
    descriptor.state.context.man = 3;
    TEST_ASSERTION( descriptor.bits == 0x0000c00000000000,                          "state.context.man = 3" );
    descriptor.state.context.man = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "state.context.man = 0" );
    // type.enumeration
    descriptor.type.enumeration = 1;
    TEST_ASSERTION( descriptor.bits == 0x0001000000000000,                          "type.enumeration = 1" );
    descriptor.type.enumeration = 255;
    TEST_ASSERTION( descriptor.bits == 0x00ff000000000000,                          "type.enumeration = 255" );
    descriptor.type.enumeration = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "type.enumeration = 0" );
    // semaphore.count
    descriptor.semaphore.count = 1;
    TEST_ASSERTION( descriptor.bits == 0x0100000000000000,                          "semaphore.count = 1" );
    descriptor.semaphore.count = 127;
    TEST_ASSERTION( descriptor.bits == 0x7f00000000000000,                          "semaphore.count = 127" );
    descriptor.semaphore.count = 0;
    TEST_ASSERTION( descriptor.bits == 0x0000000000000000,                          "semaphore.count = 0" );


    // ---------
    // FUNCTIONS
    // ---------

    // semaphore.count
    V->descriptor.semaphore.count = 0;
    TEST_ASSERTION( __vertex_get_semaphore_count( V ) == 0,                         "semaphore.count=0" );
    TEST_ASSERTION( __vertex_inc_semaphore_count( V ) == 1,                         "semaphore.count=1" );
    TEST_ASSERTION( __vertex_inc_semaphore_count( V ) == 2,                         "semaphore.count=2" );
    TEST_ASSERTION( V->descriptor.semaphore.count == 2,                             "semaphore.count correct bits" );
    TEST_ASSERTION( __vertex_dec_semaphore_count( V ) == 1,                         "semaphore.count=1" );
    TEST_ASSERTION( __vertex_dec_semaphore_count( V ) == 0,                         "semaphore.count=0" );
    TEST_ASSERTION( __vertex_inc_semaphore_count( V ) == 1,                         "semaphore.count=1" );
    __vertex_clear_semaphore_count( V );
    TEST_ASSERTION( __vertex_get_semaphore_count( V ) == 0,                         "semaphore.count=0" );
    TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant( V ),                    "semaphore.count is writer reentrant" );
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity( V ),                    "semaphore.count has reader capacity" );
    TEST_ASSERTION( __vertex_inc_semaphore_count( V ) == 1,                         "semaphore.count=1" );
    TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant( V ),                    "semaphore.count is writer reentrant" );
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity( V ),                    "semaphore.count has reader capacity" );
    TEST_ASSERTION( __vertex_inc_semaphore_count( V ) == 2,                         "semaphore.count=2" );
    TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant( V ),                    "semaphore.count is writer reentrant" );
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity( V ),                    "semaphore.count has reader capacity" );
    V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT-1;
    TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant( V ),                    "semaphore.count is writer reentrant" );
    __vertex_inc_semaphore_count( V );
    TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant( V ) == false,           "semaphore.count is NOT writer reentrant" );
    V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_READERS_LIMIT-1;
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity( V ),                    "semaphore.count has reader capacity" );
    __vertex_inc_semaphore_count( V );
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity( V ) == false,           "semaphore.count has NOT reader capacity" );
    
    // writer.threadid
    V->descriptor.writer.threadid = 0;
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == 0,                         "writer.threadid=0" );
    TEST_ASSERTION( __vertex_is_writer_current_thread( V ) == false,                "writer is not current thread" );
    TEST_ASSERTION( __vertex_set_writer_current_thread( V ) == threadid,            "writer.threadid=current" );
    TEST_ASSERTION( V->descriptor.writer.threadid == threadid,                      "writer.threadid correct bits" );
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "writer.threadid=current" );
    TEST_ASSERTION( __vertex_is_writer_current_thread( V ),                         "writer is current thread" );
    __vertex_clear_writer_thread( V );
    TEST_ASSERTION( V->descriptor.writer.threadid == 0,                             "writer.threadid correct bits" );
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == 0,                         "writer.threadid=0" );
    TEST_ASSERTION( __vertex_is_writer_current_thread( V ) == false,                "writer is not current thread" );

    // state.lock.lck
    V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_OPEN;
    TEST_ASSERTION( __vertex_is_unlocked( V ),                                      "state.lock.lck == OPEN" );
    TEST_ASSERTION( __vertex_is_locked( V ) == false,                               "state.lock.lck != LOCKED" );
    V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
    TEST_ASSERTION( __vertex_is_unlocked( V ) == false,                             "state.lock.lck != OPEN" );
    TEST_ASSERTION( __vertex_is_locked( V ),                                        "state.lock.lck == LOCKED" );
    __vertex_set_unlocked( V );
    TEST_ASSERTION( __vertex_is_unlocked( V ),                                      "state.lock.lck == OPEN" );
    __vertex_set_locked( V );
    TEST_ASSERTION( __vertex_is_locked( V ),                                        "state.lock.lck == LOCKED" );

    // state.lock.rwl
    V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_WRITABLE;
    TEST_ASSERTION( __vertex_is_writable( V ),                                      "state.lock.rwl == WRITABLE" );
    TEST_ASSERTION( __vertex_is_readonly( V ) == false,                             "state.lock.rwl != READONLY" );
    V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
    TEST_ASSERTION( __vertex_is_writable( V ) == false,                             "state.lock.rwl != WRITABLE" );
    TEST_ASSERTION( __vertex_is_readonly( V ),                                      "state.lock.rwl == READONLY" );
    __vertex_set_writable( V );
    TEST_ASSERTION( __vertex_is_writable( V ),                                      "state.lock.rwl == WRITABLE" );
    __vertex_set_readonly( V );
    TEST_ASSERTION( __vertex_is_readonly( V ),                                      "state.lock.rwl == READONLY" );
    __vertex_clear_readonly( V );
    V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_NONE;

    // writable
    GRAPH_LOCK( graph ) {
      V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_OPEN;
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_WRITABLE;
      TEST_ASSERTION( __vertex_is_locked_writable( V ) == false,                      "NOT writable" );
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
      TEST_ASSERTION( __vertex_is_locked_writable( V ) == false,                      "NOT writable" );
      V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
      TEST_ASSERTION( __vertex_is_locked_writable( V ) == false,                      "NOT writable" );
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_WRITABLE;
      TEST_ASSERTION( __vertex_is_locked_writable( V ),                               "WRITABLE" );
      V->descriptor.writer.threadid = 0;
      V->descriptor.semaphore.count = 0;
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ) == false,                     "NOT reentrant" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ) == false,    "NOT locked by current thread" );
      TEST_ASSERTION( __vertex_get_writer_recursion( V ) == 0,                        "No recursion" );
      V->descriptor.writer.threadid = threadid;
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ),                              "Reentrant" );
      TEST_ASSERTION( __vertex_inc_writer_recursion_CS( V ) == 1,                     "writer recursion = 1" );
      TEST_ASSERTION( __vertex_get_writer_recursion( V ) == 1,                        "writer recursion = 1" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ),                              "Reentrant" );
      TEST_ASSERTION( __vertex_inc_writer_recursion_CS( V ) == 2,                     "writer recursion = 2" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ),                              "Reentrant" );
      V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT-1;
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ),                              "Reentrant" );
      TEST_ASSERTION( __vertex_inc_writer_recursion_CS( V ),                          "writer recursion = max" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ) == false,                     "NOT reentrant" );
      TEST_ASSERTION( __vertex_dec_writer_recursion_CS( V ),                          "writer recursion < max" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( V ),             "Locked by current thread" );
      TEST_ASSERTION( __vertex_is_writer_reentrant( V ),                              "Reentrant" );
      V->descriptor.semaphore.count = 1;
      TEST_ASSERTION( __vertex_dec_writer_recursion_CS( V ) == 0,                     "writer recursion = 0" );
      TEST_ASSERTION( __vertex_get_writer_recursion( V ) == 0,                        "writer recursion = 0" );
    } GRAPH_RELEASE;

    
    // readonly
    GRAPH_LOCK( graph ) {
      V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_OPEN;
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
      TEST_ASSERTION( __vertex_is_locked_readonly( V ) == false,                      "NOT readonly" );
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_WRITABLE;
      TEST_ASSERTION( __vertex_is_locked_readonly( V ) == false,                      "NOT readonly" );
      V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
      TEST_ASSERTION( __vertex_is_locked_readonly( V ) == false,                      "NOT readonly" );
      V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
      TEST_ASSERTION( __vertex_is_locked_readonly( V ),                               "READONLY" );
      V->descriptor.semaphore.count = 0;
      TEST_ASSERTION( __vertex_has_reader_capacity( V ),                              "Reader capacity" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 0,                                 "readers = 0" );
      TEST_ASSERTION( __vertex_inc_readers_CS( V ) == 1,                              "readers = 1" );
      TEST_ASSERTION( V->descriptor.semaphore.count == 1,                             "readers = 1" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 1,                                 "readers = 1" );
      TEST_ASSERTION( __vertex_has_reader_capacity( V ),                              "Reader capacity" );
      TEST_ASSERTION( __vertex_inc_readers_CS( V ) == 2,                              "readers = 2" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 2,                                 "readers = 2" );
      TEST_ASSERTION( __vertex_has_reader_capacity( V ),                              "Reader capacity" );
      V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_READERS_LIMIT-1;
      TEST_ASSERTION( __vertex_has_reader_capacity( V ),                              "Reader capacity" );
      TEST_ASSERTION( __vertex_inc_readers_CS( V ),                                   "readers = max" );
      TEST_ASSERTION( __vertex_has_reader_capacity( V ) == false,                     "No reader capacity" );
      TEST_ASSERTION( __vertex_dec_readers_CS( V ),                                   "readers < max" );
      TEST_ASSERTION( __vertex_has_reader_capacity( V ),                              "Reader capacity" );
      V->descriptor.semaphore.count = 1;
      TEST_ASSERTION( __vertex_dec_readers_CS( V ) == 0,                              "readers = 0" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 0,                                 "readers = 0" );
    } GRAPH_RELEASE;


    // state.lock.wrq
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_PENDING;
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write requested" );
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_NONE;
    TEST_ASSERTION( __vertex_is_write_requested( V ) == false,                      "Write NOT requested" );
    V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_LOCKED;
    V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_READONLY;
    TEST_ASSERTION( __vertex_set_write_requested( V ) == V,                         "Write request made for V" );
    TEST_ASSERTION( V->descriptor.state.lock.wrq == VERTEX_STATE_LOCK_WRQ_PENDING,  "Write request pending" );
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write request pending" );
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "Write requested by current thread" );
    TEST_ASSERTION( __vertex_set_write_requested( V ) == NULL,                      "Can't make another write request while already pending" );
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write request pending" );
    V->descriptor.writer.threadid = threadid + 1; // simulate different thread
    TEST_ASSERTION( __vertex_clear_write_requested( V ) == NULL,                    "Can't clear another thread's write request" );
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write request pending" );
    V->descriptor.writer.threadid = threadid; // restore current threadid
    TEST_ASSERTION( __vertex_clear_write_requested( V ) == V,                       "Write request cleared" );
    TEST_ASSERTION( __vertex_is_write_requested( V ) == false,                      "Write NOT requested" );
    TEST_ASSERTION( __vertex_set_write_requested( V ) == V,                         "Write request made for V" );
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write request pending" );
    __vertex_set_writer_thread_or_redeem_write_request( V );
    TEST_ASSERTION( __vertex_is_write_requested( V ) == false,                      "Write NOT requested" );
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "Writer thread set to current thread" );
    TEST_ASSERTION( __vertex_set_write_requested( V ) == V,                         "Write request made for V" );
    TEST_ASSERTION( __vertex_is_write_requested( V ),                               "Write request pending" );
    V->descriptor.writer.threadid = 0;
    __vertex_set_writer_thread_or_redeem_write_request( V );
    TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "Writer thread set to current thread" );
    V->descriptor.state.lock.wrq = VERTEX_STATE_LOCK_WRQ_NONE;
    V->descriptor.writer.threadid = 0;
    V->descriptor.state.lock.lck = VERTEX_STATE_LOCK_LCK_OPEN;
    V->descriptor.state.lock.rwl = VERTEX_STATE_LOCK_RWL_NONE;

    // state.lock.iny
    V->descriptor.state.lock.iny = VERTEX_STATE_LOCK_INY_INARCS_NORMAL;
    TEST_ASSERTION( __vertex_is_inarcs_yielded( V ) == false,                       "Inarcs NOT yielded" );
    TEST_ASSERTION( __vertex_set_yield_inarcs( V ) == V,                            "Yield inarcs" );
    TEST_ASSERTION( __vertex_is_inarcs_yielded( V ),                                "Inarcs YIELDED" );
    TEST_ASSERTION( V->descriptor.state.lock.iny == VERTEX_STATE_LOCK_INY_INARCS_YIELDED, "Inarcs YIELDED" );
    TEST_ASSERTION( __vertex_clear_yield_inarcs( V ) == V,                          "Unyield inarcs" );
    TEST_ASSERTION( __vertex_is_inarcs_yielded( V ) == false,                       "Inarcs NOT yielded" );
    TEST_ASSERTION( V->descriptor.state.lock.iny == VERTEX_STATE_LOCK_INY_INARCS_NORMAL, "Inarcs NOT yielded" );

    // state.lock.yib
    V->descriptor.state.lock.iny = VERTEX_STATE_LOCK_YIB_INARCS_IDLE;
    TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy( V ) == false,                 "Inarcs NOT borrowed" );
    TEST_ASSERTION( __vertex_set_borrowed_inarcs_busy( V ) == V,                    "Borrow inarcs" );
    TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy( V ),                          "Inarcs BORROWED" );
    TEST_ASSERTION( V->descriptor.state.lock.yib == VERTEX_STATE_LOCK_YIB_INARCS_BUSY, "Inarcs BORROWED" );
    TEST_ASSERTION( __vertex_clear_borrowed_inarcs_busy( V ),                       "Return inarcs" );
    TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy( V ) == false,                 "Inarcs NOT borrowed" );
    TEST_ASSERTION( V->descriptor.state.lock.yib == VERTEX_STATE_LOCK_YIB_INARCS_IDLE, "Inarcs NOT borrowed" );

    TEST_ASSERTION( __vertex_is_inarcs_available( V ),                              "Inarcs are available" );
    __vertex_set_locked( V );
    TEST_ASSERTION( __vertex_is_inarcs_available( V ) == false,                     "Inarcs are NOT available because vertex is locked" );
    TEST_ASSERTION( __vertex_set_yield_inarcs( V ) == V,                            "Yield inarcs" );
    TEST_ASSERTION( __vertex_is_inarcs_available( V ),                              "Inarcs are available because they are yielded" );
    TEST_ASSERTION( __vertex_set_borrowed_inarcs_busy( V ) == V,                    "Borrow inarcs" );
    TEST_ASSERTION( __vertex_is_inarcs_available( V ) == false,                     "Inarcs are NOT available because they are borrowed" );
    TEST_ASSERTION( __vertex_clear_borrowed_inarcs_busy( V ),                       "Return inarcs" );
    TEST_ASSERTION( __vertex_clear_yield_inarcs( V ) == V,                          "Unyield inarcs" );
    TEST_ASSERTION( __vertex_is_inarcs_available( V ) == false,                     "Inarcs are NOT available because vertex is locked" );
    __vertex_set_unlocked( V );
    TEST_ASSERTION( __vertex_is_inarcs_available( V ),                              "Inarcs are available" );

    // WRITABLE transitions
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ),                          "Lockable as writable" );
    __vertex_set_locked( V );
    __vertex_set_readonly( V );
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ) == false,                 "NOT lockable as writable because it's readonly" );
    __vertex_set_writable( V );
    V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT;
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ) == false,                 "NOT lockable as writable because reentrancy limit reached" );
    V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT-1;
    __vertex_set_writer_current_thread( V );
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ),                          "Lockable as writable" );
    __vertex_clear_writer_thread( V );
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ) == false,                 "NOT lockable as writable because threadid does not match" );
    V->descriptor.semaphore.count = 0;
    __vertex_set_writer_current_thread( V );
    TEST_ASSERTION( __vertex_is_lockable_as_writable( V ),                          "Lockable as writable" );
    __vertex_set_unlocked( V );

    V->descriptor.writer.threadid = 0;
    GRAPH_LOCK( graph ) {
      TEST_ASSERTION( __vertex_lock_writable_CS( V ) == V,                            "Lock WRITABLE" );
      TEST_ASSERTION( __vertex_is_locked( V ),                                        "LOCKED" );
      TEST_ASSERTION( __vertex_is_writable( V ),                                      "WRITABLE" );
      TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "CURRENT thread" );
      TEST_ASSERTION( __vertex_get_writer_recursion( V ) == 1,                        "recursion = 1" );
      TEST_ASSERTION( __vertex_lock_writable_CS( V ) == V,                            "Lock WRITABLE" );
      TEST_ASSERTION( __vertex_get_writer_recursion( V ) == 2,                        "recursion = 2" );

      TEST_ASSERTION( __vertex_unlock_writable_CS( V ) == 1,                          "Unlock WRITABLE, recursion = 1" );
      TEST_ASSERTION( __vertex_is_locked( V ),                                        "LOCKED" );
      TEST_ASSERTION( __vertex_is_writable( V ),                                      "WRITABLE" );
      TEST_ASSERTION( __vertex_get_writer_threadid( V ) == threadid,                  "CURRENT thread" );
      TEST_ASSERTION( __vertex_unlock_writable_CS( V ) == 0,                          "Unlock WRITABLE, recursion = 0" );
      TEST_ASSERTION( __vertex_is_unlocked( V ),                                      "UNLOCKED" );
      TEST_ASSERTION( __vertex_get_writer_threadid( V ) == 0,                         "NO thread" );

      //READONLY transitions
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ),                          "Lockable as readonly" );
      __vertex_set_locked( V );
      __vertex_set_readonly( V );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ),                          "Lockable as readonly" );
      __vertex_set_writable( V );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ) == false,                 "NOT lockable as readonly" );
      __vertex_set_readonly( V );
      V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_READERS_LIMIT-1;
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ),                          "Lockable as readonly" );
      TEST_ASSERTION( __vertex_inc_readers_CS( V ),                                   "readers = max" );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ) == false,                 "NOT lockable as readonly" );
      V->descriptor.semaphore.count = 0;
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ),                          "Lockable as readonly" );
      TEST_ASSERTION( __vertex_set_write_requested( V ) == V,                         "Write request made for V" );

      // NOTE: WE HAVE DISABLED THE WRQ FEATURE UNTIL WE IMPLEMENT IT CORRECTLY
      //TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ) == false,                 "NOT lockable as readonly" );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly( V ) == true,                  "Lockable as readonly" );


      TEST_ASSERTION( __vertex_clear_write_requested( V ) == V,                       "Write request cleared" );
      __vertex_set_unlocked( V );
      __vertex_set_writable( V );

      TEST_ASSERTION( __vertex_lock_readonly_CS( V ) == V,                            "Lock READONLY" );
      TEST_ASSERTION( __vertex_is_locked( V ),                                        "LOCKED" );
      TEST_ASSERTION( __vertex_is_readonly( V ),                                      "READONLY" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 1,                                 "readers = 1" );
      TEST_ASSERTION( __vertex_lock_readonly_CS( V ) == V,                            "Lock READONLY" );
      TEST_ASSERTION( __vertex_is_locked( V ),                                        "LOCKED" );
      TEST_ASSERTION( __vertex_is_readonly( V ),                                      "READONLY" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 2,                                 "readers = 2" );
      V->descriptor.semaphore.count = VERTEX_SEMAPHORE_COUNT_READERS_LIMIT-1;
      TEST_ASSERTION( __vertex_lock_readonly_CS( V ) == V,                            "Lock READONLY" );
      TEST_ASSERTION( __vertex_is_locked( V ),                                        "LOCKED" );
      TEST_ASSERTION( __vertex_is_readonly( V ),                                      "READONLY" );
      TEST_ASSERTION( __vertex_get_readers( V ) == VERTEX_SEMAPHORE_COUNT_READERS_LIMIT, "readers = max" );

      TEST_ASSERTION( __vertex_unlock_readonly_CS( V ),                               "Unlock READONLY, more readers" );
      TEST_ASSERTION( __vertex_get_readers( V ) == VERTEX_SEMAPHORE_COUNT_READERS_LIMIT - 1, "readers < max" );
      V->descriptor.semaphore.count = 1;
      TEST_ASSERTION( __vertex_get_readers( V ) == 1,                                 "readers = 1" );
      TEST_ASSERTION( __vertex_unlock_readonly_CS( V ) == 0,                          "Unlock READONLY" );
      TEST_ASSERTION( __vertex_is_unlocked( V ),                                      "UNLOCKED" );
      TEST_ASSERTION( __vertex_is_readonly( V ) == false,                             "NOT readonly" );
      TEST_ASSERTION( __vertex_get_readers( V ) == 0,                                 "readers = 0" );

      // ESCALATION transitions
      TEST_ASSERTION( __vertex_is_readonly_lockable_as_writable( V ) == false,        "Escalation not possible" );
      TEST_ASSERTION( __vertex_lock_readonly_CS( V ) == V,                            "Lock READONLY" );
      TEST_ASSERTION( __vertex_is_readonly_lockable_as_writable( V ),                 "Escalation possible when single reader" );
      TEST_ASSERTION( __vertex_lock_readonly_CS( V ) == V,                            "Lock READONLY, recursion = 2" );
      TEST_ASSERTION( __vertex_is_readonly_lockable_as_writable( V ) == false,        "Escalation not possible with multiple readers" );
      TEST_ASSERTION( __vertex_unlock_readonly_CS( V ) == 1,                          "Unlock READONLY" );
      TEST_ASSERTION( __vertex_set_write_requested( V ) == V,                         "Write request made for V" );
      TEST_ASSERTION( __vertex_is_readonly_lockable_as_writable( V ),                 "Escalation possible when write requested by current thread" );
      V->descriptor.writer.threadid = threadid + 1;
      TEST_ASSERTION( __vertex_is_readonly_lockable_as_writable( V ) == false,        "Escalation not possible when write requested by different thread" );
      V->descriptor.writer.threadid = threadid;
      TEST_ASSERTION( __vertex_clear_write_requested( V ) == V,                       "Remove write request" );
      TEST_ASSERTION( __vertex_unlock_readonly_CS( V ) == 0,                          "Unlock READONLY" );
    } GRAPH_RELEASE;
    
    // state.context.sus
    __vertex_set_suspended_context( V );
    TEST_ASSERTION( V->descriptor.state.context.sus == VERTEX_STATE_CONTEXT_SUS_SUSPENDED,  "context sus bit ok" );
    TEST_ASSERTION( __vertex_is_active_context( V ) == false,                               "context NOT active" );
    TEST_ASSERTION( __vertex_is_suspended_context( V ),                                     "context SUSPENDED" );
    __vertex_set_active_context( V );
    TEST_ASSERTION( V->descriptor.state.context.sus == VERTEX_STATE_CONTEXT_SUS_ACTIVE,     "context sus bit ok" );
    TEST_ASSERTION( __vertex_is_active_context( V ),                                        "context ACTIVE" );
    TEST_ASSERTION( __vertex_is_suspended_context( V ) == false,                            "context NOT suspended" );

    // state.context.man
    __vertex_set_manifestation_null( V );
    TEST_ASSERTION( V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_NULL,     "manifestation NULL" );
    TEST_ASSERTION( __vertex_get_manifestation( V ) == VERTEX_STATE_CONTEXT_MAN_NULL,     "manifestation NULL" );
    TEST_ASSERTION( __vertex_is_manifestation_null( V ),                                  "manifestation NULL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( V ) == false,                         "manifestation NOT real" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( V ) == false,                      "manifestation NOT virtual" );
    __vertex_set_manifestation_real( V );
    TEST_ASSERTION( V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_REAL,     "manifestation REAL" );
    TEST_ASSERTION( __vertex_get_manifestation( V ) == VERTEX_STATE_CONTEXT_MAN_REAL,     "manifestation REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_null( V ) == false,                         "manifestation NOT null" );
    TEST_ASSERTION( __vertex_is_manifestation_real( V ),                                  "manifestation REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( V ) == false,                      "manifestation NOT virtual" );
    __vertex_set_manifestation_virtual( V );
    TEST_ASSERTION( V->descriptor.state.context.man == VERTEX_STATE_CONTEXT_MAN_VIRTUAL,  "manifestation VIRTUAL" );
    TEST_ASSERTION( __vertex_get_manifestation( V ) == VERTEX_STATE_CONTEXT_MAN_VIRTUAL,  "manifestation VIRTUAL" );
    TEST_ASSERTION( __vertex_is_manifestation_null( V ) == false,                         "manifestation NOT null" );
    TEST_ASSERTION( __vertex_is_manifestation_real( V ) == false,                         "manifestation NOT real" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( V ),                               "manifestation VIRTUAL" );

    // property.scope.def
    V->descriptor.property.scope.def = VERTEX_PROPERTY_SCOPE_LOCAL;
    TEST_ASSERTION( __vertex_scope_is_local( V ),                                         "scope is local" );
    TEST_ASSERTION( !__vertex_scope_is_global( V ),                                       "scope is not global" );
    V->descriptor.property.scope.def = VERTEX_PROPERTY_SCOPE_GLOBAL;
    TEST_ASSERTION( __vertex_scope_is_global( V ),                                        "scope is global" );
    TEST_ASSERTION( !__vertex_scope_is_local( V ),                                        "scope is not local" );
    __vertex_set_scope_local( V );
    TEST_ASSERTION( __vertex_scope_is_local( V ),                                         "scope is local" );
    TEST_ASSERTION( !__vertex_scope_is_global( V ),                                       "scope is not global" );
    __vertex_set_scope_global( V );
    TEST_ASSERTION( __vertex_scope_is_global( V ),                                        "scope is global" );
    TEST_ASSERTION( !__vertex_scope_is_local( V ),                                        "scope is not local" );

    // property.vector.vec
    V->descriptor.property.vector.vec = VERTEX_PROPERTY_VECTOR_VEC_EXISTS;
    TEST_ASSERTION( __vertex_has_vector( V ),                                             "vector exist" );
    V->descriptor.property.vector.vec = VERTEX_PROPERTY_VECTOR_VEC_NONE;
    TEST_ASSERTION( __vertex_has_vector( V ) == false,                                    "no vector" );
    __vertex_set_has_vector( V );
    TEST_ASSERTION( __vertex_has_vector( V ),                                             "vector exist" );
    __vertex_clear_has_vector( V );
    TEST_ASSERTION( __vertex_has_vector( V ) == false,                                    "no vector" );

    // property.vector.ctr
    V->descriptor.property.vector.ctr = VERTEX_PROPERTY_VECTOR_CTR_CENTROID;
    TEST_ASSERTION( __vertex_has_centroid( V ),                                           "has centroid" );
    V->descriptor.property.vector.ctr = VERTEX_PROPERTY_VECTOR_CTR_STANDARD;
    TEST_ASSERTION( __vertex_has_centroid( V ) == false,                                  "no centroid" );
    __vertex_set_has_centroid( V );
    TEST_ASSERTION( __vertex_has_centroid( V ),                                           "has centroid" );
    __vertex_clear_has_centroid( V );
    TEST_ASSERTION( __vertex_has_centroid( V ) == false,                                  "no centroid" );

    // property.degree
    V->descriptor.property.degree.in = VERTEX_PROPERTY_DEGREE_IN_ZERO;
    V->descriptor.property.degree.out = VERTEX_PROPERTY_DEGREE_OUT_ZERO;
    TEST_ASSERTION( __vertex_has_inarcs( V ) == false,                                      "no inarcs" );
    TEST_ASSERTION( __vertex_has_outarcs( V ) == false,                                     "no outarcs" );
    __vertex_set_has_inarcs( V );
    TEST_ASSERTION( V->descriptor.property.degree.in == VERTEX_PROPERTY_DEGREE_IN_NONZERO,  "has inarcs" );
    TEST_ASSERTION( __vertex_has_inarcs( V ),                                               "has inarcs" );
    TEST_ASSERTION( V->descriptor.property.degree.out == VERTEX_PROPERTY_DEGREE_OUT_ZERO,   "no outarcs" );
    TEST_ASSERTION( __vertex_has_outarcs( V ) == false,                                     "no outarcs" );
    __vertex_clear_has_inarcs( V );
    __vertex_set_has_outarcs( V );
    TEST_ASSERTION( V->descriptor.property.degree.in == VERTEX_PROPERTY_DEGREE_IN_ZERO,     "no inarcs" );
    TEST_ASSERTION( __vertex_has_inarcs( V ) == false,                                      "no inarcs" );
    TEST_ASSERTION( V->descriptor.property.degree.out == VERTEX_PROPERTY_DEGREE_OUT_NONZERO,"has outarcs" );
    TEST_ASSERTION( __vertex_has_outarcs( V ),                                              "has outarcs" );
    __vertex_set_has_inarcs( V );
    TEST_ASSERTION( V->descriptor.property.degree.in == VERTEX_PROPERTY_DEGREE_IN_NONZERO,  "has inarcs" );
    TEST_ASSERTION( __vertex_has_inarcs( V ),                                               "has inarcs" );
    TEST_ASSERTION( V->descriptor.property.degree.out == VERTEX_PROPERTY_DEGREE_OUT_NONZERO,"has outarcs" );
    TEST_ASSERTION( __vertex_has_outarcs( V ),                                              "has outarcs" );

    TEST_ASSERTION( __vertex_is_isolated( V ) == false,                                     "NOT isolated" );
    TEST_ASSERTION( __vertex_is_source( V ) == false,                                       "NOT source" );
    TEST_ASSERTION( __vertex_is_sink( V ) == false,                                         "NOT sink" );
    TEST_ASSERTION( __vertex_is_internal( V ),                                              "INTERNAL" );
    __vertex_clear_has_outarcs( V );
    TEST_ASSERTION( __vertex_is_isolated( V ) == false,                                     "NOT isolated" );
    TEST_ASSERTION( __vertex_is_source( V ) == false,                                       "NOT source" );
    TEST_ASSERTION( __vertex_is_sink( V ),                                                  "SINK" );
    TEST_ASSERTION( __vertex_is_internal( V ) == false,                                     "NOT internal" );
    __vertex_clear_has_inarcs( V );
    __vertex_set_has_outarcs( V );
    TEST_ASSERTION( __vertex_is_isolated( V ) == false,                                     "NOT isolated" );
    TEST_ASSERTION( __vertex_is_source( V ),                                                "SOURCE" );
    TEST_ASSERTION( __vertex_is_sink( V ) == false,                                         "NOT sink" );
    TEST_ASSERTION( __vertex_is_internal( V ) == false,                                     "NOT internal" );
    __vertex_clear_has_outarcs( V );
    TEST_ASSERTION( __vertex_is_isolated( V ),                                              "ISOLATED" );
    TEST_ASSERTION( __vertex_is_source( V ) == false,                                       "NOT source" );
    TEST_ASSERTION( __vertex_is_sink( V ) == false,                                         "NOT sink" );
    TEST_ASSERTION( __vertex_is_internal( V ) == false,                                     "NOT internal" );

    // type.enumeration
    V->descriptor.type.enumeration = VERTEX_TYPE_ENUMERATION_NONE;
    TEST_ASSERTION( __vertex_get_type( V ) == VERTEX_TYPE_ENUMERATION_NONE,                 "type NONE" );
    TEST_ASSERTION( __vertex_set_type( V, 1 ) == 1,                                         "set type 1" );
    TEST_ASSERTION( V->descriptor.type.enumeration == 1,                                    "type 1" );
    TEST_ASSERTION( __vertex_get_type( V ) == 1,                                            "get type 1" );
    TEST_ASSERTION( __vertex_set_type( V, 2 ) == 2,                                         "set type 2" );
    TEST_ASSERTION( V->descriptor.type.enumeration == 2,                                    "type 2" );
    TEST_ASSERTION( __vertex_get_type( V ) == 2,                                            "get type 2" );
    TEST_ASSERTION( __vertex_set_type( V, 255 ) == 255,                                     "set type 255" );
    TEST_ASSERTION( V->descriptor.type.enumeration == 255,                                  "type 255" );
    TEST_ASSERTION( __vertex_get_type( V ) == 255,                                          "get type 255" );
    __vertex_clear_type( V );
    TEST_ASSERTION( __vertex_get_type( V ) == VERTEX_TYPE_ENUMERATION_NONE,                 "type NONE" );

    // Destroy vertex
    COMLIB_OBJECT_DESTROY( V );

    CStringDelete( CSTR__id );

    // Restore graph acquisition counts
    GRAPH_LOCK( graph ) {
      int64_t delta_WL_count = orig_graph_WL_count - _vgx_graph_get_vertex_WL_count_CS( graph );
      int64_t delta_RO_count = orig_graph_RO_count - _vgx_graph_get_vertex_RO_count_CS( graph );
      _vgx_graph_inc_vertex_WL_count_delta_CS( graph, (int8_t)delta_WL_count );
      _vgx_graph_inc_vertex_RO_count_delta_CS( graph, (int8_t)delta_RO_count );
    } GRAPH_RELEASE;
    

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * TIME LIMITED WHILE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Time limited while" ) {
    int64_t n=0;
    int64_t N=1000000000000000;
    int timeout_ms = 800;
    uint64_t ts0, ts1;

    // Short time limit
    ts0 = __GET_CURRENT_MILLISECOND_TICK();
    BEGIN_TIME_LIMITED_WHILE( n < N, timeout_ms, NULL ) {
      ++n;
    } END_TIME_LIMITED_WHILE;
    ts1 = __GET_CURRENT_MILLISECOND_TICK();

    TEST_ASSERTION( n > 1000 && n < N,                        "n has been significantly incremented, got %lld", n );
    TEST_ASSERTION( (double)(ts1-ts0) / timeout_ms < 1.1,       "loop duration not too long" );
    TEST_ASSERTION( (double)(ts1-ts0) / timeout_ms > 0.9,       "loop duration not too short" );

    // Immediate timeout
    n = 0;
    BEGIN_TIME_LIMITED_WHILE( n < N, 0, NULL ) {
      ++n;
    } END_TIME_LIMITED_WHILE;
    TEST_ASSERTION( n == 1,                                   "loop exactly once, got %lld", n );

    // No timeout
    n = 0;
    BEGIN_TIME_LIMITED_WHILE( n < 1000, -1, NULL ) {
      ++n;
    } END_TIME_LIMITED_WHILE;
    TEST_ASSERTION( n == 1000,                                "loop until end condition, got %lld", n );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE VERTEX AND CHECK STATE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create vertex and check state" ) {
    int64_t n=0;
    const CString_t *CSTR__id = icstringobject.New( graph->ephemeral_string_allocator_context, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;
    DWORD threadid = GET_CURRENT_THREAD_ID();

    TEST_ASSERTION( GraphOrder(graph) == 0,                        "Graph has no vertices" );

    GRAPH_LOCK( graph ) {
      // NULL
      A = __create_and_own_vertex_writable_CS( graph, CSTR__id, &obid, VERTEX_TYPE_ENUMERATION_VERTEX, VERTEX_STATE_CONTEXT_MAN_NULL, false, NULL );
      TEST_ASSERTION( A == NULL,                                        "NULL vertex not created" );
      TEST_ASSERTION( GraphOrder(graph) == 0,                        "Graph has no vertices" );

      // REAL
      A = __create_and_own_vertex_writable_CS( graph, CSTR__id, &obid, VERTEX_TYPE_ENUMERATION_VERTEX, VERTEX_STATE_CONTEXT_MAN_REAL, false, NULL );
      TEST_ASSERTION( A != NULL,                                        "Vertex A created as REAL" );
      TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has 1 vertex (A)" );
      // Check ID
      TEST_ASSERTION( strncmp( CALLABLE(A)->IDPrefix(A), "A", 2 ) == 0,  "Vertex A ID OK" );
      TEST_ASSERTION( idmatch( COMLIB_OBJECT_GETID(A), &obid ),         "Vertex A OBID OK" );
      // Check refcount
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,   "Vertex has many owners (index and us)" );
      // Check state lock flags
      TEST_ASSERTION( __vertex_get_writer_threadid(A) == threadid,      "Current threadid match" );
      TEST_ASSERTION( __vertex_is_writer_current_thread(A),             "Current thread is writer" );
      TEST_ASSERTION( __vertex_is_locked(A),                            "Vertex is LOCKED" );
      TEST_ASSERTION( __vertex_is_unlocked(A) == false,                 "Vertex is NOT unlocked" );
      TEST_ASSERTION( __vertex_is_writable(A),                          "Vertex is WRITABLE" );
      TEST_ASSERTION( __vertex_is_readonly(A) == false,                 "Vertex is NOT readonly" );
      TEST_ASSERTION( __vertex_is_locked_writable(A),                   "Vertex is LOCKED WRITABLE" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A), "Vertex is LOCKED WRITABLE by CURRENT THREAD" );
      TEST_ASSERTION( __vertex_get_semaphore_count(A) == 1,             "Vertex semaphore count is 1" );
      TEST_ASSERTION( __vertex_is_semaphore_writer_reentrant(A),        "Vertex write lock is REENTRANT" );
      TEST_ASSERTION( __vertex_is_writer_reentrant(A),                  "Vertex write lock is REENTRANT" );
      TEST_ASSERTION( __vertex_is_locked_readonly(A) == false,          "Vertex is NOT locked readonly" );
      TEST_ASSERTION( __vertex_is_write_requested(A) == false,          "Vertex write is not requested (we already hold the lock)" );
      TEST_ASSERTION( __vertex_is_inarcs_yielded(A) == false,           "Vertex inarcs NOT yielded" );
      TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy(A) == false,     "Vertex inarcs NOT borrowed" );
      TEST_ASSERTION( __vertex_is_inarcs_available(A) == false,         "Vertex inarcs NOT available" );
      TEST_ASSERTION( __vertex_is_lockable_as_writable(A),              "Vertex is lockable as writable" );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly(A) == false,     "Vertex is NOT lockable as readonly" );
      // Re-acquire once
      TEST_ASSERTION( (Ax = __vertex_lock_writable_CS(A)) == A,         "Vertex write lock re-acquired" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A), "Vertex is LOCKED WRITABLE by CURRENT THREAD" );
      TEST_ASSERTION( __vertex_get_semaphore_count(A) == 2,             "Vertex semaphore count is 2" );
      TEST_ASSERTION( __vertex_is_writer_reentrant(A),                  "Vertex write lock is REENTRANT" );
      // Re-acquire until limit
      while( __vertex_is_lockable_as_writable(A) ) {
        TEST_ASSERTION( (Ax = __vertex_lock_writable_CS(A)) == A,       "Vertex write lock re-acquired" );
      }
      // Check recursion count
      TEST_ASSERTION( __vertex_get_semaphore_count(A) == VERTEX_SEMAPHORE_COUNT_REENTRANCY_LIMIT,  "Vertex recursion limit reached" );
      // Release all recursive locks
      while( __vertex_get_semaphore_count(A) > 1 ) {
        TEST_ASSERTION( __vertex_unlock_writable_CS(A) == __vertex_get_semaphore_count(A), "Vertex recursive lock released" );
      }
      TEST_ASSERTION( __vertex_get_semaphore_count(A) == 1,             "Vertex semaphore count is 1" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A), "Vertex is LOCKED WRITABLE by CURRENT THREAD" );
      
      // Check vertex type enumeration 
      TEST_ASSERTION( __vertex_get_type(A) == VERTEX_TYPE_ENUMERATION_VERTEX, "Vertex has default type" );

      // Check context
      TEST_ASSERTION( __vertex_is_manifestation_real(A),                "Vertex is REAL" );
      TEST_ASSERTION( __vertex_is_manifestation_virtual(A) == false,    "Vertex is NOT virtual" );
      TEST_ASSERTION( __vertex_is_suspended_context(A),                 "Vertex is in the SUSPENDED context" );
      TEST_ASSERTION( __vertex_is_active_context(A) == false,           "Vertex is NOT in the active context" );

      // Check properties
      TEST_ASSERTION( __vertex_has_inarcs(A) == false,                  "Vertex has no inarcs" );
      TEST_ASSERTION( __vertex_has_outarcs(A) == false,                 "Vertex has no outarcs" );
      TEST_ASSERTION( __vertex_is_isolated(A),                          "Vertex is ISOLATED" );
      TEST_ASSERTION( __vertex_is_sink(A) == false,                     "Vertex is NOT sink" );
      TEST_ASSERTION( __vertex_is_source(A) == false,                   "Vertex is NOT source" );
      TEST_ASSERTION( __vertex_is_internal(A) == false,                 "Vertex is NOT internal" );
      TEST_ASSERTION( __vertex_has_vector(A) == false,                  "Vertex has no vector" );
      TEST_ASSERTION( __vertex_has_centroid(A) == false,                "Vertex has no centroid" );
      TEST_ASSERTION( __vertex_scope_is_global(A),                      "Vertex scope is global" );

      // Release last lock
      TEST_ASSERTION( __vertex_unlock_writable_CS(A) == 0,              "Vertex lock fully released" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );
      Vertex_DECREF_WL( A );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex has one owner (the index)" );

      // Check state lock flags after release
      TEST_ASSERTION( __vertex_is_writer_current_thread(A) == false,    "Current thread does not own vertex" );
      TEST_ASSERTION( __vertex_is_locked(A) == false,                   "Vertex is NOT locked" );
      TEST_ASSERTION( __vertex_is_unlocked(A),                          "Vertex is UNLOCKED" );
      TEST_ASSERTION( __vertex_is_readonly(A) == false,                 "Vertex is NOT readonly" );
      TEST_ASSERTION( __vertex_is_locked_writable(A) == false,          "Vertex is NOT locked writable" );
      TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A) == false, "Vertex is NOT locked writable by current thread" );
      TEST_ASSERTION( __vertex_get_semaphore_count(A) == 0,             "Vertex semaphore count is 0" );
      TEST_ASSERTION( __vertex_is_writer_reentrant(A) == false,         "Vertex writer is NOT reentrant" );
      TEST_ASSERTION( __vertex_is_locked_readonly(A) == false,          "Vertex is NOT locked readonly" );
      TEST_ASSERTION( __vertex_is_write_requested(A) == false,          "Vertex write is not requested (we already hold the lock)" );
      TEST_ASSERTION( __vertex_is_inarcs_yielded(A) == false,           "Vertex inarcs NOT yielded" );
      TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy(A) == false,     "Vertex inarcs NOT borrowed" );
      TEST_ASSERTION( __vertex_is_inarcs_available(A),                  "Vertex inarcs AVAILABLE" );
      TEST_ASSERTION( __vertex_is_lockable_as_writable(A),              "Vertex is lockable as writable" );
      TEST_ASSERTION( __vertex_is_lockable_as_readonly(A),              "Vertex is lockable as readonly" );

      // Remove vertex from graph to get back to clean slate
      TEST_ASSERTION( _vxgraph_vxtable__unindex_vertex_CS_WL( graph, A ) == VXTABLE_VERTEX_REFCOUNT, "Vertex removed from graph" );
      A = NULL; // now invalid

      // Check clean slate
      TEST_ASSERTION( GraphOrder(graph) == 0,                        "Graph has no vertices" );

      CStringDelete( CSTR__id );
    } GRAPH_RELEASE;

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * OPEN AND PERSIST VERTEX
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Open and persist vertex" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;

    TEST_ASSERTION( GraphOrder(graph) == 0,                        "Graph has no vertices" );

    // Open vertex A
    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    A = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( A != NULL,                                          "Vertex A opened" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),                "Vertex is REAL" );
    TEST_ASSERTION( __vertex_is_suspended_context( A ),                 "Vertex is in the SUSPENDED context" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,     "Vertex has two owners (the index and us)" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                          "Graph has 1 vertex (A)" );

    // Close vertex A
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax ),  "Vertex A released and committed" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                              "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_active_context( A ),                        "Vertex is in the ACTIVE context" );
    TEST_ASSERTION( __vertex_get_writer_threadid( A ) == VERTEX_WRITER_THREADID_NONE, "No writer owns vertex" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,         "Vertex has one owner (the index)" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                              "Graph has 1 vertex (A)" );

    // Re-open A
    _vgx_reset_execution_timing_budget( &zero_timeout );
    A = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( A != NULL,                                            "Vertex A opened" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ),   "Vertex locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_active_context( A ),                      "Vertex is in the ACTIVE context" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,       "Vertex has two owners (the index and us)" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                            "Graph has 1 vertex (A)" );

    // Close vertex A
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax ),  "Vertex A released and committed" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                              "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_active_context( A ),                        "Vertex is in the ACTIVE context" );
    TEST_ASSERTION( __vertex_get_writer_threadid( A ) == VERTEX_WRITER_THREADID_NONE, "No writer owns vertex" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,         "Vertex has one owner (the index)" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                              "Graph has 1 vertex (A)" );

    CStringDelete( CSTR__id );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * RE-OPEN VERTEX READONLY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create vertex and check state" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    // Acquire
    _vgx_reset_execution_timing_budget( &zero_timeout );
    A = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &zero_timeout );
    TEST_ASSERTION( A != NULL,                                        "Vertex opened READONLY" );

    // Check ID
    TEST_ASSERTION( strncmp( CALLABLE(A)->IDPrefix(A), "A", 2 ) == 0, "Vertex A ID OK" );
    TEST_ASSERTION( idmatch( COMLIB_OBJECT_GETID(A), &obid ),         "Vertex A OBID OK" );
    // Check refcount
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );
    // Check state lock flags
    TEST_ASSERTION( __vertex_is_writer_current_thread(A) == false,    "Current thread is NOT writer" );
    TEST_ASSERTION( __vertex_is_locked(A),                            "Vertex is LOCKED" );
    TEST_ASSERTION( __vertex_is_unlocked(A) == false,                 "Vertex is NOT unlocked" );
    TEST_ASSERTION( __vertex_is_writable(A) == false,                 "Vertex it NOT writable" );
    TEST_ASSERTION( __vertex_is_readonly(A),                          "Vertex is READONLY" );
    TEST_ASSERTION( __vertex_is_locked_readonly(A),                   "Vertex is LOCKED READONLY" );
    TEST_ASSERTION( __vertex_is_active_context( A ),                  "Vertex is in the ACTIVE context" );
    TEST_ASSERTION( __vertex_get_semaphore_count(A) == 1,             "Vertex semaphore count is 1" );
    TEST_ASSERTION( __vertex_has_semaphore_reader_capacity(A),        "Vertex read lock available for others" );
    TEST_ASSERTION( __vertex_has_reader_capacity(A),                  "Vertex read lock available for others" );
    TEST_ASSERTION( __vertex_is_locked_writable(A) == false,          "Vertex is NOT locked writable" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A) == false, "Vertex is NOT locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_write_requested(A) == false,          "Vertex write is not requested (we already hold the lock)" );
    TEST_ASSERTION( __vertex_is_inarcs_yielded(A) == false,           "Vertex inarcs NOT yielded" );
    TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy(A) == false,     "Vertex inarcs NOT borrowed" );
    TEST_ASSERTION( __vertex_is_inarcs_available(A) == false,         "Vertex inarcs NOT available" );
    TEST_ASSERTION( __vertex_is_lockable_as_readonly(A),              "Vertex is lockable as readonly" );
    TEST_ASSERTION( __vertex_is_lockable_as_writable(A) == false,     "Vertex is NOT lockable as writable" );

    // Recursively re-acquire read lock
    while( __vertex_has_reader_capacity(A) ) {
      _vgx_reset_execution_timing_budget( &zero_timeout );
      Ax = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &zero_timeout );
      TEST_ASSERTION( Ax == A,                                        "Vertex readonly re-acquired" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == __vertex_get_readers(A) + VXTABLE_VERTEX_REFCOUNT, "Vertex owned by all readers plus the index" );
    }
    TEST_ASSERTION( __vertex_get_readers(A) == VERTEX_SEMAPHORE_COUNT_READERS_LIMIT, "Vertex reader count limit reached" );
    TEST_ASSERTION( __vertex_is_lockable_as_readonly(A) == false,     "Vertex is NOT lockable as readonly" );

    // Recursively release read lock
    while( __vertex_get_readers(A) > 1 ) {
      Ax = A;
      _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax );
      TEST_ASSERTION( Ax == NULL && A != NULL,                        "Vertex pointer set to NULL" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == __vertex_get_readers(A) + VXTABLE_VERTEX_REFCOUNT, "Vertex owned by all readers plus the index" );
    }
    TEST_ASSERTION( __vertex_get_readers(A) == 1,                     "Vertex has single reader" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us once)" );

    // Release
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax ), "Vertex released" );
    TEST_ASSERTION( Ax == NULL && A != NULL,                          "Vertex pointer set to NULL" );
    // Check refcount
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex has one owner (the index)" );
    // Check state lock flags
    TEST_ASSERTION( __vertex_is_writer_current_thread(A) == false,    "Current thread is NOT writer" );
    TEST_ASSERTION( __vertex_is_locked(A) == false,                   "Vertex is NOT locked" );
    TEST_ASSERTION( __vertex_is_unlocked(A),                          "Vertex is UNLOCKED" );
    TEST_ASSERTION( __vertex_is_readonly(A) == false,                 "Vertex is NOT readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly(A) == false,          "Vertex is NOT locked readonly" );
    TEST_ASSERTION( __vertex_get_semaphore_count(A) == 0,             "Vertex semaphore count is 0" );
    TEST_ASSERTION( __vertex_is_locked_writable(A) == false,          "Vertex is NOT locked writable" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread(A) == false, "Vertex is NOT locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_write_requested(A) == false,          "Vertex write is not requested (we already hold the lock)" );
    TEST_ASSERTION( __vertex_is_inarcs_yielded(A) == false,           "Vertex inarcs NOT yielded" );
    TEST_ASSERTION( __vertex_is_borrowed_inarcs_busy(A) == false,     "Vertex inarcs NOT borrowed" );
    TEST_ASSERTION( __vertex_is_inarcs_available(A),                  "Vertex inarcs available" );
    TEST_ASSERTION( __vertex_is_lockable_as_readonly(A),              "Vertex is lockable as readonly" );
    TEST_ASSERTION( __vertex_is_lockable_as_writable(A),              "Vertex is lockable as writable" );

    CStringDelete( CSTR__id );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * Ensure exclusive access when writable
   * 
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Ensure exclusive access when writable" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );

    vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_execution_timing_budget( 0, 100 );
    _vgx_start_graph_execution_timing_budget( graph, &short_timeout );


    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    // Acquire writable
    A = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( A != NULL,                                        "Vertex opened WRITABLE" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );

    BEGIN_OTHER_OWNER( A ) {
      // Try (and fail) to acquire it readonly
      _vgx_reset_execution_timing_budget( &short_timeout );
      Ax = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &short_timeout ); // timeout
      TEST_ASSERTION( Ax == NULL,                                       "Vertex NOT acquired as readonly" );
    } END_OTHER_OWNER

    BEGIN_OTHER_OWNER( A ) {
      // Try (and fail) to acquire writable as different thread
      _vgx_reset_execution_timing_budget( &short_timeout );
      Ax = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_REAL, &short_timeout, NULL ); // timeout
      TEST_ASSERTION( Ax == NULL,                                       "Vertex NOT acquired as writable" );
    } END_OTHER_OWNER

    // Try to acquire readonly when already writable
    _vgx_reset_execution_timing_budget( &short_timeout );
    Ax = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &short_timeout ); // timeout
    TEST_ASSERTION( Ax != NULL,                                       "Vertex acquired as readonly" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex has 3 owners (index + writable + readonly)" );

    // Release readonly
    _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax );
    TEST_ASSERTION( Ax == NULL,                                       "Vertex released and pointer set to NULL " );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has 2 owners (index + readonly)" );

    // Release writable
    Ax = A;
    _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax );
    TEST_ASSERTION( Ax == NULL,                                       "Vertex released and pointer set to NULL " );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex has 1 owner (index)" );

    CStringDelete( CSTR__id );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ENSURE WRITER BLOCKED WHEN READONLY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Ensure writer blocked when readonly" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;
    DWORD threadid = GET_CURRENT_THREAD_ID();

    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );

    vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_execution_timing_budget( 0, 100 );
    _vgx_start_graph_execution_timing_budget( graph, &short_timeout );
    
    // Acquire
    A = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &zero_timeout );
    TEST_ASSERTION( A != NULL,                                        "Vertex opened READONLY" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );
 
    // Try (and fail) to acquire writable
    TEST_ASSERTION( __vertex_get_writer_threadid(A) == VERTEX_WRITER_THREADID_NONE, "No write requested yet" );
    Ax = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_REAL, &short_timeout, NULL ); // timeout
    TEST_ASSERTION( Ax == NULL,                                       "Vertex NOT acquired as writable" );
    TEST_ASSERTION( __vertex_is_write_requested(A) == false,          "Vertex request cleared after timeout" );
    TEST_ASSERTION( __vertex_get_writer_threadid(A) == VERTEX_WRITER_THREADID_NONE, "No write requestor registered" );
    
    // Release
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax ), "Vertex released" );
    TEST_ASSERTION( Ax == NULL && A != NULL,                          "Vertex pointer set to NULL" );
    // Check refcount
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex has one owner (the index)" );

    CStringDelete( CSTR__id );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * OPEN AS READONLY THEN ESCALATE TO WRITABLE THEN RELAX
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Open as readonly then escalate to writable then relax" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "A" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *A;
    vgx_Vertex_t *Ax;
    DWORD threadid = GET_CURRENT_THREAD_ID();

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    // Acquire
    A = _vxgraph_state__acquire_readonly_vertex_OPEN( graph, &obid, &zero_timeout );
    TEST_ASSERTION( A != NULL,                                        "Vertex opened READONLY" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );
 
    // Escalate to writable
    _vgx_reset_execution_timing_budget( &zero_timeout );
    Ax = _vxgraph_state__escalate_to_writable_vertex_OPEN_RO( graph, A, &zero_timeout, VGX_VERTEX_RECORD_ALL, NULL );
    TEST_ASSERTION( Ax == A,                                          "Vertex escalated from readonly to writable" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_write_requested( A ) == false,        "No write request pending" );
    TEST_ASSERTION( __vertex_get_writer_threadid(A) == threadid,      "Vertex owned by current thread" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );

    // Relax to readonly
    Ax = _vxgraph_state__relax_to_readonly_vertex_OPEN_LCK( graph, A, VGX_VERTEX_RECORD_ALL );
    TEST_ASSERTION( Ax == A,                                          "Vertex relaxed from writable to readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex is locked readonly" );
    TEST_ASSERTION( __vertex_is_write_requested( A ) == false,        "No write request pending" );
    TEST_ASSERTION( __vertex_get_writer_threadid(A) == VERTEX_WRITER_THREADID_NONE, "No writer and no request pending" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex has two owners (index and us)" );

    // Release
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax ), "Vertex released" );
    TEST_ASSERTION( Ax == NULL && A != NULL,                          "Vertex pointer set to NULL" );
    // Check refcount
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex has one owner (the index)" );

    CStringDelete( CSTR__id );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * CREATE VIRTUAL VERTEX
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Create virtual vertex" ) {
    int64_t n=0;
    const CString_t *CSTR__id = NewEphemeralCString( graph, "B" );
    objectid_t obid = *CStringObid( CSTR__id );
    vgx_Vertex_t *B;
    vgx_Vertex_t *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    // Create vertex B as virtual
    B = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__id, &obid, VERTEX_STATE_CONTEXT_MAN_VIRTUAL, &zero_timeout, NULL );
    TEST_ASSERTION( B != NULL,                                          "Vertex B opened WRITABLE" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_suspended_context( B ),                 "Vertex B is in the SUSPENDED context" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( B ),             "Vertex is VIRTUAL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT,     "Vertex has two owners (index and us)" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                          "Graph has two vertices A and B" );

    // Release virtual vertex
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_vertex_OPEN_LCK( graph, &Bx ), "Vertex released" );
    TEST_ASSERTION( Bx == NULL && B != NULL,                          "Vertex pointer set to NULL" );
    B = NULL; // invalid

    // Check isolated virtual B has been removed from graph
    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex (A)" );

    CStringDelete( CSTR__id );
  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ACQUIRE INITIAL AND TERMINAL WRITABLE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Acquire initial and terminal writable" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    CSTR__idC = NewEphemeralCString( graph, "C" );
    CSTR__idD = NewEphemeralCString( graph, "D" );
    CSTR__idE = NewEphemeralCString( graph, "E" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    objectid_t obidC = *CStringObid( CSTR__idC );
    objectid_t obidD = *CStringObid( CSTR__idD );
    objectid_t obidE = *CStringObid( CSTR__idE );
    vgx_Vertex_t *A = NULL, *B = NULL, *C = NULL, *D = NULL, *E = NULL;
    vgx_Vertex_t *Ax, *Bx, *Cx, *Dx, *Ex;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex" );

    // ---
    // 1: Fail to acquire pair when terminal does not exist and requested terminal default is NULL
    // ---

    // Acquire A -> B     B does not exist and default NULL
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == -1,                                        "Can't acquire B since it doesn't exist and we requested NULL default" );
    TEST_ASSERTION( A == NULL && B == NULL,                           "No vertex pointers" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has one vertex (A)" );

    // ---
    // 2: Acquire pair when terminal does not exist and requested terminal default is VIRTUAL
    // ---
    
    // Acquire A -> B     B does not exist and VIRTUAL default
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_VIRTUAL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT,"Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A is REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_virtual( B ),           "Vertex B is VIRTUAL" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                        "Graph has two vertices (A and B" );

    // Release A -> B
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &B ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && B == NULL,                          "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A still REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 1,                        "Graph has 1 vertex (only A, virtual B was removed)" );

    // ---
    // 3: Acquire pair when terminal does not exist and requested terminal default is REAL
    // ---
    
    // Acquire A -> B     B does not exist and REAL default
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A is REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),              "Vertex B is REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                        "Graph has two vertices (A and B" );

    // Release A -> B
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A still REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),              "Vertex B still REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                        "Graph has 1 vertex (only A, virtual B was removed)" );

    // ---
    // 4: Acquire pair when both initial and terminal exist
    // ---
    
    // Acquire A -> B
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A is REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),              "Vertex B is REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                        "Graph has two vertices (A and B" );

    // Release A -> B
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A still REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( B ),              "Vertex B still REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 2,                        "Graph has 2 vertices (A and B)" );

    // ---
    // 5: Acquire pair when neither vertex exists
    // ---

    // Acquire C -> D
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &C, CSTR__idC, &obidC, &D, CSTR__idD, &obidD, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( C != NULL && D != NULL,                           "Acquired C and D" );
    TEST_ASSERTION( Vertex_REFCNT_WL(C) == VERTEX_ONE_OWNER_REFCNT, "Vertex C has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(D) == VERTEX_ONE_OWNER_REFCNT, "Vertex D has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( C ), "Vertex C is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( D ), "Vertex D is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( C ),              "Vertex C is REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( D ),              "Vertex D is REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 4,                        "Graph has 4 vertices (A, B, C and D)" );

    // Release C -> D
    Cx = C;
    Dx = D;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Cx, &Dx ), "Released two vertices" );
    TEST_ASSERTION( Cx == NULL && Dx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(C) == VXTABLE_VERTEX_REFCOUNT,"Vertex C has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(D) == VXTABLE_VERTEX_REFCOUNT,"Vertex D has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( C ),                        "Vertex C is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( D ),                        "Vertex D is unlocked" );
    TEST_ASSERTION( __vertex_is_manifestation_real( C ),              "Vertex C still REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( D ),              "Vertex D still REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 4,                        "Graph has 4 vertices (A, B, C and D)" );

    // ---
    // 6: Acquire pair when initial does not exist
    // ---

    // Acquire E -> A
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &E, CSTR__idE, &obidE, &A, CSTR__idA, &obidA, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( E != NULL && A != NULL,                           "Acquired E and A" );
    TEST_ASSERTION( Vertex_REFCNT_WL(E) == VERTEX_ONE_OWNER_REFCNT, "Vertex E has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( E ), "Vertex E is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_manifestation_real( E ),              "Vertex E is REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A is REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices (A, B, C, D and E)" );

    // Release E -> A
    Ex = E;
    Ax = A;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ex, &Ax ), "Released two vertices" );
    TEST_ASSERTION( Ex == NULL && Ax == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(E) == VXTABLE_VERTEX_REFCOUNT,"Vertex E has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( E ),                        "Vertex E is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_manifestation_real( E ),              "Vertex E still REAL" );
    TEST_ASSERTION( __vertex_is_manifestation_real( A ),              "Vertex A still REAL" );
    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices (A, B, C, D and E)" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );
    CStringDelete( CSTR__idC );
    CStringDelete( CSTR__idD );
    CStringDelete( CSTR__idE );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ACQUIRE INITIAL AND TERMINAL READONLY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Acquire initial and terminal readonly" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    CSTR__idF = NewEphemeralCString( graph, "F" );
    CSTR__idG = NewEphemeralCString( graph, "G" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    objectid_t obidF = *CStringObid( CSTR__idF );
    objectid_t obidG = *CStringObid( CSTR__idG );
    vgx_Vertex_t *A = NULL, *B = NULL, *F = NULL, *G = NULL;
    vgx_Vertex_t *Ax, *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    // ---
    // 1: Fail to acquire F and G readonly when neither exists
    // ---

    // Acquire F -> G     
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &F, &obidF, &G, &obidG, &zero_timeout );
    TEST_ASSERTION( ret == -1,                                        "Can't acquire since vertices don't exist" );
    TEST_ASSERTION( F == NULL && G == NULL,                           "No vertex pointers" );

    // ---
    // 2: Fail to acquire A and F readonly when F does not exist
    // ---
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &F, &obidF, &zero_timeout );
    TEST_ASSERTION( ret == -1,                                        "Can't acquire pair since F does't exist" );
    TEST_ASSERTION( A == NULL && F == NULL,                           "No vertex pointers" );

    // ---
    // 3: Fail to acquire F and A readonly when F does not exist
    // ---
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &F, &obidF, &A, &obidA, &zero_timeout );
    TEST_ASSERTION( ret == -1,                                        "Can't acquire pair since F does't exist" );
    TEST_ASSERTION( F == NULL && A == NULL,                           "No vertex pointers" );

    // ---
    // 3: Acquire A and B readonly when both exist
    // ---
    // Acquire A -> B
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &B, &obidB, &zero_timeout );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex A is locked readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( B ),                 "Vertex B is locked readonly" );

    // Release A -> B
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );
    CStringDelete( CSTR__idF );
    CStringDelete( CSTR__idG );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * ACQUIRE PAIR WRITABLE RECURSIVELY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Acquire pair writable recursively" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    vgx_Vertex_t *A = NULL, *B = NULL;
    vgx_Vertex_t *Ax, *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    // ---
    // 1: Acquire A and B writable
    // ---

    // Acquire A -> B
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );

    // ---
    // 2: Re-Acquire A and B writable
    // ---

    // Acquire A -> B recursively
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex A has 3 owners (index and us twice)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex B has 3 owners (index and us twice)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );

    // Release A -> B one level
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is still locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is still locked writable by current thread" );

    // ---
    // 3: Release A and B
    // ---

    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );


    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );

  } END_TEST_SCENARIO



  /*******************************************************************//**
   * ACQUIRE PAIR READONLY RECURSIVELY
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Acquire pair readonly recursively" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    vgx_Vertex_t *A = NULL, *B = NULL;
    vgx_Vertex_t *Ax, *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    // ---
    // 1: Acquire A and B readonly
    // ---

    // Acquire A -> B
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &B, &obidB, &zero_timeout );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex A is locked readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( B ),                 "Vertex B is locked readonly" );

    // ---
    // 2: Re-Acquire A and B readonly
    // ---

    // Acquire A -> B recursively
    _vgx_reset_execution_timing_budget( &zero_timeout );
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &B, &obidB, &zero_timeout );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex A has 3 owners (index and us twice)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex B has 3 owners (index and us twice)" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex A is locked readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( B ),                 "Vertex B is locked readonly" );

    // Release A -> B one level
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex A is locked readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( B ),                 "Vertex B is locked readonly" );

    // ---
    // 3: Release A and B
    // ---

    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * FAIL TO ACQUIRE WRITABLE PAIR WHEN BOTH ARE NOT AVAILABLE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Failed to acquire writable pair when both are not available" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    vgx_Vertex_t *A = NULL, *B = NULL;
    vgx_Vertex_t *Ax, *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_execution_timing_budget( 0, 100 );
    _vgx_start_graph_execution_timing_budget( graph, &short_timeout );
    _vgx_reset_execution_timing_budget( &short_timeout );

    // ---
    // 1: Acquire A and B writable
    // ---
    // Acquire A -> B
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );

    // ---
    // 2: Fake thread ID to simulate vertex owned by other thread
    // ---

    // !!! START INNER BIT MANIPULATION !!!
    A->descriptor.writer.threadid = threadid + 1; // Pretend a different thread owns vertex A as writable
    // Try (and fail) to acquire pair when initial is owned by different thread
    Ax = A;
    _vgx_reset_execution_timing_budget( &short_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &Ax, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &short_timeout, NULL );
    TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );

    B->descriptor.writer.threadid = threadid + 2; // Pretend a different thread owns vertex B as writable
    // Try (and fail) to acquire pair when initial is owned by different thread
    Ax = A;
    Bx = B;
    _vgx_reset_execution_timing_budget( &short_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &Ax, CSTR__idA, &obidA, &Bx, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &short_timeout, NULL );
    TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );

    A->descriptor.writer.threadid = threadid; // Restore correct threadid for A
    // Try (and fail) to acquire pair when initial is owned by different thread
    Bx = B;
    _vgx_reset_execution_timing_budget( &short_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &Bx, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &short_timeout, NULL );
    TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );

    B->descriptor.writer.threadid = threadid; // Restore correct threadid for B
    // Successfully re-acquire pair
    _vgx_reset_execution_timing_budget( &short_timeout );
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &short_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex A refcount unchanged" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT + 2, "Vertex B refcount unchanged" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );

    // !!! END INNER BIT MANIPULATION !!!

    // Release A -> B one level
    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is still locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is still locked writable by current thread" );

    // ---
    // 3: Release A and B
    // ---

    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * FAIL TO ACQUIRE READONLY PAIR WHEN BOTH ARE NOT AVAILABLE
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Failed to acquire writable pair when both are not available" ) {
    int64_t n=0;
    CSTR__idA = NewEphemeralCString( graph, "A" );
    CSTR__idB = NewEphemeralCString( graph, "B" );
    objectid_t obidA = *CStringObid( CSTR__idA );
    objectid_t obidB = *CStringObid( CSTR__idB );
    vgx_Vertex_t *A = NULL, *B = NULL;
    vgx_Vertex_t *Ax, *Bx;
    DWORD threadid = GET_CURRENT_THREAD_ID();
    int ret;

    vgx_ExecutionTimingBudget_t zero_timeout = _vgx_get_zero_execution_timing_budget();
    _vgx_start_graph_execution_timing_budget( graph, &zero_timeout );
    _vgx_reset_execution_timing_budget( &zero_timeout );

    vgx_ExecutionTimingBudget_t short_timeout = _vgx_get_execution_timing_budget( 0, 100 );
    _vgx_start_graph_execution_timing_budget( graph, &short_timeout );
    _vgx_reset_execution_timing_budget( &short_timeout );

    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );

    // ---
    // 1: Acquire A and B writable
    // ---

    // Acquire A -> B
    ret = _vxgraph_state__acquire_writable_initial_and_terminal_OPEN( graph, &A, CSTR__idA, &obidA, &B, CSTR__idB, &obidB, VERTEX_STATE_CONTEXT_MAN_NULL, &zero_timeout, NULL );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( A != NULL && B != NULL,                           "Acquired A and B" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( B ), "Vertex B is locked writable by current thread" );

    // ---
    // 2: Ensure readonly pair cannot be acquired
    // ---

    // Try (and fail) to acquire readonly pair when already locked writable
    BEGIN_OTHER_OWNER( A ) {
      Ax = A;
      _vgx_reset_execution_timing_budget( &short_timeout );
      ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &Ax, &obidA, &B, &obidB, &short_timeout );
      TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
      TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );
    } END_OTHER_OWNER

    // Release A and try (and fail) to acquire readonly pair
    Ax = A;
    _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax );
    TEST_ASSERTION( __vertex_is_unlocked(A),                          "Vertex A is unlocked" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has single owner (index)" );

    BEGIN_OTHER_OWNER( B ) {
      Ax = A;
      _vgx_reset_execution_timing_budget( &short_timeout );
      ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &Ax, &obidA, &B, &obidB, &short_timeout );
      TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A refcount unchanged" );
      TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );
    } END_OTHER_OWNER

    // Acquire A writable and release B and try (and fail) to acquire readonly pair
    _vgx_reset_execution_timing_budget( &zero_timeout );
    A = _vxgraph_state__acquire_writable_vertex_OPEN( graph, CSTR__idA, &obidA, VERTEX_STATE_CONTEXT_MAN_REAL, &zero_timeout, NULL );
    TEST_ASSERTION( A != NULL,                                        "Vertex A acquired" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A has two owners (index and us)" );
    TEST_ASSERTION( __vertex_is_locked_writable_by_current_thread( A ), "Vertex A is locked writable by current thread" );
    Bx = B;
    _vxgraph_state__release_vertex_OPEN_LCK( graph, &Bx );
    TEST_ASSERTION( __vertex_is_unlocked(B),                          "Vertex B is unlocked" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has single owner (index)" );

    BEGIN_OTHER_OWNER( A ) {
      Bx = B;
      _vgx_reset_execution_timing_budget( &short_timeout );
      ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &Bx, &obidB, &short_timeout );
      TEST_ASSERTION( ret == -1,                                        "Could not acquire pair" );
      TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
      TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B refcount unchanged" );
    } END_OTHER_OWNER
    
    // Release A and try (and succeed) to acquire readonly pair
    Ax = A;
    _vxgraph_state__release_vertex_OPEN_LCK( graph, &Ax );
    TEST_ASSERTION( __vertex_is_unlocked(A),                          "Vertex A is unlocked" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has single owner (index)" );

    // ---
    // 2: Acquire readonly pair when both are available
    // ---
    _vgx_reset_execution_timing_budget( &short_timeout );
    ret = _vxgraph_state__acquire_readonly_initial_and_terminal_OPEN( graph, &A, &obidA, &B, &obidB, &short_timeout );
    TEST_ASSERTION( ret == 2,                                         "Acquired 2 vertices" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VERTEX_ONE_OWNER_REFCNT, "Vertex A refcount unchanged" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VERTEX_ONE_OWNER_REFCNT, "Vertex B refcount unchanged" );
    TEST_ASSERTION( __vertex_is_locked_readonly( A ),                 "Vertex A is locked readonly" );
    TEST_ASSERTION( __vertex_is_locked_readonly( B ),                 "Vertex B is locked readonly" );

    // ---
    // 3: Release A and B
    // ---

    Ax = A;
    Bx = B;
    TEST_ASSERTION( _vxgraph_state__release_initial_and_terminal_OPEN_LCK( graph, &Ax, &Bx ), "Released two vertices" );
    TEST_ASSERTION( Ax == NULL && Bx == NULL,                         "Vertex pointers set to NULL" );
    TEST_ASSERTION( Vertex_REFCNT_WL(A) == VXTABLE_VERTEX_REFCOUNT,"Vertex A has one owner (index)" );
    TEST_ASSERTION( Vertex_REFCNT_WL(B) == VXTABLE_VERTEX_REFCOUNT,"Vertex B has one owner (index)" );
    TEST_ASSERTION( __vertex_is_unlocked( A ),                        "Vertex A is unlocked" );
    TEST_ASSERTION( __vertex_is_unlocked( B ),                        "Vertex B is unlocked" );

    CStringDelete( CSTR__idA );
    CStringDelete( CSTR__idB );

  } END_TEST_SCENARIO


  /*******************************************************************//**
   * DESTROY GRAPH
   ***********************************************************************
   */
  NEXT_TEST_SCENARIO( true, "Destroy Graph" ) {
    CALLABLE( graph )->advanced->CloseOpenVertices( graph );
    TEST_ASSERTION( GraphOrder(graph) == 5,                        "Graph has 5 vertices" );
    CALLABLE( graph )->simple->Truncate( graph, NULL );
    uint32_t owner;
    TEST_ASSERTION( igraphfactory.CloseGraph( &graph, &owner ) == 0,  "Graph destroyed" );

  } END_TEST_SCENARIO


  __DESTROY_GRAPH_FACTORY( INITIALIZED );

  CStringDelete( CSTR__graph_path );
  CStringDelete( CSTR__graph_name );


} END_UNIT_TEST

#ifdef CXPLAT_WINDOWS_X64
#pragma optimize( "", on )
#endif




#endif
