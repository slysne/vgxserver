/*
###################################################
#
# File:   _vxtest.h
# Author: Stian Lysne
#
###################################################
*/

#ifndef _VGX_VXTEST_H
#define _VGX_VXTEST_H


#include "_vgx.h"

#ifdef INCLUDE_UNIT_TESTS

static test_descriptor_set_t vgx_utest_vxoballoc[] = {
    { "vxoballoc_cstring.c",            _vgx_vxoballoc_cstring_tests },
    { "vxoballoc_vector.c",             _vgx_vxoballoc_vector_tests },
    { "vxoballoc_vertex.c",             _vgx_vxoballoc_vertex_tests },
    { "vxoballoc_graph.c",              _vgx_vxoballoc_graph_tests },
    { NULL }
};
  


static test_descriptor_set_t vgx_utest_vxvertex[] = {
    { "vxvertex_object.c",              _vgx_vxvertex_object_tests },
    { "vxvertex_property.c",            _vgx_vxvertex_property_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxarcvector[] = {
    { "vxarcvector_comparator.c",       _vgx_vxarcvector_comparator_tests },
    { "vxarcvector_filter.c",           _vgx_vxarcvector_filter_tests },
    { "vxarcvector_fhash.c",            _vgx_vxarcvector_fhash_tests },
    { "vxarcvector_cellproc.c",         _vgx_vxarcvector_cellproc_tests },
    { "vxarcvector_traverse.c",         _vgx_vxarcvector_traverse_tests },
    { "vxarcvector_exists.c",           _vgx_vxarcvector_exists_tests },
    { "vxarcvector_delete.c",           _vgx_vxarcvector_delete_tests },
    { "vxarcvector_expire.c",           _vgx_vxarcvector_expire_tests },
    { "vxarcvector_dispatch.c",         _vgx_vxarcvector_dispatch_tests },
    { "vxarcvector_serialization.c",    _vgx_vxarcvector_serialization_tests },
    { "vxarcvector_api.c",              _vgx_vxarcvector_api_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxsim[] = {
    { "vxsim_sim.c",                    _vgx_vxsim_tests },
    { "vxsim_vector.c",                 _vgx_vxsim_vector_tests },
    { "vxsim_lsh.c",                    _vgx_vxsim_lsh_tests },
    { "vxsim_centroid.c",               _vgx_vxsim_centroid_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxio[] = {
    { "vxio_uri.c",                     _vgx_vxio_uri_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxgraph[] = {
    { "vxgraph_mapping.c",              _vgx_vxgraph_mapping_tests },
    { "vxgraph_caching.c",              _vgx_vxgraph_caching_tests },
    { "vxgraph_relation.c",             _vgx_vxgraph_relation_tests },
    { "vxgraph_state.c",                _vgx_vxgraph_state_tests },
    { "vxgraph_vxtable.c",              _vgx_vxgraph_vxtable_tests },
    { "vxgraph_object.c",               _vgx_vxgraph_object_tests },
    { "vxgraph_tracker.c",              _vgx_vxgraph_tracker_tests },
    { "vxgraph_arc.c",                  _vgx_vxgraph_arc_tests },
    { "vxgraph_tick.c",                 _vgx_vxgraph_tick_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxenum[] = {
    { "vxenum.c",                       _vgx_vxenum_tests },
    { "vxenum_rel.c",                   _vgx_vxenum_rel_tests },
    { "vxenum_vtx.c",                   _vgx_vxenum_vtx_tests },
    { "vxenum_dim.c",                   _vgx_vxenum_dim_tests },
    { "vxenum_propkey.c",               _vgx_vxenum_propkey_tests },
    { "vxenum_propval.c",               _vgx_vxenum_propval_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxdurable[] = {
    { "vxdurable_registry.c",                   _vgx_vxdurable_registry_tests },
    { "vxdurable_system.c",                     _vgx_vxdurable_system_tests },
    { "vxdurable_commit.c",                     _vgx_vxdurable_commit_tests },
    { "vxdurable_serialization.c",              _vgx_vxdurable_serialization_tests },
    { "vxdurable_operation.c",                  _vgx_vxdurable_operation_tests },
    { "vxdurable_operation_buffers.c",          _vgx_vxdurable_operation_buffers_tests },
    { "vxdurable_operation_transaction.c",      _vgx_vxdurable_operation_transaction_tests },
    { "vxdurable_operation_capture.c",          _vgx_vxdurable_operation_capture_tests },
    { "vxdurable_operation_emitter.c",          _vgx_vxdurable_operation_emitter_tests },
    { "vxdurable_operation_produce_op.c",       _vgx_vxdurable_operation_produce_op_tests },
    { "vxdurable_operation_parser.c",           _vgx_vxdurable_operation_parser_tests },
    { "vxdurable_operation_consumer_service.c", _vgx_vxdurable_operation_consumer_service_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxevent[] = {
    { "vxevent_eventapi.c",             _vgx_vxevent_eventapi_tests },
    { "vxevent_eventmon.c",             _vgx_vxevent_eventmon_tests },
    { "vxevent_eventexec.c",            _vgx_vxevent_eventexec_tests },
    { NULL }
};


static test_descriptor_set_t vgx_utest_vxeval[] = {
    { "vxeval.c",                       _vgx_vxeval_tests },
    { NULL }
};


    
static test_descriptor_set_t vgx_utest_vxquery[] = {
    { "vxquery_query.c",                _vgx_vxquery_query_tests },
    { "vxquery_probe.c",                _vgx_vxquery_probe_tests },
    { "vxquery_inspect.c",              _vgx_vxquery_inspect_tests },
    { "vxquery_collector.c",            _vgx_vxquery_collector_tests },
    { "vxquery_traverse.c",             _vgx_vxquery_traverse_tests },
    { "vxquery_aggregator.c",           _vgx_vxquery_aggregator_tests },
    { "vxquery_response.c",             _vgx_vxquery_response_tests },
    { "vxquery_rank.c",                 _vgx_vxquery_rank_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vxapi[] = {
    { "vxapi_simple.c",                 _vgx_vxapi_simple_tests },
    { "vxapi_advanced.c",               _vgx_vxapi_advanced_tests },
    { NULL }
};



static test_descriptor_set_t vgx_utest_vgx_server[] = {
    { "vgx_server.c",                   _vgx_server_tests },
    { NULL }
};



typedef struct s_testsgroup_t {
  const char *name;
  const test_descriptor_set_t *tests;
} testgroup_t;



static const testgroup_t test_groups[] = {
  { "vxoballoc",    vgx_utest_vxoballoc },
  { "vxvertex",     vgx_utest_vxvertex },
  { "vxarcvector",  vgx_utest_vxarcvector },
  { "vxsim",        vgx_utest_vxsim },
  { "vxio",         vgx_utest_vxio },
  { "vxgraph",      vgx_utest_vxgraph },
  { "vxenum",       vgx_utest_vxenum },
  { "vxdurable",    vgx_utest_vxdurable },
  { "vxevent",      vgx_utest_vxevent },
  { "vxeval",       vgx_utest_vxeval },
  { "vxquery",      vgx_utest_vxquery },
  { "vxapi",        vgx_utest_vxapi },
  { "vgxserver",    vgx_utest_vgx_server },
  { 0 }
};


#define __TEST_COUNT( Set ) ((sizeof( Set ) / sizeof( test_descriptor_set_t ))-1)


#define __TOTAL_TEST_COUNT (              \
  __TEST_COUNT( vgx_utest_vxoballoc )   + \
  __TEST_COUNT( vgx_utest_vxvertex )    + \
  __TEST_COUNT( vgx_utest_vxarcvector ) + \
  __TEST_COUNT( vgx_utest_vxsim )       + \
  __TEST_COUNT( vgx_utest_vxio )        + \
  __TEST_COUNT( vgx_utest_vxgraph )     + \
  __TEST_COUNT( vgx_utest_vxenum )      + \
  __TEST_COUNT( vgx_utest_vxdurable )   + \
  __TEST_COUNT( vgx_utest_vxevent )     + \
  __TEST_COUNT( vgx_utest_vxeval )      + \
  __TEST_COUNT( vgx_utest_vxquery )     + \
  __TEST_COUNT( vgx_utest_vxapi )       + \
  __TEST_COUNT( vgx_utest_vgx_server )    \
)

#endif

#endif
