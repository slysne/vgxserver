###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Endpoints.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

from pytest.pytest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re
import random
import json

graph = None


def Setup():
    # Remove all other graphs from memory
    for graph_name in system.Registry():
        if graph_name != graph.name:
            try:
                system.DeleteGraph( graph_name, timeout=2000 )
            except:
                pass
    # Build a small graph
    graph.Truncate()
    A = graph.NewVertex( "A" )
    B = graph.NewVertex( "B", type="node" )
    C = graph.NewVertex( "C" )
    D = graph.NewVertex( "D" )
    graph.Connect( "A", "to", ["B", "C", "D"] )
    graph.Connect( "B", "connect", ["C", "D"] )
    A.SetProperty( "prop1", 1000 )
    B.SetProperty( "prop2", "one thousand" )
    A.SetVector( [random.random() for n in range(64)] )
    B.SetVector( [random.random() for n in range(64)] )
    C.SetVector( [random.random() for n in range(64)] )



def TEST_vgx_server():
    """
    Core vgx_server
    test_level=801
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vgx_server.c"] )
    except:
        Expect( False )



def TEST_endpoint__vgx_hc():
    """
    /vgx/hc
    test_level=4101
    t_nominal=1
    """

    # Send request
    bytes, headers = Support.send_request( "vgx/hc" )
    Support.assert_headers( headers, bytes )

    # Verify minimal response
    Expect( len(bytes) == 5,        "Small body" )
    Expect( bytes.startswith(b"VGX/") )



def TEST_endpoint__vgx_ping():
    """
    /vgx/ping
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "host": {
            "ip": "172.29.160.1",
            "name": "Sim10",
            "uptime": "0:03:12:59",
            "current_time": "2023-02-24 20:24:41",
            "cpu": "Intel(R) Xeon(R) W-2255 CPU @ 3.70GHz",
            "memory": 137108156416,
            "version": "vgx v3.3"
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/ping" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_time():
    """
    /vgx/time
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "time": {
            "up": "0:03:11:29",
            "start": "2023-02-24 17:11:42",
            "current": "2023-02-24 20:23:11"
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/time" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_graphsum():
    """
    /vgx/graphsum
    test_level=4101
    t_nominal=1
    """
    REF = json.loads(
    """
    {
        "graphsum": {
            "digest": "c1261113054d271e02ffc8c2ee95612d",
            "master-serial": "1689703829703000",
            "names": [null],
            "local-only": [null],
            "order": 4,
            "size": 5,
            "properties": 2,
            "vectors": 3,
            "enumerator": {
                "relationship": 2,
                "vertextype": 2,
                "key": 2,
                "string": 1,
                "dimension": 0
            },
            "vprop": {
                "bytes": 12345,
                "count": 17
            },
            "query": {
                "count": 0,
                "ns-acc": 0
            },
            "engine": {
                "exceptions": {
                    "warning": 0,
                    "error": 0,
                    "critical": 0
                }
            }
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/graphsum" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )

    # Validate values
    graphsum = R['graphsum']

    # digest
    expect_digest = system.Fingerprint()
    digest = graphsum['digest']
    Expect( digest == expect_digest,        "expected digest=%s, got %s" % (expect_digest, digest) )

    # master-serial
    master_serial = graphsum['master-serial']
    Expect( type(master_serial) is str,     "master-serial should be string, got %s" % type(master_serial) )
    try:
        int( master_serial )
    except:
        Expect( False,                      "master-serial should be a string of digits" )

    # names
    names = graphsum['names']
    Expect( type(names) is list and len(names) == 1 and names[0] == graph.name,     "got %s" % names )

    # order
    order = graphsum['order']
    Expect( order == graph.order,           "expected order=%d, got %s" % (graph.order, order) )

    # size
    size = graphsum['size']
    Expect( size == graph.size,             "expected size=%d, got %s" % (graph.order, size) )

    # properties
    expect_nprop = sum( [ len(graph[x].GetProperties()) for x in graph.Vertices() ] )
    nprop = graphsum['properties']
    Expect( nprop == expect_nprop,          "expected properties=%d, got %s" % (expect_nprop, nprop) )

    # vectors
    expect_nvec = sum([ graph[x].HasVector() for x in graph.Vertices() ])
    nvec = graphsum['vectors']
    Expect( nvec == expect_nvec,            "expected vectors=%d, got %s" % (expect_nvec, nvec) )

    # enumerator
    enum = graphsum['enumerator']
    Expect( enum.get('relationship') == len(graph.Relationships()) )
    Expect( enum.get('vertextype') == len(graph.VertexTypes()) )
    Expect( enum.get('key') == len(graph.PropertyKeys()) )
    Expect( enum.get('string') == len(graph.PropertyStringValues()) )
    Expect( enum.get('dimension') == 0 )

    # query
    query = graphsum['query']
    Expect( type(query.get('count')) is int )
    Expect( type(query.get('ns-acc')) is int )

    # engine
    exceptions = graphsum['engine']['exceptions']
    Expect( type(exceptions) is dict )
    Expect( type(exceptions.get('warning')) is int )
    Expect( type(exceptions.get('error')) is int )
    Expect( type(exceptions.get('critical')) is int )



def TEST_endpoint__vgx_status():
    """
    /vgx/status
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "name": "(interactive)",
        "data": {
            "in": 675,
            "out": 0
        },
        "connected_clients": 2,
        "total_clients": 2,
        "request": {
            "rate": 0.00,
            "serving": 1,
            "waiting": 0,
            "working": 0,
            "executors": 8,
            "count": 0,
            "plugin": 0
        },
        "dispatcher": {
          "enabled": true,
          "front-port": 9010,
          "matrix-width": 1,
          "matrix-height": 5,
          "matrix-active-channels": 17,
          "matrix-total-channels": 40,
          "matrix-allow-incomplete": false,
          "matrix-backlog": 0,
          "matrix-backlog-count": 0,
          "proxy": true
        },
        "errors": {
            "service": 0,
            "http": 0,
            "rate": 0.00,
            "codes": {}
        },
        "response_ms": {
            "mean": 0.00,
            "50.0": 0.00,
            "90.0": 0.00,
            "95.0": 0.00,
            "99.0": 0.00,
            "99.9": 0.00
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/status" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_txstat():
    """
    /vgx/txstat
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "data": {
            "bytes": {
                "throttle": 1000.0,
                "in": 0,
                "out": 0
            },
            "rate": {
                "in": 0,
                "out": 0
            },
            "opcodes": {
                "throttle": 1000.0,
                "in": 0,
                "out": 0
            },
            "operations": {
                "throttle": 1000.0,
                "in": 0,
                "out": 0
            },
            "transactions": {
                "throttle": 1000.0,
                "in": 0,
                "out": 0
            }
        },
        "halted": {
            "in": false,
            "out": false
        },
        "input-lag": 0,
        "master-serial": "1689705488578000"
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/txstat" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_peerstat():
    """
    /vgx/peerstat
    test_level=4101
    t_nominal=1
    """

    
    REF = json.loads(
    """
    {
        "name": "(interactive)",
        "host": "somewhere.local",
        "ip": "192.168.1.100",
        "adminport": 9001,
        "port": 0,
        "events": true,
        "events-running": true,
        "readonly": false,
        "durable": false,
        "executing": false,
        "digest": "4eeb257ba9f81f9643f1eb2df7688cde",
        "master-serial": "1689705488578000",
        "idle-ms": 176282,
        "persist-age": "0:00:12:59",
        "persist-ts": 1689704932,
        "persisting": 0,
        "synchronizing": 0,
        "sync-progress": 0,
        "provider": null,
        "provider-digest": "4eeb257ba9f81f9643f1eb2df7688cde",
        "subscribers": [ null ]
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/peerstat" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_meminfo():
    """
    /vgx/meminfo
    test_level=4101
    t_nominal=1
    """
    
    REF = json.loads(
    """
    {
        "memory": {
            "total": 137108156416,
            "current": {
                "available": 50730017873,
                "process": 445661184
            },
            "worst-case": {
                "available": 50730017873,
                "process": 445661184
            }
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/meminfo" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_nodestat():
    """
    /vgx/nodestat
    test_level=4101
    t_nominal=1
    """

    
    REF = json.loads(
    """
    {
        "ready": true,
        "host": "something.local",
        "ip": "172.31.128.1",
        "ips": [ null ],
        "adminport": 9501,
        "txport": 10500,
        "uptime": 1314,
        "pid": 1234,
        "service-name": null,
        "service-label": "cf9af3b43af1839cf0138ca8bd1bc830",
        "digest": "a8a6fa7673b95b16cbe407d1c7a968c7",
        "master-serial": "1690412914493908",
        "idle-ms": 747828,
        "synchronizing": 0,
        "sync-progress": 0,
        "provider": null,
        "subscribers": [ null ],
        "tx-in-halted": false,
        "tx-out-halted": false,
        "tx-in-rate": 17,
        "tx-out-rate": 0,
        "graph-order": 2902761,
        "graph-size": 2786355,
        "graph-properties": 5678,
        "graph-vectors": 2345,
        "graph-nrel": 4,
        "graph-nvtx": 2,
        "graph-ndim": 0,
        "graph-nkey": 4551,
        "graph-nval": 2445,
        "matrix": {
            "mode": "dispatch",
            "width": 5,
            "height": 2,
            "active-channels": 27,
            "total-channels": 160,
            "allow-incomplete": false
        },
        "service-in": 1,
        "service-rate": 0.00,
        "service-95th-ms": 0.00,
        "connected-clients": 24,
        "cpu": "Intel(R) Xeon(R) W-2255 CPU @ 3.70GHz",
        "memory-total": 137108156416,
        "memory-process": 8537407488,
        "memory-available": 24679468154,
        "durable": true,
        "durable-txlog": 20544211,
        "durable-max-txlog": 2147483648,
        "snapshot-writing": false,
        "snapshot-age": 893,
        "readonly": false,
        "local-only": 0
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/nodestat" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_matrix():
    """
    /vgx/matrix
    test_level=4101
    t_nominal=1
    """
    
    REF = json.loads(
    """
    [
        [
          {
            "ip": "127.0.0.1",
            "port": 9020,
            "channels": 8,
            "priority": 1,
            "?primary": true
          },
          null
        ],
        null
    ]
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/matrix" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )



def TEST_endpoint__vgx_dispatch():
    """
    /vgx/dispatch
    test_level=4101
    t_nominal=1
    """

    
    REF = json.loads(
    """
    {
        "A": {
            "dispatch": {
                "dispatched": 1,
                "completed": 1,
                "signals": 1
            },
            "executor": {
                "0": [1, 12.3],
                "1": [1, 12.3],
                "2": [1, 12.3],
                "3": [1, 12.3],
                "4": [1, 12.3],
                "5": [1, 12.3],
                "6": [1, 12.3],
                "7": [1, 12.3],
                "8": [1, 12.3],
                "9": [1, 12.3],
                "10": [1, 12.3],
                "11": [1, 12.3],
                "12": [1, 12.3],
                "13": [1, 12.3],
                "14": [1, 12.3],
                "15": [1, 12.3],
                "16": [1, 12.3],
                "17": [1, 12.3],
                "18": [1, 12.3],
                "19": [1, 12.3],
                "20": [1, 12.3],
                "21": [1, 12.3],
                "22": [1, 12.3],
                "23": [1, 12.3],
                "24": [1, 12.3],
                "25": [1, 12.3],
                "26": [1, 12.3],
                "27": [1, 12.3],
                "28": [1, 12.3],
                "29": [1, 12.3],
                "30": [1, 12.3],
                "31": [1, 12.3]
            },
            "?matrix": {
                "width": 2,
                "height": 2,
                "active-channels": 19,
                "total-channels": 96,
                "allow-incomplete": false,
                "proxy": false,
                "partitions": [
                    [
                        {
                            "host": "10.10.1.115",
                            "port": 9110,
                            "channels": 32,
                            "priority": 10,
                            "?primary": true
                        },
                        null
                    ],
                    null
                ]
            }
        },
        "B": {
            "dispatch": {
                "dispatched": 2,
                "completed": 1,
                "signals": 1
            },
            "executor": {
                "0": [1, 12.3],
                "1": [1, 12.3],
                "2": [1, 12.3],
                "3": [1, 12.3],
                "4": [1, 12.3],
                "5": [1, 12.3],
                "6": [1, 12.3],
                "7": [1, 12.3]
            }
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/dispatch" )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )








def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    Setup()
    RunTests( [__name__] )
    graph.Close()
    del graph
