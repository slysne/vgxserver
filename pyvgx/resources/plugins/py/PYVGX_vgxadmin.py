###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    PYVGX_vgxadmin.py
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

import pyvgx    
import sys
import os
import threading
import http.client
import socket
import json
import time
import random
import re
import getopt
import pprint
from io import StringIO


os.environ[ "PYVGX_NOBANNER" ] = "1"

DEFAULT_TIMEOUT = 90.0

DISPATCH_DEFAULT_CHANNELS = 32
DISPATCH_DEFAULT_PRIORITY = 2


class vgxadmin__InvalidUsageOrConfig( Exception ): pass

class vgxadmin__ServerError( Exception ): pass

class vgxadmin__OperationIncomplete( Exception ): pass

class vgxadmin__AddressError( Exception ): pass




###############################################################################
# sysplugin__TransformLegacyDescriptor
#
###############################################################################
def sysplugin__TransformLegacyDescriptor( descriptor ):
    """
    Transform old style descriptor to new style.
    Best effort only, ingore any syntax problems.
    """
    if type(descriptor) is dict:
        # Transform topology
        T = descriptor.get( "topology" )
        if type(T) is dict:
            if "transaction" not in T and "input" in T:
                T["transaction"] = T.get("input")
                del T["input"]
            if "dispatch" not in T and "search" in T:
                T["dispatch"] = T.get("search")
                del T["search"]
        # Transform graph definitions
        I = descriptor.get( "instances" )
        G = descriptor.get( "graphs" )
        if type(I) is dict and type(G) is list:
            for ident, descr in I.items():
                # ingore malformed instance
                if type(descr) is not dict:
                    continue
                # graph set to all declared graphs by default
                if "graph" not in descr:
                    descr["graph"] = " ".join(G)




###############################################################################
# sysplugin__TransformTransactionTopology
#
###############################################################################
def sysplugin__TransformTransactionTopology( topology ):
    """
    {
        "b1": {
            "t1": [
                "s1",
                "s2",
                "s3"
            ],
            "t2": None
        }
    }
    """
    T = {}
    items = []
    if topology is None:
        pass
    # dict
    elif type( topology ) is dict:
        items = topology.items()
    # list
    elif type( topology ) is list:
        for item in topology:
            if type( item ) is str:
                items.append( (item, {}) )
            elif type( item ) is list and len( item ) == 2:
                items.append( (item[0], item[1]) )
            else:
                raise ValueError( "invalid transaction topology" )
    else:
        raise ValueError( "invalid transaction topology" )
    for id, subT in items:
        T[id] = sysplugin__TransformTransactionTopology( subT )
    return T




###############################################################################
# sysplugin__ValidateSystemDescriptor
#
###############################################################################
def sysplugin__ValidateSystemDescriptor( descriptor ):
    """
    Validate system descriptor
    """
    #
    #  {
    #    "name": "Example Descriptor",
    #
    #    "graphs": ["example"],
    #
    #    "instances": {
    #        "A"    : { "graph": "example", "group":0,  "type": "admin",    "host": "10.10.1.115",   "hport": 9000,                                     "description": "Admin" },
    #        "TD"   : { "graph": "example", "group":0,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9950,                                     "description": "Top Dispatcher" },
    #        "FD"   : { "graph": "example", "group":0,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9910,                                     "description": "Feed Dispatcher" },
    #        "B1"   : { "graph": "example", "group":1,  "type": "builder",  "host": "10.10.1.115",   "hport": 9110,  "tport": 10110,                    "description": "Partition 1 Build" },
    #        "T1"   : { "graph": "example", "group":1,  "type": "txproxy",  "host": "10.10.1.115",   "hport": 9120,  "tport": 10120,  "durable": true,  "description": "Partition 1 TX Proxy" },
    #        "S1.1" : { "graph": "example", "group":1,  "type": "search",   "host": "10.10.1.115",   "hport": 9130,  "tport": 10130,                    "description": "Partition 1 Search 1" },
    #        "S1.2" : { "graph": "example", "group":1,  "type": "search",   "host": "10.10.1.115",   "hport": 9132,  "tport": 10132,                    "description": "Partition 1 Search 2" },
    #        "D1.1" : { "graph": "example", "group":1,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9150,                                     "description": "Partition 1 Dispatcher 1" },
    #        "D1.2" : { "graph": "example", "group":1,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9152,                                     "description": "Partition 1 Dispatcher 2" },
    #        "B2"   : { "graph": "example", "group":2,  "type": "builder",  "host": "10.10.1.115",   "hport": 9210,  "tport": 10210,                    "description": "Partition 2 Build" },
    #        "T2"   : { "graph": "example", "group":2,  "type": "txproxy",  "host": "10.10.1.115",   "hport": 9220,  "tport": 10220,  "durable": true,  "description": "Partition 2 TX Proxy" },
    #        "S2.1" : { "graph": "example", "group":2,  "type": "search",   "host": "10.10.1.115",   "hport": 9230,  "tport": 10230,                    "description": "Partition 2 Search 1" },
    #        "S2.2" : { "graph": "example", "group":2,  "type": "search",   "host": "10.10.1.115",   "hport": 9232,  "tport": 10232,                    "description": "Partition 2 Search 2" },
    #        "D2.1" : { "graph": "example", "group":2,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9250,                                     "description": "Partition 2 Dispatcher 1" },
    #        "D2.2" : { "graph": "example", "group":2,  "type": "dispatch", "host": "10.10.1.115",   "hport": 9252,                                     "description": "Partition 2 Dispatcher 2" }
    #    },
    #
    #    "topology": {
    #        "transaction": {
    #            "A": {},
    #            "B1": { 
    #               "T1": {
    #                   "S1.1": {},
    #                   "S1.2": {}
    #               }
    #            },
    #            "B2": { 
    #               "T2": {
    #                   "S2.1": {},
    #                   "S2.2": {}
    #               }
    #            }
    #        },
    #        "dispatch": {
    #            "TD" : [
    #                { "channels": 32, "priority": 1, "partitions":["D1.1", "D2.1"] },
    #                { "channels": 32, "priority": 1, "partitions":["D1.2", "D2.2"] }
    #            ],
    #            "D1.1" : {
    #                "S1.1": { "channels": 8, "priority": 1   },
    #                "S1.2": { "channels": 8, "priority": 1   },
    #                "T1":   { "channels": 8, "priority": 10  },
    #                "B1":   { "channels": 8, "priority": 100, "primary": 1 }
    #            },
    #            "D1.2" : {
    #                "S1.1": { "channels": 8, "priority": 1   },
    #                "S1.2": { "channels": 8, "priority": 1   },
    #                "T1":   { "channels": 8, "priority": 10  },
    #                "B1":   { "channels": 8, "priority": 100, "primary": 1 }
    #            },
    #            "D2.1" : {
    #                "S2.1": { "channels": 8, "priority": 1   },
    #                "S2.2": { "channels": 8, "priority": 1   },
    #                "T2":   { "channels": 8, "priority": 10  },
    #                "B2":   { "channels": 8, "priority": 100, "primary": 1 }
    #            },
    #            "D2.2" : {
    #                "S2.1": { "channels": 8, "priority": 1   },
    #                "S2.2": { "channels": 8, "priority": 1   },
    #                "T2":   { "channels": 8, "priority": 10  },
    #                "B2":   { "channels": 8, "priority": 100, "primary": 1 }
    #            },
    #            "FD" : [
    #                { "channels": 4, "priority": 1, "partitions":["B1", "B2"] }
    #            ]
    #        }
    #    }
    #  }
    #
    #
    def valerr( attr, val="", msg="" ):
        raise ValueError( 'System descriptor "{}"{} invalid or missing {}'.format( attr, val, msg ) )
    #
    def validate_name( D ):
        if type(D.get("name")) is not str:
            valerr( "name" )
    #
    def validate_graphs( D ):
        if "graphs" not in D:
            D["graphs"] = []
        G = D.get("graphs")
        if type(G) is not list:
            valerr( "graphs" )
        for gname in G:
            if type(gname) is not str:
                valerr( "graphs" )
    #
    INSTANCE_NAMES = set()
    def validate_instances( D ):
        builder_groups = {}
        host_ports = {}
        G = D.get("graphs")
        instances = D.get("instances")
        if type(instances) is not dict:
            valerr( "instances" )
        for id, params in instances.items():
            if id in INSTANCE_NAMES:
                raise ValueError( "System descriptor duplicate instance: {}".format(id) )
            INSTANCE_NAMES.add(id)
            if type(id) is not str:
                valerr( "instances", ": {}".format(id) )
            if type(params) is not dict:
                valerr( "instances.{}".format(id), ": {}" )
            graph = params.get("graph")
            grp = params.get("group")
            tp = params.get("type")
            host = params.get("host")
            hport = params.get("hport")
            prefix = params.get("prefix")
            s_in = params.get("s-in")
            attach = params.get("attach")
            tport = params.get("tport")
            durable = params.get("durable")
            description = params.get("description")
            # graph
            if graph is not None:
                if type(graph) is not str:
                    valerr( "instances.{}.graph".format(id) )
                for gname in graph.split():
                    if gname not in G:
                        valerr( "instances.{}.graph".format(id), msg="({} is undeclared, must be one of {})".format(gname, G) )
            # type
            valid_types = ["admin", "builder", "txproxy", "search", "dispatch", "generic"]
            if tp not in valid_types:
                valerr( "instances.{}.type".format(id), msg="(must be one of {})".format(valid_types) )
            # group
            if grp is not None:
                if type(grp) not in [int, float]:
                    valerr( "instances.{}.group".format(id), msg="(numeric value required)" )
                if tp == "builder" and grp >= 0:
                    if grp in builder_groups:
                        valerr( "instances.{}.group".format(id), msg="(duplicate builder group {})".format(grp) )
                    builder_groups[grp] = id
            # host
            if type(host) is not str:
                valerr( "instances.{}.host".format(id) )
            if host not in host_ports:
                host_ports[host] = set()
            # hport
            if type(hport) is not int or hport < 1:
                valerr( "instances.{}.hport".format(id), msg="(positive integer required)" )
            if hport in host_ports[host]:
                valerr( "instances.{}.hport".format(id), msg="(duplicate port {} on host {})".format(hport, host) )
            host_ports[host].add( hport )
            # prefix
            if prefix is not None:
                if type(prefix) is not str:
                    valerr( "instances.{}.prefix".format(id), msg="(string value required)" )
                prefix_rex = r"^[-./0-9=@A-Z_a-z~]+$"
                if re.match( prefix_rex, prefix ) is None:
                    valerr( "instances.{}.prefix".format(id), msg="(invalid characters in prefix path)" )
            # s-in
            if s_in is None:
                # Safe default for potentially unpopulated searchable instances
                params['s-in'] = s_in = False if tp in ["search", "txproxy"] else True
            if type(s_in) is not bool:
                valerr( "instances.{}.s-in".format(id), msg="(bool value required)" )
            # attach
            if attach is None:
                params['attach'] = attach = False # Never attach by default
            if type(attach) is not bool:
                valerr( "instances.{}.attach".format(id), msg="(bool value required)" )
            if attach:
                attachable_types = ["builder", "txproxy", "search"]
                if tp not in attachable_types:
                    valerr( "instances.{}.attach".format(id), msg="(instance type {} cannot be attached)".format(tp) )
            # tport
            if tport is not None and tport != 0:
                if type(tport) is not int:
                    valerr( "instances.{}.tport".format(id), msg="(integer required)" )
                if tport in [hport, hport+1]:
                    valerr( "instances.{}.tport".format(id), msg="(cannot be same as hport or hport+1)" )
                if tport in host_ports[host]:
                    valerr( "instances.{}.tport".format(id), msg="(duplicate port {} on host {})".format(tport, host) )
                host_ports[host].add( tport )
            # durable
            if durable:
                if type(durable) is not bool:
                    valerr( "instances.{}.durable".format(id), msg="(bool required)" )
                if durable is True and tport is None:
                    valerr( "instances.{}.tport".format(id), msg="(tport required when durable is true)" )
            # description
            if description and type(description) is not str:
                valerr( "instances.{}.description".format(id) )
    #
    TOPOLOGY_TRANSACTION = set()
    def validate_transaction_topology( I, T, path="topology.transaction", partition=None ):
        if type(T) is not dict:
            valerr( path )
        # Same partition for children of transaction source instances
        engine_part_number = partition if partition is not None else 1
        dispatch_part_number = partition if partition is not None else 1
        other_part_number = partition if partition is not None else 1
        for id, subT in sorted( T.items() ):
            if type(id) is not str:
                valerr( path, ": {}".format(id) )
            if id not in INSTANCE_NAMES:
                raise ValueError( 'System descriptor topology.transaction instance "{}" is undefined'.format(id) )
            if id in TOPOLOGY_TRANSACTION:
                raise ValueError( 'System descriptor topology.transaction duplicate for instance "{}"'.format(id) )
            TOPOLOGY_TRANSACTION.add( id )
            instance = I[id]
            tp = instance.get('type')
            if tp == "admin":
                prefix = 1
                part = other_part_number
                if partition is None:
                    other_part_number += 1
            elif tp == "dispatch":
                prefix = 2
                part = dispatch_part_number
                if partition is None:
                    dispatch_part_number += 1
            elif tp in ["builder", "txproxy", "search"]:
                prefix = 3
                part = engine_part_number
                if partition is None:
                    engine_part_number += 1
                if instance.get("attach") and len(subT) == 0:
                    valerr( "instances.{}.attach".format(id), msg="(no subscribers)" )
            else:
                prefix = 1
                part = 1
            # 
            sortable_float = 1000*prefix + part/1000
            instance['partition'] = "{:.3f}".format( sortable_float )
            # Recursion
            validate_transaction_topology( I, subT, path+".{}".format(id), part )
    #
    def validate_dispatch_row_params( id, replica, R ):
        channels = R.get('channels', DISPATCH_DEFAULT_CHANNELS)
        priority = R.get('priority', DISPATCH_DEFAULT_PRIORITY)
        primary = R.get('primary', False)
        partitions = R.get('partitions')
        # channels
        if type(channels) is not int:
            raise TypeError( "System descriptor topology.dispatch.{} replica {} 'channels' must be int".format(id, replica) )
        if channels < 1 or channels > 127:
            raise ValueError( "System descriptor topology.dispatch.{} replica {} 'channels':{} out of range".format(id, replica, channels) )
        # priority
        if type(priority) is not int:
            raise TypeError( "System descriptor topology.dispatch.{} replica {} 'priority' must be int".format(id, replica) )
        if type(partitions) is list and len(partitions) == 0 and priority < 0:
            pass # ok, means allow-incomplete
        elif priority < 0 or priority > 127:
            raise ValueError( "System descriptor topology.dispatch.{} replica {} 'priority':{} out of range".format(id, replica, priority) )
        # max cost
        max_cost = priority * channels
        if max_cost > 127:
            raise ValueError( "System descriptor topology.dispatch.{} replica {} max cost {} x {} = {} out of range".format(id, replica, priority, channels, max_cost) )
        # primary
        if type(primary) not in [bool, int]:
            raise TypeError( "System descriptor topology.dispatch.{} replica {} 'primary' must be bool or int".format(id, replica) )
        return channels, priority, primary
    #
    def validate_dispatch_topology( I, T, path="topology.dispatch" ):
        if type(T) is not dict:
            valerr( path )
        dispatcher_number = 0
        for id, cfg in sorted( T.items() ):
            dispatcher_number += 1
            if type(id) is not str:
                valerr( path, ": {}".format(id) )
            if id not in INSTANCE_NAMES:
                raise ValueError( 'System descriptor topology.dispatch instance "{}" is undefined'.format(id) )
            # Replicas only
            if type(cfg) is dict:
                partitions = set()
                primary_count = 0
                for engine_id, engine_cfg in cfg.items():
                    if engine_id not in INSTANCE_NAMES:
                        raise ValueError( 'System descriptor topology.dispatch.{} instance "{}" is undefined'.format(id, engine_id) )
                    if type(engine_cfg) is not dict:
                        raise ValueError( 'System descriptor topology.dispatch.{}.{} config must be dict'.format(id, engine_id) )
                    channels, priority, primary = validate_dispatch_row_params( id, engine_id, engine_cfg )
                    if primary:
                        if primary_count > 0:
                            raise ValueError( 'System descriptor topology.dispatch.{} row {} cannot have multiple primary'.format(id, r) )
                        primary_count += 1
                    engine = I[engine_id]
                    part = engine.get('partition')
                    partitions.add( part )
                # Inherit partition from replicas if they belong to the same partition
                if len(partitions) == 1:
                    partition = partitions.pop()
                # Replicas from different partitions (or dispatchers)
                else:
                    partition = "{:.3f}".format( 2000 + dispatcher_number/1000 )
                instance = I[id]
                instance['partition'] = partition

            elif type(cfg) is list:
                r = 0
                primary_count = 0
                for row in cfg:
                    r += 1
                    if type(row) is not dict:
                        raise ValueError( 'System descriptor topology.dispatch.{} row {} config must be dict'.format(id, r) )
                    partitions = row.get("partitions")
                    if partitions is None:
                        raise ValueError( 'System descriptor topology.dispatch.{} row {} partitions missing'.format(id, r) )
                    if type(partitions) is not list:
                        raise ValueError( 'System descriptor topology.dispatch.{} row {} partitions must be list'.format(id, r) )
                    channels, priority, primary = validate_dispatch_row_params( id, r, row )
                    if primary:
                        if primary_count > 0:
                            raise ValueError( 'System descriptor topology.dispatch.{} row {} cannot have multiple primary'.format(id, r) )
                        primary_count += 1
                    for engine_id in partitions:
                        if engine_id not in INSTANCE_NAMES:
                            raise ValueError( 'System descriptor topology.dispatch.{} row {} partitions engine {} is undefined'.format(id, r, engine_id) )
            else:
                raise ValueError( 'System descriptor topology.dispatch instance "{}" invalid configuration'.format(id) )
        # Assign default partition
        next = 1
        parts = {}
        for id, cfg in sorted( T.items() ):
            instance = I[id]
            if "partition" in instance:
                continue
            if type(cfg) is list and len(cfg) == 1:
                row = cfg[0]
                key = "\t".join(sorted( row['partitions'] ))
            else:
                key = id
            if key not in parts:
                parts[key] = next
                next += 1
            part = parts[key]
            sortable_float = 2000 + part/1000
            instance['partition'] = "{:.3f}".format( sortable_float )
    #
    def validate_topology( D ):
        T = D.get("topology")
        if type(T) is not dict:
            valerr( "topology" )
        # Validate
        instances = D.get("instances")
        transaction = sysplugin__TransformTransactionTopology( T.get("transaction") )
        dispatch = T.get("dispatch")
        validate_transaction_topology( instances, transaction )
        validate_dispatch_topology( instances, dispatch )
    #
    if type(descriptor) is not dict:
        raise TypeError( "System descriptor must be dict" )
    # Handle legacy descriptor, transform in-place
    sysplugin__TransformLegacyDescriptor( descriptor )
    # Validate
    validate_name( descriptor )
    validate_graphs( descriptor )
    validate_instances( descriptor )
    validate_topology( descriptor )
    return descriptor




###############################################################################
# sysplugin__GetTransactionTopologyInstances
#
###############################################################################
def sysplugin__GetTransactionTopologyInstances( descriptor=None ):
    """
    Return list of tuples: [ (id, {'host':..., ...}), (id, {...}), ... ]
    """
    def get_children( D, T ):
        C = []
        if type(T) is dict:
            I = D.get("instances",{})
            for id in T:
                instance = I.get(id)
                if instance is None:
                    raise KeyError( "unknown instance '{}' in topology.transaction".format(id) )
                C.append( (str(id), instance) )
            for id in T:
                C.extend( get_children( D, T[id] ) )
        return C
    if descriptor is None:
        descriptor = sysplugin__GetSystemDescriptor()
    T = descriptor.get("topology",{}).get("transaction", {})
    TT = sysplugin__TransformTransactionTopology( T )
    return get_children( descriptor, TT )




###############################################################################
# sysplugin__GetDispatchTopologyInstances
#
###############################################################################
def sysplugin__GetDispatchTopologyInstances( descriptor=None ):
    """
    Return list of tuples: [ (id, {'host':..., ...}), (id, {...}), ... ]
    """
    S = set()
    if descriptor is None:
        descriptor = sysplugin__GetSystemDescriptor()
    T = descriptor.get("topology",{}).get("dispatch", {})
    for dispatcher_id, engines in T.items():
        S.add( dispatcher_id )
        # Matrix
        if type(engines) is list:
            for row in engines:
                for engine_id in row.get("partitions",[]):
                    S.add( engine_id )
        # Replicas only
        elif type(engines) is dict:
            for engine_id, params in engines.items():
                S.add( engine_id )
    C = []
    I = descriptor.get("instances",{})
    for id in S:
        instance = I.get(id)
        if instance is None:
            raise KeyError( "unknown instance '{}' in topology.dispatch".format(id) )
        C.append( (str(id), instance) )
    return C
        





###############################################################################
# vgxadmin__VGXConsole
#
###############################################################################
class vgxadmin__VGXConsole( object ):
    """
    """

    def __init__( self, use_stdout=True, printf=None ):
        if use_stdout:
            self.ostream = sys.stdout
        else:
            self.ostream = StringIO()
        self.printf = printf



    def Print( self, *obj, **kwds ):
        if callable(self.printf):
            S = []
            if obj:
                S.append( ", ".join( (repr(x) for x in obj) ) )
            if kwds:
                S.append( ", ".join( ("{}={}".format(k,v) for k,v in kwds.items()) ) )
            self.printf( ", ".join(S) )
        else:
            print( *obj, **kwds, file=self.ostream )



    def IsStdout( self ):
        return self.ostream == sys.stdout



    def GetStream( self ):
        return self.ostream



    def GetLines( self ):
        if self.IsStdout():
            return []
        self.ostream.seek(0)
        lines = [line.rstrip() for line in self.ostream.readlines()]
        self.ostream.seek(0)
        self.ostream.truncate()
        return lines




###############################################################################
# vgxadmin__VGXRemote
#
###############################################################################
class vgxadmin__VGXRemote( object ):
    """
    """

    HTTP_CONNECTIONS = {}
    CLEANUP_THRESHOLD = 64
    CLEANUP_COUNT = 16 # Clean up 1/4 of old connections

    def __init__( self, host, port, name="", description="", console=None ):
        super().__init__()
        self.host = host
        self.port = port
        self.name = name
        self.description = description
        if console is None:
            self.console = vgxadmin__VGXConsole()
        else:
            self.console = console
        self.__endpoint_cache = {}
        self.__nreq = 0


    def __repr__( self ):
        ident = "{} {}".format(self.name, self.description)
        return "{} - {}:{}".format(ident, self.host, self.port)
    
    
    
    def CleanupHTTPConnection( self, key ):
        _, conn = vgxadmin__VGXRemote.HTTP_CONNECTIONS.pop( key, (None,None) )
        if conn is not None:
            conn.close()



    def CloseHTTPConnection( self ):
        key = "{}:{}:{}".format( self.host, self.port, threading.get_ident() )
        self.CleanupHTTPConnection( key )



    def CleanupHTTPConnections( self ):
        # Too many connections
        if len(vgxadmin__VGXRemote.HTTP_CONNECTIONS) > vgxadmin__VGXRemote.CLEANUP_THRESHOLD:
            for key, entry in sorted( vgxadmin__VGXRemote.HTTP_CONNECTIONS.items(), key=lambda x:x[1][0] )[:vgxadmin__VGXRemote.CLEANUP_COUNT]:
                self.CleanupHTTPConnection( key )



    def NewHTTPConnection( self, timeout=4.0, force_new=False ):
        try:
            self.CleanupHTTPConnections()
        except Exception as err:
            self.console.Print( "ADMIN HTTPConnection cleanup failed: {}".format(err) )
        key = "{}:{}:{:.1f}:{}".format( self.host, self.port, timeout, threading.get_ident() )
        if force_new:
            self.CleanupHTTPConnection( key )
        _, conn = vgxadmin__VGXRemote.HTTP_CONNECTIONS.get( key, (None,None) )
        if conn is None:
            conn = http.client.HTTPConnection( self.host, self.port, timeout=timeout )
            vgxadmin__VGXRemote.HTTP_CONNECTIONS[key] = time.time(), conn
        return conn



    def SendRequest( self, path, params={}, content=None, headers={}, auto_executor=True, timeout=4.0 ):
        attempts = 3
        force_new = False
        sleep = 0.0
        while attempts > 0:
            try:
                conn = self.NewHTTPConnection( timeout=timeout, force_new=force_new )
                if content is None:
                    method = "GET"
                else:
                    method = "POST"
                P = []
                for k,v in params.items():
                    P.append( "{}={}".format(k,v) )
                pstr = "&".join( P)
                if not path.startswith("/"):
                    path = "/{}".format( path )
                if pstr:
                    url = "{}?{}".format( path, pstr )
                else:
                    url = path
                if auto_executor:
                    headers['x-vgx-builtin-min-executor'] = attempts
                conn.request( method, url, body=content, headers=headers )
                response = conn.getresponse()
                data = response.read()
                headers = {}
                for k,v in response.getheaders():
                    headers[ k.lower() ] = v
                if headers.get("content-type","").startswith("application/json;"):
                    data = json.loads( data )
                    data = data.get("response",data)
                if response.status != 200:
                    conn.close()
                return (response.status, response.reason, data, headers)
            except (http.client.NotConnected, http.client.ImproperConnectionState, socket.timeout) as cex:
                attempts -= 1
                timeout += 0.25
                sleep += 0.1
                force_new = True
            except:
                self.CloseHTTPConnection()
                raise
            time.sleep(sleep)



    def RaiseServerError( self, message, exc=None, detail=None ):
        error = "{}: {} exception: {}".format(message, self, exc)
        if detail:
            error = "{} detail: {}".format( error, detail )
        raise vgxadmin__ServerError( error )



    def GetAuthToken( self, retry=3 ):
        if not self.Ping( retry=retry ):
            raise vgxadmin__ServerError( "Unreachable: {}".format(self) )
        response = None
        E = []
        for n in range(retry):
            try:
                response = self.SendRequest( "/vgx/builtin/ADMIN_GetAuthToken", headers={'accept': 'application/json'}, timeout=10.0 )
                status, reason, data, headers = response
                if status == 200:
                    # Success
                    return data.get("authtoken")
            except Exception as err:
                E.append(err)
            time.sleep(0.5)
        self.RaiseServerError( "Failed to get authtoken", E, response )



    def LogCommand(self, command):
        self.__nreq += 1
        self.console.Print( "[{} {}]: {}".format( self.__nreq, self.name, command ) )



    def SendAdminRequest( self, command, params={}, content=None, timeout=10.0, retry=3 ):
        self.LogCommand(command)
        path = "/vgx/builtin/ADMIN_{}".format( command )
        response = None
        E = []
        for n in range(retry):
            P = { "authtoken": self.GetAuthToken() }
            P.update( params )
            try:
                response = self.SendRequest( path, params=P, content=content, headers={'accept': 'application/json'}, timeout=timeout )
                status, reason, data, headers = response
                if status == 200:
                    # Success
                    return data
                elif status == 500 and type(data) is dict:
                    plugin = data.get("message",{}).get("plugin",{})
                    if "PermissionError" in plugin.get("exception","") and plugin.get("value","").startswith("Expired"):
                        continue
            except Exception as err:
                E.append(err)
            time.sleep(0.5)
        self.RaiseServerError( "Failed to send admin request", E, response )



    def HC( self, timeout=1.0, retry=1 ):
        for n in range(retry):
            try:
                status, reason, data, headers = self.SendRequest( "/vgx/hc", auto_executor=False, timeout=timeout )
                if status == 200 and data.startswith(b"VGX/3"):
                    # Success
                    return True
            except:
                pass
            time.sleep(0.5)
        return False

    

    def Ping( self, timeout=1.0, retry=1 ):
        for n in range(retry):
            try:
                response = self.SendRequest( "/vgx/ping", headers={'accept': 'application/json'}, timeout=timeout )
                status, reason, data, headers = response
                if status == 200 and type(data.get("host")) is dict:
                    # Success
                    return True
            except:
                pass
            time.sleep(0.5)
        return False



    def ClearEndpointCache( self ):
        self.__endpoint_cache.clear()



    def GetDataByKey( self, key, data, dflt ):
        if key is None:
            return data
        if type(key) is str:
            return data.get(key,dflt)
        if type(key) in [list,tuple]:
            for k in key:
                if type(data) is not dict:
                    return None
                data = data.get(k,dflt)
            return data
        raise TypeError( key )



    def Endpoint( self, path, key=None, dflt=None, params={}, cache=False, retry=3 ):
        if cache and path in self.__endpoint_cache:
            # Success (cached)
            return self.GetDataByKey( key, self.__endpoint_cache[ path ], dflt )
        response = None
        E = []
        for n in range(retry):
            try:
                response = self.SendRequest( path, params=params, headers={'accept': 'application/json'}, timeout=DEFAULT_TIMEOUT )
                status, reason, data, headers = response
                if status == 200:
                    if cache:
                        self.__endpoint_cache[ path ] = data
                    elif path in self.__endpoint_cache:
                        del self.__endpoint_cache[ path ]
                    # Success
                    return self.GetDataByKey( key, data, dflt )
            except Exception as err:
                E.append(err)
            time.sleep(0.5)
        self.RaiseServerError( "Endpoint request failed", E, response )
 



###############################################################################
# vgxadmin__VGXInstance
#
###############################################################################
class vgxadmin__VGXInstance( object ):
    """
    """

    DEFAULT_TYPE = "generic"
    DEFAULT_HOST = "127.0.0.1"

    
    def __init__( self, id, data, console=None ):
        if console is None:
            self.console = vgxadmin__VGXConsole()
        else:
            self.console = console
        # id
        self.id = id
        # group
        self.group = data.get("group",-1)
        # graph
        self.graph = data.get("graph")
        self.graphs = self.graph.split() if self.graph else []
        # type
        self.type = data.get("type", vgxadmin__VGXInstance.DEFAULT_TYPE)
        # host/ip
        self.host = data.get("host", vgxadmin__VGXInstance.DEFAULT_HOST)
        self.ip = vgxadmin__VGXInstance.host_is_ip( self.host )
        # hport/aport
        self.hport = data.get("hport",0)
        self.aport = self.hport + 1 if self.hport > 0 else 0
        # prefix
        self.prefix = data.get("prefix")
        # s-in
        self.s_in = data.get("s-in",False)
        # attach
        self.attach = data.get("attach",False)
        # tport
        self.tport = data.get("tport",0)
        # durable
        self.durable = data.get("durable",False)
        # description
        self.description = data.get("description", "{} {} at {}:{}".format(self.type.capitalize(), self.id, self.host, self.hport) )
        # local until proven otherwise
        self.local = True 
        #
        self.subscribers = {}
        self.cfdispatcher = None
        self.remote = vgxadmin__VGXRemote( host=self.host, port=self.aport, name=self.id, description=self.description, console=self.console )


    @staticmethod
    def host_is_ip( host ):
        try:
            octets = list( map( int, host.split(".") ) )
            assert len( octets ) == 4
            for b in octets:
                assert b >= 0 and b <= 127
        except:
            return None
        return host


    def __repr__( self ):
        return "{} {}:{}".format(self.id, self.host, self.hport)



    def IsLocal( self ):
        return self.local



    def ExpectResultAction( self, result, action ):
        if type(result) is not dict:
            raise vgxadmin__OperationIncomplete( "{}".format(result) )
        if result.get("action") != action:
            raise vgxadmin__OperationIncomplete( "expected action {}, got {}".format(action, result) )



    def AddSubscriber( self, instance ):
        if instance.id not in self.subscribers:
            self.local = False
            instance.local = False
            self.subscribers[ instance.id ] = instance



    def SetDispatcherConfig( self, config ):
        self.cfdispatcher = config



    def GetConfig( self ):
        D = {
            "graph": self.graph,
            "group": self.group,
            "id": self.id,
            "type": self.type,
            "host": self.host,
            "ip": self.ip,
            "hport": self.hport,
            "prefix": self.prefix,
            "s-in": self.s_in,
            "aport": self.aport,
            "tport": self.tport,
            "attach": self.attach,
            "durable": self.durable,
            "description": self.description,
            "subscribers": list(self.subscribers.keys()),
            "cfdispatcher": self.cfdispatcher
        }
        return json.dumps(D, indent=4)



    def HC( self, timeout=1.0 ):
        return self.remote.HC( timeout=timeout )



    def Descriptor( self ):
        return self.remote.Endpoint( "/vgx/builtin/system_descriptor" )



    def UpdateDescriptor( self, descriptor ):
        J = json.dumps( descriptor.AsDict() )
        return self.remote.SendAdminRequest( "SystemDescriptor", content=J )



    def ClearEndpointCache( self ):
        self.remote.ClearEndpointCache()

    

    def Ping( self, key=None, dflt=None, cache=False ):
        return self.remote.Endpoint( "/vgx/ping", key=key, dflt=dflt, cache=cache )



    def Nodestat( self, key=None, dflt=None, cache=False ):
        return self.remote.Endpoint( "/vgx/nodestat", key=key, dflt=dflt, cache=cache )



    def Status( self, key=None, dflt=None, cache=False ):
        return self.remote.Endpoint( "/vgx/status", key=key, dflt=dflt, cache=cache )



    def WaitForIdle( self, timeout=30, idle_after_ms=5000 ):
        deadline = time.time() + timeout
        idle_ms = self.Nodestat( "idle-ms", 0 )
        while idle_ms < idle_after_ms:
            time.sleep(1)
            idle_ms = self.Nodestat( "idle-ms", 0 )
            if time.time() > deadline:
                raise vgxadmin__OperationIncomplete( "Instance {} is busy".format(self) )
        return "idle"



    def Digest( self ):
        return self.Nodestat( "digest" )



    def IsReadonly( self ):
        return self.Nodestat( "readonly" )



    def IsServiceIn( self ):
        return self.Nodestat( "service-in" ) == 1



    def ServiceIn( self ):
        return self.remote.SendAdminRequest( "ServiceIn" )



    def ServiceOut( self ):
        return self.remote.SendAdminRequest( "ServiceOut" )



    def Bind( self, durable=None ):
        self.WaitForIdle()
        if not self.tport:
            raise vgxadmin__InvalidUsageOrConfig( "{} undefined transaction input port".format( self ) )
        if durable is None:
            durable = self.durable
        result = self.remote.SendAdminRequest( "Bind", params={"port":self.tport, "durable":int(durable)}, timeout=DEFAULT_TIMEOUT )
        self.ExpectResultAction( result, "bind" )
        deadline = time.time() + 15
        while self.Nodestat( "txport", 0 ) == 0:
            if time.time() > deadline:
                raise vgxadmin__OperationIncomplete( "{} Bind() failed".format(self) )
            time.sleep(1)
        return result



    def Unbind( self):
        return self.remote.SendAdminRequest( "Unbind", timeout=DEFAULT_TIMEOUT )



    def Attach( self, sync=False, destinations=None ):
        self.WaitForIdle()
        if len( self.subscribers ) == 0:
            return
        addresses = []
        if destinations is None:
            destinations = self.subscribers.values()
        for sub in destinations:
            dest = "{}:{}".format( sub.host, sub.aport )
            addresses.append( dest )
        goal = {
            "control": {
                "nosync": True if sync is False else False
            },
            "destinations": addresses
        }
        goalJSON = json.dumps( goal )
        try:
            self.WaitForIdle( timeout=30 )
        except Exception as err:
            raise vgxadmin__OperationIncomplete( "{}, cannot perform attach at this time".format(err) )
        return self.remote.SendAdminRequest( "UpdateSubscribers", content=goalJSON, timeout=DEFAULT_TIMEOUT )



    def Detach( self ):
        return self.remote.SendAdminRequest( "DetachAll", timeout=DEFAULT_TIMEOUT )



    def RestartHTTP( self ):
        return self.remote.SendAdminRequest( "RestartHTTP", timeout=DEFAULT_TIMEOUT )



    def Unsubscribe( self ):
        return self.remote.SendAdminRequest( "Unsubscribe", timeout=DEFAULT_TIMEOUT )



    def OpDump( self ):
        bound = self.Nodestat( "txport", 0 )
        if bound != 0:
            raise vgxadmin__InvalidUsageOrConfig( "Cannot dump subscriber instance {} (txport {})".format(self, bound) )
        subs = self.Nodestat( "subscribers", [] )
        if len(subs) > 0:
            raise vgxadmin__InvalidUsageOrConfig( "Cannot dump provider instance {} ({} subscribers)".format(self, len(subs)) )
        sysroot = self.remote.Endpoint( "/vgx/storage" ).get("sysroot")
        if sysroot is None:
            raise vgxadmin__InvalidUsageOrConfig( "Unknown sysroot for {}".format(self) )
        ident = "{}".format( int(time.time()) )
        uri = "file:///{}/opdump_{}.tx".format( sysroot, ident )
        attach_result = self.remote.SendAdminRequest( "Attach", params={"uri": uri}, timeout=DEFAULT_TIMEOUT )
        self.ExpectResultAction( attach_result, "attached" )
        sync_result = self.Sync( "hard" )
        if sync_result.get("status") != "complete":
            while self.Nodestat( "synchronizing", 0 ) == 1:
                time.sleep(1)
            sync_result["status"] = "complete"
        self.Detach()
        return (attach_result, sync_result)



    def Sync( self, mode ):
        self.WaitForIdle()
        return self.remote.SendAdminRequest( "Sync", params={"mode":mode}, timeout=DEFAULT_TIMEOUT )



    def ForceCopy( self, source, destination ):
        ret = None
        # Detach source from any currently attached subscribers
        result = source.Detach()
        self.ExpectResultAction( result, "detached" )
        # Detach destination from any currently attached subscribers
        result = destination.Detach()
        self.ExpectResultAction( result, "detached" )
        # Temporarily bind destination to accept transaction input
        was_bound = False
        if destination.Nodestat( "txport", 0 ) > 0:
            result = destination.Unbind()
            was_bound = True
            self.ExpectResultAction( result, "unbind" )
            time.sleep(1)
        try:
            result = destination.Bind( durable=False )
            self.ExpectResultAction( result, "bind" )
            # Attach source to temporary transaction input of destination
            uri = "vgx://{}:{}".format( destination.host, destination.tport )
            result = source.remote.SendAdminRequest( "Attach", params={"uri": uri}, timeout=DEFAULT_TIMEOUT )
            self.ExpectResultAction( result, "attached" )
            # Perform hard sync from source to destination
            time.sleep(1)
            destination.PauseIn()
            time.sleep(1)
            destination.Truncate()
            destination.ResumeIn()
            time.sleep(1)
            result = source.Sync( mode="soft" )
            self.ExpectResultAction( result, "sync" )
            ret = result
            # Wait for fingerprints to match
            deadline_sync_started = time.time() + 60.0
            deadline_completion = -1
            sync_started = False
            sync_ended = False
            sync_check_attempts = 3
            try:
                while destination.Digest() != source.Digest():
                    time.sleep(3)
                    try:
                        if source.Nodestat( "synchronizing", 0 ) == 1:
                            sync_started = True
                            sync_check_attempts = 3
                        elif sync_started and not sync_ended:
                            sync_ended = True
                            deadline_completion = time.time() + 60.0
                    except Exception as err:
                        # source nodestat failed
                        if sync_started:
                            if sync_check_attempts < 1:
                                raise vgxadmin__OperationIncomplete( "force copy status unknown" )
                            sync_check_attempts -= 1
                    # Sync start timeout
                    if not sync_started and time.time() > deadline_sync_started:
                        if sync_check_attempts < 1:
                            raise vgxadmin__OperationIncomplete( "unable to start force copy sync" )
                        sync_check_attempts -= 1
                    # Sync completion timeout
                    if sync_ended and time.time() > deadline_completion:
                        raise vgxadmin__OperationIncomplete( "force copy sync is incomplete / inconsistent" )
            except Exception as err:
                self.console.Print( err )
            # Detach source from destination
            result = source.Detach()
            self.ExpectResultAction( result, "detached" )
            # Check digest
            if destination.Digest() != source.Digest():
                raise vgxadmin__OperationIncomplete( "digest mismatch after force copy sync attempt" )
        finally:
            # Unbind destination
            if not was_bound:
                try:
                    result = destination.Unbind()
                    self.ExpectResultAction( result, "unbind" )
                except Exception as uerr:
                    self.console.Print( "{} Unbind() error {}".format(destination, uerr) )
        return ret



    def ReverseSync( self ):
        self.WaitForIdle()
        selected_sub = None
        sn = -1
        for sub in self.subscribers.values():
            sub_sn = int( sub.Nodestat( "master-serial", 0 ) )
            if sub_sn > sn:
                selected_sub = sub
                sn = sub_sn
        return self.ForceCopy( source=selected_sub, destination=self )



    def CancelSync( self ):
        return self.remote.SendAdminRequest( "CancelSync", timeout=DEFAULT_TIMEOUT )



    def RollingForwardSync( self ):
        """
        Perform rolling update of subscribers
        """
        ret = {}
        # Ensure at least two S-IN subscribers
        S_IN_PRE = set()
        S_RO = []
        for sub in self.subscribers.values():
            if sub.IsReadonly():
                S_RO.append( sub.id )
            try:
                if sub.HC():
                    S_IN_PRE.add(sub.id)
            except Exception as hcerr:
                self.console.Print( "{}: {}".format(sub, hcerr) )
        if len(S_IN_PRE) < 2:
            raise vgxadmin__InvalidUsageOrConfig( "Too few S-IN destinations for automatic rolling update" )
        if len(S_RO) > 0:
            raise vgxadmin__InvalidUsageOrConfig( "Readonly destinations exist: {}".format( sub ) )
        # Get state
        self.WaitForIdle()
        nodestat = self.Nodestat()
        target_size = nodestat['graph-size']
        target_order = nodestat['graph-order']
        # Should we re-attach all subscribers once sync complete?
        reattach_when_done = True if len( nodestat.get("subscribers") ) > 0 else False
        # Should we resume tx input once complete?
        resume_tx_in_when_done = False if nodestat.get("tx-in-halted") is True else True
        # Detach all subscribers
        self.PauseIn()
        self.Detach()
        # Perform rolling update
        for sub in self.subscribers.values():
            # Get subscriber state digest
            pre_digest = sub.Digest()
            pre_s_in = sub.IsServiceIn()
            try:
                # Wait for subscriber idle
                #self.console.Print( "Will do {} WaitForIdle()".format(sub) )
                sub.WaitForIdle()
                # Make sure subscriber is S-OUT
                #self.console.Print( "Will do {} ServiceOut()".format(sub) )
                sub.ServiceOut()
                # Hard sync
                #self.console.Print( "Will do {} ForceCopy()".format(sub) )
                r = self.ForceCopy( source=self, destination=sub )
                sub_nodestat = sub.Nodestat()
                assert sub_nodestat['graph-size'] == target_size
                assert sub_nodestat['graph-order'] == target_order
                # Return to S-IN if that was the original state
                if sub.id in S_IN_PRE:
                    #self.console.Print( "Will do {} ServiceIn()".format(sub) )
                    sub.ServiceIn()
                    # Wait for any dispatcher(s) connected to the just S-IN'ed instance
                    # to re-register the instance as an active part of the back-end.
                    time.sleep(5) # 1 second is enough, wait longer to be safe
                ret[sub.id] = r
            except Exception as err:
                if pre_s_in:
                    if sub.Digest() == pre_digest:
                        sub.ServiceIn()
                    else:
                        self.console.Print( "Destination digest changes, unable to restore S-IN" )
                raise
            finally:
                pass
        # Re-attach if we had subscribers before sync
        if reattach_when_done:
            self.Attach()
        # Resume tx input if that was the original state
        if resume_tx_in_when_done:
            self.ResumeIn()
        return ret



    def Persist( self ):
        return self.remote.SendAdminRequest( "Persist", timeout=DEFAULT_TIMEOUT )



    def Truncate( self ):
        return self.remote.SendAdminRequest( "Truncate", timeout=DEFAULT_TIMEOUT )



    def PauseIn( self ):
        return self.remote.SendAdminRequest( "SuspendTxInput", timeout=DEFAULT_TIMEOUT )



    def ResumeIn( self ):
        return self.remote.SendAdminRequest( "ResumeTxInput", timeout=DEFAULT_TIMEOUT )



    def PauseOut( self ):
        return self.remote.SendAdminRequest( "SuspendTxOutput", timeout=DEFAULT_TIMEOUT )



    def ResumeOut( self ):
        return self.remote.SendAdminRequest( "ResumeTxOutput", timeout=DEFAULT_TIMEOUT )



    def PauseTTL( self ):
        return self.remote.SendAdminRequest( "SuspendEvents", timeout=DEFAULT_TIMEOUT )



    def ResumeTTL( self ):
        return self.remote.SendAdminRequest( "ResumeEvents", timeout=DEFAULT_TIMEOUT )



    def ReadonlyGraph( self ):
        return self.remote.SendAdminRequest( "SetReadonly", timeout=DEFAULT_TIMEOUT )



    def WritableGraph( self ):
        return self.remote.SendAdminRequest( "ClearReadonly", timeout=DEFAULT_TIMEOUT )



    def ResetMetrics( self ):
        return self.remote.SendAdminRequest( "ResetMetrics", timeout=DEFAULT_TIMEOUT )



    def ReloadPlugins( self, plugins_json=None ):
        if plugins_json is None:
            return self.remote.SendAdminRequest( "ReloadPlugins", timeout=DEFAULT_TIMEOUT )
        else:
            return self.remote.SendAdminRequest( "ReloadPlugins", content=plugins_json, timeout=DEFAULT_TIMEOUT )



    def Command( self, gr, cmd ):
        return self.remote.SendAdminRequest( "Console", params={"graph":gr}, content=cmd, timeout=DEFAULT_TIMEOUT )



    def Throttle( self, rate, unit ):
        return self.remote.SendAdminRequest( "Throttle", params={"rate":float(rate), "unit":unit}, timeout=DEFAULT_TIMEOUT )



    def Shutdown( self ):
        params = {
            "authshutdown": self.remote.GetAuthToken(),
            "persist": int(self.durable)
        }
        return self.remote.SendAdminRequest( "Shutdown", params=params )





###############################################################################
# vgxadmin__Descriptor
#
###############################################################################
class vgxadmin__Descriptor( object ):
    """
    vgx.cf format (JSON):

    {
        "name": <str>,                      // <str> = System name
        "graphs": [ <str>, ... ],           // <str> = Name of graph instance
        "instances": {
            <id>: {                         // <id> = Unique instance identifier string, e.g. "S1" for searcher 1
                "graph": <str>,             // <str> = Name of graph for this instance
                "group": <float>,           // <float> = Display group number
                "type": <str>,              // <str> in "builder", "txproxy", "search", "dispatch", "admin", "generic"
                "host": <str>,              // <str> = Host name or IP
                "hport": <int>,             // <int> = Instance HTTP main port
                "tport": <int>,             // <int> = Instance transaction input port (default: 0)
                "durable": <bool>,          // <bool> = true to enable TX-log, false for no TX-log
                "description": <str>        // <str> = Optional instance description
            },
            ...                             // ... All instances must be defined
        },
        "common" : {
            "prefix": <str>,                // <str> = Instance HTTP path prefix
        },
        "topology": {
            "transaction": {
                <id>: {                     // Transaction interconnect configuration
                    <id>: {                 // can be of any depth, but loops are not allowed
                        <id>: {             // for obvious reasons
                            ...
                        },
                        ...
                    },
                    ...
                }
            },
            "dispatch": {
                <id>: {                     // <id> = Instance of type "dispatch"
                    <id>: {                 // <id> = Instance of type "dispatch" or "search"
                        "channels": <int>,  // <int> = Number of allowed socket connections into this instance
                        "priority": <int>,  // <int> = Lower value receives more traffic than higher value
                        "primary": <bool>   // <bool> = true means instance allows data injection
                    },               
                    ...
                },
                ...                         // Any number of dispatchers allowed
            }
        }
    }


    EXAMPLE:
    {
        "name": "Test system 1",
        "graphs": ["g1"],
        "instances": {
            "d1": { "graph": "g1", "group": 1, "type": "dispatch", "host": "10.130.30.102",  "hport": 9910,                                    "description": "Dispatcher 1" },
            "b1": { "graph": "g1", "group": 2, "type": "builder",  "host": "10.130.30.102",  "hport": 9010,  "tport": 10010,                   "description": "Builder 1" },
            "t1": { "graph": "g1", "group": 2, "type": "txproxy",  "host": "10.130.30.102",  "hport": 9110,  "tport": 10110, "durable": true,  "description": "TXProxy 1" },
            "s1": { "graph": "g1", "group": 3, "type": "search",   "host": "10.130.30.102",  "hport": 9210,  "tport": 10210,                   "description": "Search 1" },
            "s2": { "graph": "g1", "group": 3, "type": "search",   "host": "10.130.30.102",  "hport": 9220,  "tport": 10220,                   "description": "Search 2" },
            "s3": { "graph": "g1", "group": 3, "type": "search",   "host": "10.130.30.102",  "hport": 9230,  "tport": 10230,                   "description": "Search 3" }
        },
        "common": {
            "prefix": "myservice"
        },
        "topology": {
            "transaction": {
                "b1": {
                    "t1": {
                        "s1": {},
                        "s2": {},
                        "s3": {}
                    }
                }
            },
            "dispatch": {
                "d1": {
                    "s1": { "channels": 32, "priority": 1 },
                    "s2": { "channels": 32, "priority": 1 },
                    "s3": { "channels": 32, "priority": 1 }
                }
            }
        }
    }



    """

    def __init__( self, descriptor, remote=None, console=None, printf=None ):
        if console is None:
            self.console = vgxadmin__VGXConsole( printf=printf )
        else:
            self.console = console
        self.data = ""
        self.name = ""
        self.instances = {}
        self.transaction_topology = {}
        self.dispatch_topology = {}
        self.graphs = []
        self.remote = remote
        self.Reload( descriptor )



    def AsDict( self ):
        I = {}
        for instance in self.instances.values():
            I[instance.id] = {
                "group": instance.group,
                "type": instance.type,
                "host": instance.host,
                "hport": instance.hport,
                "tport": instance.tport,
                "durable": instance.durable,
                "description": instance.description
            }
        D = {
            "name": self.name,
            "instances": I,
            "topology": {
                "transaction": self.transaction_topology,
                "dispatch": self.dispatch_topology
            },
            "graphs": self.graphs
        }
        return D



    def __repr__( self ):
        buffer = StringIO()
        pprint.pprint( self.AsDict(), stream=buffer, indent=2 )
        buffer.seek(0)
        return buffer.getvalue()
        


    def _process_transaction_topology( self, topology ):
        """
        {
            "b1": {
                "t1": {
                    "s1": {},
                    "s2": {},
                    "s3": {}
                },
                "t2": {}
            }
        }
        """
        I = []
        for id, T in topology.items():
            instance = self.Get( id )
            I.append( instance )
            for sub_instance in self._process_transaction_topology( T ):
                instance.AddSubscriber( sub_instance )
        return I



    @staticmethod
    def _replicas_append_row_params( replicas, params ):
        entry = {
            "channels": int( params.get("channels", DISPATCH_DEFAULT_CHANNELS) ),
            "priority": int( params.get("priority", DISPATCH_DEFAULT_PRIORITY) )
        }
        if bool( params.get("primary", False ) ): # i.e. writable
            entry["primary"] = True
        replicas.append( entry )



    def _get_engine_params( self, engine_id ):
        engine = self.Get( engine_id )
        return {
            "host": engine.host,
            "port": engine.hport
        }



    def _process_dispatch_topology( self, topology ):
        """
        Replicas only:
        {
            "d1": {
                "s1": { "channels": 32, "priority": 1 },
                "s2": { "channels": 32, "priority": 1 },
                "s3": { "channels": 32, "priority": 1 }
            },
            "d2": {
                "s1": { "channels": 32, "priority": 1 },
                "s2": { "channels": 32, "priority": 1 },
                "s3": { "channels": 32, "priority": 1 }
            },
            "d3": {
                "d1": { "channels": 64, "priority": 1 },
                "d2": { "channels": 64, "priority": 1 }
            }
        }

        With partitions:
        {
            "d1": [
                { "channels": 32, "priority": 1, "description": "ROW1",
                  "partitions": ["s1.1", "s2.1", "s3.1"]
                },
                { "channels": 32, "priority": 1, "description": "ROW2",
                  "partitions": ["s1.2", "s2.2", "s3.2"]
                }
            ]
        }

        Configure internal dispatcher config for a single partition, 
        from the partition's descriptor.
        """
        def get_partition( partitions, p ):
            try:
                return partitions[p]
            except IndexError:
                partition = []
                partitions.append( partition )
                return partition



        for dispatcher_id, T in topology.items():
            dispatcher = self.Get( dispatcher_id )
            options = {}
            replicas = []
            partitions = []
            config = {
                "options"   : options,
                "replicas"  : replicas,
                "partitions": partitions
            }
            # Matrix topology (partitions and replicas)
            if type(T) is list:
                for row in T:
                    partition_ids = row.get( "partitions", [] )
                    row_priority = row.get( "priority", 0 )
                    # Empty partition set -> allow incomplete (partial) results
                    if row_priority < 0 and partition_ids == []:
                        options["allow-incomplete"] = True
                        continue
                    vgxadmin__Descriptor._replicas_append_row_params( replicas, row )
                    p = 0
                    for engine_id in partition_ids:
                        engine = self._get_engine_params( engine_id )
                        partition = get_partition( partitions, p )
                        p += 1
                        partition.append( engine )
            # Replica topology
            elif type(T) is dict:
                partition = get_partition( partitions, 0 )
                for engine_id, engine_params in T.items():
                    vgxadmin__Descriptor._replicas_append_row_params( replicas, engine_params )
                    engine = self._get_engine_params( engine_id )
                    partition.append( engine )
            else:
                raise vgxadmin__InvalidUsageOrConfig( "bad topology for dispatcher {}".format( dispatcher_id ) )
            dispatcher.SetDispatcherConfig( config )



    def Reload( self, descriptor ):
        if type(descriptor) is dict:
            self.data = descriptor
        else:
            f = open( descriptor )
            try:
                self.data = json.loads( f.read() )
            finally:
                f.close()

        # Normalize
        sysplugin__TransformLegacyDescriptor( self.data )
        # Extract main sections
        name = self.data.get("name")
        graphs = self.data.get("graphs")
        instances = self.data.get("instances")
        common = self.data.get("common", {})
        topology = self.data.get("topology", {})
        # Check main section types
        if name and type(name) is not str:
            raise vgxadmin__InvalidUsageOrConfig( "name must be str, got {}".format(type(name)) )
        if type(graphs) is not list:
            raise vgxadmin__InvalidUsageOrConfig( "graphs must be list, got {}".format(type(graphs)) )
        if type(instances) is not dict:
            raise vgxadmin__InvalidUsageOrConfig( "instances must be dict, got {}".format(type(instances)) )
        if type(common) is not dict:
            raise vgxadmin__InvalidUsageOrConfig( "common must be dict, got {}".format(type(common)) )
        if type(topology) is not dict:
            raise vgxadmin__InvalidUsageOrConfig( "topology must be dict, got {}".format(type(topology)) )
        # Transfer common parameters to all instances where undefined
        for instance in instances.values():
            if type( instance ) is dict:
                for k,v in common.items():
                    if k not in instance:
                        instance[k] = v
        # Validate
        sysplugin__ValidateSystemDescriptor( self.data )
        # Name
        self.name = name if name else "Unspecified {}-node System".format(len(instances))
        # Graph names
        self.graphs = graphs
        # Prepare to backfill topology as needed for instances not specified in topology
        TT = sysplugin__GetTransactionTopologyInstances( self.data )
        DT = sysplugin__GetDispatchTopologyInstances( self.data )
        A = set( [id for id,_ in TT] ).union( set( [id for id,_ in DT] ) )
        self.transaction_topology = sysplugin__TransformTransactionTopology( topology.get("transaction",{}) )
        self.dispatch_topology = topology.get("dispatch",{})
        # Instances
        if not instances:
            raise vgxadmin__InvalidUsageOrConfig( "at least one instance required" )
        self.instances = {}
        for id, entry in instances.items():
            instance = vgxadmin__VGXInstance( id=id, data=entry, console=self.console )
            self.instances[id] = instance
            if id not in A:
                self.transaction_topology[id] = {}
        # Topology
        # Process transaction topology
        self._process_transaction_topology( self.transaction_topology )
        # Process dispatch topology
        self._process_dispatch_topology( self.dispatch_topology )



    def Get( self, id ):
        instance = None
        if id == "." and self.remote:
            for entry in self.instances.values():
                if entry.aport == self.remote.port and entry.host == self.remote.host:
                    instance = entry
        elif "*" in id:
            raise vgxadmin__InvalidUsageOrConfig( "Wildcard not allowed for this operation" )
        else:
            instance = self.instances.get(id)
        if instance is None:
            raise vgxadmin__InvalidUsageOrConfig( "Unknown instance id: {}".format(id) )
        return instance



    def PrintSummary( self, instances, detail=False ):
        def isnum( value ):
            try:
                float(value.replace(",",""))
                return True
            except:
                return False
        def prepad( value, pad ):
            return pad if isnum( value ) else ""
        def postpad( value, pad ):
            return "" if isnum( value ) else pad
        def fmt( value ):
            s = "{}".format( value )
            if isnum(s) and not s.isdigit():
                return "{:.1f}".format( float(s) )
            else:
                return s
        def fmt_mem_gib( value ):
            return "{:,}".format( value//(1<<30) )
        def fmt_mem_mib( value ):
            return "{:,}".format( value//(1<<20) )
        def fmt_sin( value ):
            return "S-IN" if value > 0 else "S-OUT"
        def fmt_version( value ):
            return value.split()[1].removeprefix('v')
        def fmt_dhms( value ):
            s = value
            D = s // 86400
            s -= D*86400
            H = s // 3600
            s -= H*3600
            M = s // 60
            s -= M*60
            return "{}d {:02d}:{:02d}:{:02d}".format( D, H, M, s )
        def fmt_flt( value ):
            return "{:.1f}".format( float(value) )
        def fmt_int( value ):
            return "{:,}".format( value )

        include_level = 1 if detail else 0
        allitems = [ 
                  (0, "Id",        "Nodestat", None,                   None),
                  (1, "Ver",       "Ping",     ["host","version"],     fmt_version),
                  (0, "Uptime",    "Nodestat", "uptime",               fmt_dhms),
                  (0, "Host",      "Nodestat", "host",                 fmt),
                  (0, "IP",        "Nodestat", "ip",                   fmt),
                  (0, "APort",     "Nodestat", "adminport",            fmt),
                  (1, "TXPort",    "Nodestat", "txport",               fmt),
                  (0, "S",         "Status",   ["request","serving"],  fmt_sin),
                  (1, "RPS",       "Status",   ["request","rate"],     fmt_flt),
                  (1, "95th(ms)",  "Status",   ["response_ms","95.0"], fmt_flt),
                  (1, "CPU",       "Nodestat", "cpu",                  fmt),
                  (1, "Mem(GiB)",  "Nodestat", "memory-total",         fmt_mem_gib),
                  (1, "Use(MiB)",  "Nodestat", "memory-process",       fmt_mem_mib),
                  (0, "Order",     "Nodestat", "graph-order",          fmt_int),
                  (0, "Size",      "Nodestat", "graph-size",           fmt_int),
                  (0, "Service",   "Nodestat", "service-name",         fmt)
                ]
        items = []
        for level, label, method, key, render in allitems:
            if level > include_level:
                continue
            items.append( [label, method, key, render] )
        info = {}
        info[None] = [[str(label), ""] for label,m,k,r in items]
        N = len( instances )
        for instance in instances:
            N -= 1
            if self.console.IsStdout():
                if N > 0:
                    self.console.Print("\r{:3d} {}".format(N, instance.id), end="", flush=True )
                else:
                    self.console.Print("\r{:32}".format(''), flush=True )
            info[instance.id] = []
            try:
                instance.HC( timeout=0.1 )
                running = True
            except:
                running = False
            instance.ClearEndpointCache()
            for label, method, key, render in items:
                if key is None:
                    val = instance.id
                else:
                    if running and render:
                        try:
                            data = getattr( instance, method )( key, cache=True )
                            val = render( data if data is not None else "?" )
                        except Exception as rerr:
                            val = "{}".format(rerr)
                    else:
                        val = ""
                info[instance.id].append( [val, ""] )
            instance.ClearEndpointCache()
        for pos in range(len(items)):
            maxsz = 0
            for id, data in info.items():
                sz = len(data[pos][0])
                if sz > maxsz:
                    maxsz = sz
            for id, data in info.items():
                sz = len(data[pos][0])
                data[pos][1] = " " * (maxsz - sz)
        line = "  ".join( [ "-" * (len(value)+len(pad)) for value, pad in info[None] ] )
        self.console.Print(line)
        for id, data in info.items():
            cols = "  ".join( [ "{}{}{}".format(prepad(value,pad), value, postpad(value,pad)) for value, pad in data ] )
            self.console.Print( cols )
            if id is None:
                self.console.Print(line)
        self.console.Print(line)


    def GetMultiple( self, id="*", ifrunning=True, printsum=False, detail=False, confirm=None ):
        if confirm is not None and not self.console.IsStdout():
            raise Exception( "Operation not allowed" )
        if id == "*" or id == "@":
            I = self.instances.values()
        elif "*" in id or "[" in id:
            patt = id.replace( ".", r"\." ).replace( "*", ".*" )
            rex = re.compile( patt )
            I = []
            for instance in self.instances.values():
                if rex.match( instance.id ):
                    I.append( instance )
        elif id in self.instances:
            I = [self.Get( id )]
        else:
            I = [self.Get(x.strip()) for x in id.split(",")]
        running = []
        if ifrunning or confirm:
            for instance in I:
                try:
                    instance.HC( timeout=0.51 )
                    running.append( instance )
                except:
                    self.console.Print( "Not running: {}".format( instance.id ) )
        if I:
            if printsum:
                self.PrintSummary( I, detail=detail )
            if confirm and running:
                ids = [instance.id for instance in running]
                answer = input( "Confirm {}: {} (y/n) ".format(confirm, ids) )
                if answer != 'y':
                    return []
        if ifrunning:
            return running
        else:
            return I



    def Start( self, id="*" ):
        rdname = "tmp_{}{}".format( int(time.time()*1000), random.randint( 1<<28, 1<<63 ) )
        f = open( rdname, "w" )
        try:
            f.write( json.dumps(self.data), indent=4 )
            f.close()
            f = None
            R = []
            for instance in self.GetMultiple( id, ifrunning=False ):
                try:
                    instance.Nodestat()
                    self.console.Print( "Already running: {}".format( instance.id ) )
                    continue
                except:
                    pass
                cmd = "python -m vgxinstance {} {}".format( instance.id, rdname )
                self.console.Print( "Starting: {}".format( instance.id ) )
                time.sleep(1)
                if sys.platform.startswith("win"):
                    os.system( "start /MIN {}".format( cmd ) )
                else:
                    os.system( "{} &".format( cmd ) )
                R.append( (instance, "Start") )
        finally:
            if f is not None:
                f.close()
            #os.remove(rdname)



    @staticmethod
    def _target( method, instance, console, lock, R, args ):
        try:
            try:
                ret = method( *args )
            except Exception as err:
                if lock.acquire( timeout=2.0 ):
                    try:
                        console.Print(err)
                    finally:
                        lock.release()
                ret = err
            if lock.acquire( timeout=2.0 ):
                try:
                    R.append( (instance, ret) )
                finally:
                    lock.release()
        except:
            pass



    def Concurrent( self, method_name, id="*", args=() ):
        lock = threading.RLock()
        R = []
        W = []
        if type(id) is str:
            instances = self.GetMultiple( id )
        elif type(id) is list:
            instances = []
            for entry in id:
                if type(entry) is str:
                    instances.append( self.Get(entry) )
                elif type(entry) is vgxadmin__VGXInstance:
                    instances.append( entry )
                else:
                    raise vgxadmin__InvalidUsageOrConfig( "Invalid identifier for concurrent operation: {} {}".format( type(entry), entry ) )
        else:
            raise vgxadmin__InvalidUsageOrConfig( "Invalid identifier for concurrent operation: {} {}".format( type(id), id ) )
        for instance in instances:
            method = getattr( instance, method_name )
            w = threading.Thread( target=self._target, args=(method, instance, self.console, lock, R, args) )
            W.append( w )
        for w in W:
            w.start()
        while len([1 for w in W if w.is_alive()]) > 0:
            time.sleep(0.5)
        for w in W:
            w.join( timeout=1.0 )
        return R




###############################################################################
# vgxadmin__VGXAdmin
#
###############################################################################
class vgxadmin__VGXAdmin( object ):
    """
    """


    @staticmethod
    def Usage( err=None, program="vgxadmin", console=None ):
        if console is None:
            console = vgxadmin__VGXConsole()
        else:
            console = console

        console.Print()
        console.Print( "usage: {} [<address|id>] <options>".format( program ) )

        message = """
    -a, --attach <id>[,<sub>[,...]] Attach instance to subscribers
    -B, --bind <id>                 Bind transaction input port
    -k, --cancelsync <id>           Terminate sync in progress
    -f, --cf <file>                 Use this local system descriptor file
    -C, --command <id>,<gr>,<cmd>   Send console command <cmd> to graph <gr>
    -c, --confirm                   Confirm operation (skip y/n prompt)
    -Z, --descriptor <id>           Update instance system descriptor
    -d, --detach <id>               Detach instance from subscribers
    -E, --endpoint <path>           Send request to <address>
    -K, --forcecopy <src>,<dst>     Force hard sync from <src> to <dst>
    -h, --help                      Show this help message
    -Q, --instancecfg <id>          Show instance configuration
    -n, --nodestat <id>[,<key>]     Nodestat
    -M, --opdump <id>               Dump instance data to output file
    -p, --pausein <id>              Pause transaction input
    -P, --pauseout <id>             Pause transaction output
    -t, --pausettl <id>             Pause TTL event processor
    -W, --persist <id>              Write instance data to disk
    -g, --readonly <id>             Make graph(s) readonly
    -N, --reloadplugins <id>[,<pd>] Reload or add plugins in <pd> json file
    -m, --resetmetrics <id>         Clear performance and error counters
    -D, --restarthttp <id>          Restart HTTP server with refreshed config
    -r, --resumein <id>             Resume transaction input
    -R, --resumeout <id>            Resume transaction output
    -T, --resumettl <id>            Resume TTL event processor (if enabled)
    -Y, --reversesync <id>          Reverse sync instance data from attached subscriber
    -L, --rollingupdate <id>        Forward sync subscribers one at a time in S-OUT
    -I, --servicein <id>            Service in
    -i, --serviceout <id>           Service out
    -J, --show                      Show effective system descriptor
    -s, --start <id>                Start instance on the local host
    -S, --status <id>               System summary
    -x, --stop <id>                 Stop instance
    -y, --sync <id>[,<mode>]        Sync data to subscribers (<mode> [repair|hard|soft])
    -V, --throttle <id>[,<r>,<u>]   Throttle TX input rate <r>, unit <u>
    -X, --truncate <id>             Erase instance data
    -U, --unbind <id>               Unbind transaction input port
    -u, --unsubscribe <id>          Detach instance from provider
    -v, --version                   VGX Server version
    -w, --waitforidle <id>          Wait until instance input is idle
    -G, --writable <id>             Make graph(s) writable
        """
        console.Print(message)

        console.Print()
        if err is not None:
            console.Print( "ERROR: {}".format(err) )
            console.Print()


    @staticmethod
    def GetArgs( a, dflt ):
        A = [ x.strip() for x in a.split(",",len(dflt)-1) ]
        A.extend( dflt[ len(A): ] )
        return A    


    @staticmethod
    def Run( arguments, address=None, program="vgxadmin", print_to_stdout=True, default_descriptor_filename=None ):

        if type(arguments) is str:
            arguments = arguments.split()

        console = vgxadmin__VGXConsole( print_to_stdout )

        RESULT = []

        try:
            paramdef = [
                    ("attach=",          "a:"),
                    ("bind=",            "B:"),
                    ("confirm",          "c" ),
                    ("command=",         "C:"),
                    ("detach=",          "d:"),
                    ("restarthttp=",     "D:"),
                    ("endpoint=",        "E:"),
                    ("cf=",              "f:"),
                    ("readonly=",        "g:"),
                    ("writable=",        "G:"),
                    ("help",             "h" ),
                    ("serviceout=",      "i:"),
                    ("servicein=",       "I:"),
                    ("show",             "J" ),
                    ("cancelsync=",      "k:"),
                    ("forcecopy=",       "K:"),
                    ("rollingupdate=",   "L:"),
                    ("resetmetrics=",    "m:"),
                    ("opdump=",          "M:"),
                    ("nodestat=",        "n:"),
                    ("reloadplugins=",   "N:"),
                    ("pausein=",         "p:"),
                    ("pauseout=",        "P:"),
                    ("instancecfg=",     "Q:"),
                    ("resumein=",        "r:"),
                    ("resumeout=",       "R:"),
                    ("start=",           "s:"),
                    ("status=",          "S:"),
                    ("pausettl=",        "t:"),
                    ("resumettl=",       "T:"),
                    ("unsubscribe=",     "u:"),
                    ("unbind=",          "U:"),
                    ("version",          "v" ),
                    ("throttle=",        "V:"),
                    ("waitforidle=",     "w:"),
                    ("persist=",         "W:"),
                    ("stop=",            "x:"),
                    ("truncate=",        "X:"),
                    ("sync=",            "y:"),
                    ("reversesync=",     "Y:"),
                    ("descriptor=",      "Z:")
                ]

            if address:
                long_with_val = set([ "--{}".format(x.removesuffix('=')) for x,_ in paramdef if x.endswith('=') ])
                short_with_val = set([ "-{}".format(x.removesuffix(':')) for _,x in paramdef if x.endswith(':') ])
                with_val = long_with_val.union( short_with_val )
                modified = []
                sz = len( arguments )
                last_i = sz-1
                for i in range( sz ):
                    arg = arguments[i]
                    modified.append( arg )
                    if arg in with_val:
                        if i < last_i and not arguments[i+1].startswith('-'):
                            continue
                        modified.append('.')
                arguments = modified
                #console.Print( "DEBUG: Modified Arguments:\n{}\n".format( arguments ) )

            short = "".join([ x for _,x in paramdef ])
            long = [x for x,_ in paramdef]
            opts, args = getopt.getopt( arguments, short, long )

            if len(sys.argv) == 1:
                raise vgxadmin__InvalidUsageOrConfig()

            for o, a in opts:
                if o in ( "-h", "--help" ):
                    raise vgxadmin__InvalidUsageOrConfig()

            descriptor = None
            remote = None
            filename = default_descriptor_filename
            for o, a in opts:
                if o in ( "-f", "--cf" ):
                    filename = a

            if address:
                m = re.match( r"^(\S+):(\d+)$", address )
                if m:
                    try:
                        h = m.group(1)
                        p = int( m.group(2) )
                        remote = vgxadmin__VGXRemote( host=h, port=p, console=console )
                        D = remote.Endpoint( "/vgx/builtin/system_descriptor" )
                        descriptor = vgxadmin__Descriptor( descriptor=D, remote=remote, console=console )
                    except Exception as ex:
                        raise vgxadmin__AddressError( "{} -> {}".format(address, ex) )

            if descriptor is None:
                if filename is not None:
                    descriptor = vgxadmin__Descriptor( descriptor=filename, console=console )
                else:
                    try:
                        if pyvgx.system.IsInitialized():
                            D = sysplugin__GetSystemDescriptor()
                            descriptor = vgxadmin__Descriptor( descriptor=D, console=console )
                    except Exception as descr_ex:
                        raise vgxadmin__InvalidUsageOrConfig( "No descriptor: {}".format( descr_ex ) )

            if address:
                if remote is None:
                    remote = descriptor.remote = descriptor.Get( address ).remote
                    

            if remote is not None and len( opts ) == 0:
                try:
                    console.Print( "{}\n".format( pyvgx.version(1).replace('pyvgx', 'vgxadmin') ) )
                    console.Print( "System    : {}".format( descriptor.name ) )
                    nodestat = remote.Endpoint( "/vgx/nodestat" )
                    ping = remote.Endpoint( "/vgx/ping" ).get("host",{})
                    meminfo = remote.Endpoint( "/vgx/meminfo" ).get("memory",{})
                    console.Print( "Service   : {}".format( nodestat.get("service-name") ) )
                    console.Print( "Version   : {}".format( ping.get("version") ) )
                    console.Print( "Hostname  : {}".format( ping.get("name") ) )
                    console.Print( "IP        : {}".format( ping.get("ip") ) )
                    console.Print( "Uptime    : {}".format( ping.get("uptime") ) )
                    console.Print( "CPU       : {}".format( ping.get("cpu") ) )
                    memtotal = meminfo.get("total",1)
                    memavail = meminfo.get("current",{}).get("available",0)
                    memvgx = meminfo.get("current",{}).get("process",0)
                    console.Print( "Memory    : System Total     = {:,} MiB".format( memtotal >> 20 ) )
                    console.Print( "            System Available = {:.1f}%".format( 100.0*memavail/memtotal ) )
                    console.Print( "            VGX Instance     = {:.1f}%".format( 100.0*memvgx/memtotal ) )
                except Exception as ex:
                    raise vgxadmin__AddressError( "{} -> {}".format(remote, ex) )


            CONFIRMED = False
            for o, a in opts:
                if o in ( "-c", "--confirm" ):
                    CONFIRMED = True

            for o, a in opts:
                try:
                    R = []
                    if o in ( "-a", "--attach" ):
                        id, subs = vgxadmin__VGXAdmin.GetArgs( a, [None, None] )
                        if subs is None:
                            for instance in descriptor.GetMultiple( a ):
                                ret = instance.Attach()
                                R.append( (instance, ret) )
                        else:
                            destinations = [descriptor.Get(x.strip()) for x in subs.split(",")]
                            instance = descriptor.Get(id)
                            ret = instance.Attach( destinations=destinations )
                            R.append( (instance, ret) )
                    elif o in ( "-B", "--bind" ):
                        instance = descriptor.Get( a )
                        ret = instance.Bind()
                        R.append( (instance, ret) )

                    elif o in ( "-C", "--command" ):
                        id, gr, cmd = vgxadmin__VGXAdmin.GetArgs( a, [None, None, None] )
                        R = descriptor.Concurrent( "Command", id, (gr, cmd) )

                    elif o in ( "-d", "--detach" ):
                        for instance in descriptor.GetMultiple( a ):
                            ret = instance.Detach()
                            R.append( (instance, ret) )

                    elif o in ( "-D", "--restarthttp" ):
                        R = descriptor.Concurrent( "RestartHTTP", a )

                    elif o in ( "-E", "--endpoint" ):
                        if remote is None:
                            raise vgxadmin__InvalidUsageOrConfig( "Option '{}' requires <address>".format(o) )
                        console.Print( json.dumps( remote.Endpoint( a ), indent=4 ) )
                        #pprint.pprint( remote.Endpoint( a ), indent=2, stream=console.GetStream() )

                    elif o in ( "-g", "--readonly" ):
                        R = descriptor.Concurrent( "ReadonlyGraph", a )

                    elif o in ( "-G", "--writable" ):
                        R = descriptor.Concurrent( "WritableGraph", a )

                    elif o in ( "-i", "--serviceout" ):
                        R = descriptor.Concurrent( "ServiceOut", a )

                    elif o in ( "-I", "--servicein" ):
                        R = descriptor.Concurrent( "ServiceIn", a )

                    elif o in ( "-J", "--show" ):
                        console.Print( json.dumps( descriptor.AsDict(), indent=4 ) )

                    elif o in ( "-k", "--cancelsync" ):
                        instance = descriptor.Get( a )
                        ret = instance.CancelSync()
                        R.append( (instance, ret) )

                    elif o in ( "-K", "--forcecopy" ):
                        src, dest = vgxadmin__VGXAdmin.GetArgs( a, [None, None] )
                        source = descriptor.Get( src )
                        destination = descriptor.Get( dest )
                        ret = source.ForceCopy( source=source, destination=destination )
                        R.append( (source, destination, ret) )

                    elif o in ( "-L", "--rollingupdate" ):
                        instance = descriptor.Get( a )
                        ret = instance.RollingForwardSync()
                        R.append( (instance, ret) )

                    elif o in ( "-m", "--resetmetrics" ):
                        R = descriptor.Concurrent( "ResetMetrics", a )

                    elif o in ( "-M", "--opdump" ):
                        instance = descriptor.Get( a )
                        ret = instance.OpDump()
                        R.append( (instance, ret) )

                    elif o in ( "-n", "--nodestat" ):
                        id, key = vgxadmin__VGXAdmin.GetArgs( a, [None, None] )
                        for instance in descriptor.GetMultiple( id ):
                            if key is None:
                                console.Print( json.dumps( instance.Nodestat(), indent=4 ) )
                                #pprint.pprint( instance.Nodestat(), indent=2, stream=console.GetStream() )
                            else:
                                console.Print( "{} {}={}".format(instance, key, instance.Nodestat(key)) )

                    elif o in ( "-N", "--reloadplugins" ):
                        id, plugins_json_file = vgxadmin__VGXAdmin.GetArgs( a, [None, None] )
                        if plugins_json_file is not None:
                            f = open( plugins_json_file, "r" )
                            plugin_json = f.read()
                            f.close()
                            json.loads( plugin_json ) # verify json loadable
                            R = descriptor.Concurrent( "ReloadPlugins", id, (plugin_json,) )
                        else:
                            R = descriptor.Concurrent( "ReloadPlugins", id )

                    elif o in ( "-p", "--pausein" ):
                        R = descriptor.Concurrent( "PauseIn", a )

                    elif o in ( "-P", "--pauseout" ):
                        R = descriptor.Concurrent( "PauseOut", a )

                    elif o in ( "-Q", "--instancecfg" ):
                        instance = descriptor.Get( a )
                        ret = instance.GetConfig()
                        console.Print( ret )

                    elif o in ( "-r", "--resumein" ):
                        R = descriptor.Concurrent( "ResumeIn", a )

                    elif o in ( "-R", "--resumeout" ):
                        R = descriptor.Concurrent( "ResumeOut", a )

                    elif o in ( "-s", "--start" ):
                        ret = descriptor.Start( a )
                        R.append( ret )

                    elif o in ( "-S", "--status" ):
                        descriptor.GetMultiple( a, printsum=True, detail=True )

                    elif o in ( "-t", "--pausettl" ):
                        R = descriptor.Concurrent( "PauseTTL", a )

                    elif o in ( "-T", "--resumettl" ):
                        R = descriptor.Concurrent( "ResumeTTL", a )

                    elif o in ( "-u", "--unsubscribe" ):
                        for instance in descriptor.GetMultiple( a ):
                            ret = instance.Unsubscribe()
                            R.append( (instance, ret) )

                    elif o in ( "-U", "--unbind" ):
                        instance = descriptor.Get( a )
                        ret = instance.Unbind()
                        R.append( (instance, ret) )

                    elif o in ( "-v", "--version" ):
                        v = [line for line in pyvgx.version(2).split('\n') if line.startswith('vgx')][0].split()[1]
                        console.Print( v )

                    elif o in ( "-V", "--throttle" ):
                        id, rate, unit = vgxadmin__VGXAdmin.GetArgs( a, [None, -1.0, "bytes"] )
                        R = descriptor.Concurrent( "Throttle", id, (rate, unit) )

                    elif o in ( "-w", "--waitforidle" ):
                        R = descriptor.Concurrent( "WaitForIdle", a )

                    elif o in ( "-W", "--persist" ):
                        R = descriptor.Concurrent( "Persist", a )

                    elif o in ( "-x", "--stop" ):
                        instances = descriptor.GetMultiple( a, printsum=True, confirm="SHUTDOWN" if not CONFIRMED else None )
                        if instances:
                            R = descriptor.Concurrent( "Shutdown", a )

                    elif o in ( "-X", "--truncate" ):
                        instances = descriptor.GetMultiple( a, printsum=True, confirm="DELETE ALL DATA" if not CONFIRMED else None )
                        if instances:
                            R = descriptor.Concurrent( "Truncate", a )

                    elif o in ( "-y", "--sync" ):
                        id, mode = vgxadmin__VGXAdmin.GetArgs( a, [None, "repair"] )
                        for instance in descriptor.GetMultiple( id ):
                            ret = instance.Sync( mode )
                            R.append( (instance, ret) )

                    elif o in ( "-Y", "--reversesync" ):
                        instance = descriptor.Get( a )
                        ret = instance.ReverseSync()
                        R.append( (instance, ret) )

                    elif o in ( "-Z", "--descriptor" ):
                        for instance in descriptor.GetMultiple( a ):
                            ret = instance.UpdateDescriptor( descriptor )
                            R.append( (instance, ret) )

                    if R and type(R) is list:
                        i = 0
                        for i in range(len(R)):
                            if type( R[i] ) is tuple:
                                R[i] = list(R[i])
                            if type( R[i] ) is list and type(R[i][0]) is vgxadmin__VGXInstance:
                                R[i][0] = str(R[i][0])

                    RESULT.extend( R )
                
                except vgxadmin__InvalidUsageOrConfig:
                    raise

                except vgxadmin__ServerError:
                    raise

                except vgxadmin__OperationIncomplete:
                    raise

                except Exception as err:
                    if program is not None:
                        pprint.pprint( err.args, stream=console.GetStream() )
                    raise

        except vgxadmin__AddressError as aerr:
            raise

        except vgxadmin__InvalidUsageOrConfig as uerr:
            if uerr.args:
                RESULT.append( "{}".format( uerr ) )
            else:
                vgxadmin__VGXAdmin.Usage( program=program, console=console )

        except vgxadmin__ServerError as serr:
            RESULT.append( str(serr) )

        except vgxadmin__OperationIncomplete as operr:
            RESULT.append( "Operation incomplete: {}".format( operr ) )

        except getopt.GetoptError as err:
            RESULT.append( str(err) )

        except Exception as anyerr:
            RESULT.append( str(anyerr) )

        if not console.IsStdout():
            RESULT.insert( 0, {"trace": console.GetLines()} )

        return RESULT
    

import pyvgx
pyvgx.Descriptor = vgxadmin__Descriptor
pyvgx.VGXRemote = vgxadmin__VGXRemote
pyvgx.VGXAdmin = vgxadmin__VGXAdmin
