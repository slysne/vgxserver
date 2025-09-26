###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_system_overview.py
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
import json



def sysplugin__system_overview( request:pyvgx.PluginRequest, headers:dict ):
    """
    Multi-node system status
    """


    def VGXInstanceMetrics( host, port, nodetype ):
        """
        Return aggregate info from vgx node
        """
        hc, headers = sysplugin__SendAdminRequest( host, port, path="/vgx/hc", timeout=0.25 )
        info = {
            "nodestat"  : {}
        }
        if nodetype == "dispatch":
            info[ "matrix" ] = {}
        for page in list(info.keys()):
            endpoint = "/vgx/{}".format( page )
            try:
                data, headers = sysplugin__SendAdminRequest( host, port, path=endpoint, timeout=2.0 )
                info[page] = data.decode() if type(data) is bytes else data
            except:
                pass
        info["hc"] = hc.decode()
        return info



    def GetNodeInfo( id, instance ):
        # Create return entry
        entry = {
            "id"        : id,
            "hostname"  : None,
            "adminport" : None,
            "nodetype"  : None,
            "message"   : None,
            "group"     : instance.get('group',-1),
            "partition" : instance.get('partition',-1)
        }

        # TEST
        if entry['group'] is None:
            raise Exception( "In GetNodeInfo instance.get('group',-1) returned None!" )

        if entry['group'] < 0:
            entry['group'] = instance.get('partition',"1000.0001")
        # Address 
        try:
            entry['hostname'] = instance.get('host')
            entry['adminport'] = int(instance.get('hport')) + 1
            entry['nodetype'] = instance.get('type')
        except Exception as ex:
            entry['message'] = "Bad system descriptor entry: {}".format( instance )
            entry['error'] = True
            return entry
        # Get details from address
        try:
            metrics = VGXInstanceMetrics( entry['hostname'], entry['adminport'], entry['nodetype'] )
            entry.update( metrics )
        except Exception as ex:
            entry['message'] = "Failed to get VGX Instance Metrics: {}".format( ex )
            entry['error'] = True
        return entry



    def NodestatDict( instance ):
        nodestat = instance.get( "nodestat" )
        return nodestat if type(nodestat) is dict else {}



    def ResolveDispatcherInfo( instances ):
        # Derive key from ip and port
        def x_key( ip=None, aport=None, hport=None ):
            if ip is None or (aport is None and hport is None):
                return None
            if hport is not None:
                aport = hport + 1
            key = "{}:{}".format( ip, aport )
            return key
        # Derive instance map key from nodestat entry
        def instance_key( instance ):
            nodestat = NodestatDict( instance )
            return x_key( ip=nodestat.get("ip"), aport=nodestat.get("adminport") )
        # Derive instance map key from matrix entry
        def matrix_key( mx_entry ):
            return x_key( ip=mx_entry.get("ip"), hport=mx_entry.get("port") )
        instance_map = {}
        #   instance_map = {
        #       "127.0.0.1:9000" : {
        #           "id": "B1",
        #           "nodetype": "builder",
        #           "idle-ms": 123,
        #           "digest": "d40e4c3d9674cb8e3e5e58e1b098e02c",
        #           "consistent": True,
        #           "complete": True,
        #           "degraded": False,
        #           "s-in": True
        #       },
        #       ...
        #   }
        #
        for instance in instances:
            if instance.get( "id" ) is None:
                continue
            key = instance_key( instance )
            if key is None:
                continue
            nodestat = NodestatDict( instance )
            instance_map[ key ] = {
                "id": instance.get( "id" ),
                "nodetype": instance.get( "nodetype" ),
                "idle-ms": nodestat.get( "idle-ms", 0 ),
                "digest": nodestat.get( "digest" ),
                "consistent": True,
                "complete": True,
                "degraded": False,
                "s-in": instance.get( "hc" ) == "VGX/3"
            }
        # Enhance matrix with additional info
        for instance in instances:
            if instance.get( "nodetype" ) == "dispatch":
                matrix = instance.get( "matrix" )
                if matrix is None:
                    continue
                consistent = True
                complete = len(matrix) > 0
                degraded = False
                for partition in matrix:
                    digest = None
                    idle_ms = 0
                    s_in_count = 0
                    for replica in partition:
                        replica_engine = instance_map.get( matrix_key( replica ), {} )
                        replica["id"] = replica_engine.get( "id", None )
                        replica["nodetype"] = replica_engine.get( "nodetype", None )
                        # s-in
                        if replica_engine.get("s-in") is not True:
                            replica["serving"] = False
                            degraded = True
                        else:
                            s_in_count += 1
                            replica["serving"] = True
                        # engine digest
                        if replica["nodetype"] != "dispatch":
                            replica_digest = replica_engine.get( "digest", 0 )
                            replica_idle_ms = replica_engine.get( "idle-ms", 0 )
                            replica["digest"] = replica_digest
                            if digest is None:
                                digest = replica_digest
                            elif digest != replica_digest and replica_idle_ms > 30000:
                                consistent = False
                    if s_in_count == 0:
                        complete = False
                # Assign instance flag
                key = instance_key( instance )
                instance_map.get( key, {} )["consistent"] = consistent
                instance_map.get( key, {} )["complete"] = complete
                instance_map.get( key, {} )["degraded"] = degraded
        # Traverse to inherit flags
        for instance in instances:
            matrix = instance.get( "matrix" )
            if matrix is None:
                continue
            entry = instance_map.get( instance_key( instance ), {} )
            consistent = entry.get( "consistent", False )
            complete = entry.get( "complete", False )
            degraded = entry.get( "degraded", True )
            for partition in matrix:
                for replica in partition:
                    if replica.get( "nodetype" ) == "dispatch":
                        dispatcher = instance_map.get( matrix_key( replica ) )
                        if dispatcher is None:
                            continue
                        if not dispatcher["consistent"]:
                            consistent = False
                        if not dispatcher["complete"]:
                            complete = False
                        if dispatcher["degraded"]:
                            degraded = True
            instance["matrix-consistent"] = consistent
            instance["matrix-complete"] = complete
            instance["matrix-degraded"] = degraded



    def sortkey(x):
        try:
            p = float(x.get('group'))
        except:
            p = 0.0
        t = {'admin':0, 'dispatch':1, 'builder':2, 'txproxy':3, 'search':4}.get(x.get('nodetype'),5)
        i = x.get('id')
        return "{:08.3f}_{}_{}".format(p,t,i)


    result = {
        "version": pyvgx.version(1),
        "id": None,
        "descriptor": None,
        "instances": [],
        "message": None,
        "error": False
    }


    try:
        result["descriptor"] = sysplugin__GetSystemDescriptor()
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True
        return result

    try:
        instances = set()
        # Transaction topology
        for id, node in sysplugin__GetTransactionTopologyInstances():
            if id not in instances:
                instances.add(id)
                instance_info = GetNodeInfo( id, node )
                result["instances"].append( instance_info )
        # Dispatch topology
        for id, node in sysplugin__GetDispatchTopologyInstances():
            if id not in instances:
                instances.add(id)
                instance_info = GetNodeInfo( id, node )
                result["instances"].append( instance_info )
        # Dispatcher info
        ResolveDispatcherInfo( result["instances"] )
        # Sort by type, then instance id
        result["instances"].sort( key=sortkey )
        # Unique label for instance executing plugin
        this_label = pyvgx.system.UniqueLabel()
        for instance_info in result["instances"]:
            id = instance_info.get('id','?')
            nodestat = instance_info.get('nodestat',{})
            # This instance info is for the instance executing the plugin
            if nodestat.get('service-label') == this_label:
                result['id'] = id
                break

    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True

    return result



pyvgx.system.AddPlugin( plugin=sysplugin__system_overview )
