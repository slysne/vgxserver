###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BUILTIN_system_rates.py
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



###############################################################################
# sysplugin__system_rates
#
###############################################################################
def sysplugin__system_rates( request:pyvgx.PluginRequest, headers:dict, idlist:str="" ):
    """
    Multi-node system service rates
    """

    R = []
    result = {
        "instances": R,
        "message": None
    }

    try:
        D = sysplugin__GetSystemDescriptor()
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True
        return result

    try:
        instances = D.get("instances",{})
        idlist = idlist.replace(" ","")
        include = [(x,instances[x]) for x in idlist.split(",") if x in instances]
        for id, instance in include:
            host = instance.get("host")
            aport = instance.get("hport",-1) + 1
            if host and aport:
                try:
                    sysplugin__SendAdminRequest( host, aport, path="/vgx/hc", timeout=0.2 )
                    status, headers = sysplugin__SendAdminRequest( host, aport, path="/vgx/status", timeout=2.0 )
                    rates = status["response_ms"]
                    rates["rate"] = status["request"]["rate"]
                    clients = {}
                    clients["connected"] = status["connected_clients"]
                    matrix = {}
                    matrix["active-channels"] = status["dispatcher"]["matrix-active-channels"];
                    matrix["total-channels"] = status["dispatcher"]["matrix-total-channels"];
                except:
                    rates = None
                    clients = None
                    matrix = None
            R.append({
                "id": id,
                "hostname": host,
                "adminport": aport,
                "rates": rates,
                "clients": clients,
                "matrix": matrix
            })
    except Exception as err:
        result["message"] = "{}".format( err )
        result["error"] = True

    return result

pyvgx.system.AddPlugin( plugin=sysplugin__system_rates )
