###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    vgxdemoservice.py
# Author:  Stian Lysne slysne.dev@gmail.com
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

import argparse
import inspect
import code
import webbrowser

import pyvgx



VGX_SINGLENODE_SAMPLE_CF = {
    "name": "Single Node VGX Demo System",
    "instances": {
        "G1":   { "type": "generic", "host":"127.0.0.1", "hport": 9000, "description": "Generic Instance" }
    },
    "topology": {
        "transaction": {
            "G1": None
        },
        "dispatch": {
        }
    },
    "graphs": ["g1"]
}



VGX_MULTINODE_SAMPLE_CF = {
    "name": "Multi Node VGX Demo System",
    "instances": {

        "A1":   { "group": 1000.001, "type": "admin",    "hport": 9000,                                     "description": "Admin" },
        "TD":   { "group": 2000.001, "type": "dispatch", "hport": 9990,                                     "description": "Top Dispatcher" },
        "B0.1": {                    "type": "builder",  "hport": 9100,   "tport": 10100,  "attach": True,  "description": "Builder 0.1" },
        "S1.1": {                    "type": "search",   "hport": 9110,   "tport": 10110,                   "description": "Search 1.1" },
        "B0.2": {                    "type": "builder",  "hport": 9200,   "tport": 10200,  "attach": True,  "description": "Builder 0.2" },
        "S1.2": {                    "type": "search",   "hport": 9210,   "tport": 10210,                   "description": "Search 1.2" }
    },

    "common": {
        "host": "127.0.0.1",
        "s-in": True
    },

    "topology": {
        "transaction": {
            "A1": {},
            "B0.1": [
                "S1.1"
            ],
            "B0.2": [
                "S1.2"
            ]
        },
        "dispatch": {
            "TD": [
                { "channels": 16, "priority": 1,  "partitions": [ "S1.1", "S1.2" ] },
                { "channels": 4,  "priority": 20, "partitions": [ "B0.1", "B0.2" ], "primary": 1 }
            ]
        }
    },

    "graphs": ["g1"]
}




###############################################################################
# LocalSamplePlugin
#
###############################################################################
def LocalSamplePlugin( request:pyvgx.PluginRequest, message:str="" ):
    """
    """
    response = pyvgx.PluginResponse()
    if not message:
        response.Append( "You said nothing, I say 'hello'" )
    else:
        response.Append( "You said '{}'".format(message) )
    return response



# ----------------------------
# Service Endpoint definitions
# ----------------------------
def GetPluginDefinitions():
    """
    Returns:
    [ 
      {
        "name"      : <str>,
        "package"   : <str>,
        "module"    : <str>,
        "engine"    : <func_or_str_or_None>,
        "pre"       : <func_or_str_or_None>,
        "post"      : <func_or_str_or_None>,
        "graph"     : <str>
      }, 
      ...
    ]
    """

    plugins = [
        # endpoint: /vgx/plugin/search
        { 
            "name"   : "search",

            # Python package name
            "package": __package__,

            # Name of module containing plugin code
            "module" : "vgxdemoplugin",

            # Name of function within module in package to
            # be executed by leaf instances
            "engine" : "Search",
            
            # Names of functions within module in package to
            # be executed by dispatcher instances
            "pre"    : None,
            "post"   : None,

            # Bind plugins to graph defined in descriptor
            "graph"  : "*"
        },

        # endpoint: /vgx/plugin/add
        { 
            "name"   : "add",
            "package": __package__,
            "module" : "vgxdemoplugin",
            "engine" : "Add",
            "pre"    : "PreAdd",
            "post"   : None,
            "graph"  : "*"
        },

        # endpoint: /vgx/plugin/hello
        {
            "name"   : "hello",
            "engine" : LocalSamplePlugin
        }

    ]

    return plugins




###############################################################################
# RunService
#
###############################################################################
def RunService( demo, instance_id, vgxroot=None, descriptor_file=None, interactive=False ):
    """
    Run VGX Server instance
    """
    
    # Initialize core library to enable instance startup
    pyvgx.initadmin()

    # Service endpoint definitions
    plugins = GetPluginDefinitions()

    # VGX System descriptor
    if descriptor_file is not None:
        descriptor = pyvgx.VGXInstance.GetDescriptor( descriptor_file )
    else:
        if demo == 'single':
            descriptor = pyvgx.Descriptor( descriptor=VGX_SINGLENODE_SAMPLE_CF )
        elif demo == 'multi':
            descriptor = pyvgx.Descriptor( descriptor=VGX_MULTINODE_SAMPLE_CF )
        else:
            raise ValueError( demo )

    # Start VGX instance
    if vgxroot is None:
        vgxroot = "vgxservice"
    instance = pyvgx.VGXInstance.StartInstance( id         = instance_id,
                                                descriptor = descriptor,
                                                basedir    = vgxroot,
                                                plugins    = plugins)

    #
    if instance.type in ["admin", "generic"]:
        system_url = f"http://{instance.ip}:{instance.aport}/system"
        try:
            webbrowser.open( system_url )
        except:
            print( "failed to open system overview in browser" )

    # Interact before entring server loop
    if interactive:
        frame = inspect.currentframe()
        GL = globals()
        GL.update( frame.f_back.f_locals )
        code.interact( banner="Interactive Mode", local=GL )

    # Run until SIGINT
    pyvgx.system.RunServer( name=instance.description )




###############################################################################
# main
#
###############################################################################
def main():
    """
    Parse command line arguments and run service
    """

    # Instantiate the parser
    parser = argparse.ArgumentParser(
        prog        = "vgxdemoservice",
        description = "Demo VGX Service",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )

    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    parser.add_argument( "--demo",       required=True, metavar="DEMO",
                                                        choices=["single","multi","custom"],
                                                                            help = "Demo service mode" )
 
    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    parser.add_argument( "--instanceid", required=True, metavar="ID",       help = "Reference to identifier in system descriptor 'instances'" )
    
    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    parser.add_argument( "--descriptor",                metavar="FILE",     help = "System descriptor file" )
    
    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    parser.add_argument( "--vgxroot",                                       help = "VGX root directory" )
    
    #----------------------------------------------------------------------------------------------------------------------------------------------------------
    parser.add_argument( "--interactive",               default=False,
                                                        action=argparse.BooleanOptionalAction,
                                                                            help = "Launch interactive console on startup" )
    #----------------------------------------------------------------------------------------------------------------------------------------------------------


    args = parser.parse_args()

    try:
        RunService(
            demo            = args.demo,
            instance_id     = args.instanceid,
            vgxroot         = args.vgxroot,
            descriptor_file = args.descriptor,
            interactive     = args.interactive
        )
    except Exception as err:
        pyvgx.LogError( err.args[0] )



if __name__ == "__main__":
    main()
