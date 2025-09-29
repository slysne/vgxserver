###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    CustomPlugin.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re
import json

graph = None




###############################################################################
# get_plugin
#
###############################################################################
def get_plugin( name ):
    """
    """
    # Send request
    bytes, headers = Support.send_request( "vgx/plugins", json=True )
    # Check headers
    Support.assert_headers( headers, bytes, "application/json" )

    D = json.loads( bytes )
    plugin_list = D.get( 'response' )

    # response is a list of plugins
    Expect( type( plugin_list ) is list,                "plugin_list is list, got %s" % type( plugin_list ) )

    # Path match
    match_path = "/vgx/plugin/%s" % name

    for plugin in plugin_list:
        # Plugin is dict
        Expect( type(plugin) is dict,                   "plugin is dict, got %s" % type( plugin ) )
        path = plugin.get( 'path' )
        description = plugin.get( 'description' )
        parameters = plugin.get( 'parameters' )
        bound_graph = plugin.get( 'bound_graph' )
        # path
        Expect( path.startswith( '/vgx/plugin/' ) )
        # description
        Expect( type(description) is list,              "description is list of lines, got %s" % type( description ) )
        # parameters
        Expect( type(parameters) is dict,               "parameters is dict, got %s" % type( parameters ) )

        # Match
        if path == match_path:
            return plugin

    raise KeyError( "plugin not found: %s" % name )





###############################################################################
# not_a_plugin
#
###############################################################################
def not_a_plugin():
    """
    """
    pass




###############################################################################
# plugin_func_1
#
###############################################################################
def plugin_func_1( request, x:int, y:float=25.5 ):
    """
    This is plugin function 1
    """
    return x * y




###############################################################################
# TEST_custom_plugin
#
###############################################################################
def TEST_custom_plugin():
    """
    test_level=4101
    t_nominal=1
    """

    # Fail to add a non-conforming plugin function
    try:
        system.AddPlugin( not_a_plugin )
        Expect( False,                                  "Should not be able to add non-conforming plugin function" )
    except TypeError as err:
        errstr = str( err )
        Expect( "plugin function signature incomplete" in errstr,  "unexpected TypeError: %s" % errstr )
    except Exception as ex:
        Expect( False,                                  "unexpected exception: %s" % ex )

    # ---------------
    # Add a plugin
    # ---------------
    system.AddPlugin( plugin_func_1 )

    # Find plugin in plugin list
    P = get_plugin( "plugin_func_1" )

    # Check docstring
    doc = plugin_func_1.__doc__.strip()
    descr = "".join(P['description']).strip()
    Expect( descr == doc,                               "description should match function's docstring" )

    # Check parameters
    params = P['parameters']
    # request
    Expect( "PluginRequest" in params['request'],       "'request' parameter should have type 'PluginRequest', got '%s'" % params['request'] )
    # x:int
    Expect( params['x'] == 'int',                       "'x' parameter should have type 'int', got '%s'" % params['x'] )
    # y:float=25.5
    Expect( type(params['y']) is list,                  "'y' parameter spec should be 'list', got '%s'" % type( params['y'] ) )
    Expect( params['y'][0] == 'float',                  "'y' parameter should have type 'float', got '%s'" % params['y'][0] )
    Expect( params['y'][1] == 25.5,                     "'y' parameter should have default value 25.5, got '%s'" % params['y'][1] )

    # Check bound graph
    bg = P['bound_graph']
    Expect( bg == 'None',                               "bound_graph should be None, got %s" % bg )
    
    # ---------------
    # Test the plugin
    # ---------------

    # --
    # ERROR
    # --

    # No parameters should generate error
    Support.send_request( "vgx/plugin/plugin_func_1", expect_status=500 )

    # --
    # OK
    # --
    bytes, headers = Support.send_request( "vgx/plugin/plugin_func_1?x=5", json=True )
    R = json.loads( bytes )
    # status
    status = R.get('status')
    Expect( status == 'OK',                             "status should be 'OK', got '%s'" % status )
    # response
    value = R.get('response')
    Expect( value == 127.5,                             "value should be 127.5, got %s" % value )

    # -----------------
    # Remove the plugin
    # -----------------
    system.RemovePlugin( 'plugin_func_1' )
    bytes, headers = Support.send_request( "vgx/plugin/plugin_func_1?x=5", expect_status=404 )
    





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
