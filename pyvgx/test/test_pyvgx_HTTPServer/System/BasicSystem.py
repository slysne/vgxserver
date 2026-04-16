###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    SimpleProxy.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import json
import os
import time
import urllib
import multiprocessing
import subprocess

from . import service




def vgxadmin( cmdline ):
    result = subprocess.run(
        ['vgxadmin', *cmdline.split()],
        capture_output=True,
        text=True
    )
    return result.stdout



def WaitUntilReady( instances, id, timeout=30.0 ):
    host = instances[id]['host']
    aport = instances[id]['hport'] + 1
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            R = urllib.request.Request( f"http://{host}:{aport}/vgx/hc" )
            U = urllib.request.urlopen( R )
            print( f"({id} returned {U.status})" )
            if U.status == 200:
                return True
        except:
            time.sleep(1)
    return False




def StartSystem( descriptor_file ):
    """
    """
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass


    vgx_cf = json.loads( vgxadmin( f"-f {descriptor_file} -J" ) )
    instances = vgx_cf.get("instances", {})

    SERVERS = []
    try:
        # Start backend server engines in new processes
        nobanner = os.getenv( "PYVGX_NOBANNER" )
        os.environ["PYVGX_NOBANNER"] = "1"

        # Instance IDs
        ids = [ k for k,v in sorted( instances.items(), key=lambda x:x[1].get('group'), reverse=1 ) ]

        for id in ids:
            params = {
                "instance_id": id,
                "vgxroot": "instances",
                "descriptor_file": descriptor_file
            }
            server = multiprocessing.Process( target=service.RunService, kwargs=params )
            SERVERS.append( (id, server) )

        for id, server in SERVERS:
            print( f"Starting instance: {id}" )
            server.start()
            time.sleep(0.33)

        if nobanner is None:
            del os.environ["PYVGX_NOBANNER"]
        
        # Wait for engines ready
        for id, server in SERVERS:
            print( f"Waiting for instance: {id}" )
            ready = WaitUntilReady( instances, id )
            Expect( ready,      f"Server instance {id} failed to start" )
            print( f"Instance is ready: {id}" )

        return SERVERS
    except:
        for id, server in SERVERS:
            if server.exitcode is None:
                server.kill()
        raise



def StopSystem( descriptor_file ):
    """
    """
    return vgxadmin( f"-f {descriptor_file} --stop @ --confirm" )



def CheckSystem( descriptor_file, admin_id ):
    """
    """
    vgx_cf = json.loads( vgxadmin( f"-f {descriptor_file} -J" ) )
    instances = vgx_cf.get("instances", {})

    ids = [ k for k,v in sorted( instances.items(), key=lambda x:x[1].get('group'), reverse=1 ) ]
    for id in ids:
        tp = instances[id]['type']
        echo = vgxadmin( f"{id} -f {descriptor_file} --endpoint /vgx/plugin/SimpleEcho?message=hello_{id}" )
        if tp == 'dispatch':
            Expect( f"pre_hello_{id}" in echo, f"server instance should echo, got '{echo}' " )
        else:
            Expect( f"engine_hello_{id}" in echo, f"server instance should echo, got '{echo}' " )
        time.sleep(0.5)

    try:
        system_overview = vgxadmin( f"{admin_id} -f {descriptor_file} --endpoint /vgx/builtin/system_overview" )
        json.loads( system_overview )
    except json.JSONDecodeError as jerr:
        Expect( False, f"bad system_overview: {jerr}" )
    except Exception as ex:
        Expect( False, f"system_overview failed: {ex}" )

    print( "System OK" )




###############################################################################
# TEST_BasicSystem
#
###############################################################################
def TEST_BasicSystem():
    """
    Basic System Test
    test_level=4101
    """

    thisdir = os.path.dirname(__file__)
    descriptor_file = os.path.join( thisdir, "vgx.cf" )


    admin_id = None
    f = None
    try:
        f = open( descriptor_file )
        vgx_cf = json.loads( f.read() )
        instances = vgx_cf['instances']
        for id in instances:
            if instances[id]['type'] == 'admin':
                admin_id = id
                break
        Expect( admin_id is not None, "no admin" )
    except json.JSONDecodeError as jerr:
        Expect( False, f"Bad test data: {jerr}" )
    finally:
        if f is not None:
            f.close()
    
    try:
        StartSystem( descriptor_file )

        print( "Checking all instances" )
        CheckSystem( descriptor_file, admin_id )

    finally:
        print( "Stopping all instances" )
        result = StopSystem( descriptor_file )
        print( result )









###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
