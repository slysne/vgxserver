###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    _http_support.py
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
from pyvgx import *
import pyvgx
import urllib.request
import urllib.error
import json
import re




###############################################################################
# get_server_host_port
#
###############################################################################
def get_server_host_port():
    """
    """
    server = system.Status()['httpserver']['server']
    m = re.match( r"http://([^:]+):(\d+)", server )
    host = m.group(1)
    port = int(m.group(2))
    return host, port




###############################################################################
# send_request
#
###############################################################################
def send_request( path, headers={}, expect_status=200, admin=False, json=False, address=None ):
    """
    """
    if address:
        host, port = address
    else:
        host, port = get_server_host_port()
    if admin:
        port += 1
    # Make request
    R = urllib.request.Request( "http://%s:%d/%s" % (host, port, path) )
    if json:
        R.add_header( "accept", "application/json" )
    for k,v in headers.items():
        R.add_header( k, v )

    # Open
    try:
        U = urllib.request.urlopen( R )
        if expect_status != 200:
            Expect( False, "status should be %s, got 200" % expect_status )
    except urllib.error.HTTPError as http_err:
        if expect_status == 200:
            Expect( False, "status should be 200, got %s" % http_err.status )
        elif http_err.status == expect_status:
            return ('b',{})
        else:
            Expect( False, "status should be %s, got %s" % (expect_status, http_err.status) )
    except Exception as ex:
        Expect( False, "unexpected error %s" % ex )

    # Check
    status = U.status
    Expect( status == expect_status,   "HTTP Status should be %d, got %d" % (expect_status, status) )
    # Get content and headers
    bytes = U.read()
    headers = {}
    for k,v in U.getheaders():
        headers[ k.lower() ] = v
    return (bytes, headers)




###############################################################################
# assert_headers
#
###############################################################################
def assert_headers( header_dict, content, expected_content_type=None ):
    """
    """
    # Server
    server = header_dict.get( 'server' )
    if server is not None:
        Expect( re.match( r"VGX/\d", server),        "Server should be 'VGX/X', got '%s'" % server )

    # Content-Type
    content_type = header_dict.get( 'content-type' )
    if expected_content_type is not None:
        Expect( content_type is not None )
        Expect( content_type.startswith( expected_content_type ), "Content-Type should be '%s', got '%s'" % (expected_content_type, content_type) )

    # Content-Length
    content_length = header_dict.get( 'content-length' )
    Expect( content_length is not None,             "'Content-Length' header field" )
    Expect( re.match( r"\d+", content_length ),     "Content-Length should be digits" )
    N = int( content_length )
    Expect( N == len( content ),                    "Content-Length (%d) should match bytes received, got %d" % (N, len(content)) )




###############################################################################
# response_from_json
#
###############################################################################
def response_from_json( json_bytes, expect_status="OK", expect_base_port=None ):
    """
    """
    try:
        D = json.loads( json_bytes )
    except Exception as err:
        Expect( True,                           "data should be json, got exception %s" % err )

    # Verify status
    status = D.get('status')
    Expect( status == expect_status,            "status=%s, got %s" % (expect_status, status) )

    # Verify port
    if 'port' in D:
        port = D.get('port')
        Expect( type(port) is list,                 "port should be list, got %s" % type(port) )
        Expect( len(port) == 2,                     "port list should have two items, got %d" % len(port) )
        if expect_base_port:
            base_port = port[0] - port[1]
            Expect( base_port == expect_base_port,  "base port should be %s, got %s" % (expect_base_port, base_port) )

    # Verify exec_id
    if 'exec_id' in D:
        exec_id = D.get('exec_id')
        Expect( type(exec_id) is int,               "exec_id should be int, got %s" % type(exec_id) )

    # Verify exec_ms
    exec_ms = D.get('exec_ms')
    Expect( type(exec_ms) is float,             "exec_ms should be float, got %s" % type(exec_ms) )

    return D.get('response')




###############################################################################
# validate_structure
#
###############################################################################
def validate_structure( REF, subject, recursion=0 ):
    """
    """
    # collection
    if type(REF) in [dict, list]:
        # dict
        if type(REF) is dict:
            Expect( type(subject) is dict,          "type should be dict, got %s" % type(subject) )
            dsz = 0
            for key, val in REF.items():
                if key.startswith( "?" ):
                    key = key.removeprefix( "?" )
                    if key not in subject:
                        continue
                else:
                    dsz += 1
                    # Field
                    Expect( key in subject,             "should contain key '%s'" % key )
                # Validate
                validate_structure( val, subject[key], recursion+1 )
            Expect( len(subject) >= dsz,            "dict length should be at least %d, got %d" % (dsz, len(subject)) )

        # list
        else:
            Expect( type(subject) is list,          "type should be list, got %s" % type(subject) )
            # Special case: wildcard list, ignore length and content
            if len(REF) == 1 and REF[0] is None:
                pass
            # Special case: reference entry for any number of subject entries
            elif len(REF) == 2 and REF[1] is None:
                REF_entry = REF[0]
                for entry in subject:
                    validate_structure( REF_entry, entry, recursion+1 )
            # Match all entries in refrence list
            else:
                Expect( len(REF) == len(subject),       "list length should be %d, got %d" % (len(REF), len(subject)) )
                for a,b in zip(REF, subject):
                    # Validate
                    validate_structure( a, b, recursion+1 )
    # single item
    else:
        # int/float
        if type(REF) in [int, float]:
            T = [int, float]
        # any other type
        else:
            T = [type(REF)]
        # Verify expected type
        Expect( type( subject ) in T,               "type should be one of %s, got %s" % (T, type(subject)) )
